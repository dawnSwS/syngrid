use anyhow::{Result, anyhow};
use reqwest::{Client, Method, header};
use serde_json::{json, Value};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use chrono::Utc;
use std::fmt::Write;

const BASE_URL: &str = "https://eapi.binance.com";

#[derive(Clone)]
pub struct BinanceAdapter {
    api_key: String,
    secret: String,
    client: Client,
}

impl BinanceAdapter {
    pub fn new(api_key: &str, secret: &str) -> Self {
        let mut headers = header::HeaderMap::new();
        headers.insert("X-MBX-APIKEY", header::HeaderValue::from_str(api_key).unwrap_or(header::HeaderValue::from_static("")));
        Self {
            api_key: api_key.into(),
            secret: secret.into(),
            client: Client::builder().default_headers(headers).build().unwrap(),
        }
    }

    fn sign(&self, query: &str) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(self.secret.as_bytes()).unwrap();
        mac.update(query.as_bytes());
        let result = mac.finalize().into_bytes();
        let mut hex = String::with_capacity(result.len() * 2);
        for byte in result {
            write!(&mut hex, "{:02x}", byte).unwrap();
        }
        hex
    }

    async fn execute(&self, method: Method, path: &str, mut params: Vec<(&str, String)>, private: bool) -> Result<Value> {
        let url = format!("{}{}", BASE_URL, path);
        if private {
            params.push(("timestamp", Utc::now().timestamp_millis().to_string()));
            params.push(("recvWindow", "5000".to_string()));
        }
        let query = params.iter().map(|(k, v)| format!("{}={}", k, v)).collect::<Vec<_>>().join("&");
        let final_url = if private {
            format!("{}?{}&signature={}", url, query, self.sign(&query))
        } else {
            if query.is_empty() { url } else { format!("{}?{}", url, query) }
        };

        let req = self.client.request(method, &final_url);
        let resp = req.send().await?;
        let status = resp.status();
        let text = resp.text().await?;

        if !status.is_success() {
            return Err(anyhow!("{} {}", status, text));
        }

        let json: Value = serde_json::from_str(&text).unwrap_or(json!({}));
        if let Some(code) = json.get("code") {
            if code.as_i64().unwrap_or(0) != 0 {
                return Err(anyhow!("{} {}", code, json["msg"]));
            }
        }
        Ok(json)
    }

    pub async fn get_options_info(&self, symbol: &str, s0: f64, delta: f64, n: i32) -> Result<(f64, f64, String, String)> {
        let base = symbol.split('-').next().unwrap_or("ETH");
        
        let b_call = s0 * (1.0 + delta * n as f64);
        let mu_call = s0 * (1.0 + delta * (n as f64 + 1.0) / 2.0);
        let b_put = s0 * (1.0 - delta * n as f64);
        let mu_put = s0 * (1.0 - delta * (n as f64 + 1.0) / 2.0);

        let ticker = self.execute(Method::GET, "/eapi/v1/ticker", vec![], false).await?;
        let mut calls = Vec::new();
        let mut puts = Vec::new();

        if let Some(arr) = ticker.as_array() {
            for v in arr {
                let sym = v["symbol"].as_str().unwrap_or("");
                if !sym.starts_with(base) { continue; }
                let parts: Vec<&str> = sym.split('-').collect();
                if parts.len() != 4 { continue; }
                let strike = parts[2].parse::<f64>().unwrap_or(0.0);
                let opt_type = parts[3];
                let ask = v["askPrice"].as_str().unwrap_or("0").parse::<f64>().unwrap_or(0.0);
                
                if ask <= 0.0 || ask > 0.15 * s0 { continue; }

                if opt_type == "C" && strike >= b_call {
                    calls.push((sym.to_string(), strike, ask));
                } else if opt_type == "P" && strike <= b_put {
                    puts.push((sym.to_string(), strike, ask));
                }
            }
        }

        let optimize = |opts: &[(String, f64, f64)], mu: f64, is_call: bool| -> (f64, String) {
            let valid_opts: Vec<_> = opts.iter()
                .filter(|(_, _, c)| *c > 0.0 && *c < 0.15 * s0)
                .collect();

            if valid_opts.is_empty() { return (if is_call { s0 * 1.1 } else { s0 * 0.9 }, "".to_string()); }
            let len = valid_opts.len();
            if len == 1 {
                let best = &valid_opts[0];
                return (if is_call { best.1 + best.2 } else { best.1 - best.2 }, best.0.clone());
            }

            let mut data: Vec<(String, f64, f64, f64)> = valid_opts.into_iter()
                .map(|(sym, k, c)| (sym.clone(), *k, *c, (*k - mu).abs()))
                .collect();

            let mut x_indices: Vec<usize> = (0..len).collect();
            x_indices.sort_by(|&a, &b| data[a].3.partial_cmp(&data[b].3).unwrap());
            let mut rank_x = vec![0.0; len];
            for (rank, &idx) in x_indices.iter().enumerate() {
                rank_x[idx] = rank as f64 / (len - 1) as f64;
            }

            let mut y_indices: Vec<usize> = (0..len).collect();
            y_indices.sort_by(|&a, &b| data[a].2.partial_cmp(&data[b].2).unwrap());
            let mut rank_y = vec![0.0; len];
            for (rank, &idx) in y_indices.iter().enumerate() {
                rank_y[idx] = rank as f64 / (len - 1) as f64;
            }

            let mut best_idx = 0;
            let mut min_dist = f64::MAX;
            for i in 0..len {
                let dist = rank_x[i].powi(2) + rank_y[i].powi(2);
                if dist < min_dist {
                    min_dist = dist;
                    best_idx = i;
                }
            }

            let best = &data[best_idx];
            let p_be = if is_call { best.1 + best.2 } else { best.1 - best.2 };
            (p_be, best.0.clone())
        };

        let (p_be_up, call_sym) = optimize(&calls, mu_call, true);
        let (p_be_down, put_sym) = optimize(&puts, mu_put, false);

        Ok((p_be_down, p_be_up, put_sym, call_sym))
    }

    pub async fn get_option_position(&self, opt_symbol: &str) -> Result<f64> {
        if opt_symbol.is_empty() { return Ok(0.0); }
        let res = self.execute(Method::GET, "/eapi/v1/position", vec![("symbol", opt_symbol.to_string())], true).await?;
        if let Some(arr) = res.as_array() {
            for v in arr {
                if v["symbol"].as_str() == Some(opt_symbol) {
                    let pos = v["position"].as_str().or(v["positionAmt"].as_str()).unwrap_or("0").parse::<f64>().unwrap_or(0.0);
                    return Ok(pos.abs());
                }
            }
        } else if res["symbol"].as_str() == Some(opt_symbol) {
             let pos = res["position"].as_str().or(res["positionAmt"].as_str()).unwrap_or("0").parse::<f64>().unwrap_or(0.0);
             return Ok(pos.abs());
        }
        Ok(0.0)
    }

    pub async fn buy_option(&self, opt_symbol: &str, size: f64) -> Result<()> {
        if opt_symbol.is_empty() || size < 0.01 { return Ok(()); }
        let ticker = self.execute(Method::GET, "/eapi/v1/ticker", vec![("symbol", opt_symbol.to_string())], false).await?;
        let ask = if let Some(arr) = ticker.as_array() {
            arr.first().and_then(|v| v["askPrice"].as_str()).unwrap_or("0").parse::<f64>().unwrap_or(0.0)
        } else {
            ticker["askPrice"].as_str().unwrap_or("0").parse::<f64>().unwrap_or(0.0)
        };
        if ask <= 0.0 { return Err(anyhow!("Invalid ask price")); }
        let price = ask * 1.05;

        let params = vec![
            ("symbol", opt_symbol.to_string()),
            ("side", "BUY".to_string()),
            ("type", "LIMIT".to_string()),
            ("quantity", format!("{:.2}", size)),
            ("price", format!("{:.2}", price)),
            ("timeInForce", "IOC".to_string()),
        ];
        self.execute(Method::POST, "/eapi/v1/order", params, true).await?;
        Ok(())
    }
}