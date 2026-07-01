use anyhow::{Result, anyhow};
use reqwest::{Client, Method, header};
use serde_json::{json, Value};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use chrono::Utc;
use std::fmt::Write;

#[derive(Clone)]
pub struct BinanceAdapter {
    secret: String,
    client: Client,
    base_url: String,
}

impl BinanceAdapter {
    pub fn new(api_key: &str, secret: &str, base_url: &str) -> Self {
        let mut headers = header::HeaderMap::new();
        headers.insert("X-MBX-APIKEY", header::HeaderValue::from_str(api_key).unwrap_or(header::HeaderValue::from_static("")));
        headers.insert("x-relay-target", header::HeaderValue::from_static("binance"));
        
        Self {
            secret: secret.into(),
            client: Client::builder().default_headers(headers).build().unwrap(),
            base_url: base_url.trim_end_matches('/').to_string(),
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
        let url = format!("{}{}", self.base_url, path);
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

    pub async fn get_exchange_info(&self) -> Result<Value> {
        self.execute(Method::GET, "/eapi/v1/exchangeInfo", vec![], false).await
    }

    pub async fn get_account(&self) -> Result<Value> {
        self.execute(Method::GET, "/eapi/v1/account", vec![], true).await
    }

    pub async fn get_positions(&self, symbol: Option<&str>) -> Result<Value> {
        let mut params = vec![];
        if let Some(s) = symbol {
            params.push(("symbol", s.to_string()));
        }
        self.execute(Method::GET, "/eapi/v1/position", params, true).await
    }

    pub async fn get_open_orders(&self, symbol: Option<&str>) -> Result<Value> {
        let mut params = vec![];
        if let Some(s) = symbol {
            params.push(("symbol", s.to_string()));
        }
        self.execute(Method::GET, "/eapi/v1/openOrders", params, true).await
    }

    pub async fn get_options_info(&self, symbol: &str, s0: f64, target_delta: f64) -> Result<(f64, f64, String, String)> {
        let base = symbol.split('-').next().unwrap_or("ETH");

        let ticker = self.execute(Method::GET, "/eapi/v1/ticker", vec![], false).await?;
        let mark = self.execute(Method::GET, "/eapi/v1/mark", vec![], false).await?;
        
        let mut ask_prices = std::collections::HashMap::new();
        if let Some(arr) = ticker.as_array() {
            for v in arr {
                let sym = v["symbol"].as_str().unwrap_or("");
                let ask = v["askPrice"].as_str().unwrap_or("0").parse::<f64>().unwrap_or(0.0);
                ask_prices.insert(sym.to_string(), ask);
            }
        }

        let now = Utc::now().timestamp();
        let mut target_expiry = 0;

        if let Some(arr) = mark.as_array() {
            for v in arr {
                let sym = v["symbol"].as_str().unwrap_or("");
                if !sym.starts_with(base) { continue; }
                let parts: Vec<&str> = sym.split('-').collect();
                if parts.len() != 4 { continue; }
                
                if let Ok(date) = chrono::NaiveDate::parse_from_str(parts[1], "%y%m%d") {
                    if let Some(dt) = date.and_hms_opt(8, 0, 0) {
                        let expiry = dt.and_utc().timestamp();
                        if expiry as f64 - now as f64 >= 12.0 * 3600.0 {
                            if target_expiry == 0 || expiry < target_expiry {
                                target_expiry = expiry;
                            }
                        }
                    }
                }
            }
        }

        let mut best_call = None;
        let mut min_call_diff = f64::MAX;
        
        let mut best_put = None;
        let mut min_put_diff = f64::MAX;

        if let Some(arr) = mark.as_array() {
            for v in arr {
                let sym = v["symbol"].as_str().unwrap_or("");
                if !sym.starts_with(base) { continue; }
                let parts: Vec<&str> = sym.split('-').collect();
                if parts.len() != 4 { continue; }
                
                let ok_expiry = if let Ok(date) = chrono::NaiveDate::parse_from_str(parts[1], "%y%m%d") {
                    if let Some(dt) = date.and_hms_opt(8, 0, 0) {
                        dt.and_utc().timestamp() == target_expiry
                    } else { false }
                } else { false };

                if !ok_expiry { continue; }

                let strike = parts[2].parse::<f64>().unwrap_or(0.0);
                let opt_type = parts[3];
                let ask = *ask_prices.get(sym).unwrap_or(&0.0);
                
                if ask <= 0.0 || ask > 0.15 * s0 { continue; }

                let delta = v["delta"].as_f64().or_else(|| v["delta"].as_str().and_then(|s| s.parse::<f64>().ok())).unwrap_or(0.0);

                if opt_type == "C" {
                    let diff = (delta - target_delta).abs();
                    if diff < min_call_diff {
                        min_call_diff = diff;
                        best_call = Some((strike, sym.to_string()));
                    }
                } else if opt_type == "P" {
                    let diff = (delta - (-target_delta)).abs();
                    if diff < min_put_diff {
                        min_put_diff = diff;
                        best_put = Some((strike, sym.to_string()));
                    }
                }
            }
        }

        let (p_be_up, call_sym) = best_call.unwrap_or((s0 * 1.1, "".to_string()));
        let (p_be_down, put_sym) = best_put.unwrap_or((s0 * 0.9, "".to_string()));

        Ok((p_be_down, p_be_up, put_sym, call_sym))
    }

    pub async fn get_option_position(&self, opt_symbol: &str) -> Result<f64> {
        if opt_symbol.is_empty() { return Ok(0.0); }
        let res = self.execute(Method::GET, "/eapi/v1/position", vec![("symbol", opt_symbol.to_string())], true).await?;
        if let Some(arr) = res.as_array() {
            for v in arr {
                if v["symbol"].as_str() == Some(opt_symbol) {
                    let pos = v["quantity"].as_str().or(v["position"].as_str()).or(v["positionAmt"].as_str()).unwrap_or("0").parse::<f64>().unwrap_or(0.0);
                    return Ok(pos.abs());
                }
            }
        } else if res["symbol"].as_str() == Some(opt_symbol) {
             let pos = res["quantity"].as_str().or(res["position"].as_str()).or(res["positionAmt"].as_str()).unwrap_or("0").parse::<f64>().unwrap_or(0.0);
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
