use anyhow::{Result, anyhow};
use reqwest::{Client, Method, header};
use serde_json::{json, Value};
use tokio::sync::{mpsc, RwLock};
use std::sync::Arc;
use std::collections::HashMap;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use base64::Engine;
use chrono::Utc;

use futures_util::{StreamExt, SinkExt};
use tokio_tungstenite::{connect_async, tungstenite::client::IntoClientRequest, tungstenite::protocol::Message};

use crate::domain::models::{PosSide, WsCommand};

#[derive(Default)]
pub struct WsState {
    pub price: f64,
    pub pos_long: f64,
    pub pos_short: f64,
    pub open_orders: HashMap<String, (f64, f64)>,
}

#[derive(Clone)]
pub struct OkxAdapter {
    api_key: String,
    secret: String,
    pass: String,
    client: Client,
    base_url: String,
    pub ws_state: Arc<RwLock<WsState>>,
}

impl OkxAdapter {
    pub fn new(key: &str, sec: &str, pass: &str, base_url: &str) -> Self {
        let mut headers = header::HeaderMap::new();
        headers.insert(
            header::HeaderName::from_static("x-relay-target"),
            header::HeaderValue::from_static("okx"),
        );

        let client = Client::builder()
            .default_headers(headers)
            .timeout(std::time::Duration::from_secs(15))
            .build()
            .unwrap();

        Self {
            api_key: key.into(),
            secret: sec.into(),
            pass: pass.into(),
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            ws_state: Arc::new(RwLock::new(WsState::default())),
        }
    }

    fn sign(&self, timestamp: &str, method: &str, path: &str, body: &str) -> String {
        let message = format!("{}{}{}{}", timestamp, method, path, body);
        let mut mac = Hmac::<Sha256>::new_from_slice(self.secret.as_bytes()).unwrap();
        mac.update(message.as_bytes());
        base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
    }

    async fn execute_rest(&self, method: Method, path: &str, body: Option<&Value>) -> Result<Value> {
        let url = format!("{}{}", self.base_url, path);
        let timestamp = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
        
        let body_str = body.map(|b| b.to_string()).unwrap_or_default();
        let sign = self.sign(&timestamp, method.as_str(), path, &body_str);

        let mut req = self.client.request(method, &url)
            .header("OK-ACCESS-KEY", &self.api_key)
            .header("OK-ACCESS-SIGN", sign)
            .header("OK-ACCESS-TIMESTAMP", timestamp)
            .header("OK-ACCESS-PASSPHRASE", &self.pass);

        if !body_str.is_empty() {
            req = req.header("Content-Type", "application/json").body(body_str);
        }

        let resp = req.send().await?;
        let status = resp.status();
        let text = resp.text().await?;

        if !status.is_success() {
            return Err(anyhow!("{} {}", status, text));
        }

        let json_resp: Value = serde_json::from_str(&text).unwrap_or(json!({}));
        
        if json_resp["code"].as_str() != Some("0") {
            return Err(anyhow!("{} {}", json_resp["code"], json_resp["msg"]));
        }

        Ok(json_resp)
    }

    pub async fn get_balance(&self, ccy: &str) -> Result<f64> {
        let path = format!("/api/v5/account/balance?ccy={}", ccy);
        let res = self.execute_rest(Method::GET, &path, None).await?;
        
        let mut eq = 0.0;
        if let Some(data) = res["data"].as_array() {
            if let Some(first) = data.first() {
                if let Some(details) = first["details"].as_array() {
                    for d in details {
                        if d["ccy"].as_str() == Some(ccy) {
                            eq = d["eq"].as_str().unwrap_or("0").parse().unwrap_or(0.0);
                            break;
                        }
                    }
                }
            }
        }
        Ok(eq)
    }

    pub async fn transfer_to_trading(&self, ccy: &str, amt: f64) -> Result<()> {
        let payload = json!({
            "ccy": ccy,
            "amt": amt.to_string(),
            "from": "6",
            "to": "18",
            "type": "0"
        });
        self.execute_rest(Method::POST, "/api/v5/asset/transfer", Some(&payload)).await?;
        Ok(())
    }

    pub async fn set_position_mode(&self) -> Result<()> {
        let payload = json!({ "posMode": "long_short_mode" });
        if let Err(e) = self.execute_rest(Method::POST, "/api/v5/account/set-position-mode", Some(&payload)).await {
            if !e.to_string().contains("51059") {
                return Err(e);
            }
        }
        Ok(())
    }

    pub async fn get_position(&self, _symbol: &str) -> Result<(f64, f64)> {
        let state = self.ws_state.read().await;
        Ok((state.pos_long, state.pos_short))
    }

    pub async fn get_open_orders(&self, _symbol: &str) -> Result<HashMap<String, (f64, f64)>> {
        let state = self.ws_state.read().await;
        Ok(state.open_orders.clone())
    }

    pub async fn get_current_price(&self, _symbol: &str) -> Result<f64> {
        let state = self.ws_state.read().await;
        Ok(state.price)
    }

    pub async fn set_leverage(&self, symbol: &str, pos_side: PosSide, leverage: u32) -> Result<()> {
        let side_str = match pos_side { PosSide::Long => "long", PosSide::Short => "short" };
        let payload = json!({
            "instId": symbol,
            "lever": leverage.to_string(),
            "mgnMode": "cross",
            "posSide": side_str
        });
        self.execute_rest(Method::POST, "/api/v5/account/set-leverage", Some(&payload)).await?;
        Ok(())
    }

    pub async fn subscribe_events(&self, symbol: &str) -> Result<(mpsc::Sender<WsCommand>, mpsc::Receiver<()>)> {
        if let Ok(res) = self.execute_rest(Method::GET, &format!("/api/v5/account/positions?instId={}", symbol), None).await {
            if let Some(arr) = res.get("data").and_then(|d| d.as_array()) {
                let mut st = self.ws_state.write().await;
                for item in arr {
                    let pos = item["pos"].as_str().unwrap_or("0").parse::<f64>().unwrap_or(0.0).abs();
                    match item["posSide"].as_str() {
                        Some("long") => st.pos_long = pos,
                        Some("short") => st.pos_short = pos,
                        _ => {}
                    }
                }
            }
        }

        if let Ok(res) = self.execute_rest(Method::GET, &format!("/api/v5/trade/orders-pending?instId={}", symbol), None).await {
            if let Some(arr) = res.get("data").and_then(|d| d.as_array()) {
                let mut st = self.ws_state.write().await;
                for order in arr {
                    if let Some(id) = order.get("clOrdId").and_then(|id| id.as_str()) {
                        if id.starts_with("NGB") {
                            let sz = order["sz"].as_str().unwrap_or("0").parse::<f64>().unwrap_or(0.0);
                            let acc = order["accFillSz"].as_str().unwrap_or("0").parse::<f64>().unwrap_or(0.0);
                            let px = order["px"].as_str().unwrap_or("0").parse::<f64>().unwrap_or(0.0);
                            st.open_orders.insert(id.to_string(), (sz - acc, px));
                        }
                    }
                }
            }
        }

        let ws_url = format!("{}/ws/v5/private", self.base_url.replace("https://", "wss://").replace("http://", "ws://"));
        let mut request = ws_url.into_client_request()?;
        
        request.headers_mut().insert(
            header::HeaderName::from_static("x-relay-target"),
            header::HeaderValue::from_static("okx")
        );

        let (ws_stream, _) = connect_async(request).await?;
        let (mut write, mut read) = ws_stream.split();

        let timestamp = Utc::now().timestamp().to_string();
        let sign = self.sign(&timestamp, "GET", "/users/self/verify", "");
        
        let login_msg = json!({
            "op": "login",
            "args": [{ "apiKey": self.api_key, "passphrase": self.pass, "timestamp": timestamp, "sign": sign }]
        });
        write.send(Message::Text(login_msg.to_string())).await?;

        loop {
            match read.next().await {
                Some(Ok(Message::Text(text))) => {
                    let json: Value = serde_json::from_str(&text).unwrap_or(json!({}));
                    if json["event"] == "login" && json["code"] == "0" {
                        break;
                    }
                }
                Some(Ok(_)) => continue,
                _ => return Err(anyhow!("WS disconnected during login")),
            }
        }

        let sub_msg = json!({
            "op": "subscribe",
            "args": [
                { "channel": "orders", "instType": "SWAP", "instId": symbol },
                { "channel": "positions", "instType": "SWAP", "instId": symbol },
                { "channel": "tickers", "instId": symbol }
            ]
        });
        write.send(Message::Text(sub_msg.to_string())).await?;

        let (tx_event, rx_event) = mpsc::channel(100);
        let (tx_cmd, mut rx_cmd) = mpsc::channel::<WsCommand>(100);
        let symbol_clone = symbol.to_string();
        let state_clone = self.ws_state.clone();

        tokio::spawn(async move {
            let mut ping_interval = tokio::time::interval(std::time::Duration::from_secs(20));

            loop {
                tokio::select! {
                    _ = ping_interval.tick() => {
                        let _ = write.send(Message::Text("ping".to_string())).await;
                    }
                    cmd_res = rx_cmd.recv() => {
                        let Some(cmd) = cmd_res else { break };
                        match cmd {
                            WsCommand::Place(cmds) => {
                                for chunk in cmds.chunks(20) {
                                    let args: Vec<Value> = chunk.iter().map(|c| {
                                        let side_str = match c.side { crate::domain::models::Side::Buy => "buy", crate::domain::models::Side::Sell => "sell" };
                                        let pos_side_str = match c.pos_side { PosSide::Long => "long", PosSide::Short => "short" };
                                        json!({
                                            "instId": &symbol_clone, "tdMode": "cross", "side": side_str, "posSide": pos_side_str,
                                            "ordType": "post_only", "px": c.price, "sz": c.size, "clOrdId": c.cl_ord_id
                                        })
                                    }).collect();
                                    
                                    let msg = json!({ "id": Utc::now().timestamp_millis().to_string(), "op": "batch-orders", "args": args });
                                    let _ = write.send(Message::Text(msg.to_string())).await;
                                }
                            }
                            WsCommand::Cancel(ids) => {
                                for chunk in ids.chunks(20) {
                                    let args: Vec<Value> = chunk.iter().map(|id| {
                                        json!({ "instId": &symbol_clone, "clOrdId": id })
                                    }).collect();
                                    
                                    let msg = json!({ "id": Utc::now().timestamp_millis().to_string(), "op": "cancel-batch-orders", "args": args });
                                    let _ = write.send(Message::Text(msg.to_string())).await;
                                }
                            }
                        }
                    }
                    msg_res = read.next() => {
                        match msg_res {
                            Some(Ok(Message::Text(text))) => {
                                if text == "pong" { continue; }
                                let json: Value = serde_json::from_str(&text).unwrap_or(json!({}));
                                
                                let mut trigger = false;

                                let op = json.get("op").and_then(|o| o.as_str()).unwrap_or("");
                                if op == "cancel-batch-orders" {
                                    if let Some(data) = json.get("data").and_then(|d| d.as_array()) {
                                        let mut st = state_clone.write().await;
                                        for item in data {
                                            let s_code = item.get("sCode").and_then(|c| c.as_str()).unwrap_or("0");
                                            if s_code != "0" {
                                                if let Some(cl_ord_id) = item.get("clOrdId").and_then(|id| id.as_str()) {
                                                    if st.open_orders.remove(cl_ord_id).is_some() {
                                                        trigger = true;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                
                                if let Some(data) = json.get("data").and_then(|d| d.as_array()) {
                                    let channel = json.get("arg").and_then(|a| a.get("channel")).and_then(|c| c.as_str()).unwrap_or("");

                                    match channel {
                                        "tickers" => {
                                            if let Some(first) = data.first() {
                                                if let Ok(px) = first["last"].as_str().unwrap_or("0").parse::<f64>() {
                                                    let mut st = state_clone.write().await;
                                                    st.price = px;
                                                }
                                            }
                                        }
                                        "positions" => {
                                            let mut st = state_clone.write().await;
                                            for item in data {
                                                let pos = item["pos"].as_str().unwrap_or("0").parse::<f64>().unwrap_or(0.0).abs();
                                                match item["posSide"].as_str() {
                                                    Some("long") => {
                                                        if (st.pos_long - pos).abs() > 1e-6 {
                                                            st.pos_long = pos;
                                                            trigger = true;
                                                        }
                                                    }
                                                    Some("short") => {
                                                        if (st.pos_short - pos).abs() > 1e-6 {
                                                            st.pos_short = pos;
                                                            trigger = true;
                                                        }
                                                    }
                                                    _ => {}
                                                }
                                            }
                                        }
                                        "orders" => {
                                            let mut st = state_clone.write().await;
                                            for order in data {
                                                let cl_ord_id = order["clOrdId"].as_str().unwrap_or("").to_string();
                                                if !cl_ord_id.starts_with("NGB") { continue; }

                                                let state_str = order["state"].as_str().unwrap_or("");
                                                if state_str == "live" || state_str == "partially_filled" {
                                                    let sz = order["sz"].as_str().unwrap_or("0").parse::<f64>().unwrap_or(0.0);
                                                    let acc = order["accFillSz"].as_str().unwrap_or("0").parse::<f64>().unwrap_or(0.0);
                                                    let px = order["px"].as_str().unwrap_or("0").parse::<f64>().unwrap_or(0.0);
                                                    let rem_sz = sz - acc;
                                                    
                                                    let update = match st.open_orders.get(&cl_ord_id) {
                                                        Some(&(old_sz, old_px)) => (old_sz - rem_sz).abs() > 1e-6 || (old_px - px).abs() > 1e-6,
                                                        None => true,
                                                    };
                                                    if update {
                                                        st.open_orders.insert(cl_ord_id, (rem_sz, px));
                                                        trigger = true;
                                                    }
                                                } else {
                                                    if st.open_orders.remove(&cl_ord_id).is_some() { trigger = true; }
                                                }
                                            }
                                        }
                                        _ => {}
                                    }

                                    if trigger {
                                        let _ = tx_event.try_send(());
                                    }
                                }
                            }
                            Some(Ok(_)) => continue,
                            Some(Err(_)) | None => break,
                        }
                    }
                }
            }
        });

        Ok((tx_cmd, rx_event))
    }
}
