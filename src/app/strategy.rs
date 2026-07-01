use anyhow::{Result, bail};
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc;
use serde_json::{json, Value};

use crate::domain::models::{PosSide, Side, OrderCommand, WsCommand, format_cl_ord_id, parse_cl_ord_id};
use crate::domain::math::{Config, StrategyGeometry, price_at, format_float};
use crate::adapters::okx::OkxAdapter;
use crate::adapters::binance::BinanceAdapter;

pub struct NeutralGridBot {
    okx_api: OkxAdapter,
    binance_api: BinanceAdapter,
    config: Config,
}

impl NeutralGridBot {
    pub fn new(okx_api: OkxAdapter, binance_api: BinanceAdapter, config: Config) -> Self {
        Self { okx_api, binance_api, config }
    }

    pub async fn run(&self, symbol: &str) -> Result<()> {
        let _ = self.binance_api.get_exchange_info().await;
        let _ = self.binance_api.get_account().await;
        let _ = self.binance_api.get_positions(None).await;
        let _ = self.binance_api.get_open_orders(None).await;

        let (tx_cmd, stream) = self.okx_api.subscribe_events(symbol).await?;
        
        let mut retries = 0;
        let mut current_price = self.okx_api.get_current_price(symbol).await.unwrap_or(0.0);
        while current_price <= 0.0 {
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            current_price = self.okx_api.get_current_price(symbol).await.unwrap_or(0.0);
            retries += 1;
            if retries > 20 { bail!("Timeout waiting for initial WS data"); }
        }

        let geo = self.deploy_array(symbol, current_price).await?;
        self.start_event_loop(symbol, geo, tx_cmd, stream).await?;
        Ok(())
    }

    async fn deploy_array(&self, symbol: &str, current_price: f64) -> Result<StrategyGeometry> {
        let current_ts = chrono::Utc::now().timestamp();
        let trading_day = (current_ts - 8 * 3600) / 86400;
        
        let exe_path = std::env::current_exe().unwrap_or_default();
        let state_dir = exe_path.parent().unwrap_or(std::path::Path::new("."));
        let state_file = state_dir.join(".ngb_state.json");
        
        let saved_state: Option<Value> = std::fs::read_to_string(&state_file)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok());

        let last_buy_day = saved_state.as_ref().and_then(|v| v["last_buy_day"].as_i64()).unwrap_or(0);

        let mut actual_config = self.config.clone();
        let geo;

        if trading_day > last_buy_day {
            let (p_be_down, p_be_up, put_sym, call_sym) = self.binance_api.get_options_info(symbol, current_price, self.config.delta).await?;
            actual_config.p_be_down = p_be_down;
            actual_config.p_be_up = p_be_up;
            geo = actual_config.calculate()?;

            let opt_qty = (geo.actual_q_s * self.config.ct_val * 100.0).round() / 100.0;

            if opt_qty >= 0.01 {
                if !call_sym.is_empty() {
                    let call_pos = self.binance_api.get_option_position(&call_sym).await?;
                    if call_pos < opt_qty {
                        self.binance_api.buy_option(&call_sym, opt_qty - call_pos).await?;
                    }
                }
                if !put_sym.is_empty() {
                    let put_pos = self.binance_api.get_option_position(&put_sym).await?;
                    if put_pos < opt_qty {
                        self.binance_api.buy_option(&put_sym, opt_qty - put_pos).await?;
                    }
                }
            }

            let new_state = json!({
                "last_buy_day": trading_day,
                "p_be_down": p_be_down,
                "p_be_up": p_be_up,
                "call_sym": call_sym,
                "put_sym": put_sym
            });
            let _ = std::fs::write(&state_file, new_state.to_string());
        } else {
            let st = saved_state.unwrap_or(json!({}));
            actual_config.p_be_down = st["p_be_down"].as_f64().unwrap_or(self.config.p_be_down);
            actual_config.p_be_up = st["p_be_up"].as_f64().unwrap_or(self.config.p_be_up);
            geo = actual_config.calculate()?;
            
            let opt_qty = (geo.actual_q_s * self.config.ct_val * 100.0).round() / 100.0;

            if let Some(call_sym) = st["call_sym"].as_str() {
                if !call_sym.is_empty() && opt_qty >= 0.01 {
                    let _ = self.binance_api.get_option_position(call_sym).await?;
                }
            }
            if let Some(put_sym) = st["put_sym"].as_str() {
                if !put_sym.is_empty() && opt_qty >= 0.01 {
                    let _ = self.binance_api.get_option_position(put_sym).await?;
                }
            }
        }

        self.okx_api.set_position_mode().await?;
        self.okx_api.set_leverage(symbol, PosSide::Long, 100).await?;
        self.okx_api.set_leverage(symbol, PosSide::Short, 100).await?;

        let bal = self.okx_api.get_balance("USDT").await?;
        if bal < geo.margin_total {
            self.okx_api.transfer_to_trading("USDT", geo.margin_total - bal).await?;
        }

        Ok(geo)
    }

    async fn reconciliation_scan(api: &OkxAdapter, config: &Config, symbol: &str, geo: &StrategyGeometry, tx_cmd: &mpsc::Sender<WsCommand>) -> Result<()> {
        let current_price = match api.get_current_price(symbol).await {
            Ok(p) if p > 0.0 => p,
            _ => return Ok(()),
        };
        
        let (long_pos, short_pos) = api.get_position(symbol).await.unwrap_or((0.0, 0.0));
        let open_orders = api.get_open_orders(symbol).await.unwrap_or_default();

        let mut ideal_orders = HashMap::new();
        let mut add_ideal = |pos_side: PosSide, side: Side, k: i32, size: f64| {
            let size_f = (size / config.lot_sz).round() * config.lot_sz;
            if size_f < config.lot_sz - 1e-6 { return; }
            let price_f = price_at(geo, config.tick_sz, k);
            ideal_orders.insert((pos_side, side, k), (size_f, price_f));
        };

        let mut rem_long = (long_pos / config.lot_sz).round() * config.lot_sz;
        let q_grid_f = (geo.q_grid / config.lot_sz).round() * config.lot_sz;
        for i in 1..=geo.n {
            let filled = if i == geo.n { rem_long } else { rem_long.min(q_grid_f) };
            rem_long -= filled;
            add_ideal(PosSide::Long, Side::Sell, -i + 1, filled);
            add_ideal(PosSide::Long, Side::Buy, -i, q_grid_f - filled);
        }

        let mut rem_short = (short_pos / config.lot_sz).round() * config.lot_sz;
        for i in 1..=geo.n {
            let filled = if i == geo.n { rem_short } else { rem_short.min(q_grid_f) };
            rem_short -= filled;
            add_ideal(PosSide::Short, Side::Buy, i - 1, filled);
            add_ideal(PosSide::Short, Side::Sell, i, q_grid_f - filled);
        }

        let mut valid_open = HashSet::new();
        let mut to_cancel = Vec::new();

        for (id, &(rem_sz, open_px)) in &open_orders {
            let mut keep = false;
            if let Some(logic) = parse_cl_ord_id(id) {
                if let Some(&(ideal_sz, ideal_px)) = ideal_orders.get(&logic) {
                    if !valid_open.contains(&logic) && (rem_sz - ideal_sz).abs() < config.lot_sz * 0.1 && (open_px - ideal_px).abs() < config.tick_sz * 0.1 {
                        valid_open.insert(logic);
                        keep = true;
                    }
                }
            }
            if !keep && id.starts_with("NGB") {
                to_cancel.push(id.clone());
            }
        }

        let mut missing_cmds = Vec::new();
        for (logic, &(size, price)) in &ideal_orders {
            if !valid_open.contains(logic) {
                if logic.1 == Side::Buy && price >= current_price { continue; }
                if logic.1 == Side::Sell && price <= current_price { continue; }

                missing_cmds.push(OrderCommand {
                    cl_ord_id: format_cl_ord_id(logic.0, logic.1, logic.2),
                    pos_side: logic.0,
                    side: logic.1,
                    price: format_float(price, config.tick_sz, geo.tick_decimals),
                    size: format_float(size, config.lot_sz, geo.lot_decimals),
                });
            }
        }

        if !to_cancel.is_empty() { let _ = tx_cmd.send(WsCommand::Cancel(to_cancel)).await; }
        if !missing_cmds.is_empty() { let _ = tx_cmd.send(WsCommand::Place(missing_cmds)).await; }
        Ok(())
    }

    async fn start_event_loop(&self, symbol: &str, mut geo: StrategyGeometry, tx_cmd: mpsc::Sender<WsCommand>, mut stream: mpsc::Receiver<()>) -> Result<()> {
        let mut recon_interval = tokio::time::interval(std::time::Duration::from_secs(10));
        
        let mut reload_interval = tokio::time::interval(std::time::Duration::from_secs(3600));
        reload_interval.tick().await;

        let _ = Self::reconciliation_scan(&self.okx_api, &self.config, symbol, &geo, &tx_cmd).await;

        loop {
            tokio::select! {
                _ = reload_interval.tick() => {
                    let _ = self.binance_api.get_exchange_info().await;
                    if let Ok(current_price) = self.okx_api.get_current_price(symbol).await {
                        if current_price > 0.0 {
                            geo = self.deploy_array(symbol, current_price).await?;
                        }
                    }
                }
                _ = recon_interval.tick() => {
                    let _ = Self::reconciliation_scan(&self.okx_api, &self.config, symbol, &geo, &tx_cmd).await;
                }
                event_opt = stream.recv() => {
                    if event_opt.is_none() { break; }
                    
                    tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;
                    while let Ok(_) = stream.try_recv() {} 
                    
                    let _ = Self::reconciliation_scan(&self.okx_api, &self.config, symbol, &geo, &tx_cmd).await;
                }
            }
        }
        
        bail!("WS stream disconnected")
    }
}
