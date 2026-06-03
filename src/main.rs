mod domain;
mod adapters;
mod app;

use domain::math::Config;
use adapters::okx::OkxAdapter;
use adapters::binance::BinanceAdapter;
use app::strategy::NeutralGridBot;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config {
        p_be_up: 3100.0,
        p_be_down: 2500.0,
        fee_rate: 0.0005,
        mmr: 0.004,
        q_s: 0.1,
        grids: 10,
        tick_sz: 0.01,
        lot_sz: 0.01,
        ct_val: 0.1,
        delta: 0.004,
    };

    let okx_api = OkxAdapter::new("OKX_API_KEY", "OKX_SECRET", "OKX_PASS");
    let binance_api = BinanceAdapter::new("BN_API_KEY", "BN_SECRET");

    let bot = NeutralGridBot::new(okx_api, binance_api, config);
    bot.run("ETH-USDT-SWAP").await?;

    Ok(())
}