mod domain;
mod adapters;
mod app;
mod utils;

use domain::math::Config;
use adapters::okx::OkxAdapter;
use adapters::binance::BinanceAdapter;
use app::strategy::NeutralGridBot;
use utils::wakelock::WakeLock;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _wake_lock = WakeLock::new("okx_bot_wakelock");

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
        delta: 0.1,
    };

    // 使用 option_env! 宏在编译期提取环境变量，无需将密钥暴露在代码库中
    let worker_url = option_env!("WORKER_URL").unwrap_or("https://syngrid-api-relay.your-username.workers.dev");

    let okx_api = OkxAdapter::new(
        option_env!("OKX_API_KEY").unwrap_or(""),
        option_env!("OKX_SECRET").unwrap_or(""),
        option_env!("OKX_PASS").unwrap_or(""),
        worker_url
    );
    let binance_api = BinanceAdapter::new(
        option_env!("BN_API_KEY").unwrap_or(""),
        option_env!("BN_SECRET").unwrap_or(""),
        worker_url
    );

    let bot = NeutralGridBot::new(okx_api, binance_api, config);
    
    // 网络断开保护循环：当网络阻断导致抛出错误或静默结束时，将延迟 5 秒自动进行安全重启重连
    let bot_task = tokio::spawn(async move {
        loop {
            if let Err(e) = bot.run("ETH-USDT-SWAP").await {
                eprintln!("Bot run ended with error: {:?}. Restarting in 5s...", e);
            } else {
                eprintln!("Bot run ended unexpectedly. Restarting in 5s...");
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        }
    });

    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate())?;
        let mut sigint = signal(SignalKind::interrupt())?;

        tokio::select! {
            res = bot_task => {
                if let Err(e) = res {
                    eprintln!("Task Error: {:?}", e);
                }
            },
            _ = sigterm.recv() => println!("Received SIGTERM, shutting down..."),
            _ = sigint.recv() => println!("Received SIGINT, shutting down..."),
        }
    }
    
    #[cfg(not(unix))]
    {
        tokio::select! {
            res = bot_task => {
                if let Err(e) = res {
                    eprintln!("Task Error: {:?}", e);
                }
            },
            _ = tokio::signal::ctrl_c() => println!("Received Ctrl-C, shutting down..."),
        }
    }

    Ok(())
}


