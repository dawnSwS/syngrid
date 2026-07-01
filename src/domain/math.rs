use anyhow::{Result, bail};

#[derive(Clone, Debug)]
pub struct StrategyGeometry {
    pub p_center: f64,
    pub q: f64,
    pub n: i32,
    pub q_grid: f64,
    pub actual_q_s: f64,
    pub margin_total: f64,
    pub tick_decimals: usize,
    pub lot_decimals: usize,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub p_be_up: f64,
    pub p_be_down: f64,
    #[allow(dead_code)]
    pub fee_rate: f64,
    pub mmr: f64,
    pub q_s: f64,
    pub grids: usize,
    pub tick_sz: f64,
    pub lot_sz: f64,
    pub ct_val: f64,
    pub delta: f64,
}

impl Config {
    pub fn calculate(&self) -> Result<StrategyGeometry> {
        let p_center = (self.p_be_up * self.p_be_down).sqrt();
        let n = self.grids as i32;
        let n_total = (2 * n) as f64;
        let q = (self.p_be_up / self.p_be_down).powf(1.0 / n_total);

        let q_grid = (self.q_s / (n as f64) / self.lot_sz).round() * self.lot_sz;
        if q_grid < self.lot_sz {
            bail!("{} < {}", q_grid, self.lot_sz);
        }

        let actual_q_s = q_grid * (n as f64);
        let avg_entry_short = p_center * q.powf((n as f64 + 1.0) / 2.0);

        let margin_total = actual_q_s * self.ct_val * ((self.p_be_up - avg_entry_short).max(0.0) + self.p_be_up * self.mmr);

        let format_decimals = |val: f64| -> usize {
            let s = format!("{:.10}", val);
            let trimmed = s.trim_end_matches('0');
            if let Some(idx) = trimmed.find('.') {
                trimmed.len() - idx - 1
            } else {
                0
            }
        };

        let tick_decimals = format_decimals(self.tick_sz);
        let lot_decimals = format_decimals(self.lot_sz);

        Ok(StrategyGeometry {
            p_center,
            q,
            n,
            q_grid,
            actual_q_s,
            margin_total,
            tick_decimals,
            lot_decimals,
        })
    }
}

pub fn price_at(geo: &StrategyGeometry, tick_sz: f64, k: i32) -> f64 {
    let raw = geo.p_center * geo.q.powi(k);
    (raw / tick_sz).round() * tick_sz
}

pub fn format_float(val: f64, step: f64, decimals: usize) -> String {
    let rounded = (val / step).round() * step;
    format!("{:.*}", decimals, rounded)
}

