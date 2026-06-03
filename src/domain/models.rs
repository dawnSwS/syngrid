use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PosSide {
    Long,
    Short,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone)]
pub struct OrderCommand {
    pub cl_ord_id: String,
    pub pos_side: PosSide,
    pub side: Side,
    pub price: String,
    pub size: String,
}

pub enum WsCommand {
    Place(Vec<OrderCommand>),
    Cancel(Vec<String>),
}

static ORDER_COUNTER: AtomicUsize = AtomicUsize::new(0);

pub fn format_cl_ord_id(pos_side: PosSide, side: Side, k: i32) -> String {
    let ps = match pos_side { PosSide::Long => "L", PosSide::Short => "S" };
    let s = match side { Side::Buy => "B", Side::Sell => "S" };
    let k_str = if k < 0 { format!("M{}", k.abs()) } else { k.to_string() };
    
    let count = ORDER_COUNTER.fetch_add(1, Ordering::Relaxed);
    let r = RandomState::new().build_hasher().finish();
    
    let mut id = format!("NGB{}{}{}R{:x}{:x}", ps, s, k_str, r, count);
    id.truncate(32);
    id
}

pub fn parse_cl_ord_id(id: &str) -> Option<(PosSide, Side, i32)> {
    if !id.starts_with("NGB") { return None; }
    
    let r_idx = id.find('R')?;
    let logic_part = &id[..r_idx];
    let bytes = logic_part.as_bytes();
    if bytes.len() < 5 { return None; }
    
    let ps = match bytes[3] { b'L' => PosSide::Long, b'S' => PosSide::Short, _ => return None };
    let s = match bytes[4] { b'B' => Side::Buy, b'S' => Side::Sell, _ => return None };
    
    let k_str = &logic_part[5..];
    let k = if let Some(stripped) = k_str.strip_prefix('M') {
        -(stripped.parse::<i32>().ok()?)
    } else {
        k_str.parse::<i32>().ok()?
    };
    Some((ps, s, k))
}