//! Optional oracle integration for cross-currency pricing.
//!
//! Includes a deviation circuit breaker that rejects price spikes exceeding a
//! configurable basis-point threshold relative to the median of recent samples.
//!
//! Staleness is controlled at runtime by `OracleConfig.max_age_seconds`
//! (`set_oracle_config`), not by a compile-time `MAX_ORACLE_AGE`. The threshold
//! is vault-global by design; see `docs/oracle_pricing.md` ("Per-merchant
//! staleness threshold") for the rationale and future override sketch.

use crate::safe_math::{safe_add, safe_div, safe_mul, safe_pow, safe_sub};
use crate::types::{
    DataKey, Error, OracleConfig, OracleDeviationBreakerEvent, OraclePrice,
    OraclePriceHistoryMeta, Subscription,
};
use soroban_sdk::{Address, Env, Symbol, Vec};

const KEY_ORACLE_ENABLED: &str = "oracle_enabled";
const KEY_ORACLE_ADDR: &str = "oracle_addr";
const KEY_ORACLE_MAX_AGE: &str = "oracle_max_age";
const KEY_ORACLE_DEVIATION_BPS: &str = "oracle_deviation_bps";

/// Number of recent price samples retained per token for the deviation check.
const ORACLE_PRICE_HISTORY_SIZE: u32 = 10;

// ── Oracle config ──────────────────────────────────────────────────────────────

pub fn set_oracle_config(
    env: &Env,
    enabled: bool,
    oracle: Option<Address>,
    max_age_seconds: u64,
) -> Result<(), Error> {
    #[cfg(not(feature = "oracle-pricing"))]
    {
        let _ = (env, enabled, oracle, max_age_seconds);
        return Err(Error::InvalidInput);
    }
    #[cfg(feature = "oracle-pricing")]
    {
        if enabled {
            if oracle.is_none() {
                return Err(Error::OracleNotConfigured);
            }
            if max_age_seconds == 0 {
                return Err(Error::InvalidInput);
            }
        }
        let storage = env.storage().instance();
        storage.set(&Symbol::new(env, KEY_ORACLE_ENABLED), &enabled);
        if let Some(ref addr) = oracle {
            storage.set(&Symbol::new(env, KEY_ORACLE_ADDR), addr);
        } else {
            storage.remove(&Symbol::new(env, KEY_ORACLE_ADDR));
        }
        storage.set(&Symbol::new(env, KEY_ORACLE_MAX_AGE), &max_age_seconds);
        Ok(())
    }
}

pub fn get_oracle_config(env: &Env) -> OracleConfig {
    #[cfg(not(feature = "oracle-pricing"))]
    {
        let _ = env;
        return OracleConfig {
            enabled: false,
            oracle: None,
            max_age_seconds: 0,
        };
    }
    #[cfg(feature = "oracle-pricing")]
    {
        let storage = env.storage().instance();
        OracleConfig {
            enabled: storage
                .get(&Symbol::new(env, KEY_ORACLE_ENABLED))
                .unwrap_or(false),
            oracle: storage.get::<_, Address>(&Symbol::new(env, KEY_ORACLE_ADDR)),
            max_age_seconds: storage
                .get(&Symbol::new(env, KEY_ORACLE_MAX_AGE))
                .unwrap_or(0u64),
        }
    }
}

// ── Deviation circuit breaker ──────────────────────────────────────────────────

/// Set the maximum allowed price deviation in basis points.
///
/// A value of `0` means any deviation at all will be rejected (strict mode).
/// When unset (default), the deviation check is skipped entirely.
pub fn set_oracle_deviation_bps(env: &Env, bps: u32) {
    #[cfg(feature = "oracle-pricing")]
    env.storage()
        .instance()
        .set(&Symbol::new(env, KEY_ORACLE_DEVIATION_BPS), &bps);
    #[cfg(not(feature = "oracle-pricing"))]
    let _ = (env, bps);
}

/// Read the deviation threshold.
///
/// Returns `None` when no threshold has been configured (check disabled).
pub fn get_oracle_deviation_bps(env: &Env) -> Option<u32> {
    #[cfg(feature = "oracle-pricing")]
    {
        env.storage()
            .instance()
            .get(&Symbol::new(env, KEY_ORACLE_DEVIATION_BPS))
    }
    #[cfg(not(feature = "oracle-pricing"))]
    {
        let _ = env;
        None
    }
}

/// Read the price history for a given token.
pub fn get_oracle_price_history(env: &Env, token: &Address) -> Vec<i128> {
    #[cfg(feature = "oracle-pricing")]
    {
        load_prices(env, token)
    }
    #[cfg(not(feature = "oracle-pricing"))]
    {
        let _ = (env, token);
        Vec::new(env)
    }
}

/// Load the ring-buffer metadata for `token`.
fn load_history_meta(env: &Env, token: &Address) -> OraclePriceHistoryMeta {
    env.storage()
        .instance()
        .get::<DataKey, OraclePriceHistoryMeta>(&DataKey::OraclePriceHistoryMeta(token.clone()))
        .unwrap_or(OraclePriceHistoryMeta { head: 0, count: 0 })
}

/// Persist updated ring-buffer metadata.
fn save_history_meta(env: &Env, token: &Address, meta: &OraclePriceHistoryMeta) {
    env.storage().instance().set(
        &DataKey::OraclePriceHistoryMeta(token.clone()),
        meta,
    );
}

/// Read the timestamp of the most recently recorded sample for `token`.
///
/// Returns `None` when the ring buffer is empty (no prior entry to compare
/// against).
fn load_latest_timestamp(env: &Env, token: &Address) -> Option<u64> {
    let meta = load_history_meta(env, token);
    if meta.count == 0 {
        return None;
    }
    let last_slot = (meta.head + ORACLE_PRICE_HISTORY_SIZE - 1) % ORACLE_PRICE_HISTORY_SIZE;
    env.storage()
        .instance()
        .get::<DataKey, u64>(&DataKey::OraclePriceHistoryTimestamp(token.clone(), last_slot))
}

/// Read all samples currently in the ring buffer for `token` (in insertion order).
fn load_prices(env: &Env, token: &Address) -> Vec<i128> {
    let meta = load_history_meta(env, token);
    let mut out = Vec::new(env);
    if meta.count == 0 {
        return out;
    }
    let n = meta.count.min(ORACLE_PRICE_HISTORY_SIZE);
    // tail = (head - count) mod size  (logical oldest)
    let size = ORACLE_PRICE_HISTORY_SIZE;
    let tail = if meta.count < size {
        0u32
    } else {
        meta.head
    };
    for i in 0..n {
        let slot = (tail + i) % size;
        if let Some(price) = env.storage().instance().get::<DataKey, i128>(
            &DataKey::OraclePriceHistoryEntry(token.clone(), slot),
        ) {
            out.push_back(price);
        }
    }
    out
}

/// Append a new price sample to the ring buffer for `token`.
///
/// The entry is only accepted when its `timestamp` is strictly greater than
/// the timestamp of the most recently recorded entry. Out-of-order (or
/// duplicate-timestamp) entries are discarded so they never enter the ring
/// buffer and therefore never contribute to the TWAP calculation.
fn record_price(env: &Env, token: &Address, price: i128, timestamp: u64) {
    if let Some(prev_ts) = load_latest_timestamp(env, token) {
        if timestamp <= prev_ts {
            // Out-of-order entry: discard rather than corrupt the TWAP.
            return;
        }
    }
    let mut meta = load_history_meta(env, token);
    let slot = meta.head;
    meta.head = (meta.head + 1) % ORACLE_PRICE_HISTORY_SIZE;
    meta.count = meta.count.saturating_add(1);
    save_history_meta(env, token, &meta);
    env.storage().instance().set(
        &DataKey::OraclePriceHistoryEntry(token.clone(), slot),
        &price,
    );
    env.storage().instance().set(
        &DataKey::OraclePriceHistoryTimestamp(token.clone(), slot),
        &timestamp,
    );
}

/// Compute the median of a sorted price Vec.
/// Panics / returns the lower-middle for even-length lists.
fn median_of_sorted(sorted: &Vec<i128>) -> i128 {
    let len = sorted.len();
    sorted.get(len / 2).unwrap()
}

/// Sort prices using selection sort (N ≤ 10, so this is fine).
fn sort_prices(prices: &Vec<i128>, env: &Env) -> Vec<i128> {
    let n = prices.len();
    let mut sorted = Vec::new(env);
    for i in 0..n {
        sorted.push_back(prices.get(i).unwrap());
    }
    for i in 0..n {
        let mut min_idx = i;
        for j in (i + 1)..n {
            if sorted.get(min_idx).unwrap() > sorted.get(j).unwrap() {
                min_idx = j;
            }
        }
        if min_idx != i {
            let tmp = sorted.get(i).unwrap();
            sorted.set(i, sorted.get(min_idx).unwrap());
            sorted.set(min_idx, tmp);
        }
    }
    sorted
}

/// Check whether `new_price` deviates too far from the median of the last N
/// samples for `token`. If the deviation exceeds the configured threshold (or
/// the threshold is zero), return `Err(Error::OracleDeviationTooHigh)`.
///
/// On success the new price is recorded in the ring buffer.
fn check_deviation_and_record(
    env: &Env,
    token: &Address,
    new_price: i128,
) -> Result<(), Error> {
    let threshold_opt = get_oracle_deviation_bps(env);
    let threshold = match threshold_opt {
        Some(t) => t,
        None => return record_and_ok(env, token, new_price),
    };

    let prices = load_prices(env, token);
    let median = if prices.len() > 0 {
        let sorted = sort_prices(&prices, env);
        median_of_sorted(&sorted)
    } else {
        // Bootstrap: no history yet, always accept
        record_price(env, token, new_price, env.ledger().timestamp());
        return Ok(());
    };

    let diff = if new_price > median {
        new_price - median
    } else {
        median - new_price
    };
    // deviation_bps = (diff * 10_000) / 
