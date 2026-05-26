//! 两种发送策略。
//!
//! - `harmonic_mode` : Harmonic 直发（不加 tip）+ Astralane/Temporal 带 90% tip
//! - `fallback_mode` : 全量平台，三个宏按各平台特性自由组合

use crate::{SendContext, SendRoute, TipStrategy, TxDispacher, fire::fire_client};
use grpc_client::TransactionFormat;
use sol_slot_leader::SlotOracle;
use nonce_cache::{TxConfirmError, confirm_tx, tx_result_channel};
use sol_tx_send::platform_clients::BuildTx;
use solana_sdk::{instruction::Instruction, signature::Signature};
use std::collections::HashSet;

pub(crate) async fn dispatch<O: SlotOracle>(
    d: &TxDispacher<O>,
    ixs: &[Instruction],
    ctx: &SendContext,
    route: SendRoute,
    tip_strategy: Option<TipStrategy>,
    cu: (Option<u32>, Option<u64>),
    timeout_secs: u64,
) -> anyhow::Result<(Signature, TransactionFormat)> {
    let result = match route {
        SendRoute::Harmonic => harmonic_mode(d, ixs, ctx, tip_strategy, cu, timeout_secs).await,
        SendRoute::Jito     => jito_mode(d, ixs, ctx, tip_strategy, timeout_secs).await,
        SendRoute::Fallback => fallback_mode(d, ixs, ctx, tip_strategy, cu, timeout_secs).await,
    };
    result.map_err(|e| anyhow::anyhow!("send failed: {}", e))
}

// ── 内部辅助 ──────────────────────────────────────────────────────────────────

/// `Option<TipStrategy>` → `Option<u64>`：
/// `None` 直接返回 `None`，让 `fire_client` 内部走平台 `get_min_tip_amount()`。
#[inline]
fn opt_tip(strategy: Option<TipStrategy>, platform_min: u64) -> Option<u64> {
    strategy.map(|s| s.compute(platform_min))
}

/// 带默认比例的 tip 计算：`None` 时用 `default_ratio × platform_min`。
#[inline]
fn tip_or_default(strategy: Option<TipStrategy>, platform_min: u64, default_ratio: f64) -> Option<u64> {
    Some(match strategy {
        Some(s) => s.compute(platform_min),
        None => (platform_min as f64 * default_ratio) as u64,
    })
}

// ── harmonic_mode ─────────────────────────────────────────────────────────────

async fn harmonic_mode<O: SlotOracle>(
    d: &TxDispacher<O>,
    ixs: &[Instruction],
    ctx: &SendContext,
    tip_strategy: Option<TipStrategy>,
    cu: (Option<u32>, Option<u64>),
    timeout_secs: u64,
) -> Result<(Signature, TransactionFormat), TxConfirmError> {
    let rx = tx_result_channel::subscribe();
    let mut sigs = HashSet::new();
    let cu_no_price = (cu.0, None);

    // AstralaneQuic / Temporal 用 tip_strategy × 0.9
    let tip_09 = tip_strategy.map(|s| s.scaled(0.9));

    macro_rules! fire_no_price {
        ($client_opt:expr, tip: $tip:expr $(,)?) => {
            if let Some(c) = &$client_opt {
                let min = c.as_ref().get_min_tip_amount();
                let tip = opt_tip($tip, min);
                fire_client(c, ixs, &ctx.payer, tip, &ctx.hash_param, &cu_no_price, &ctx.alt, None, &mut sigs);
            }
        };
    }

    // Harmonic：将 tip_strategy 转为 cu_price（Harmonic 竞价 = priority fee，无需 SOL 转账）
    //
    // 公式：cu_price (micro-lamports/CU) = tip_lamports × 1_000_000 / cu_limit
    // 外界只管传 tip_strategy，此处偷偷做转换，对调用方透明。
    // HarmonicBlockEngine::uses_tip_transfer()=false，不管传什么 tip 都不会生成 SOL 转账指令。
    #[cfg(feature = "harmonic")]
    if let Some(c) = &d.harmonic {
        let cu_limit = cu.0.unwrap_or(200_000) as u64;
        let tip_lamports = tip_strategy
            .map(|s| s.compute(0)) // Harmonic min=0；Absolute(n)→n，Ratio→0
            .unwrap_or(0);
        let tip_derived_cu_price = if cu_limit > 0 && tip_lamports > 0 {
            tip_lamports.saturating_mul(1_000_000) / cu_limit
        } else {
            0
        };
        // 取 MAX：tip 转换值 vs 调用方原始 cu_price
        // Harmonic revert protection 保证失败不付钱，取高的竞价更有力且无额外风险
        let harmonic_cu_price = tip_derived_cu_price.max(cu.1.unwrap_or(0));
        let harmonic_cu = (cu.0, if harmonic_cu_price > 0 { Some(harmonic_cu_price) } else { None });
        // tip=None，uses_tip_transfer()=false 保证不生成 SOL 转账指令
        fire_client(c, ixs, &ctx.payer, None, &ctx.hash_param, &harmonic_cu, &ctx.alt, None, &mut sigs);
    }

    // AstralaneQuic / Temporal：tip_strategy × 0.9，不带 cu_price
    #[cfg(feature = "astralane_quic")]
    fire_no_price!(d.astralane_quic, tip: tip_09);

    #[cfg(feature = "temporal")]
    fire_no_price!(d.temporal, tip: tip_09);

    // 其他所有平台：tip_strategy，不带 cu_price
    #[cfg(feature = "everstake_quic")]
    fire_no_price!(d.everstake_quic, tip: tip_strategy);

    #[cfg(feature = "everstake")]
    fire_no_price!(d.everstake, tip: tip_strategy);

    #[cfg(feature = "flash_block")]
    fire_no_price!(d.flash_block, tip: tip_strategy);

    #[cfg(feature = "astralane")]
    fire_no_price!(d.astralane, tip: tip_strategy);

    #[cfg(feature = "nodeone")]
    fire_no_price!(d.nodeone, tip: tip_strategy);

    #[cfg(feature = "blockrazor")]
    fire_no_price!(d.blockrazor, tip: tip_strategy);

    #[cfg(feature = "helius")]
    fire_no_price!(d.helius, tip: tip_strategy);

    #[cfg(feature = "zeroslot")]
    fire_no_price!(d.zeroslot, tip: tip_strategy);

    #[cfg(feature = "nextblock")]
    fire_no_price!(d.nextblock, tip: tip_strategy);

    #[cfg(feature = "stellium")]
    fire_no_price!(d.stellium, tip: tip_strategy);

    #[cfg(feature = "jito")]
    fire_no_price!(d.jito, tip: tip_strategy);

    log::info!("[harmonic_mode] fired {} tx(s)", sigs.len());
    confirm_tx(rx, sigs, timeout_secs).await
}

// ── jito_mode ─────────────────────────────────────────────────────────────────

/// Jito 节点出块：只发带 tip 的版本，跳过所有纯 cu_price 的交易。
///
/// Jito 的出块优先级由 tip（SOL 转账）决定，cu_price 对排序几乎无帮助。
/// 所以只发 `fire_no_price!` 版本（有 tip 无 cu_price），
/// 原来仅 `fire_with_price!` 的平台直接跳过。
async fn jito_mode<O: SlotOracle>(
    d: &TxDispacher<O>,
    ixs: &[Instruction],
    ctx: &SendContext,
    tip_strategy: Option<TipStrategy>,
    timeout_secs: u64,
) -> Result<(Signature, TransactionFormat), TxConfirmError> {
    let rx = tx_result_channel::subscribe();
    let mut sigs = HashSet::new();
    let cu_no_price = (None, None); // 不带 cu_limit 也不带 cu_price

    macro_rules! fire_tip_only {
        ($client_opt:expr $(,)?) => {
            if let Some(c) = &$client_opt {
                let min = c.as_ref().get_min_tip_amount();
                let tip = opt_tip(tip_strategy, min);
                fire_client(c, ixs, &ctx.payer, tip, &ctx.hash_param, &cu_no_price, &ctx.alt, None, &mut sigs);
            }
        };
    }

    // 只发带 tip 的版本：fire_both 平台取 no_price 那笔，fire_with_price 平台跳过
    #[cfg(feature = "astralane_quic")]  fire_tip_only!(d.astralane_quic);
    #[cfg(all(feature = "astralane", not(feature = "astralane_quic")))]
    fire_tip_only!(d.astralane);

    #[cfg(feature = "flash_block")]     fire_tip_only!(d.flash_block);
    #[cfg(feature = "temporal")]        fire_tip_only!(d.temporal);
    #[cfg(feature = "zeroslot")]        fire_tip_only!(d.zeroslot);
    #[cfg(feature = "jito")]            fire_tip_only!(d.jito);

    // everstake_quic / everstake / nodeone / blockrazor / helius / nextblock / stellium
    // 这些平台在 fallback 里只发 cu_price 版本，Jito 模式下跳过

    log::info!("[jito_mode] fired {} tx(s)", sigs.len());
    confirm_tx(rx, sigs, timeout_secs).await
}

// ── fallback_mode ─────────────────────────────────────────────────────────────

async fn fallback_mode<O: SlotOracle>(
    d: &TxDispacher<O>,
    ixs: &[Instruction],
    ctx: &SendContext,
    tip_strategy: Option<TipStrategy>,
    cu: (Option<u32>, Option<u64>),
    timeout_secs: u64,
) -> Result<(Signature, TransactionFormat), TxConfirmError> {
    let rx = tx_result_channel::subscribe();
    let mut sigs = HashSet::new();
    let cu_no_price = (cu.0, None);

    // ── 三个宏 ──────────────────────────────────────────────────────────────
    //
    // 参数说明（所有 tip 均为 Option<TipStrategy>）：
    //   None          → fire_client 内部走平台 get_min_tip_amount()（平台默认）
    //   Some(Ratio(r))  → platform_min × r
    //   Some(Absolute(n)) → 精确 n lamports
    //
    // fire_both!(client, tip_with_price, tip_no_price)
    //   发两笔：带 cu_price 的用 tip_with_price，不带 cu_price 的用 tip_no_price。
    //   tip_with_price 通常取一个较小的值（避免被平台以 tip 不足为由丢弃，但不浪费费用）。
    //
    // fire_with_price!(client, tip)
    //   只发带 cu_price 的那笔。
    //
    // fire_no_price!(client, tip)
    //   只发不带 cu_price 的那笔。

    macro_rules! fire_with_price {
        ($client_opt:expr, tip: $tip:expr $(,)?) => {
            if let Some(c) = &$client_opt {
                let min = c.as_ref().get_min_tip_amount();
                let tip = opt_tip($tip, min);
                fire_client(c, ixs, &ctx.payer, tip, &ctx.hash_param, &cu, &ctx.alt, None, &mut sigs);
            }
        };
    }

    macro_rules! fire_both {
        ($client_opt:expr, with_price: $tip1:expr, no_price: $tip2:expr $(,)?) => {
            if let Some(c) = &$client_opt {
                let min = c.as_ref().get_min_tip_amount();
                let t1 = opt_tip($tip1, min);
                let t2 = opt_tip($tip2, min);
                fire_client(c, ixs, &ctx.payer, t1, &ctx.hash_param, &cu,          &ctx.alt, None, &mut sigs);
                fire_client(c, ixs, &ctx.payer, t2, &ctx.hash_param, &cu_no_price, &ctx.alt, None, &mut sigs);
            }
        };
    }

    macro_rules! fire_no_price {
        ($client_opt:expr, tip: $tip:expr $(,)?) => {
            if let Some(c) = &$client_opt {
                let min = c.as_ref().get_min_tip_amount();
                let tip = opt_tip($tip, min);
                fire_client(c, ixs, &ctx.payer, tip, &ctx.hash_param, &cu_no_price, &ctx.alt, None, &mut sigs);
            }
        };
    }

    // ── 各平台按特性组合 ────────────────────────────────────────────────────
    // 以下为示例配置，按实际平台特性调整：
    //
    //   fire_both!   → 适合既接受 cu_price 又接受 tip 的平台（大多数）
    //   fire_with_price! → 只想走 cu_price 竞价的平台
    //   fire_no_price!  → 主要靠 tip 排序的平台（如 Jito bundle）
    //
    // tip_with_price 推荐用略高于 1.0 的值（例如 Ratio(1.05) = 平台 min × 1.05），
    // 保证平台接受的同时不浪费太多费用。
    // tip_no_price 用 tip_strategy（调用方指定的完整 tip）。

    #[cfg(feature = "everstake_quic")]
    fire_with_price!(d.everstake_quic, tip: Some(TipStrategy::Ratio(1.05)));

    // everstake_quic 开启时 HTTP 版自动跳过，quic 未开启时按 feature 决定
    #[cfg(all(feature = "everstake", not(feature = "everstake_quic")))]
    fire_with_price!(d.everstake, tip: Some(TipStrategy::Ratio(1.05)));

    #[cfg(feature = "astralane_quic")]
    fire_both!(d.astralane_quic,
        with_price: Some(TipStrategy::Ratio(1.05)),
        no_price:   tip_strategy,
    );

    // astralane_quic 开启时 HTTP 版自动跳过，quic 未开启时按 feature 决定
    #[cfg(all(feature = "astralane", not(feature = "astralane_quic")))]
    fire_both!(d.astralane,
        with_price: Some(TipStrategy::Ratio(1.05)),
        no_price:   tip_strategy,
    );

    #[cfg(feature = "flash_block")]
    fire_both!(d.flash_block,
        with_price: Some(TipStrategy::Ratio(1.05)),
        no_price:   tip_strategy,
    );



    #[cfg(feature = "nodeone")]
    fire_with_price!(d.nodeone, tip: Some(TipStrategy::Ratio(1.05)));

    #[cfg(feature = "blockrazor")]
    fire_with_price!(d.blockrazor, tip: Some(TipStrategy::Ratio(1.05)));

    #[cfg(feature = "temporal")]
    fire_both!(d.temporal,
        with_price: Some(TipStrategy::Ratio(1.05)),
        no_price:   tip_strategy,
    );

    #[cfg(feature = "helius")]
    fire_with_price!(d.helius, tip: Some(TipStrategy::Ratio(1.05)));

    #[cfg(feature = "zeroslot")]
    fire_both!(d.zeroslot,
        with_price: Some(TipStrategy::Ratio(1.05)),
        no_price:   tip_strategy,
    );

    #[cfg(feature = "nextblock")]
    fire_with_price!(d.nextblock, tip: Some(TipStrategy::Ratio(1.05)));

    #[cfg(feature = "stellium")]
    fire_with_price!(d.stellium, tip: Some(TipStrategy::Ratio(1.05)));

    // Jito bundle 靠 tip 排序，cu_price 意义不大 → 只发 no_price 版本
    #[cfg(feature = "jito")]
    fire_no_price!(d.jito, tip: tip_strategy);

    log::info!("[fallback_mode] fired {} tx(s)", sigs.len());
    confirm_tx(rx, sigs, timeout_secs).await
}

// ── dispatch_cheap ─────────────────────────────────────────────────────────────

/// 低成本发送：不走 oracle 路由，只发少数几家平台单轮，省费用。
///
/// 选取原则：接受 tip 且延迟低的平台，不做双轮竞价。
/// 对应原 `send_utils::send_cheap` 的平台选择逻辑。
pub(crate) async fn dispatch_cheap<O: SlotOracle>(
    d: &TxDispacher<O>,
    ixs: &[Instruction],
    ctx: &SendContext,
    tip_strategy: Option<TipStrategy>,
    cu: (Option<u32>, Option<u64>),
    timeout_secs: u64,
) -> anyhow::Result<(Signature, TransactionFormat)> {
    let rx = tx_result_channel::subscribe();
    let mut sigs = HashSet::new();

    macro_rules! fire_cheap {
        ($client_opt:expr $(,)?) => {
            if let Some(c) = &$client_opt {
                let min = c.as_ref().get_min_tip_amount();
                let tip = opt_tip(tip_strategy, min);
                fire_client(c, ixs, &ctx.payer, tip, &ctx.hash_param, &cu, &ctx.alt, None, &mut sigs);
            }
        };
    }

    // QUIC 优先；有 QUIC 时对应 HTTP 版自动跳过
    #[cfg(feature = "everstake_quic")]
    fire_cheap!(d.everstake_quic);
    #[cfg(all(feature = "everstake", not(feature = "everstake_quic")))]
    fire_cheap!(d.everstake);

    #[cfg(feature = "astralane_quic")]
    fire_cheap!(d.astralane_quic);
    #[cfg(all(feature = "astralane", not(feature = "astralane_quic")))]
    fire_cheap!(d.astralane);

    // 少数 HTTP 平台
    #[cfg(feature = "flash_block")]     fire_cheap!(d.flash_block);
    // Jito（靠 tip 排序，便宜但有效）
    #[cfg(feature = "jito")]            fire_cheap!(d.jito);

    log::info!("[dispatch_cheap] fired {} tx(s)", sigs.len());
    confirm_tx(rx, sigs, timeout_secs)
        .await
        .map_err(|e| anyhow::anyhow!("send_cheap failed: {}", e))
}
