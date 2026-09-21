//! 两种发送策略。
//!
//! - `harmonic_mode` : Harmonic 直发（不加 tip）+ Astralane/Temporal 带 90% tip
//! - `fallback_mode` : 全量平台，三个宏按各平台特性自由组合

use crate::{
    CostConfig, CostTxConfig, SendContext, SendRoute, TipStrategy, TxDispacher, fire::fire_client, fire::fire_v1_client,
};
use ahash::AHashSet as HashSet;
use grpc_client::TransactionFormat;
use nonce_cache::{TxConfirmError, confirm_tx, tx_result_channel};
use sol_slot_leader::SlotOracle;
use sol_tx_send::platform_clients::{BuildTx, V1TxConfig};
use solana_sdk::{instruction::Instruction, signature::Signature};
use std::sync::{Arc, LazyLock, Mutex};

/// 通用 memo 标签，来源于环境变量 MEMO_TAG，默认 "default"
pub static MEMO_TAG: LazyLock<String> = LazyLock::new(|| std::env::var("MEMO_TAG").unwrap_or_else(|_| "default".to_string()));

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
        SendRoute::Jito => jito_mode(d, ixs, ctx, tip_strategy, cu, timeout_secs).await,
        SendRoute::Fallback => fallback_mode(d, ixs, ctx, tip_strategy, cu, timeout_secs).await,
        SendRoute::TipOnly => tip_only_auto(d, ixs, ctx, tip_strategy, cu, timeout_secs).await,
    };
    result.map_err(|e| anyhow::Error::from(e).context("send failed"))
}

// ── dispatch_with_cost ────────────────────────────────────────────────────────

/// 单预算竞价分发——把 `CostConfig` 展开为两个 tip 值 + 一个 cu_price，按平台性质
/// 分别喂给「带 cu_price 那笔」和「纯 tip 那笔」，然后走 route → mode 分发。
///
/// 为什么不能纯转接给 `dispatch`：`dispatch` 只接受一个 `tip_strategy`，无法同时表达
/// cost 语义需要的**两个 tip 值**——
/// - **低 tip 比例**（`tip_rate`）：给带 cu_price 那笔做保底（替代 fallback 里写死的 1.05）；
/// - **高 tip 绝对值**（`cost_amount`）：给纯 tip / no_price 那笔。
///
/// 若把 `cost` 一刀切转成 `Absolute(cost_amount)` 传给 `dispatch`，带 price 那笔会拿不到
/// `tip_rate`（被写死 1.05 挡住），`tip_rate` 字段形同虚设。故 Fallback 用全新
/// `fallback_cost_mode` 显式区分两值；Harmonic/Jito/TipOnly 本就单 tip 语义，复用原 mode
/// 并把 `Absolute(cost_amount)` 作为其 tip 预算。
pub(crate) async fn dispatch_with_cost<O: SlotOracle>(
    d: &TxDispacher<O>,
    ixs: &[Instruction],
    ctx: &SendContext,
    route: SendRoute,
    config: CostConfig,
    timeout_secs: u64,
) -> anyhow::Result<(Signature, TransactionFormat)> {
    let result = match route {
        SendRoute::Fallback => fallback_cost_mode(d, ixs, ctx, config, timeout_secs).await,
        // Harmonic / Jito / TipOnly：单 tip 语义，cost 全额作为 tip 预算，
        // cu_limit 透传、无 cu_price（Harmonic 主体内部会把 tip 转 cu_price）。
        SendRoute::Harmonic => {
            harmonic_mode(
                d,
                ixs,
                ctx,
                Some(TipStrategy::Absolute(config.cost_amount)),
                (Some(config.cu_limit), None),
                timeout_secs,
            )
            .await
        }
        SendRoute::Jito => {
            jito_mode(
                d,
                ixs,
                ctx,
                Some(TipStrategy::Absolute(config.cost_amount)),
                (Some(config.cu_limit), None),
                timeout_secs,
            )
            .await
        }
        SendRoute::TipOnly => {
            tip_only_auto(
                d,
                ixs,
                ctx,
                Some(TipStrategy::Absolute(config.cost_amount)),
                (Some(config.cu_limit), None),
                timeout_secs,
            )
            .await
        }
    };
    result.map_err(|e| anyhow::Error::from(e).context("send_with_cost failed"))
}



// ── 内部辅助 ──────────────────────────────────────────────────────────────────

/// `Option<TipStrategy>` → `Option<u64>`。
/// `None` 掉头返回 `None`，让 `fire_client` 走平台默认。
/// `Some` 时取 `max(strategy_output, platform_min × 1.02)`，保证至少比最低价高 2%。
#[inline]
fn opt_tip(strategy: Option<TipStrategy>, platform_min: u64) -> Option<u64> {
    let floor = (platform_min as f64 * 1.02) as u64;
    strategy.map(|s| s.compute(platform_min).max(floor))
}

/// 带默认比例的 tip 计算：`None` 时用 `default_ratio × platform_min`。
/// 最终值同样不低于 `platform_min × 1.02`。
#[inline]
fn tip_or_default(strategy: Option<TipStrategy>, platform_min: u64, default_ratio: f64) -> Option<u64> {
    let floor = (platform_min as f64 * 1.02) as u64;
    Some(match strategy {
        Some(s) => s.compute(platform_min).max(floor),
        None => ((platform_min as f64 * default_ratio) as u64).max(floor),
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
                fire_client(
                    c,
                    ixs,
                    &ctx.payer,
                    tip,
                    &ctx.hash_param,
                    &cu_no_price,
                    &ctx.alt,
                    Some(&*MEMO_TAG),
                    &mut sigs,
                );
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
        let harmonic_cu = (
            cu.0,
            if harmonic_cu_price > 0 {
                Some(harmonic_cu_price)
            } else {
                None
            },
        );
        // tip=None，uses_tip_transfer()=false 保证不生成 SOL 转账指令
        fire_client(
            c,
            ixs,
            &ctx.payer,
            None,
            &ctx.hash_param,
            &harmonic_cu,
            &ctx.alt,
            Some(&*MEMO_TAG),
            &mut sigs,
        );
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
    fire_no_price!(d.helius_max, tip: tip_strategy);

    #[cfg(feature = "helius")]
    fire_no_price!(d.helius_swqos, tip: tip_strategy);

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
    cu: (Option<u32>, Option<u64>),
    timeout_secs: u64,
) -> Result<(Signature, TransactionFormat), TxConfirmError> {
    let rx = tx_result_channel::subscribe();
    let mut sigs = HashSet::new();
    // Jito 模式：只带 tip，不带 cu_price，cu_limit 从调用方透传
    let cu_no_price = (cu.0, None);

    macro_rules! fire_tip_only {
        ($client_opt:expr $(,)?) => {
            if let Some(c) = &$client_opt {
                let min = c.as_ref().get_min_tip_amount();
                let tip = opt_tip(tip_strategy, min);
                fire_client(
                    c,
                    ixs,
                    &ctx.payer,
                    tip,
                    &ctx.hash_param,
                    &cu_no_price,
                    &ctx.alt,
                    Some(&*MEMO_TAG),
                    &mut sigs,
                );
            }
        };
    }

    // 只发带 tip 的版本：fire_both 平台取 no_price 那笔，fire_with_price 平台跳过
    #[cfg(feature = "astralane_quic")]
    fire_tip_only!(d.astralane_quic);
    #[cfg(all(feature = "astralane", not(feature = "astralane_quic")))]
    fire_tip_only!(d.astralane);

    #[cfg(feature = "flash_block")]
    fire_tip_only!(d.flash_block);
    #[cfg(feature = "temporal")]
    fire_tip_only!(d.temporal);
    #[cfg(feature = "zeroslot")]
    fire_tip_only!(d.zeroslot);
    #[cfg(feature = "jito")]
    fire_tip_only!(d.jito);

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
                fire_client(
                    c,
                    ixs,
                    &ctx.payer,
                    tip,
                    &ctx.hash_param,
                    &cu,
                    &ctx.alt,
                    Some(&*MEMO_TAG),
                    &mut sigs,
                );
            }
        };
    }

    macro_rules! fire_both {
        ($client_opt:expr, with_price: $tip1:expr, no_price: $tip2:expr $(,)?) => {
            if let Some(c) = &$client_opt {
                let min = c.as_ref().get_min_tip_amount();
                let t1 = opt_tip($tip1, min);
                let t2 = opt_tip($tip2, min);
                fire_client(
                    c,
                    ixs,
                    &ctx.payer,
                    t1,
                    &ctx.hash_param,
                    &cu,
                    &ctx.alt,
                    Some(&*MEMO_TAG),
                    &mut sigs,
                );
                fire_client(
                    c,
                    ixs,
                    &ctx.payer,
                    t2,
                    &ctx.hash_param,
                    &cu_no_price,
                    &ctx.alt,
                    Some(&*MEMO_TAG),
                    &mut sigs,
                );
            }
        };
    }

    macro_rules! fire_no_price {
        ($client_opt:expr, tip: $tip:expr $(,)?) => {
            if let Some(c) = &$client_opt {
                let min = c.as_ref().get_min_tip_amount();
                let tip = opt_tip($tip, min);
                fire_client(
                    c,
                    ixs,
                    &ctx.payer,
                    tip,
                    &ctx.hash_param,
                    &cu_no_price,
                    &ctx.alt,
                    Some(&*MEMO_TAG),
                    &mut sigs,
                );
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
    fire_with_price!(d.helius_max, tip: Some(TipStrategy::Ratio(1.05)));

    #[cfg(feature = "helius")]
    fire_with_price!(d.helius_swqos, tip: Some(TipStrategy::Ratio(1.05)));

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

// ── fallback_cost_mode ────────────────────────────────────────────────────────

/// cost 语义下的 fallback（对应 `CostConfig` 单预算）。
///
/// 与 [`fallback_mode`] 结构一致，但显式区分 cost 语义需要的**两个 tip 值**，
/// 避免「带 cu_price 那笔」和「纯 tip 那笔」被一刀切塞同一个 tip：
/// - **带 price 那笔**（`fire_with_price` / `fire_both` 的 with_price 笔）：
///   tip = `Ratio(config.tip_rate)`（低比例保底，替代 fallback 写死的 1.05），
///   cu_price = `cost_amount × 1e6 / cu_limit`（cost 全额换算到 gas）。
/// - **纯 tip 那笔**（`fire_both` 的 no_price 笔 / jito）：tip = `Absolute(cost_amount)` 全额。
///
/// single nonce → 全平台只有一笔上链、只付一次费，故两笔各自尽力花到 `cost_amount`。
async fn fallback_cost_mode<O: SlotOracle>(
    d: &TxDispacher<O>,
    ixs: &[Instruction],
    ctx: &SendContext,
    config: CostConfig,
    timeout_secs: u64,
) -> Result<(Signature, TransactionFormat), TxConfirmError> {
    let CostConfig {
        cost_amount,
        cu_limit,
        tip_rate,
    } = config;

    let rx = tx_result_channel::subscribe();
    let mut sigs = HashSet::new();

    // 带 price 那笔的 cu_price：cost 全额经 cu_limit 换算（micro-lamports/CU）。
    let cu_price = if cu_limit > 0 {
        cost_amount.saturating_mul(1_000_000) / cu_limit as u64
    } else {
        0
    };
    let cu = (
        Some(cu_limit),
        if cu_price > 0 { Some(cu_price) } else { None },
    );
    let cu_no_price = (Some(cu_limit), None);

    // 两个 tip 值：带 price 那笔用低比例保底，纯 tip 那笔用全额。
    let with_price_tip = Some(TipStrategy::Ratio(tip_rate));
    let no_price_tip = Some(TipStrategy::Absolute(cost_amount));

    macro_rules! fire_with_price {
        ($client_opt:expr $(,)?) => {
            if let Some(c) = &$client_opt {
                let min = c.as_ref().get_min_tip_amount();
                let tip = opt_tip(with_price_tip, min);
                fire_client(
                    c,
                    ixs,
                    &ctx.payer,
                    tip,
                    &ctx.hash_param,
                    &cu,
                    &ctx.alt,
                    Some(&*MEMO_TAG),
                    &mut sigs,
                );
            }
        };
    }

    macro_rules! fire_both {
        ($client_opt:expr $(,)?) => {
            if let Some(c) = &$client_opt {
                let min = c.as_ref().get_min_tip_amount();
                let t1 = opt_tip(with_price_tip, min);
                let t2 = opt_tip(no_price_tip, min);
                fire_client(
                    c,
                    ixs,
                    &ctx.payer,
                    t1,
                    &ctx.hash_param,
                    &cu,
                    &ctx.alt,
                    Some(&*MEMO_TAG),
                    &mut sigs,
                );
                fire_client(
                    c,
                    ixs,
                    &ctx.payer,
                    t2,
                    &ctx.hash_param,
                    &cu_no_price,
                    &ctx.alt,
                    Some(&*MEMO_TAG),
                    &mut sigs,
                );
            }
        };
    }

    macro_rules! fire_no_price {
        ($client_opt:expr $(,)?) => {
            if let Some(c) = &$client_opt {
                let min = c.as_ref().get_min_tip_amount();
                let tip = opt_tip(no_price_tip, min);
                fire_client(
                    c,
                    ixs,
                    &ctx.payer,
                    tip,
                    &ctx.hash_param,
                    &cu_no_price,
                    &ctx.alt,
                    Some(&*MEMO_TAG),
                    &mut sigs,
                );
            }
        };
    }

    log::info!(
        "[fallback_cost_mode] cost={} cu_limit={} tip_rate={} → cu_price={}",
        cost_amount,
        cu_limit,
        tip_rate,
        cu_price
    );

    // ── 平台组合（与 fallback_mode 一致，只是 tip 值换成 cost 语义） ─────
    #[cfg(feature = "everstake_quic")]
    fire_with_price!(d.everstake_quic);
    #[cfg(all(feature = "everstake", not(feature = "everstake_quic")))]
    fire_with_price!(d.everstake);

    #[cfg(feature = "astralane_quic")]
    fire_both!(d.astralane_quic);
    #[cfg(all(feature = "astralane", not(feature = "astralane_quic")))]
    fire_both!(d.astralane);

    #[cfg(feature = "flash_block")]
    fire_both!(d.flash_block);

    #[cfg(feature = "nodeone")]
    fire_with_price!(d.nodeone);
    #[cfg(feature = "blockrazor")]
    fire_with_price!(d.blockrazor);

    #[cfg(feature = "temporal")]
    fire_both!(d.temporal);

    #[cfg(feature = "helius")]
    fire_with_price!(d.helius_max);

    #[cfg(feature = "helius")]
    fire_with_price!(d.helius_swqos);

    #[cfg(feature = "zeroslot")]
    fire_both!(d.zeroslot);

    #[cfg(feature = "nextblock")]
    fire_with_price!(d.nextblock);
    #[cfg(feature = "stellium")]
    fire_with_price!(d.stellium);

    // Jito bundle 靠 tip 排序 → 只发 no_price（全额 tip）
    #[cfg(feature = "jito")]
    fire_no_price!(d.jito);

    log::info!("[fallback_cost_mode] fired {} tx(s)", sigs.len());
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
                fire_client(
                    c,
                    ixs,
                    &ctx.payer,
                    tip,
                    &ctx.hash_param,
                    &cu,
                    &ctx.alt,
                    Some(&*MEMO_TAG),
                    &mut sigs,
                );
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
    #[cfg(feature = "flash_block")]
    fire_cheap!(d.flash_block);
    // Jito（靠 tip 排序，便宜但有效）
    #[cfg(feature = "jito")]
    fire_cheap!(d.jito);

    log::info!("[dispatch_cheap] fired {} tx(s)", sigs.len());
    confirm_tx(rx, sigs, timeout_secs)
        .await
        .map_err(|e| anyhow::anyhow!("send_cheap failed: {}", e))
}

// ── tip_only_auto ────────────────────────────────────────────────────────

/// Tip-only 自动模式：cu_price 由 cu_limit 反推，总价不超过 0.0001 SOL。
async fn tip_only_auto<O: SlotOracle>(
    d: &TxDispacher<O>,
    ixs: &[Instruction],
    ctx: &SendContext,
    tip_strategy: Option<TipStrategy>,
    cu: (Option<u32>, Option<u64>),
    timeout_secs: u64,
) -> Result<(Signature, TransactionFormat), TxConfirmError> {
    let rx = tx_result_channel::subscribe();
    let mut sigs = HashSet::new();

    // cu_price：以 max_price（总 priority fee ≤ 0.0001 SOL 反推）作为上限，
    // 调用方显式传入的小值原样保留（不强制抬到上限），只有超过上限才被 clamp；
    // 无输入时用 80% 上限兜底（保持原有默认行为）。
    let cu_limit = cu.0.unwrap_or(200_000);
    let max_price = 100_000u64.saturating_mul(1_000_000) / cu_limit as u64;
    let cu_price = match cu.1 {
        Some(p) => p.min(max_price),
        None => (max_price as f64 * 0.8) as u64,
    };

    // 用 tip_strategy 或 5000 lamports 作为最低 tip
    let min_tip_floor = tip_strategy.map(|s| s.compute(0)).unwrap_or(5_000);

    // Helius Max 先发（高优先级单发）
    #[cfg(feature = "helius")]
    if let Some(c) = &d.helius_max {
        let tip = Some(min_tip_floor.max(c.as_ref().get_min_tip_amount()));
        fire_client(
            c,
            ixs,
            &ctx.payer,
            tip,
            &ctx.hash_param,
            &(Some(cu_limit), Some(cu_price)),
            &ctx.alt,
            Some(&*MEMO_TAG),
            &mut sigs,
        );
    }

    fire_all_parallel(
        d,
        ixs,
        ctx,
        min_tip_floor,
        cu_limit,
        Some(cu_price),
        Some(MEMO_TAG.to_string()),
        &mut sigs,
    )
    .await;
    log::info!(
        "[tip_only_auto] cu_limit={cu_limit} cu_price={cu_price} fired {} tx(s)",
        sigs.len()
    );
    confirm_tx(rx, sigs, timeout_secs).await
}

// ── dispatch_tip_only ────────────────────────────────────────────────────────

/// 纯 tip 竞价：不参与 cu_price 竞争，全平台一发，tip 至少 `min_tip_floor`。
pub(crate) async fn dispatch_tip_only<O: SlotOracle>(
    d: &TxDispacher<O>,
    ixs: &[Instruction],
    ctx: &SendContext,
    route: SendRoute,
    min_tip_floor: u64,
    cu_limit: u32,
    timeout_secs: u64,
) -> anyhow::Result<(Signature, TransactionFormat)> {
    let result = match route {
        SendRoute::Harmonic => tip_only_harmonic(d, ixs, ctx, min_tip_floor, cu_limit, timeout_secs).await,
        SendRoute::Jito => tip_only_jito(d, ixs, ctx, min_tip_floor, cu_limit, timeout_secs).await,
        SendRoute::Fallback => tip_only_fallback(d, ixs, ctx, min_tip_floor, cu_limit, timeout_secs).await,
        SendRoute::TipOnly => tip_only_fallback(d, ixs, ctx, min_tip_floor, cu_limit, timeout_secs).await,
    };
    result.map_err(|e| anyhow::Error::from(e).context("send_tip_only failed"))
}

/// 并行版 fire_all_tip_platforms：所有平台并发 build+sign+send。
/// 总延迟 = max(单个平台 build 时间) 而非 sum。
async fn fire_all_parallel(
    d: &TxDispacher<impl SlotOracle>,
    ixs: &[Instruction],
    ctx: &SendContext,
    min_tip_floor: u64,
    cu_limit: u32,
    cu_price: Option<u64>,
    memo: Option<String>,
    sigs: &mut HashSet<Signature>,
) {
    use sol_tx_send::platform_clients::{BuildTx, BuildV0Tx, SendTx};
    use std::sync::Mutex;

    let sigs_shared = Arc::new(Mutex::new(Vec::new()));
    let mut handles = Vec::new();
    let cu = (Some(cu_limit), cu_price);

    macro_rules! spawn_fire {
        ($client_opt:expr) => {
            if let Some(c) = &$client_opt {
                let c = Arc::clone(c);
                let ixs = ixs.to_vec();
                let payer = ctx.payer.clone();
                let tip = Some(min_tip_floor.max(c.get_min_tip_amount()));
                let hash_param = ctx.hash_param.clone();
                let cu = cu;
                let alt = ctx.alt.clone();
                let memo = memo.clone();
                let sigs = sigs_shared.clone();
                handles.push(tokio::spawn(async move {
                    let memo_ref: Option<&str> = memo.as_deref();
                    let memo_vec: Option<Vec<&str>> = memo_ref.map(|m| vec![m]);
                    match c.build_v0_tx(&ixs, &payer, &tip, &hash_param, &cu, &alt, memo_vec) {
                        Ok(env) => {
                            let sig = env.sig();
                            let tx = env.inner_tx().clone();
                            log::info!("[par] 🚀 {} sending {}", c, sig);
                            sigs.lock().unwrap().push(sig);
                            let sender = Arc::clone(&c);
                            tokio::spawn(async move {
                                let _ = sender.send_tx(&tx).await;
                            });
                        }
                        Err(e) => {
                            log::error!("[par] {} build: {}", c, e);
                        }
                    }
                }));
            }
        };
    }

    #[cfg(feature = "everstake_quic")]
    spawn_fire!(d.everstake_quic);
    #[cfg(all(feature = "everstake", not(feature = "everstake_quic")))]
    spawn_fire!(d.everstake);
    #[cfg(feature = "astralane_quic")]
    spawn_fire!(d.astralane_quic);
    #[cfg(all(feature = "astralane", not(feature = "astralane_quic")))]
    spawn_fire!(d.astralane);
    #[cfg(feature = "flash_block")]
    spawn_fire!(d.flash_block);
    #[cfg(feature = "temporal")]
    spawn_fire!(d.temporal);
    #[cfg(feature = "zeroslot")]
    spawn_fire!(d.zeroslot);
    #[cfg(feature = "nodeone")]
    spawn_fire!(d.nodeone);
    #[cfg(feature = "blockrazor")]
    spawn_fire!(d.blockrazor);
    #[cfg(feature = "helius")]
    spawn_fire!(d.helius_max);
    #[cfg(feature = "helius")]
    spawn_fire!(d.helius_swqos);
    #[cfg(feature = "nextblock")]
    spawn_fire!(d.nextblock);
    #[cfg(feature = "stellium")]
    spawn_fire!(d.stellium);

    for h in handles {
        let _ = h.await;
    }
    for sig in sigs_shared.lock().unwrap().iter() {
        sigs.insert(*sig);
    }
}

// ── 辅助宏：全平台 fire（harmonic 和 jito-only 之外的所有平台） ───────────

macro_rules! fire_all_tip_platforms {
    ($d:expr, $fire:ident) => {
        #[cfg(feature = "everstake_quic")]
        $fire!($d.everstake_quic);
        #[cfg(all(feature = "everstake", not(feature = "everstake_quic")))]
        $fire!($d.everstake);
        #[cfg(feature = "astralane_quic")]
        $fire!($d.astralane_quic);
        #[cfg(all(feature = "astralane", not(feature = "astralane_quic")))]
        $fire!($d.astralane);
        #[cfg(feature = "flash_block")]
        $fire!($d.flash_block);
        #[cfg(feature = "temporal")]
        $fire!($d.temporal);
        #[cfg(feature = "zeroslot")]
        $fire!($d.zeroslot);
        #[cfg(feature = "nodeone")]
        $fire!($d.nodeone);
        #[cfg(feature = "blockrazor")]
        $fire!($d.blockrazor);
        #[cfg(feature = "helius")]
        $fire!($d.helius_max);
        #[cfg(feature = "helius")]
        $fire!($d.helius_swqos);
        #[cfg(feature = "nextblock")]
        $fire!($d.nextblock);
        #[cfg(feature = "stellium")]
        $fire!($d.stellium);
    };
}

/// Harmonic 出块：发 Harmonic + 全平台 tip-only（不加 cu_price）。
/// Harmonic 的 tip 会被内部转为 cu_price 竞价。
async fn tip_only_harmonic<O: SlotOracle>(
    d: &TxDispacher<O>,
    ixs: &[Instruction],
    ctx: &SendContext,
    min_tip_floor: u64,
    cu_limit: u32,
    timeout_secs: u64,
) -> anyhow::Result<(Signature, TransactionFormat)> {
    let rx = tx_result_channel::subscribe();
    let mut sigs = HashSet::new();

    // Harmonic: tip → cu_price 转换（Harmonic 竞价 = priority fee）
    #[cfg(feature = "harmonic")]
    if let Some(c) = &d.harmonic {
        let cu = cu_limit as u64;
        let cu_price = if cu > 0 {
            min_tip_floor.saturating_mul(1_000_000) / cu
        } else {
            0
        };
        let harmonic_cu = (Some(cu_limit), if cu_price > 0 { Some(cu_price) } else { None });
        fire_client(
            c,
            ixs,
            &ctx.payer,
            None,
            &ctx.hash_param,
            &harmonic_cu,
            &ctx.alt,
            Some(&*MEMO_TAG),
            &mut sigs,
        );
    }

    fire_all_parallel(
        d,
        ixs,
        ctx,
        min_tip_floor,
        cu_limit,
        None,
        Some(MEMO_TAG.to_string()),
        &mut sigs,
    )
    .await;
    log::info!("[tip_only_harmonic] fired {} tx(s)", sigs.len());
    confirm_tx(rx, sigs, timeout_secs)
        .await
        .map_err(|e| anyhow::anyhow!("send_tip_only(harmonic) failed: {}", e))
}

/// Jito 出块：只发 tip-capable 平台（Jito/QUIC/FlashBlock 等）。
async fn tip_only_jito<O: SlotOracle>(
    d: &TxDispacher<O>,
    ixs: &[Instruction],
    ctx: &SendContext,
    min_tip_floor: u64,
    cu_limit: u32,
    timeout_secs: u64,
) -> anyhow::Result<(Signature, TransactionFormat)> {
    let rx = tx_result_channel::subscribe();
    let mut sigs = HashSet::new();
    let cu_no_price = (Some(cu_limit), None);

    macro_rules! fire_tip {
        ($client_opt:expr $(,)?) => {
            if let Some(c) = &$client_opt {
                let min = c.as_ref().get_min_tip_amount();
                let tip = Some(min_tip_floor.max(min));
                fire_client(
                    c,
                    ixs,
                    &ctx.payer,
                    tip,
                    &ctx.hash_param,
                    &cu_no_price,
                    &ctx.alt,
                    Some(&*MEMO_TAG),
                    &mut sigs,
                );
            }
        };
    }

    // Jito 模式：只发 tip-capable 平台
    #[cfg(feature = "astralane_quic")]
    fire_tip!(d.astralane_quic);
    #[cfg(all(feature = "astralane", not(feature = "astralane_quic")))]
    fire_tip!(d.astralane);
    #[cfg(feature = "flash_block")]
    fire_tip!(d.flash_block);
    #[cfg(feature = "temporal")]
    fire_tip!(d.temporal);
    #[cfg(feature = "zeroslot")]
    fire_tip!(d.zeroslot);
    #[cfg(feature = "jito")]
    fire_tip!(d.jito);

    log::info!("[tip_only_jito] fired {} tx(s)", sigs.len());
    confirm_tx(rx, sigs, timeout_secs)
        .await
        .map_err(|e| anyhow::anyhow!("send_tip_only(jito) failed: {}", e))
}

/// Fallback：全平台 tip-only 一发。
async fn tip_only_fallback<O: SlotOracle>(
    d: &TxDispacher<O>,
    ixs: &[Instruction],
    ctx: &SendContext,
    min_tip_floor: u64,
    cu_limit: u32,
    timeout_secs: u64,
) -> anyhow::Result<(Signature, TransactionFormat)> {
    let rx = tx_result_channel::subscribe();
    let mut sigs = HashSet::new();

    fire_all_parallel(
        d,
        ixs,
        ctx,
        min_tip_floor,
        cu_limit,
        None,
        Some(MEMO_TAG.to_string()),
        &mut sigs,
    )
    .await;
    log::info!("[tip_only_fallback] fired {} tx(s)", sigs.len());
    confirm_tx(rx, sigs, timeout_secs)
        .await
        .map_err(|e| anyhow::anyhow!("send_tip_only(fallback) failed: {}", e))
}

// ══════════════════════════════════════════════════════════════════════════════
// V1 版本
//
// 与 V0 的唯一差异是**构建参数**：
//    V0: `cu: (Option<u32>, Option<u64>)` + `alt: &Arc<Vec<AddressLookupTableAccount>>`
//    V1: `config: V1TxConfig`（CU 上限 + priority_fee + loaded_accounts_data_size + heap）
//
// V1 不支持地址查找表，账户全部内联，故没有 alt 参数。
//
// **priority_fee 的语义与 V0 的 cu_price 不同**：
//   V0 `cu_price` 是单价（micro-lamports/CU），总费 = cu_price × cu_limit / 1e6；
//   V1 `priority_fee` 是**总额**（lamports）。故 V0 里所有 `x × 1_000_000 / cu_limit`
//   的换算在 V1 下**全部去掉**，直接把 lamports 总额塞进 `priority_fee`。
//
// 各模式的平台组合与行为与 V0 完全一致，只换构建参数。
// ══════════════════════════════════════════════════════════════════════════════

/// V1 版 [`dispatch`]。`route` / `tip_strategy` / FIFO 语义与 V0 一致。
pub(crate) async fn dispatch_v1<O: SlotOracle>(
    d: &TxDispacher<O>,
    ixs: &[Instruction],
    ctx: &SendContext,
    route: SendRoute,
    tip_strategy: Option<TipStrategy>,
    config: V1TxConfig,
    timeout_secs: u64,
) -> anyhow::Result<(Signature, TransactionFormat)> {
    let result = match route {
        SendRoute::Harmonic => harmonic_mode_v1(d, ixs, ctx, tip_strategy, config, timeout_secs).await,
        SendRoute::Jito => jito_mode_v1(d, ixs, ctx, tip_strategy, config, timeout_secs).await,
        SendRoute::Fallback => fallback_mode_v1(d, ixs, ctx, tip_strategy, config, timeout_secs).await,
        SendRoute::TipOnly => tip_only_auto_v1(d, ixs, ctx, tip_strategy, config, timeout_secs).await,
    };
    result.map_err(|e| anyhow::Error::from(e).context("send_v1 failed"))
}

// ── dispatch_with_cost_v1 ─────────────────────────────────────────────────────

/// V1 版 [`dispatch_with_cost`] —— **单预算 `cost`，由各 mode 决定落地通道**。
///
/// 与 [`dispatch_v1`] 的分工：
///
/// - `dispatch_v1`：调用方**已经决定**了落地形式（`tip_strategy` + `config.priority_fee`）
/// - 本函数：调用方只给**意图**（`CostTxConfig.cost`），**落地由各 mode 按平台性质决定**
///
/// # 为什么不能只转接给 `dispatch_v1`
///
/// `dispatch_v1` 只接受**一个** `V1TxConfig`，而 cost 语义在 Fallback 下需要
/// **两套 config**（带 price 笔 `priority_fee=cost`、纯 tip 笔 `priority_fee=None`）
/// **加两个 tip 值**（带 price 笔 `Ratio(tip_rate)`、纯 tip 笔 `Absolute(cost)`）。
/// 这是 V0 `dispatch_with_cost` 早就解决的问题，V1 照搬同样的结构。
///
/// 与 V0 的差异：V0 用 `cu_price = cost × 1e6 / cu_limit`，V1 直接
/// `priority_fee = cost`（两者数学等价：`priority_fee = cu_price × cu_limit / 1e6`）。
pub(crate) async fn dispatch_with_cost_v1<O: SlotOracle>(
    d: &TxDispacher<O>,
    ixs: &[Instruction],
    ctx: &SendContext,
    route: SendRoute,
    config: CostTxConfig,
    timeout_secs: u64,
) -> anyhow::Result<(Signature, TransactionFormat)> {
    let result = match route {
        SendRoute::Harmonic => harmonic_cost_mode_v1(d, ixs, ctx, config, timeout_secs).await,
        SendRoute::Jito => jito_cost_mode_v1(d, ixs, ctx, config, timeout_secs).await,
        SendRoute::TipOnly => tip_only_cost_mode_v1(d, ixs, ctx, config, timeout_secs).await,
        // Fallback 是**兜底**：路由没命中具体出块源时走这条，需要双发（with_price + 纯 tip）。
        SendRoute::Fallback => fallback_cost_mode_v1(d, ixs, ctx, config, timeout_secs).await,
    };
    result.map_err(|e| anyhow::Error::from(e).context("send_with_cost_v1 failed"))
}

// ── harmonic_mode_v1 ──────────────────────────────────────────────────────────

/// V1 版 [`harmonic_mode`]。
///
/// Harmonic 自己：tip 折算成 `priority_fee`（**V1 下无需除 cu_limit**，priority_fee 本就是总额）。
/// 其他平台：只有 SOL tip，`priority_fee = None`。
async fn harmonic_mode_v1<O: SlotOracle>(
    d: &TxDispacher<O>,
    ixs: &[Instruction],
    ctx: &SendContext,
    tip_strategy: Option<TipStrategy>,
    config: V1TxConfig,
    timeout_secs: u64,
) -> Result<(Signature, TransactionFormat), TxConfirmError> {
    let rx = tx_result_channel::subscribe();
    let mut sigs = HashSet::new();
    let V1TxConfig {
        compute_unit_limit,
        loaded_accounts_data_size_limit,
        heap_size,
        ..
    } = config;
    // no_price 那笔：保留 cu 上限相关参数，priority_fee = None
    let cfg_no_price = V1TxConfig {
        priority_fee: None,
        compute_unit_limit,
        loaded_accounts_data_size_limit,
        heap_size,
    };

    // AstralaneQuic / Temporal 用 tip_strategy × 0.9
    let tip_09 = tip_strategy.map(|s| s.scaled(0.9));

    macro_rules! fire_v1_no_price {
        ($client_opt:expr, tip: $tip:expr $(,)?) => {
            if let Some(c) = &$client_opt {
                let min = c.as_ref().get_min_tip_amount();
                let tip = opt_tip($tip, min);
                fire_v1_client(
                    c,
                    ixs,
                    ctx,
                    tip,
                    cfg_no_price,
                    Some(&*MEMO_TAG),
                    &mut sigs,
                );
            }
        };
    }

    // Harmonic：tip_strategy → priority_fee（**总额，不除 cu_limit**）
    #[cfg(feature = "harmonic")]
    if let Some(c) = &d.harmonic {
        let tip_lamports = tip_strategy.map(|s| s.compute(0)).unwrap_or(0);
        // 取 MAX：tip 转换值 vs 调用方原始 priority_fee
        let priority_fee = tip_lamports.max(config.priority_fee.unwrap_or(0));
        let harmonic_cfg = V1TxConfig {
            priority_fee: if priority_fee > 0 { Some(priority_fee) } else { None },
            compute_unit_limit,
            loaded_accounts_data_size_limit,
            heap_size,
        };
        // tip=None，uses_tip_transfer()=false 保证不生成 SOL 转账指令
        fire_v1_client(
            c,
            ixs,
            ctx,
            None,
            harmonic_cfg,
            Some(&*MEMO_TAG),
            &mut sigs,
        );
    }

    #[cfg(feature = "astralane_quic")]
    fire_v1_no_price!(d.astralane_quic, tip: tip_09);
    #[cfg(feature = "temporal")]
    fire_v1_no_price!(d.temporal, tip: tip_09);

    #[cfg(feature = "everstake_quic")]
    fire_v1_no_price!(d.everstake_quic, tip: tip_strategy);
    #[cfg(feature = "everstake")]
    fire_v1_no_price!(d.everstake, tip: tip_strategy);
    #[cfg(feature = "flash_block")]
    fire_v1_no_price!(d.flash_block, tip: tip_strategy);
    #[cfg(feature = "astralane")]
    fire_v1_no_price!(d.astralane, tip: tip_strategy);
    #[cfg(feature = "nodeone")]
    fire_v1_no_price!(d.nodeone, tip: tip_strategy);
    #[cfg(feature = "blockrazor")]
    fire_v1_no_price!(d.blockrazor, tip: tip_strategy);
    #[cfg(feature = "helius")]
    fire_v1_no_price!(d.helius_max, tip: tip_strategy);
    #[cfg(feature = "helius")]
    fire_v1_no_price!(d.helius_swqos, tip: tip_strategy);
    #[cfg(feature = "zeroslot")]
    fire_v1_no_price!(d.zeroslot, tip: tip_strategy);
    #[cfg(feature = "nextblock")]
    fire_v1_no_price!(d.nextblock, tip: tip_strategy);
    #[cfg(feature = "stellium")]
    fire_v1_no_price!(d.stellium, tip: tip_strategy);
    #[cfg(feature = "jito")]
    fire_v1_no_price!(d.jito, tip: tip_strategy);

    log::info!("[harmonic_mode_v1] fired {} tx(s)", sigs.len());
    confirm_tx(rx, sigs, timeout_secs).await
}

// ── jito_mode_v1 ──────────────────────────────────────────────────────────────

/// V1 版 [`jito_mode`]：只发带 tip 的版本，`priority_fee = None`。
async fn jito_mode_v1<O: SlotOracle>(
    d: &TxDispacher<O>,
    ixs: &[Instruction],
    ctx: &SendContext,
    tip_strategy: Option<TipStrategy>,
    config: V1TxConfig,
    timeout_secs: u64,
) -> Result<(Signature, TransactionFormat), TxConfirmError> {
    let rx = tx_result_channel::subscribe();
    let mut sigs = HashSet::new();
    let cfg_no_price = V1TxConfig {
        priority_fee: None,
        ..config
    };

    macro_rules! fire_v1_tip_only {
        ($client_opt:expr $(,)?) => {
            if let Some(c) = &$client_opt {
                let min = c.as_ref().get_min_tip_amount();
                let tip = opt_tip(tip_strategy, min);
                fire_v1_client(
                    c,
                    ixs,
                    ctx,
                    tip,
                    cfg_no_price,
                    Some(&*MEMO_TAG),
                    &mut sigs,
                );
            }
        };
    }

    #[cfg(feature = "astralane_quic")]
    fire_v1_tip_only!(d.astralane_quic);
    #[cfg(all(feature = "astralane", not(feature = "astralane_quic")))]
    fire_v1_tip_only!(d.astralane);
    #[cfg(feature = "flash_block")]
    fire_v1_tip_only!(d.flash_block);
    #[cfg(feature = "temporal")]
    fire_v1_tip_only!(d.temporal);
    #[cfg(feature = "zeroslot")]
    fire_v1_tip_only!(d.zeroslot);
    #[cfg(feature = "jito")]
    fire_v1_tip_only!(d.jito);

    log::info!("[jito_mode_v1] fired {} tx(s)", sigs.len());
    confirm_tx(rx, sigs, timeout_secs).await
}

// ── fallback_mode_v1 ──────────────────────────────────────────────────────────

/// V1 版 [`fallback_mode`]：平台组合与 tip 语义与 V0 完全一致，
/// 只把 `cu` 换成 `V1TxConfig`（with_price 那笔用调用方 `priority_fee`，no_price 那笔设 None）。
async fn fallback_mode_v1<O: SlotOracle>(
    d: &TxDispacher<O>,
    ixs: &[Instruction],
    ctx: &SendContext,
    tip_strategy: Option<TipStrategy>,
    config: V1TxConfig,
    timeout_secs: u64,
) -> Result<(Signature, TransactionFormat), TxConfirmError> {
    let rx = tx_result_channel::subscribe();
    let mut sigs = HashSet::new();
    // 带 price 那笔：priority_fee 用调用方传入值
    let cfg_with_price = config;
    // 纯 tip 那笔：priority_fee = None
    let cfg_no_price = V1TxConfig {
        priority_fee: None,
        ..config
    };

    macro_rules! fire_v1_with_price {
        ($client_opt:expr, tip: $tip:expr $(,)?) => {
            if let Some(c) = &$client_opt {
                let min = c.as_ref().get_min_tip_amount();
                let tip = opt_tip($tip, min);
                fire_v1_client(
                    c,
                    ixs,
                    ctx,
                    tip,
                    cfg_with_price,
                    Some(&*MEMO_TAG),
                    &mut sigs,
                );
            }
        };
    }

    macro_rules! fire_v1_both {
        ($client_opt:expr, with_price: $tip1:expr, no_price: $tip2:expr $(,)?) => {
            if let Some(c) = &$client_opt {
                let min = c.as_ref().get_min_tip_amount();
                let t1 = opt_tip($tip1, min);
                let t2 = opt_tip($tip2, min);
                fire_v1_client(
                    c,
                    ixs,
                    ctx,
                    t1,
                    cfg_with_price,
                    Some(&*MEMO_TAG),
                    &mut sigs,
                );
                fire_v1_client(
                    c,
                    ixs,
                    ctx,
                    t2,
                    cfg_no_price,
                    Some(&*MEMO_TAG),
                    &mut sigs,
                );
            }
        };
    }

    macro_rules! fire_v1_no_price {
        ($client_opt:expr, tip: $tip:expr $(,)?) => {
            if let Some(c) = &$client_opt {
                let min = c.as_ref().get_min_tip_amount();
                let tip = opt_tip($tip, min);
                fire_v1_client(
                    c,
                    ixs,
                    ctx,
                    tip,
                    cfg_no_price,
                    Some(&*MEMO_TAG),
                    &mut sigs,
                );
            }
        };
    }

    #[cfg(feature = "everstake_quic")]
    fire_v1_with_price!(d.everstake_quic, tip: Some(TipStrategy::Ratio(1.05)));
    #[cfg(all(feature = "everstake", not(feature = "everstake_quic")))]
    fire_v1_with_price!(d.everstake, tip: Some(TipStrategy::Ratio(1.05)));

    #[cfg(feature = "astralane_quic")]
    fire_v1_both!(d.astralane_quic,
        with_price: Some(TipStrategy::Ratio(1.05)),
        no_price:   tip_strategy,
    );
    #[cfg(all(feature = "astralane", not(feature = "astralane_quic")))]
    fire_v1_both!(d.astralane,
        with_price: Some(TipStrategy::Ratio(1.05)),
        no_price:   tip_strategy,
    );

    #[cfg(feature = "flash_block")]
    fire_v1_both!(d.flash_block,
        with_price: Some(TipStrategy::Ratio(1.05)),
        no_price:   tip_strategy,
    );

    #[cfg(feature = "nodeone")]
    fire_v1_with_price!(d.nodeone, tip: Some(TipStrategy::Ratio(1.05)));
    #[cfg(feature = "blockrazor")]
    fire_v1_with_price!(d.blockrazor, tip: Some(TipStrategy::Ratio(1.05)));

    #[cfg(feature = "temporal")]
    fire_v1_both!(d.temporal,
        with_price: Some(TipStrategy::Ratio(1.05)),
        no_price:   tip_strategy,
    );

    #[cfg(feature = "helius")]
    fire_v1_with_price!(d.helius_max, tip: Some(TipStrategy::Ratio(1.05)));
    #[cfg(feature = "helius")]
    fire_v1_with_price!(d.helius_swqos, tip: Some(TipStrategy::Ratio(1.05)));

    #[cfg(feature = "zeroslot")]
    fire_v1_both!(d.zeroslot,
        with_price: Some(TipStrategy::Ratio(1.05)),
        no_price:   tip_strategy,
    );

    #[cfg(feature = "nextblock")]
    fire_v1_with_price!(d.nextblock, tip: Some(TipStrategy::Ratio(1.05)));
    #[cfg(feature = "stellium")]
    fire_v1_with_price!(d.stellium, tip: Some(TipStrategy::Ratio(1.05)));

    #[cfg(feature = "jito")]
    fire_v1_no_price!(d.jito, tip: tip_strategy);

    log::info!("[fallback_mode_v1] fired {} tx(s)", sigs.len());
    confirm_tx(rx, sigs, timeout_secs).await
}

// ══════════════════════════════════════════════════════════════════════════════
// V1 cost 语义（单预算，由各 mode 决定落地通道）
//
// 与上面 `*_mode_v1` 的**唯一区别**：
//   `*_mode_v1`  ：调用方已决定落地形式（tip_strategy + config.priority_fee）
//   `*_cost_mode_v1`：调用方只给意图（CostTxConfig.cost），落地由本函数决定
//
// 各平台落地规则（与 V0 `dispatch_with_cost` / `fallback_cost_mode` 一致）：
//   · Harmonic        → priority_fee = cost（它本就是 gas 竞价，不生成 SOL 转账）
//   · Jito / TipOnly  → tip = Absolute(cost)（这些平台靠 SOL tip 排序）
//   · Fallback 带price笔 → priority_fee = cost + tip = Ratio(tip_rate) 保底
//   · Fallback 纯tip笔   → tip = Absolute(cost)
// ══════════════════════════════════════════════════════════════════════════════

/// Harmonic 出块的 cost 语义版：`cost → priority_fee`。
///
/// Harmonic 的 `uses_tip_transfer()=false`（竞价 = priority fee），所以 `cost`
/// **全额落成 `priority_fee`**，不生成 SOL 转账。其余平台 `priority_fee=None`，`cost → tip`。
///
/// 对应 V0 [`dispatch_with_cost`] 的 `SendRoute::Harmonic` 分支
/// （那里传 `Absolute(cost)` 作 tip，由 `harmonic_mode` 内部转成 cu_price）。
async fn harmonic_cost_mode_v1<O: SlotOracle>(
    d: &TxDispacher<O>,
    ixs: &[Instruction],
    ctx: &SendContext,
    config: CostTxConfig,
    timeout_secs: u64,
) -> Result<(Signature, TransactionFormat), TxConfirmError> {
    // cost 落地到 tip 通道（FireBlock 等平台靠 SOL tip）；Harmonic 内部会把它转 priority_fee
    harmonic_mode_v1(
        d,
        ixs,
        ctx,
        Some(TipStrategy::Absolute(config.cost)),
        config.as_tip_channel_config(),
        timeout_secs,
    )
    .await
}

/// Jito 出块的 cost 语义版：`cost → tip`（全额）。
///
/// Jito 靠 SOL tip 排序，`priority_fee` 对排序无帮助，故 `cost` 全落 tip。
///
/// 对应 V0 [`dispatch_with_cost`] 的 `SendRoute::Jito` 分支。
async fn jito_cost_mode_v1<O: SlotOracle>(
    d: &TxDispacher<O>,
    ixs: &[Instruction],
    ctx: &SendContext,
    config: CostTxConfig,
    timeout_secs: u64,
) -> Result<(Signature, TransactionFormat), TxConfirmError> {
    jito_mode_v1(
        d,
        ixs,
        ctx,
        Some(TipStrategy::Absolute(config.cost)),
        config.as_tip_channel_config(),
        timeout_secs,
    )
    .await
}

/// TipOnly 出块的 cost 语义版：`cost → tip`（全额）。
///
/// 对应 V0 [`dispatch_with_cost`] 的 `SendRoute::TipOnly` 分支。
///
/// ⚠️ `tip_only_auto_v1` 内部有 `priority_fee ≤ 100_000` 的硬上限
/// （见该函数），`cost` 若超过会被 clamp —— 这是 V0 就有的行为，保持一致。
async fn tip_only_cost_mode_v1<O: SlotOracle>(
    d: &TxDispacher<O>,
    ixs: &[Instruction],
    ctx: &SendContext,
    config: CostTxConfig,
    timeout_secs: u64,
) -> Result<(Signature, TransactionFormat), TxConfirmError> {
    tip_only_auto_v1(
        d,
        ixs,
        ctx,
        Some(TipStrategy::Absolute(config.cost)),
        config.as_tip_channel_config(),
        timeout_secs,
    )
    .await
}

/// Fallback 出块的 cost 语义版 —— **双发**，显式区分两个通道。
///
/// 与 [`fallback_mode_v1`] 结构一致，但 tip / fee 的取值来自 cost 语义：
///
/// - **带 price 那笔**（`fire_v1_with_price!` / `fire_v1_both!` 的 with_price 笔）：
///   `priority_fee = cost` 全额，tip = `Ratio(tip_rate)`（低比例保底，
///   防止平台因 tip 不足丢弃；这正是 `tip_rate` 存在的原因）。
/// - **纯 tip 那笔**（`fire_v1_both!` 的 no_price 笔 / jito）：
///   tip = `Absolute(cost)` 全额，`priority_fee = None`。
///
/// single nonce → 全平台只有一笔上链、只付一次费，故两笔各自尽力花到 `cost`。
///
/// 对应 V0 的 [`fallback_cost_mode`]。
async fn fallback_cost_mode_v1<O: SlotOracle>(
    d: &TxDispacher<O>,
    ixs: &[Instruction],
    ctx: &SendContext,
    config: CostTxConfig,
    timeout_secs: u64,
) -> Result<(Signature, TransactionFormat), TxConfirmError> {
    let rx = tx_result_channel::subscribe();
    let mut sigs = HashSet::new();

    // 带 price 那笔：cost 全额 → priority_fee
    let cfg_with_price = config.as_fee_config();
    // 纯 tip 那笔：priority_fee = None，cost 全落 tip
    let cfg_no_price = config.as_tip_channel_config();

    // 两个 tip 值：带 price 那笔用低比例保底，纯 tip 那笔用全额。
    let with_price_tip = Some(TipStrategy::Ratio(config.tip_rate));
    let no_price_tip = Some(TipStrategy::Absolute(config.cost));

    macro_rules! fire_v1_with_price {
        ($client_opt:expr $(,)?) => {
            if let Some(c) = &$client_opt {
                let min = c.as_ref().get_min_tip_amount();
                let tip = opt_tip(with_price_tip, min);
                fire_v1_client(c, ixs, ctx, tip, cfg_with_price, Some(&*MEMO_TAG), &mut sigs);
            }
        };
    }

    macro_rules! fire_v1_both {
        ($client_opt:expr $(,)?) => {
            if let Some(c) = &$client_opt {
                let min = c.as_ref().get_min_tip_amount();
                let t1 = opt_tip(with_price_tip, min);
                let t2 = opt_tip(no_price_tip, min);
                fire_v1_client(c, ixs, ctx, t1, cfg_with_price, Some(&*MEMO_TAG), &mut sigs);
                fire_v1_client(c, ixs, ctx, t2, cfg_no_price, Some(&*MEMO_TAG), &mut sigs);
            }
        };
    }

    macro_rules! fire_v1_no_price {
        ($client_opt:expr $(,)?) => {
            if let Some(c) = &$client_opt {
                let min = c.as_ref().get_min_tip_amount();
                let tip = opt_tip(no_price_tip, min);
                fire_v1_client(c, ixs, ctx, tip, cfg_no_price, Some(&*MEMO_TAG), &mut sigs);
            }
        };
    }

    log::info!(
        "[fallback_cost_mode_v1] cost={} cu_limit={} tip_rate={}",
        config.cost,
        config.cu_limit,
        config.tip_rate
    );

    // ── 平台组合（与 fallback_mode_v1 逐项一致，只是 tip/fee 换成 cost 语义）──
    #[cfg(feature = "everstake_quic")]
    fire_v1_with_price!(d.everstake_quic);
    #[cfg(all(feature = "everstake", not(feature = "everstake_quic")))]
    fire_v1_with_price!(d.everstake);

    #[cfg(feature = "astralane_quic")]
    fire_v1_both!(d.astralane_quic);
    #[cfg(all(feature = "astralane", not(feature = "astralane_quic")))]
    fire_v1_both!(d.astralane);

    #[cfg(feature = "flash_block")]
    fire_v1_both!(d.flash_block);

    #[cfg(feature = "nodeone")]
    fire_v1_with_price!(d.nodeone);
    #[cfg(feature = "blockrazor")]
    fire_v1_with_price!(d.blockrazor);

    #[cfg(feature = "temporal")]
    fire_v1_both!(d.temporal);

    #[cfg(feature = "helius")]
    fire_v1_with_price!(d.helius_max);
    #[cfg(feature = "helius")]
    fire_v1_with_price!(d.helius_swqos);

    #[cfg(feature = "zeroslot")]
    fire_v1_both!(d.zeroslot);

    #[cfg(feature = "nextblock")]
    fire_v1_with_price!(d.nextblock);
    #[cfg(feature = "stellium")]
    fire_v1_with_price!(d.stellium);

    // Jito bundle 靠 tip 排序 → 只发 no_price（全额 tip）
    #[cfg(feature = "jito")]
    fire_v1_no_price!(d.jito);

    log::info!("[fallback_cost_mode_v1] fired {} tx(s)", sigs.len());
    confirm_tx(rx, sigs, timeout_secs).await
}

// ── tip_only_auto_v1 ──────────────────────────────────────────────────────────
/// V1 版 [`tip_only_auto`]。`priority_fee = 调用方传入值`（不超 0.0001 SOL 上限）。
async fn tip_only_auto_v1<O: SlotOracle>(
    d: &TxDispacher<O>,
    ixs: &[Instruction],
    ctx: &SendContext,
    tip_strategy: Option<TipStrategy>,
    config: V1TxConfig,
    timeout_secs: u64,
) -> Result<(Signature, TransactionFormat), TxConfirmError> {
    let rx = tx_result_channel::subscribe();
    let mut sigs = HashSet::new();

    // priority_fee 总额上限 0.0001 SOL（100_000 lamports）
    let max_fee = 100_000u64;
    let priority_fee = match config.priority_fee {
        Some(p) => p.min(max_fee),
        None => (max_fee as f64 * 0.8) as u64,
    };
    let cfg = V1TxConfig {
        priority_fee: Some(priority_fee),
        ..config
    };

    let min_tip_floor = tip_strategy.map(|s| s.compute(0)).unwrap_or(5_000);

    #[cfg(feature = "helius")]
    if let Some(c) = &d.helius_max {
        let tip = Some(min_tip_floor.max(c.as_ref().get_min_tip_amount()));
        fire_v1_client(
            c,
            ixs,
            ctx,
            tip,
            cfg,
            Some(&*MEMO_TAG),
            &mut sigs,
        );
    }

    fire_all_parallel_v1(d, ixs, ctx, min_tip_floor, cfg, Some(MEMO_TAG.to_string()), &mut sigs).await;
    log::info!("[tip_only_auto_v1] priority_fee={priority_fee} fired {} tx(s)", sigs.len());
    confirm_tx(rx, sigs, timeout_secs).await
}

// ── dispatch_cheap_v1 ─────────────────────────────────────────────────────────

/// V1 版 [`dispatch_cheap`]：平台组合与 V0 一致。
pub(crate) async fn dispatch_cheap_v1<O: SlotOracle>(
    d: &TxDispacher<O>,
    ixs: &[Instruction],
    ctx: &SendContext,
    tip_strategy: Option<TipStrategy>,
    config: V1TxConfig,
    timeout_secs: u64,
) -> anyhow::Result<(Signature, TransactionFormat)> {
    let rx = tx_result_channel::subscribe();
    let mut sigs = HashSet::new();

    macro_rules! fire_v1_cheap {
        ($client_opt:expr $(,)?) => {
            if let Some(c) = &$client_opt {
                let min = c.as_ref().get_min_tip_amount();
                let tip = opt_tip(tip_strategy, min);
                fire_v1_client(
                    c,
                    ixs,
                    ctx,
                    tip,
                    config,
                    Some(&*MEMO_TAG),
                    &mut sigs,
                );
            }
        };
    }

    #[cfg(feature = "everstake_quic")]
    fire_v1_cheap!(d.everstake_quic);
    #[cfg(all(feature = "everstake", not(feature = "everstake_quic")))]
    fire_v1_cheap!(d.everstake);
    #[cfg(feature = "astralane_quic")]
    fire_v1_cheap!(d.astralane_quic);
    #[cfg(all(feature = "astralane", not(feature = "astralane_quic")))]
    fire_v1_cheap!(d.astralane);
    #[cfg(feature = "flash_block")]
    fire_v1_cheap!(d.flash_block);
    #[cfg(feature = "jito")]
    fire_v1_cheap!(d.jito);

    log::info!("[dispatch_cheap_v1] fired {} tx(s)", sigs.len());
    confirm_tx(rx, sigs, timeout_secs)
        .await
        .map_err(|e| anyhow::anyhow!("send_cheap_v1 failed: {}", e))
}

// ── dispatch_tip_only_v1 ──────────────────────────────────────────────────────

/// V1 版 [`dispatch_tip_only`]。
pub(crate) async fn dispatch_tip_only_v1<O: SlotOracle>(
    d: &TxDispacher<O>,
    ixs: &[Instruction],
    ctx: &SendContext,
    route: SendRoute,
    min_tip_floor: u64,
    config: V1TxConfig,
    timeout_secs: u64,
) -> anyhow::Result<(Signature, TransactionFormat)> {
    let result = match route {
        SendRoute::Harmonic => tip_only_harmonic_v1(d, ixs, ctx, min_tip_floor, config, timeout_secs).await,
        SendRoute::Jito => tip_only_jito_v1(d, ixs, ctx, min_tip_floor, config, timeout_secs).await,
        SendRoute::Fallback => tip_only_fallback_v1(d, ixs, ctx, min_tip_floor, config, timeout_secs).await,
        SendRoute::TipOnly => tip_only_fallback_v1(d, ixs, ctx, min_tip_floor, config, timeout_secs).await,
    };
    result.map_err(|e| anyhow::Error::from(e).context("send_tip_only_v1 failed"))
}

/// V1 版 [`fire_all_parallel`]：并发 build+sign+send，用 `build_v1_tx`。
async fn fire_all_parallel_v1(
    d: &TxDispacher<impl SlotOracle>,
    ixs: &[Instruction],
    ctx: &SendContext,
    min_tip_floor: u64,
    config: V1TxConfig,
    memo: Option<String>,
    sigs: &mut HashSet<Signature>,
) {
    use sol_tx_send::platform_clients::{BuildV1Tx, SendTx};

    let sigs_shared = Arc::new(Mutex::new(Vec::new()));
    let mut handles = Vec::new();

    macro_rules! spawn_fire_v1 {
        ($client_opt:expr) => {
            if let Some(c) = &$client_opt {
                let c = Arc::clone(c);
                let ixs = ixs.to_vec();
                let payer = ctx.payer.clone();
                let tip = Some(min_tip_floor.max(c.get_min_tip_amount()));
                let hash_param = ctx.hash_param.clone();
                let config = config;
                let memo = memo.clone();
                let sigs = sigs_shared.clone();
                handles.push(tokio::spawn(async move {
                    let memo_ref: Option<&str> = memo.as_deref();
                    let memo_vec: Option<Vec<&str>> = memo_ref.map(|m| vec![m]);
                    match c.build_v1_tx(&ixs, &payer, &tip, &hash_param, config, memo_vec) {
                        Ok(env) => {
                            let sig = env.sig();
                            let tx = env.inner_tx().clone();
                            log::info!("[par-v1] 🚀 {} sending {}", c, sig);
                            sigs.lock().unwrap().push(sig);
                            let sender = Arc::clone(&c);
                            tokio::spawn(async move {
                                let _ = sender.send_tx(&tx).await;
                            });
                        }
                        Err(e) => {
                            log::error!("[par-v1] {} build: {}", c, e);
                        }
                    }
                }));
            }
        };
    }

    #[cfg(feature = "everstake_quic")]
    spawn_fire_v1!(d.everstake_quic);
    #[cfg(all(feature = "everstake", not(feature = "everstake_quic")))]
    spawn_fire_v1!(d.everstake);
    #[cfg(feature = "astralane_quic")]
    spawn_fire_v1!(d.astralane_quic);
    #[cfg(all(feature = "astralane", not(feature = "astralane_quic")))]
    spawn_fire_v1!(d.astralane);
    #[cfg(feature = "flash_block")]
    spawn_fire_v1!(d.flash_block);
    #[cfg(feature = "temporal")]
    spawn_fire_v1!(d.temporal);
    #[cfg(feature = "zeroslot")]
    spawn_fire_v1!(d.zeroslot);
    #[cfg(feature = "nodeone")]
    spawn_fire_v1!(d.nodeone);
    #[cfg(feature = "blockrazor")]
    spawn_fire_v1!(d.blockrazor);
    #[cfg(feature = "helius")]
    spawn_fire_v1!(d.helius_max);
    #[cfg(feature = "helius")]
    spawn_fire_v1!(d.helius_swqos);
    #[cfg(feature = "nextblock")]
    spawn_fire_v1!(d.nextblock);
    #[cfg(feature = "stellium")]
    spawn_fire_v1!(d.stellium);

    for h in handles {
        let _ = h.await;
    }
    for sig in sigs_shared.lock().unwrap().iter() {
        sigs.insert(*sig);
    }
}

/// V1 版 [`tip_only_harmonic`]。
async fn tip_only_harmonic_v1<O: SlotOracle>(
    d: &TxDispacher<O>,
    ixs: &[Instruction],
    ctx: &SendContext,
    min_tip_floor: u64,
    config: V1TxConfig,
    timeout_secs: u64,
) -> anyhow::Result<(Signature, TransactionFormat)> {
    let rx = tx_result_channel::subscribe();
    let mut sigs = HashSet::new();

    // Harmonic：tip → priority_fee（**总额，不除 cu_limit**）
    #[cfg(feature = "harmonic")]
    if let Some(c) = &d.harmonic {
        let cfg = V1TxConfig {
            priority_fee: if min_tip_floor > 0 { Some(min_tip_floor) } else { None },
            ..config
        };
        fire_v1_client(
            c,
            ixs,
            ctx,
            None,
            cfg,
            Some(&*MEMO_TAG),
            &mut sigs,
        );
    }

    let cfg_no_price = V1TxConfig {
        priority_fee: None,
        ..config
    };
    fire_all_parallel_v1(
        d,
        ixs,
        ctx,
        min_tip_floor,
        cfg_no_price,
        Some(MEMO_TAG.to_string()),
        &mut sigs,
    )
    .await;
    log::info!("[tip_only_harmonic_v1] fired {} tx(s)", sigs.len());
    confirm_tx(rx, sigs, timeout_secs)
        .await
        .map_err(|e| anyhow::anyhow!("send_tip_only_v1(harmonic) failed: {}", e))
}

/// V1 版 [`tip_only_jito`]。
async fn tip_only_jito_v1<O: SlotOracle>(
    d: &TxDispacher<O>,
    ixs: &[Instruction],
    ctx: &SendContext,
    min_tip_floor: u64,
    config: V1TxConfig,
    timeout_secs: u64,
) -> anyhow::Result<(Signature, TransactionFormat)> {
    let rx = tx_result_channel::subscribe();
    let mut sigs = HashSet::new();
    let cfg_no_price = V1TxConfig {
        priority_fee: None,
        ..config
    };

    macro_rules! fire_v1_tip {
        ($client_opt:expr $(,)?) => {
            if let Some(c) = &$client_opt {
                let min = c.as_ref().get_min_tip_amount();
                let tip = Some(min_tip_floor.max(min));
                fire_v1_client(
                    c,
                    ixs,
                    ctx,
                    tip,
                    cfg_no_price,
                    Some(&*MEMO_TAG),
                    &mut sigs,
                );
            }
        };
    }

    #[cfg(feature = "astralane_quic")]
    fire_v1_tip!(d.astralane_quic);
    #[cfg(all(feature = "astralane", not(feature = "astralane_quic")))]
    fire_v1_tip!(d.astralane);
    #[cfg(feature = "flash_block")]
    fire_v1_tip!(d.flash_block);
    #[cfg(feature = "temporal")]
    fire_v1_tip!(d.temporal);
    #[cfg(feature = "zeroslot")]
    fire_v1_tip!(d.zeroslot);
    #[cfg(feature = "jito")]
    fire_v1_tip!(d.jito);

    log::info!("[tip_only_jito_v1] fired {} tx(s)", sigs.len());
    confirm_tx(rx, sigs, timeout_secs)
        .await
        .map_err(|e| anyhow::anyhow!("send_tip_only_v1(jito) failed: {}", e))
}

/// V1 版 [`tip_only_fallback`]。
async fn tip_only_fallback_v1<O: SlotOracle>(
    d: &TxDispacher<O>,
    ixs: &[Instruction],
    ctx: &SendContext,
    min_tip_floor: u64,
    config: V1TxConfig,
    timeout_secs: u64,
) -> anyhow::Result<(Signature, TransactionFormat)> {
    let rx = tx_result_channel::subscribe();
    let mut sigs = HashSet::new();
    let cfg_no_price = V1TxConfig {
        priority_fee: None,
        ..config
    };

    fire_all_parallel_v1(
        d,
        ixs,
        ctx,
        min_tip_floor,
        cfg_no_price,
        Some(MEMO_TAG.to_string()),
        &mut sigs,
    )
    .await;
    log::info!("[tip_only_fallback_v1] fired {} tx(s)", sigs.len());
    confirm_tx(rx, sigs, timeout_secs)
        .await
        .map_err(|e| anyhow::anyhow!("send_tip_only_v1(fallback) failed: {}", e))
}
