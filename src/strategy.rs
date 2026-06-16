//! 两种发送策略。
//!
//! - `harmonic_mode` : Harmonic 直发（不加 tip）+ Astralane/Temporal 带 90% tip
//! - `fallback_mode` : 全量平台，三个宏按各平台特性自由组合

use crate::{SendContext, SendRoute, TipStrategy, TxDispacher, fire::fire_client};
use ahash::AHashSet as HashSet;
use grpc_client::TransactionFormat;
use nonce_cache::{TxConfirmError, confirm_tx, tx_result_channel};
use sol_slot_leader::SlotOracle;
use sol_tx_send::platform_clients::BuildTx;
use solana_sdk::{instruction::Instruction, signature::Signature};
use std::sync::{LazyLock, Arc, Mutex};

/// 通用 memo 标签，来源于环境变量 MEMO_TAG，默认 "default"
pub static MEMO_TAG: LazyLock<String> =
    LazyLock::new(|| std::env::var("MEMO_TAG").unwrap_or_else(|_| "default".to_string()));

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
    };
    result.map_err(|e| anyhow::Error::from(e).context("send failed"))
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
fn tip_or_default(
    strategy: Option<TipStrategy>,
    platform_min: u64,
    default_ratio: f64,
) -> Option<u64> {
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
        SendRoute::Harmonic => {
            tip_only_harmonic(d, ixs, ctx, min_tip_floor, cu_limit, timeout_secs).await
        }
        SendRoute::Jito => tip_only_jito(d, ixs, ctx, min_tip_floor, cu_limit, timeout_secs).await,
        SendRoute::Fallback => {
            tip_only_fallback(d, ixs, ctx, min_tip_floor, cu_limit, timeout_secs).await
        }
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
    memo: Option<String>,
    sigs: &mut HashSet<Signature>,
) {
    use std::sync::Mutex;
    use sol_tx_send::platform_clients::{BuildTx, BuildV0Tx, SendTxEncoded};

    let sigs_shared = Arc::new(Mutex::new(Vec::new()));
    let mut handles = Vec::new();
    let cu = (Some(cu_limit), None);

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
                            let b64 = match env.inner_tx().to_base64() {
                                Ok(b) => b,
                                Err(e) => { log::error!("[par] {} serialize: {}", c, e); return; }
                            };
                            log::info!("[par] 🚀 {} sending {}", c, sig);
                            sigs.lock().unwrap().push(sig);
                            let sender = Arc::clone(&c);
                            tokio::spawn(async move { let _ = sender.send_tx_encoded(&b64).await; });
                        }
                        Err(e) => { log::error!("[par] {} build: {}", c, e); }
                    }
                }));
            }
        };
    }

    #[cfg(feature = "everstake_quic")] spawn_fire!(d.everstake_quic);
    #[cfg(all(feature = "everstake", not(feature = "everstake_quic")))] spawn_fire!(d.everstake);
    #[cfg(feature = "astralane_quic")] spawn_fire!(d.astralane_quic);
    #[cfg(all(feature = "astralane", not(feature = "astralane_quic")))] spawn_fire!(d.astralane);
    #[cfg(feature = "flash_block")] spawn_fire!(d.flash_block);
    #[cfg(feature = "temporal")] spawn_fire!(d.temporal);
    #[cfg(feature = "zeroslot")] spawn_fire!(d.zeroslot);
    #[cfg(feature = "nodeone")] spawn_fire!(d.nodeone);
    #[cfg(feature = "blockrazor")] spawn_fire!(d.blockrazor);
    #[cfg(feature = "helius")] spawn_fire!(d.helius);
    #[cfg(feature = "nextblock")] spawn_fire!(d.nextblock);
    #[cfg(feature = "stellium")] spawn_fire!(d.stellium);

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
        $fire!($d.helius);
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
        let harmonic_cu = (
            Some(cu_limit),
            if cu_price > 0 { Some(cu_price) } else { None },
        );
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

    fire_all_parallel(d, ixs, ctx, min_tip_floor, cu_limit, Some(MEMO_TAG.to_string()), &mut sigs).await;
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

    fire_all_parallel(d, ixs, ctx, min_tip_floor, cu_limit, Some(MEMO_TAG.to_string()), &mut sigs).await;
    log::info!("[tip_only_fallback] fired {} tx(s)", sigs.len());
    confirm_tx(rx, sigs, timeout_secs)
        .await
        .map_err(|e| anyhow::anyhow!("send_tip_only(fallback) failed: {}", e))
}
