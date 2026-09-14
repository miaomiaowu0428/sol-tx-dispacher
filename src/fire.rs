//! fire — 单平台 build + fire-and-forget 辅助函数。
//!
//! 核心技巧：在 block 内 build（持有 &C 借用），提取 sig + b64，
//! block 结束时 TxEnvelope 被 drop（借用释放），再 Arc::clone 用于 spawn。
//! 这样避免了 TxEnvelope<'a, C> 的 'a 生命周期跨 spawn 边界问题。

use ahash::AHashSet as HashSet;
use sol_tx_send::platform_clients::{BuildTx, BuildV0Tx, BuildV1Tx, HashParam, SendTx, V1TxConfig};
use solana_sdk::{
    instruction::Instruction,
    message::AddressLookupTableAccount,
    signature::{Keypair, Signature},
};
use std::{fmt::Display, sync::Arc};

/// 对某个平台发起一次 fire-and-forget 发送，将 sig 插入 `sigs`。
///
/// - `tip`：`None` 走平台最低 tip；`Some(0)` 完全不加 tip 指令。
/// - `cu`：`(cu_limit, cu_price)`，`None` 表示不设。
pub fn fire_client<C>(
    client: &Arc<C>,
    ixs: &[Instruction],
    payer: &Arc<Keypair>,
    tip: Option<u64>,
    hash_param: &HashParam,
    cu: &(Option<u32>, Option<u64>),
    alt: &Arc<Vec<AddressLookupTableAccount>>,
    memo: Option<&str>,
    sigs: &mut HashSet<Signature>,
) where
    C: BuildV0Tx + BuildTx + SendTx + Display + Sync + Send + 'static,
{
    // ① build（借用 *client），提取 sig + tx，TxEnvelope 在 block 尾部 drop
    let build_t0 = std::time::Instant::now();
    let (sig, tx) = {
        let memo_vec: Option<Vec<&str>> = memo.map(|m| vec![m]);
        match client.build_v0_tx(ixs, payer, &tip, hash_param, cu, alt, memo_vec) {
            Ok(env) => {
                let sig = env.sig();
                let tx = env.inner_tx().clone();
                (sig, tx)
                // env dropped here → 借用释放
            }
            Err(e) => {
                log::error!("[fire] {} build failed: {}", client, e);
                return;
            }
        }
    };
    let build_elapsed = build_t0.elapsed();

    // ② 借用已释放，可以 clone Arc
    sigs.insert(sig);
    log::info!("[fire] 🚀 {} sending {}", client, sig);

    let sender = Arc::clone(client);
    let platform = client.to_string();
    tokio::spawn(async move {
        let send_t0 = std::time::Instant::now();
        if let Err(e) = sender.send_tx(&tx).await {
            log::error!("[fire] {} send failed: {}", sender, e);
        }
        // ③ 单条 log：构建耗时 + 发送耗时（发送为交给平台、不等链上确认），按 sig 可查
        log::info!(
            "📤 [send] platform={platform} sig={sig} 构建耗时:{build_elapsed:?} 发送耗时:{:?}",
            send_t0.elapsed()
        );
    });
}

/// V1 版本的 `fire_client`。
///
/// 与 V0 版的差异只在**构建参数**：
/// - 没有 `cu: (limit, price)` → 改为 `config: V1TxConfig`（CU 上限 + priority fee 等）
/// - 没有 `alt` → V1 不支持地址查找表，账户全部内联
///
/// 发送路径完全一致（`SendTx::send_tx` 接收的都是 `VersionedTransaction`）。
pub fn fire_v1_client<C>(
    client: &Arc<C>,
    ixs: &[Instruction],
    payer: &Arc<Keypair>,
    tip: Option<u64>,
    hash_param: &HashParam,
    config: V1TxConfig,
    memo: Option<&str>,
    sigs: &mut HashSet<Signature>,
) where
    C: BuildV1Tx + BuildTx + SendTx + Display + Sync + Send + 'static,
{
    // ① build（借用 *client），提取 sig + tx，TxEnvelope 在 block 尾部 drop
    let build_t0 = std::time::Instant::now();
    let (sig, tx) = {
        let memo_vec: Option<Vec<&str>> = memo.map(|m| vec![m]);
        match client.build_v1_tx(ixs, payer, &tip, hash_param, config, memo_vec) {
            Ok(env) => {
                let sig = env.sig();
                let tx = env.inner_tx().clone();
                (sig, tx)
                // env dropped here → 借用释放
            }
            Err(e) => {
                log::error!("[fire-v1] {} build failed: {}", client, e);
                return;
            }
        }
    };
    let build_elapsed = build_t0.elapsed();

    // ② 借用已释放，可以 clone Arc
    sigs.insert(sig);
    log::info!("[fire-v1] 🚀 {} sending {}", client, sig);

    let sender = Arc::clone(client);
    let platform = client.to_string();
    tokio::spawn(async move {
        let send_t0 = std::time::Instant::now();
        if let Err(e) = sender.send_tx(&tx).await {
            log::error!("[fire-v1] {} send failed: {}", sender, e);
        }
        log::info!(
            "📤 [send-v1] platform={platform} sig={sig} 构建耗时:{build_elapsed:?} 发送耗时:{:?}",
            send_t0.elapsed()
        );
    });
}
