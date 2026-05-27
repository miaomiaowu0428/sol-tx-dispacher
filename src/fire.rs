//! fire — 单平台 build + fire-and-forget 辅助函数。
//!
//! 核心技巧：在 block 内 build（持有 &C 借用），提取 sig + b64，
//! block 结束时 TxEnvelope 被 drop（借用释放），再 Arc::clone 用于 spawn。
//! 这样避免了 TxEnvelope<'a, C> 的 'a 生命周期跨 spawn 边界问题。

use sol_tx_send::platform_clients::{BuildTx, BuildV0Tx, HashParam, SendTxEncoded};
use solana_sdk::{
    instruction::Instruction,
    message::AddressLookupTableAccount,
    signature::{Keypair, Signature},
    signer::Signer,
};
use ahash::AHashSet as HashSet;
use std::{ fmt::Display, sync::Arc};

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
    C: BuildV0Tx + BuildTx + SendTxEncoded + Display + Sync + Send + 'static,
{
    // ① build（借用 *client），提取 sig + b64，TxEnvelope 在 block 尾部 drop
    let (sig, b64) = {
        let memo_vec: Option<Vec<&str>> = memo.map(|m| vec![m]);
        match client.build_v0_tx(ixs, payer, &tip, hash_param, cu, alt, memo_vec) {
            Ok(env) => {
                let sig = env.sig();
                match env.inner_tx().to_base64() {
                    Ok(b) => (sig, b),
                    Err(e) => {
                        log::error!("[fire] {} serialize failed: {}", client, e);
                        return;
                    }
                }
                // env dropped here → 借用释放
            }
            Err(e) => {
                log::error!("[fire] {} build failed: {}", client, e);
                return;
            }
        }
    };

    // ② 借用已释放，可以 clone Arc
    sigs.insert(sig);
    log::info!("[fire] 🚀 {} sending {}", client, sig);

    let sender = Arc::clone(client);
    tokio::spawn(async move {
        if let Err(e) = sender.send_tx_encoded(&b64).await {
            log::error!("[fire] {} send failed: {}", sender, e);
        }
    });
}
