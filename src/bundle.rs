//! 多平台 bundle 发送器。
//!
//! 将同一组原子 bundle（多笔交易）并发代理到多个平台，并在发送后通过
//! `nonce_cache` 的 confirm 系列方法确认任意一笔上链。`append` 参数与
//! `sol_tx_send::platform_clients::BundleBuilder` 完全对齐，`send()` 内部完成
//! 订阅 → 并发发送 → 整理每平台首笔签名 → 确认的完整流程。
//!
//! 本模块不依赖 slot/oracle 路由——纯多平台广播。适合"同一批交易必须原子执行、
//! 想提高落地概率"的场景（如 snipe 的 buy+tip 组合、迁移+买入打包等）。
//!
//! # 使用方式
//!
//! ```rust,ignore
//! use sol_tx_dispacher::bundle::MultiBundleSender;
//!
//! let multi = MultiBundleSender::new(vec![
//!     Box::new(flash_block_client),  // 各平台需实现 BundleSender
//!     Box::new(jito_client),
//!     Box::new(astralane_client),
//! ]);
//!
//! let (sig, tx) = Ok(multi)
//!     .and_then(|m| m.append(&ixs1, &signers1, &None, &nonce, &cu, &alt, Some(vec!["memo"])))
//!     .and_then(|m| m.append(&ixs2, &signers2, &tip, &nonce, &cu, &alt, None))
//!     .map_err(|e| e.msg)?
//!     .send(60)   // 60s 内确认任意一笔上链
//!     .await?;
//! ```

use ahash::AHashSet as HashSet;
use grpc_client::TransactionFormat;
use nonce_cache::{TxConfirmError, confirm_success_tx, tx_result_channel};
use sol_tx_send::platform_clients::{BundleBuilder, BundleError, BundleSender, HashParam};
use solana_sdk::{
    instruction::Instruction,
    message::AddressLookupTableAccount,
    signature::{Keypair, Signature},
};

/// 多平台 bundle 发送器。
///
/// 内部为每个平台维护一个 [`BundleBuilder`]，`append()` 把同一组参数应用到所有平台
/// （每个平台用自己的 tip 地址 / max_tx_size）；`send()` 并发发送所有平台的 bundle，
/// 收集全部成功签名，全部失败才返回错误。
pub struct MultiBundleSender {
    builders: Vec<BundleBuilder>,
}

/// [`MultiBundleSender::append`] 失败时携带所有保留的 builder（含失败平台的原状态），
/// 不丢已添加的交易，可用 [`MultiBundleError::into_builders`] + [`MultiBundleSender::from_builders`] 恢复。
pub struct MultiBundleError {
    pub msg: String,
    pub builders: Vec<BundleBuilder>,
}

impl MultiBundleError {
    pub fn into_builders(self) -> Vec<BundleBuilder> {
        self.builders
    }
}

impl MultiBundleSender {
    /// 用一组平台 bundle 发送者构造多平台发送器。
    ///
    /// 每个 sender 对应一个目标平台（如 jito / flash_block / astralane）。
    pub fn new(senders: Vec<Box<dyn BundleSender>>) -> Self {
        Self {
            builders: senders.into_iter().map(BundleBuilder::new).collect(),
        }
    }

    /// 从一组已构建的 builder 恢复（配合 [`MultiBundleError::into_builders`] 在 append
    /// 失败后继续追加交易）。
    pub fn from_builders(builders: Vec<BundleBuilder>) -> Self {
        Self { builders }
    }

    pub fn len(&self) -> usize {
        self.builders.len()
    }
    pub fn is_empty(&self) -> bool {
        self.builders.is_empty()
    }

    /// 给所有平台各添加一笔交易。参数与 `BundleBuilder::append` 完全一致。
    /// 链式调用：`multi.append(...)?.append(...)?.send().await`
    ///
    /// 任一平台 append 失败即整体返回 [`MultiBundleError`]（携带所有保留 builder，
    /// 便于恢复继续追加）。
    pub fn append(
        mut self,
        ixs: &[Instruction],
        signers: &[&Keypair],
        tip: &Option<u64>,
        nonce: &HashParam,
        cu: &(Option<u32>, Option<u64>),
        address_lookup_tables: &[AddressLookupTableAccount],
        memo: Option<Vec<&str>>,
    ) -> Result<Self, MultiBundleError> {
        let mut next = Vec::with_capacity(self.builders.len());
        for b in self.builders {
            match b.append(ixs, signers, tip, nonce, cu, address_lookup_tables, memo.clone()) {
                Ok(b) => next.push(b),
                Err(e) => {
                    let BundleError { msg, builder } = e;
                    next.push(builder);
                    return Err(MultiBundleError { msg, builders: next });
                }
            }
        }
        self.builders = next;
        Ok(self)
    }

    /// 并发发送所有平台的 bundle，并确认任意一笔上链。
    ///
    /// 完整流程：
    /// 1. 发送前先订阅 `tx_result_channel`，避免漏掉极速确认；
    /// 2. 所有平台并发发送（全部 spawn 出去，再统一 await）；**平台发送失败只打日志，
    ///    不参与结果**；
    /// 3. 整理每平台 bundle 的**第一笔**签名作为监听集合
    ///    （bundle 一荣俱荣一损俱损，确认第一笔 = 整个 bundle 已落地）；
    /// 4. 调用 `confirm_success_tx`：交易失败 / Meta 缺失只打日志继续等，
    ///    只等任一平台成功确认或超时；
    /// 5. 最终只有两种结局——成功返回 `(Signature, TransactionFormat)`，或失败表现为
    ///    超时 `TxConfirmError::Timeout`（bundle 原子性，不存在部分成功的失败路径）。
    pub async fn send(self, confirm_timeout_secs: u64) -> Result<(Signature, TransactionFormat), TxConfirmError> {
        // 1. 订阅必须在发送之前
        let rx = tx_result_channel::subscribe();

        // 2. 并发发送所有平台 bundle
        if self.builders.is_empty() {
            return Err(TxConfirmError::Timeout {
                expected_sigs: vec![],
                timeout_secs: confirm_timeout_secs,
            });
        }
        let mut handles = Vec::with_capacity(self.builders.len());
        for b in self.builders {
            handles.push(tokio::spawn(async move { b.send().await }));
        }

        // 3. 收集每平台第一笔签名；发送失败不关心，只打 log
        let mut first_sigs: HashSet<Signature> = HashSet::new();
        for h in handles {
            match h.await {
                Ok(Ok(sigs)) => {
                    if let Some(first) = sigs.first() {
                        first_sigs.insert(*first);
                    }
                }
                Ok(Err(e)) => log::error!("[MultiBundleSender] 平台 bundle 发送失败: {e}"),
                Err(e) => log::error!("[MultiBundleSender] task join error: {e}"),
            }
        }
        if first_sigs.is_empty() {
            // 没有任何平台成功发出签名 → 归为超时失败
            log::error!("[MultiBundleSender] 所有平台 bundle 发送失败，无签名可监听");
            return Err(TxConfirmError::Timeout {
                expected_sigs: vec![],
                timeout_secs: confirm_timeout_secs,
            });
        }

        // 4. 确认：忽略交易失败/Meta 缺失（打日志继续等），只等成功或超时
        confirm_success_tx(rx, first_sigs, confirm_timeout_secs).await
    }
}
