//! 多平台 bundle 发送器。
//!
//! 将同一组原子 bundle（多笔交易）并发代理到多个平台，并在发送后通过
//! `nonce_cache` 的 confirm 系列方法确认任意一笔上链。`append` 参数与
//! `sol_tx_send::platform_clients::BundleBuilder` 完全对齐，`send()` 内部完成
//! 订阅 → 并发发送 → 整理各平台全部签名 → 确认的完整流程。
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
use sol_tx_send::platform_clients::{BundleBuilder, BundleBuilderV1, BundleError, BundleSender, HashParam, V1TxConfig};
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
    /// 3. 整理每平台 bundle 的**全部**签名作为监听集合
    ///    （任一平台任一笔确认到 = 整个 bundle 已落地）；
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

        // 3. 收集每平台 bundle 的【全部】签名；发送失败不关心，只打 log。
        //    只盯首笔有风险：若首笔结果没被推送（网络/订阅原因），即使整个 bundle 已落地也会误报超时。
        //    监听全部笔后，任一平台任一笔确认到即视为成功。
        let mut watch_sigs: HashSet<Signature> = HashSet::new();
        for h in handles {
            match h.await {
                Ok(Ok(sigs)) => {
                    watch_sigs.extend(sigs);
                }
                Ok(Err(e)) => log::error!("[MultiBundleSender] 平台 bundle 发送失败: {e}"),
                Err(e) => log::error!("[MultiBundleSender] task join error: {e}"),
            }
        }
        if watch_sigs.is_empty() {
            // 没有任何平台成功发出签名 → 归为超时失败
            log::error!("[MultiBundleSender] 所有平台 bundle 发送失败，无签名可监听");
            return Err(TxConfirmError::Timeout {
                expected_sigs: vec![],
                timeout_secs: confirm_timeout_secs,
            });
        }

        // 4. 确认：忽略交易失败/Meta 缺失（打日志继续等），只等成功或超时
        confirm_success_tx(rx, watch_sigs, confirm_timeout_secs).await
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// V1 版多平台 bundle 发送器
//
// # 什么时候用（V1 并没有让 bundle 失去价值）
//
// V1 只提高**账户数**（64 inline）与**字节数**（4096），**没有**提高单笔的
// 指令/账户上限 —— 一笔交易依旧最多锁 64 个账户。所以「migrate + sell」这种
// 指令条数多、账户去重后仍超 64 的场景，还是必须拆成多笔 bundle，
// 只是每笔内部改用 V1 编码。
//
// # 与 [`MultiBundleSender`]（V0 版）的差异
//
// | | V0 版 | 本类型 |
// |---|---|---|
// | `append` 计费参数 | `cu: &(Option<u32>, Option<u64>)` | `config: V1TxConfig` |
// | `append` 的 ALT | `address_lookup_tables: &[…]` | **无**（V1 不支持查找表） |
//
// `send()` / 确认流程与 V0 版**完全一致**（传输层与确认逻辑不区分交易版本）。
// ══════════════════════════════════════════════════════════════════════════════

/// V1 版多平台 bundle 发送器（与 [`MultiBundleSender`] 一一对应）。
pub struct MultiBundleSenderV1 {
    builders: Vec<BundleBuilderV1>,
}

/// [`MultiBundleSenderV1::append`] 失败时携带所有保留的 builder。
pub struct MultiBundleErrorV1 {
    pub msg: String,
    pub builders: Vec<BundleBuilderV1>,
}

impl MultiBundleErrorV1 {
    pub fn into_builders(self) -> Vec<BundleBuilderV1> {
        self.builders
    }
}

impl MultiBundleSenderV1 {
    /// 用一组平台 bundle 发送者构造。
    pub fn new(senders: Vec<Box<dyn BundleSender>>) -> Self {
        Self {
            builders: senders.into_iter().map(BundleBuilderV1::new).collect(),
        }
    }

    /// 从一组已构建的 builder 恢复（配合 [`MultiBundleErrorV1::into_builders`]）。
    pub fn from_builders(builders: Vec<BundleBuilderV1>) -> Self {
        Self { builders }
    }

    pub fn len(&self) -> usize {
        self.builders.len()
    }
    pub fn is_empty(&self) -> bool {
        self.builders.is_empty()
    }

    /// 给所有平台各添加一笔 **V1** 交易。参数与 [`BundleBuilderV1::append`] 一致。
    ///
    /// 链式调用：`multi.append(...)?.append(...)?.send(60).await`
    pub fn append(
        mut self,
        ixs: &[Instruction],
        signers: &[&Keypair],
        tip: &Option<u64>,
        nonce: &HashParam,
        config: V1TxConfig,
        memo: Option<Vec<&str>>,
    ) -> Result<Self, MultiBundleErrorV1> {
        let mut next = Vec::with_capacity(self.builders.len());
        for b in self.builders {
            match b.append(ixs, signers, tip, nonce, config, memo.clone()) {
                Ok(b) => next.push(b),
                Err(e) => {
                    let BundleError { msg, builder } = e;
                    next.push(builder);
                    return Err(MultiBundleErrorV1 { msg, builders: next });
                }
            }
        }
        self.builders = next;
        Ok(self)
    }

    /// 并发发送所有平台的 bundle，并确认任意一笔上链。
    ///
    /// 流程与 [`MultiBundleSender::send`] 逐字相同（传输层不区分交易版本）：
    /// 订阅 → 并发发送 → 收集全部签名 → 等任一成功或超时。
    pub async fn send(self, confirm_timeout_secs: u64) -> Result<(Signature, TransactionFormat), TxConfirmError> {
        // 1. 订阅必须在发送之前
        let rx = tx_result_channel::subscribe();

        if self.builders.is_empty() {
            return Err(TxConfirmError::Timeout {
                expected_sigs: vec![],
                timeout_secs: confirm_timeout_secs,
            });
        }

        // 2. 并发发送所有平台 bundle
        let mut handles = Vec::with_capacity(self.builders.len());
        for b in self.builders {
            handles.push(tokio::spawn(async move { b.send().await }));
        }

        // 3. 收集每平台 bundle 的【全部】签名；任一平台任一笔确认到即视为成功
        let mut watch_sigs: HashSet<Signature> = HashSet::new();
        for h in handles {
            match h.await {
                Ok(Ok(sigs)) => {
                    watch_sigs.extend(sigs);
                }
                Ok(Err(e)) => log::error!("[MultiBundleSenderV1] 平台 bundle 发送失败: {e}"),
                Err(e) => log::error!("[MultiBundleSenderV1] task join error: {e}"),
            }
        }
        if watch_sigs.is_empty() {
            log::error!("[MultiBundleSenderV1] 所有平台 bundle 发送失败，无签名可监听");
            return Err(TxConfirmError::Timeout {
                expected_sigs: vec![],
                timeout_secs: confirm_timeout_secs,
            });
        }

        // 4. 确认：忽略交易失败/Meta 缺失（打日志继续等），只等成功或超时
        confirm_success_tx(rx, watch_sigs, confirm_timeout_secs).await
    }
}
