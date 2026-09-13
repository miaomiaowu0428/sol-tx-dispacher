//! SendContext — 发送所需的账户 / hash 上下文。
//!
//! ⚠️ **这是统一的发送上下文**：新代码一律用 [`SendContext`]（配 [`crate::TxDispacher`]），
//! 不要再新写各 crate 私有的发送上下文类型。
//! （老代码里的 `trade-solana-impl::send_utils::TxSenderContext` 是同一套字段的早期版本，
//!   已存在的调用点保留不动，新写的直接用这里。）

use sol_tx_send::platform_clients::HashParam;
use solana_sdk::{message::AddressLookupTableAccount, pubkey::Pubkey, signature::Keypair, signer::Signer};
use std::sync::Arc;

/// 发送上下文：payer、hash（nonce / blockhash）、ALT。
#[derive(Clone)]
pub struct SendContext {
    pub payer: Arc<Keypair>,
    pub hash_param: HashParam,
    /// 地址查找表（无 ALT 时传 `Arc::new(vec![])`）
    pub alt: Arc<Vec<AddressLookupTableAccount>>,
}

impl SendContext {
    pub fn new(payer: Arc<Keypair>, hash_param: HashParam, alt: Arc<Vec<AddressLookupTableAccount>>) -> Self {
        Self { payer, hash_param, alt }
    }

    /// 使用 nonce 账户构建，自动查询最新 hash（**不带 ALT**）。
    pub async fn from_nonce(payer: Arc<Keypair>, nonce_account: Pubkey) -> Self {
        Self::from_nonce_with_alts(payer, nonce_account, Vec::new()).await
    }

    /// 使用 nonce 账户构建，自动查询最新 hash，并带上**已加载好的** ALT。
    ///
    /// ALT 账户由调用方自己加载 —— 各进程的 ALT 来源不同（环境变量 / DB / 链上拉取），
    /// 本 crate 不该知道这些。这里只负责按 `key` **去重**后装进上下文。
    pub async fn from_nonce_with_alts(
        payer: Arc<Keypair>,
        nonce_account: Pubkey,
        alts: impl IntoIterator<Item = AddressLookupTableAccount>,
    ) -> Self {
        let hash = nonce_cache::get_nonce_hash(nonce_account).await;
        let (alt, _) = merge_alts(&[], alts);
        Self {
            hash_param: HashParam::NonceAccount {
                account: nonce_account,
                authority: payer.pubkey(),
                hash,
            },
            payer,
            alt: Arc::new(alt),
        }
    }

    /// 使用最新 blockhash 构建（适合时延不敏感场景，**不带 ALT**）。
    pub async fn from_blockhash(
        payer: Arc<Keypair>,
        rpc: &solana_client::nonblocking::rpc_client::RpcClient,
    ) -> anyhow::Result<Self> {
        Self::from_blockhash_with_alts(payer, rpc, Vec::new()).await
    }

    /// 使用最新 blockhash 构建，并带上**已加载好的** ALT。
    pub async fn from_blockhash_with_alts(
        payer: Arc<Keypair>,
        rpc: &solana_client::nonblocking::rpc_client::RpcClient,
        alts: impl IntoIterator<Item = AddressLookupTableAccount>,
    ) -> anyhow::Result<Self> {
        let hash = rpc.get_latest_blockhash().await?;
        let (alt, _) = merge_alts(&[], alts);
        Ok(Self {
            hash_param: HashParam::Blockhash(hash),
            payer,
            alt: Arc::new(alt),
        })
    }

    /// 追加一批 ALT，返回**实际新增**的数量。
    ///
    /// 典型用法：`dex-router` 的 `RouteResponse.alts` 是「这一跳归哪些 ALT 管」，
    /// 拿回来直接 `ctx.add_alt(route.alts.iter().cloned())` 就能把它的账户 key
    /// 压进查找表 —— V0 有 1232 字节上限，两跳合一的交易光 key 就 ~1.4KB。
    ///
    /// 按 [`AddressLookupTableAccount::key`] 去重：已存在的（含构造时装的）
    /// 和**本批内重复**的都跳过；顺序保持「已有的在前、新加的在后」。
    pub fn add_alt(&mut self, alts: impl IntoIterator<Item = AddressLookupTableAccount>) -> usize {
        let (merged, added) = merge_alts(&self.alt, alts);
        if added > 0 {
            self.alt = Arc::new(merged);
        }
        added
    }

    /// [`Self::add_alt`] 的链式版本。
    pub fn with_alts(mut self, alts: impl IntoIterator<Item = AddressLookupTableAccount>) -> Self {
        self.add_alt(alts);
        self
    }
}

/// 合并两组 ALT：按 `key` 去重（已有优先，顺序 = 已有在前、新增在后）。
///
/// 返回 `(合并结果, 新增数)`。纯函数，方便测试。
pub fn merge_alts(
    existing: &[AddressLookupTableAccount],
    new: impl IntoIterator<Item = AddressLookupTableAccount>,
) -> (Vec<AddressLookupTableAccount>, usize) {
    let mut seen: ahash::AHashSet<Pubkey> = existing.iter().map(|a| a.key).collect();
    let mut merged = existing.to_vec();
    let mut added = 0usize;
    for a in new {
        if !seen.insert(a.key) {
            continue; // 已存在（含本批内重复）
        }
        merged.push(a);
        added += 1;
    }
    (merged, added)
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::hash::Hash;

    fn mk_alt(n: usize) -> AddressLookupTableAccount {
        AddressLookupTableAccount {
            key: Pubkey::new_unique(),
            addresses: (0..n).map(|_| Pubkey::new_unique()).collect(),
        }
    }

    #[test]
    fn merge_alts_dedups_by_key_and_keeps_order() {
        let a = mk_alt(2);
        let b = mk_alt(3);
        let (merged, added) = merge_alts(&[a.clone()], [a.clone(), b.clone()]);
        assert_eq!(added, 1, "已存在的重复项不该计入新增");
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].key, a.key, "已有的必须排在新加的之前");
        assert_eq!(merged[1].key, b.key);
    }

    #[test]
    fn merge_alts_skips_duplicates_inside_the_batch() {
        let a = mk_alt(1);
        let (merged, added) = merge_alts(&[], [a.clone(), a.clone(), a.clone()]);
        assert_eq!(added, 1);
        assert_eq!(merged.len(), 1);
    }

    #[test]
    fn add_alt_reports_added_and_ignores_dupes() {
        let mut ctx = SendContext::new(
            Arc::new(Keypair::new()),
            HashParam::Blockhash(Hash::new_unique()),
            Arc::new(vec![mk_alt(1)]),
        );
        let dup = ctx.alt[0].clone();
        let fresh = mk_alt(2);
        assert_eq!(ctx.add_alt([dup, fresh.clone()]), 1, "只有 fresh 是新增");
        assert_eq!(ctx.alt.len(), 2);
        assert_eq!(ctx.add_alt([fresh]), 0, "再来一次全是重复");
        assert_eq!(ctx.alt.len(), 2);
    }

    #[test]
    fn with_alts_is_chainable() {
        let a = mk_alt(1);
        let ctx = SendContext::new(
            Arc::new(Keypair::new()),
            HashParam::Blockhash(Hash::new_unique()),
            Arc::new(vec![]),
        )
        .with_alts([a.clone(), a.clone()]);
        assert_eq!(ctx.alt.len(), 1, "批内重复也要去重");
        assert_eq!(ctx.alt[0].key, a.key);
    }
}
