//! SendContext — 发送所需的账户 / hash 上下文，与 trade-solana-impl 的 TxSenderContext 平行。

use sol_tx_send::platform_clients::HashParam;
use solana_sdk::{message::AddressLookupTableAccount, pubkey::Pubkey, signature::Keypair, signer::Signer};
use std::sync::Arc;

/// 发送上下文：payer、hash（nonce / blockhash）、ALT。
#[derive(Clone)]
pub struct SendContext {
    pub payer: Arc<Keypair>,
    pub hash_param: HashParam,
    /// 地址查找表（无 ALT 时传 Arc::new(vec![])）
    pub alt: Arc<Vec<AddressLookupTableAccount>>,
}

impl SendContext {
    pub fn new(
        payer: Arc<Keypair>,
        hash_param: HashParam,
        alt: Arc<Vec<AddressLookupTableAccount>>,
    ) -> Self {
        Self { payer, hash_param, alt }
    }

    /// 使用 nonce 账户构建，自动查询最新 hash。
    pub async fn from_nonce(payer: Arc<Keypair>, nonce_account: Pubkey) -> Self {
        let hash = nonce_cache::get_nonce_hash(nonce_account).await;
        Self {
            hash_param: HashParam::NonceAccount {
                account: nonce_account,
                authority: payer.pubkey(),
                hash,
            },
            payer,
            alt: Arc::new(vec![]),
        }
    }

    /// 使用最新 blockhash 构建（适合时延不敏感场景）。
    pub async fn from_blockhash(
        payer: Arc<Keypair>,
        rpc: &solana_client::nonblocking::rpc_client::RpcClient,
    ) -> anyhow::Result<Self> {
        let hash = rpc.get_latest_blockhash().await?;
        Ok(Self {
            hash_param: HashParam::Blockhash(hash),
            payer,
            alt: Arc::new(vec![]),
        })
    }
}
