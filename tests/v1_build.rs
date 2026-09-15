//! V1 构建路径验证：确认 dispatcher 的 V1 接口能把 `V1TxConfig` 正确落进消息。
//!
//! 这些测试**不发送交易**，只覆盖构建阶段（build → 消息 → 签名），
//! 因为 `send_*` 系列需要真实网络与 nonce 账户。
//!
//! 重点验证两件在 V0→V1 迁移中最容易搞错的事：
//! 1. `priority_fee` 是 **lamports 总额**，不会被再除 `cu_limit`；
//! 2. V1 消息里 **没有 ComputeBudget 指令**（cu/fee 都在 config 里）。

use sol_tx_send::platform_clients::{BuildV1Tx, HashParam, V1TxConfig};
use solana_sdk::{
    hash::Hash,
    instruction::{AccountMeta, Instruction},
    message::VersionedMessage,
    pubkey::Pubkey,
    signature::Keypair,
};
use std::sync::Arc;

/// 取一个 tip 平台客户端（Jito 最简：需要 tip 转账）。
fn jito() -> sol_tx_send::platform_clients::jito::Jito {
    sol_tx_send::platform_clients::jito::Jito::new("test-uuid")
}

fn dummy_ix() -> Instruction {
    Instruction {
        program_id: Pubkey::new_unique(),
        accounts: vec![AccountMeta::new_readonly(Pubkey::new_unique(), false)],
        data: vec![1, 2, 3],
    }
}

#[test]
fn v1_message_carries_priority_fee_and_no_compute_budget_ix() {
    let client = jito();
    let payer = Arc::new(Keypair::new());
    let hash = HashParam::Blockhash(Hash::new_unique());
    let ixs = [dummy_ix()];

    let cfg = V1TxConfig {
        priority_fee: Some(12_345),
        compute_unit_limit: Some(200_000),
        loaded_accounts_data_size_limit: None,
        heap_size: None,
    };

    let env = client
        .build_v1_tx(&ixs, &payer, &Some(0), &hash, cfg, None)
        .expect("build_v1_tx failed");

    let msg = &env.inner_tx().message;
    // 必须是 V1 消息
    let VersionedMessage::V1(v1) = msg else {
        panic!("expected V1 message, got {:?}", msg);
    };

    // ① priority_fee 原样传入（**不**被除 cu_limit）
    assert_eq!(v1.config.priority_fee, Some(12_345), "priority_fee 必须原样传进 V1 config");
    assert_eq!(v1.config.compute_unit_limit, Some(200_000));

    // ② V1 消息里不能有 ComputeBudget 指令
    let compute_budget = const_accounts::COMPUTE_BUDGET_PROGRAM;
    let keys = &v1.account_keys;
    assert!(
        !v1.instructions.iter().any(|ix| *ix.program_id(keys) == compute_budget),
        "V1 消息里不应出现 ComputeBudget 指令（cu/fee 走 config）"
    );

    // ③ 只签了一次、签名数量 = 1
    assert_eq!(env.inner_tx().signatures.len(), 1);
}

#[test]
fn v1_none_priority_fee_stays_none() {
    let client = jito();
    let payer = Arc::new(Keypair::new());
    let hash = HashParam::Blockhash(Hash::new_unique());

    let cfg = V1TxConfig {
        priority_fee: None,
        compute_unit_limit: Some(50_000),
        ..Default::default()
    };

    let env = client
        .build_v1_tx(&[dummy_ix()], &payer, &Some(0), &hash, cfg, None)
        .expect("build_v1_tx failed");

    let VersionedMessage::V1(ref v1) = env.inner_tx().message else {
        panic!("expected V1 message");
    };
    assert_eq!(v1.config.priority_fee, None, "None 应保持 None");
    assert_eq!(v1.config.compute_unit_limit, Some(50_000));
}

#[test]
fn v1_tip_transfer_still_emitted_for_tip_platforms() {
    let client = jito();
    let payer = Arc::new(Keypair::new());
    let hash = HashParam::Blockhash(Hash::new_unique());

    // Jito uses_tip_transfer()=true → 应生成一笔 SOL 转账
    let env = client
        .build_v1_tx(&[dummy_ix()], &payer, &Some(10_000), &hash, V1TxConfig::default(), None)
        .expect("build_v1_tx failed");

    let VersionedMessage::V1(ref v1) = env.inner_tx().message else {
        panic!("expected V1 message");
    };
    let keys = &v1.account_keys;
    assert!(
        v1.instructions
            .iter()
            .any(|ix| *ix.program_id(keys) == const_accounts::SYSTEM_PROGRAM),
        "tip 平台应生成 SOL tip 转账指令"
    );
}

// ══════════════════════════════════════════════════════════════════════════════
// V1 bundle 构建器
//
// V1 只提高「账户数」和「字节数」，**没提高单笔能锁的账户上限**（依旧 64）。
// 所以 migrate + sell 这类指令条数多的场景，**还是得拆成多笔 bundle**，
// 只是每笔内部换 V1 编码。这几个测试覆盖 V1 bundle 的构建阶段。
// ══════════════════════════════════════════════════════════════════════════════

use sol_tx_send::platform_clients::{BundleBuilderV1, BundleSender};
use solana_sdk::{signature::Signature, transaction::VersionedTransaction};

/// 极简 BundleSender（只用于构建阶段测试，`send` 直接报错）。
struct DummySender {
    tip: Pubkey,
    max_size: usize,
}

#[async_trait::async_trait]
impl BundleSender for DummySender {
    async fn send_bundle(&self, _txs: &[VersionedTransaction]) -> Result<Vec<Signature>, String> {
        Err("dummy sender: 测试不发送".into())
    }
    fn tip_address(&self) -> Pubkey {
        self.tip
    }
    fn max_tx_size(&self) -> usize {
        self.max_size
    }
}

fn dummy_sender() -> Box<dyn BundleSender> {
    Box::new(DummySender {
        tip: Pubkey::new_unique(),
        max_size: 4096,
    })
}

/// V1 bundle：两笔都应是 V1 消息，且各自带上自己的 config。
#[test]
fn v1_bundle_builds_two_v1_messages_with_own_config() {
    let payer = Arc::new(Keypair::new());
    let hash = HashParam::Blockhash(Hash::new_unique());

    let tx1_ix = [dummy_ix()]; // 模拟 migrate
    let tx2_ix = [dummy_ix(), dummy_ix()]; // 模拟 sell + close_ata

    let builder = BundleBuilderV1::new(dummy_sender())
        .append(
            &tx1_ix,
            &[payer.as_ref()],
            &None,
            &hash,
            V1TxConfig {
                compute_unit_limit: Some(500_000),
                ..Default::default()
            },
            None,
        )
        .unwrap_or_else(|e| panic!("append tx1 失败: {}", e.msg))
        .append(
            &tx2_ix,
            &[payer.as_ref()],
            &Some(10_000),
            &hash,
            V1TxConfig {
                compute_unit_limit: Some(150_000),
                ..Default::default()
            },
            Some(vec!["memo"]),
        )
        .unwrap_or_else(|e| panic!("append tx2 失败: {}", e.msg));

    assert_eq!(builder.len(), 2, "bundle 应有 2 笔");
    assert!(!builder.is_full());

    // 每笔都必须是 V1 消息，且 config 各自独立
    let cus: Vec<Option<u32>> = builder
        .transactions()
        .iter()
        .map(|t| match &t.message {
            VersionedMessage::V1(v1) => v1.config.compute_unit_limit,
            other => panic!("bundle 里出现了非 V1 交易: {other:?}"),
        })
        .collect();
    assert_eq!(cus, vec![Some(500_000), Some(150_000)], "两笔应各带自己的 cu_limit");
}

/// V1 bundle 里**不能**出现 ComputeBudget 指令（cu/fee 都在 config 里）。
#[test]
fn v1_bundle_has_no_compute_budget_ix() {
    let payer = Arc::new(Keypair::new());
    let hash = HashParam::Blockhash(Hash::new_unique());

    let builder = BundleBuilderV1::new(dummy_sender())
        .append(&[dummy_ix()], &[payer.as_ref()], &None, &hash, V1TxConfig::default(), None)
        .unwrap_or_else(|e| panic!("append 失败: {}", e.msg));

    let txs = builder.transactions();
    let compute_budget = const_accounts::COMPUTE_BUDGET_PROGRAM;
    let VersionedMessage::V1(v1) = &txs[0].message else {
        panic!("expected V1");
    };
    let keys = &v1.account_keys;
    assert!(
        !v1.instructions.iter().any(|ix| *ix.program_id(keys) == compute_budget),
        "V1 bundle 里不应出现 ComputeBudget 指令"
    );
}

/// bundle 上限（5 笔）是**协议限制**，V1 并没有放宽。
#[test]
fn v1_bundle_respects_max_txs_limit() {
    let payer = Arc::new(Keypair::new());
    let hash = HashParam::Blockhash(Hash::new_unique());

    let mut b = BundleBuilderV1::new(dummy_sender());
    for _ in 0..5 {
        b = b
            .append(&[dummy_ix()], &[payer.as_ref()], &None, &hash, V1TxConfig::default(), None)
            .unwrap_or_else(|e| panic!("append within limit 失败: {}", e.msg));
    }
    assert!(b.is_full(), "5 笔后应满");

    let err = b
        .append(&[dummy_ix()], &[payer.as_ref()], &None, &hash, V1TxConfig::default(), None)
        .err().expect("第 6 笔应被拒");
    assert!(err.msg.contains("bundle full"), "错误信息应说明 bundle 已满: {}", err.msg);
}
