//! sol-tx-dispacher
//!
//! 根据 `SlotOracle` 提供的 leader 信息，自适应地选择发送策略：
//!
//! - `Harmonic` leader → Harmonic 直发 + Astralane / Temporal 按 90% tip
//! - 已知非 Harmonic    → 0 tip 0 gas（省费用，不给不认识的服务商涨小费）
//! - 未知 / NoopOracle  → fallback：所有平台并发双轮（等价原 send_fast）
//!
//! # 使用方式
//!
//! ```rust,ignore
//! let dispacher = TxDispacher::builder()
//!     .oracle(oracle)
//!     .astralane(Astralane::init_with(key, region))
//!     .temporal(Temporal::init_with(key, region))
//!     .harmonic(HarmonicBlockEngine::init_with(Some(uuid), region))
//!     .build();
//!
//! let ctx = SendContext::from_nonce(payer, nonce_account).await;
//! let sig = dispacher.send(&ixs, &ctx, current_slot, (Some(200_000), Some(50_000)), 60).await?;
//! ```

mod builder;
mod context;
mod fire;
mod strategy;

pub use builder::TxDispacherBuilder;
pub use context::SendContext;

use sol_slot_leader::SlotOracle;
use nonce_cache::TxConfirmError;
use std::sync::Arc;

// ── TipStrategy ───────────────────────────────────────────────────────────────

/// Tip 计算策略（与 trade-solana-impl send_utils 保持相同语义）。
#[derive(Debug, Clone, Copy)]
pub enum TipStrategy {
    /// 绝对数额（lamports）
    Absolute(u64),
    /// 相对于各平台最低 tip 的比例（例如 0.9 = 90%，1.1 = 110%）
    Ratio(f64),
}

impl TipStrategy {
    /// 根据平台最低 tip 计算实际 tip 数额。
    pub fn compute(&self, platform_min: u64) -> u64 {
        match self {
            TipStrategy::Absolute(amt) => *amt,
            TipStrategy::Ratio(r) => (platform_min as f64 * r) as u64,
        }
    }

    /// 在当前策略基础上再乘以缩放系数。
    ///
    /// `Absolute(n).scaled(f)` = `Absolute(n × f)`
    /// `Ratio(r).scaled(f)`    = `Ratio(r × f)`
    pub fn scaled(self, factor: f64) -> Self {
        match self {
            TipStrategy::Absolute(n) => TipStrategy::Absolute((n as f64 * factor) as u64),
            TipStrategy::Ratio(r) => TipStrategy::Ratio(r * factor),
        }
    }
}

// ── feature-gated 平台客户端导入 ──────────────────────────────────────────────

#[cfg(feature = "astralane")]
use sol_tx_send::platform_clients::astralane::Astralane;
#[cfg(feature = "astralane_quic")]
use sol_tx_send::platform_clients::astralane_quic::client::AstralaneQuic;
#[cfg(feature = "blockrazor")]
use sol_tx_send::platform_clients::blockrazor::Blockrazor;
#[cfg(feature = "everstake")]
use sol_tx_send::platform_clients::ever_stake::EverStake;
#[cfg(feature = "everstake_quic")]
use sol_tx_send::platform_clients::ever_stake_quic::EverStakeQuic;
#[cfg(feature = "flash_block")]
use sol_tx_send::platform_clients::flash_block::FlashBlock;
#[cfg(feature = "harmonic")]
use sol_tx_send::platform_clients::harmonic::HarmonicBlockEngine;
#[cfg(feature = "helius")]
use sol_tx_send::platform_clients::helius::Helius;
#[cfg(feature = "jito")]
use sol_tx_send::platform_clients::jito::Jito;
#[cfg(feature = "nextblock")]
use sol_tx_send::platform_clients::nextblock::NextBlock;
#[cfg(feature = "nodeone")]
use sol_tx_send::platform_clients::nodeone::NodeOne;
#[cfg(feature = "stellium")]
use sol_tx_send::platform_clients::stellium::Stellium;
#[cfg(feature = "temporal")]
use sol_tx_send::platform_clients::temporal::Temporal;
#[cfg(feature = "zeroslot")]
use sol_tx_send::platform_clients::zeroslot::ZeroSlot;

// ── 发送路由决策 ──────────────────────────────────────────────────────────────

/// 根据 oracle 查询结果得出的路由决策。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendRoute {
    /// Harmonic 系节点出块：走 Harmonic 直发 + Astralane/Temporal 90% tip
    Harmonic,
    /// Jito 节点出块：只发带 tip 的版本，跳过纯 cu_price 的交易
    Jito,
    /// 其他所有节点（含 DB 无记录 / NoopOracle）：退化到 send_fast
    Fallback,
}

// ── TxDispacher ───────────────────────────────────────────────────────────────

/// slot-aware 交易分发器。
///
/// 通过 [`TxDispacherBuilder`] 构造，各平台客户端按 feature 开关。
pub struct TxDispacher<O: SlotOracle> {
    oracle: O,

    #[cfg(feature = "astralane")]
    pub(crate) astralane: Option<Arc<Astralane>>,
    #[cfg(feature = "astralane_quic")]
    pub(crate) astralane_quic: Option<Arc<AstralaneQuic>>,
    #[cfg(feature = "everstake")]
    pub(crate) everstake: Option<Arc<EverStake>>,
    #[cfg(feature = "everstake_quic")]
    pub(crate) everstake_quic: Option<Arc<EverStakeQuic>>,
    #[cfg(feature = "flash_block")]
    pub(crate) flash_block: Option<Arc<FlashBlock>>,
    #[cfg(feature = "nodeone")]
    pub(crate) nodeone: Option<Arc<NodeOne>>,
    #[cfg(feature = "blockrazor")]
    pub(crate) blockrazor: Option<Arc<Blockrazor>>,
    #[cfg(feature = "temporal")]
    pub(crate) temporal: Option<Arc<Temporal>>,
    #[cfg(feature = "helius")]
    pub(crate) helius: Option<Arc<Helius>>,
    #[cfg(feature = "zeroslot")]
    pub(crate) zeroslot: Option<Arc<ZeroSlot>>,
    #[cfg(feature = "nextblock")]
    pub(crate) nextblock: Option<Arc<NextBlock>>,
    #[cfg(feature = "stellium")]
    pub(crate) stellium: Option<Arc<Stellium>>,
    #[cfg(feature = "jito")]
    pub(crate) jito: Option<Arc<Jito>>,
    #[cfg(feature = "harmonic")]
    pub(crate) harmonic: Option<Arc<HarmonicBlockEngine>>,
}

/// 将 anyhow::Error 转为 TxConfirmError（downcast 根因，downcast 不出当 Other）
fn into_tx_confirm_err(e: anyhow::Error) -> TxConfirmError {
    e.downcast::<TxConfirmError>()
        .unwrap_or_else(|e| TxConfirmError::Other(format!("{:#}", e)))
}

impl<O: SlotOracle> TxDispacher<O> {
    /// 返回 builder。
    pub fn builder(oracle: O) -> TxDispacherBuilder<O> {
        TxDispacherBuilder::new(oracle)
    }

    /// 查询当前 slot 的路由决策（不发送）。
    /// 查询 `target_slot` 的 leader 类型并返回路由决策。
    /// 调用方自行决定传当前 slot 还是 current_slot + N。
    pub fn resolve_route(&self, target_slot: u64) -> SendRoute {
        match self.oracle.leader_at(target_slot) {
            Some(info) if info.is_harmonic() => SendRoute::Harmonic,
            Some(info) if info.is_jito() => SendRoute::Jito,
            _ => SendRoute::Fallback,
        }
    }

    /// 主发送入口。
    ///
    /// - `tip_strategy` 为 `None` 时各策略使用内置默认：
    /// - `target_slot`：预期交易落地的 slot。调用方自行决定偏移量：
    ///   - 信号与交易同 slot（最常见）→ 直接传 `signal_slot`
    ///   - 信号在 slot 末尾、交易可能落下一个 slot → 传 `signal_slot + 1`
    /// - `tip_strategy` 为 `None` 时各策略使用内置默认：
    ///   - `Harmonic` leader：Astralane / Temporal 按 90% tip，Harmonic 路径不加 tip
    ///   - `Fallback`：各平台按自身最低 tip
    /// - 显式传入 `tip_strategy` 会覆盖默认值。
    pub async fn send(
        &self,
        ixs: &[solana_sdk::instruction::Instruction],
        ctx: &SendContext,
        target_slot: u64,
        tip_strategy: Option<TipStrategy>,
        cu: (Option<u32>, Option<u64>),
        confirm_timeout_secs: u64,
    ) -> Result<(solana_sdk::signature::Signature, grpc_client::TransactionFormat), TxConfirmError> {
        let route = self.resolve_route(target_slot);
        log::info!("[TxDispacher] slot={} route={:?}", target_slot, route);
        strategy::dispatch(
            self,
            ixs,
            ctx,
            route,
            tip_strategy,
            cu,
            confirm_timeout_secs,
        )
        .await
        .map_err(into_tx_confirm_err)
    }

    /// 低成本发送——不走 oracle 路由，只发少数平台单轮。
    ///
    /// 适合卖出等不极限抢速的场景，屾岜山全量平台广播带来的额外费用。
    /// - 或者直接不带 tip/cu_price （传 `None`）走平台默认最低费。
    pub async fn send_cheap(
        &self,
        ixs: &[solana_sdk::instruction::Instruction],
        ctx: &SendContext,
        tip_strategy: Option<TipStrategy>,
        cu: (Option<u32>, Option<u64>),
        confirm_timeout_secs: u64,
    ) -> Result<(solana_sdk::signature::Signature, grpc_client::TransactionFormat), TxConfirmError> {
        strategy::dispatch_cheap(self, ixs, ctx, tip_strategy, cu, confirm_timeout_secs).await
            .map_err(into_tx_confirm_err)
    }

    /// 纯 tip 竞价发送——不参与 cu_price 竞争，只靠 SOL tip 抢排序。
    ///
    /// tip 至少为 `min_tip_floor`（lamports），平台 min_tip 也取其大者。
    /// 路由逻辑与 `send()` 一致：Harmonic 出块走 Harmonic，Jito 出块只发 tip，
    /// 其余全平台一发。适合高价值 snipe：tip 给够，不浪费 cu_price 费用。
    pub async fn send_tip_only(
        &self,
        ixs: &[solana_sdk::instruction::Instruction],
        ctx: &SendContext,
        target_slot: u64,
        min_tip_floor: u64,
        cu_limit: u32,
        confirm_timeout_secs: u64,
    ) -> Result<(solana_sdk::signature::Signature, grpc_client::TransactionFormat), TxConfirmError> {
        let route = self.resolve_route(target_slot);
        log::info!(
            "[TxDispacher::send_tip_only] slot={} route={:?} tip_floor={}",
            target_slot,
            route,
            min_tip_floor
        );
        strategy::dispatch_tip_only(
            self,
            ixs,
            ctx,
            route,
            min_tip_floor,
            cu_limit,
            confirm_timeout_secs,
        )
        .await
        .map_err(into_tx_confirm_err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sol_slot_leader::{ClientType, LeaderInfo, NoopOracle, SlotOracle};
    use sol_tx_send::platform_clients::{HashParam, Region};
    use solana_sdk::{hash::Hash, signature::Keypair};
    use std::sync::Arc;

    // ── 测试用 MockOracle ─────────────────────────────────────────────────────

    /// 可以指定 is_harmonic 返回值的 mock oracle。
    struct MockOracle {
        harmonic: bool,
        name: Option<&'static str>,
    }

    impl SlotOracle for MockOracle {
        fn leader_at(&self, _slot: u64) -> Option<LeaderInfo> {
            Some(LeaderInfo {
                client_type: if self.harmonic {
                    ClientType::HarmonicAgave
                } else {
                    ClientType::Agave
                },
                name: self.name.map(str::to_string),
            })
        }
    }

    // ── resolve_route 单元测试 ────────────────────────────────────────────────

    #[test]
    fn noop_oracle_always_fallback() {
        let d = TxDispacher::builder(Arc::new(NoopOracle)).build();
        assert_eq!(d.resolve_route(422_000_000), SendRoute::Fallback);
    }

    #[test]
    fn harmonic_client_type_routes_to_harmonic() {
        let d = TxDispacher::builder(Arc::new(MockOracle {
            harmonic: true,
            name: None,
        }))
        .build();
        assert_eq!(d.resolve_route(100), SendRoute::Harmonic);
    }

    #[test]
    fn non_harmonic_client_type_routes_to_fallback() {
        let d = TxDispacher::builder(Arc::new(MockOracle {
            harmonic: false,
            name: None,
        }))
        .build();
        assert_eq!(d.resolve_route(100), SendRoute::Fallback);
    }

    #[test]
    fn harmonic_in_name_routes_to_harmonic_even_if_type_is_other() {
        // client_type 是 Agave（Other），但 name 里有 harmonic 字样
        let oracle = MockOracle {
            harmonic: false,           // client_type = Agave
            name: Some("Harmonic-SG"), // name 含 harmonic
        };
        let d = TxDispacher::builder(oracle).build();
        assert_eq!(d.resolve_route(100), SendRoute::Harmonic);
    }

    // ── TipStrategy 单元测试 ──────────────────────────────────────────────────

    #[test]
    fn tip_strategy_ratio() {
        let min = 1_000_000u64;
        assert_eq!(TipStrategy::Ratio(0.9).compute(min), 900_000);
        assert_eq!(TipStrategy::Ratio(1.0).compute(min), 1_000_000);
        assert_eq!(TipStrategy::Ratio(1.1).compute(min), 1_100_000);
    }

    #[test]
    fn tip_strategy_absolute() {
        assert_eq!(TipStrategy::Absolute(500_000).compute(1_000_000), 500_000);
        assert_eq!(TipStrategy::Absolute(0).compute(1_000_000), 0);
    }

    // ── 调用格式展示（不实际发送，只演示 API 形状）─────────────────────────

    /// 展示完整调用链，直接看这个函数就能理解怎么用。
    /// 标记 `#[allow(dead_code)]` 使其不触发警告但仍参与类型检查。
    #[allow(dead_code)]
    async fn _full_usage_example() {
        // ── 1. 构造 Oracle ──────────────────────────────────────────────────
        // 有 DB：
        //   let oracle = (
        //       sol_slot_leader::SlotLeaderCache::new(
        //           sol_slot_leader::DbConfig::new("mysql://..."),
        //           "https://rpc.example.com",
        //       ).await.unwrap()
        //   );
        //   oracle.spawn_refresh_task();
        //
        // 无 DB（旧项目 fallback，行为等价原 send_fast）：
        let oracle = NoopOracle;

        // ── 2. 构造 Dispacher，链式注入各平台 ──────────────────────────────
        let dispacher = TxDispacher::builder(oracle)
            // feature = "astralane"
            .astralane(
                sol_tx_send::platform_clients::astralane::Astralane::init_with(
                    "ASTRALANE_API_KEY",
                    Region::Amsterdam,
                ),
            )
            // feature = "temporal"
            .temporal(
                sol_tx_send::platform_clients::temporal::Temporal::init_with(
                    "TEMPORAL_KEY",
                    Region::Amsterdam,
                ),
            )
            // feature = "harmonic"（等文档确认协议后传真实 UUID）
            // .harmonic(HarmonicBlockEngine::init_with(Some("UUID"), Region::Amsterdam))
            .build();

        // ── 3. 构造发送上下文 ───────────────────────────────────────────────
        let payer = Arc::new(Keypair::new());
        let ctx = SendContext::new(
            payer.clone(),
            HashParam::Blockhash(Hash::default()), // 实际用 rpc.get_latest_blockhash().await
            Arc::new(vec![]),                      // 无 ALT
        );
        // 或者用 nonce：
        // let ctx = SendContext::from_nonce(payer, nonce_pubkey).await;

        // ── 4. 发送 ────────────────────────────────────────────────────────
        let ixs = vec![]; // 填入实际指令
        let current_slot = 422_231_110u64;

        let _sig = dispacher
            .send(
                &ixs,
                &ctx,
                current_slot,
                None, // tip_strategy: None → 各模式用内置默认
                // Some(TipStrategy::Ratio(1.2)), // 或显式指定倍率
                // Some(TipStrategy::Absolute(500_000)), // 或显式指定绝对值（lamports）
                (Some(200_000), Some(50_000)), // (cu_limit, cu_price_micro_lamports)
                60,                            // confirm_timeout_secs
            )
            .await;
    }
}
