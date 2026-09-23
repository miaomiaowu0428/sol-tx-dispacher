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
mod bundle;
mod context;
mod fifo;
mod fire;
mod strategy;

pub use builder::TxDispacherBuilder;
pub use bundle::{MultiBundleError, MultiBundleErrorV1, MultiBundleSender, MultiBundleSenderV1};
pub use context::{SendContext, merge_alts};

use fifo::FIFO_LEADERS;
use nonce_cache::TxConfirmError;
use sol_slot_leader::SlotOracle;
use sol_tx_send::platform_clients::{BundleSender, V1TxConfig};
use std::sync::Arc;

/// 走 tip-only 模式的 leader vote account（只靠 tip 竞价，不参与 cu_price 竞争）
const TIP_ONLY_LEADERS: &[solana_sdk::pubkey::Pubkey] = &[
    solana_sdk::pubkey!("HEL1USMZKAL2odpNBj2oCjffnFGaYwmbGmyewGv1e2TU"),
    solana_sdk::pubkey!("E1r4Psq84tHfQ6aPTvvDka4U3u8zPVD7gEUrH25RdxHL"),
    solana_sdk::pubkey!("Fd7btgySsrjuo25CJCj7oE7VPMyezDhnx7pZkj2v69Nk"),
    solana_sdk::pubkey!("5pPRHniefFjkiaArbGX3Y8NUysJmQ9tMZg3FrFGwHzSm"),
    solana_sdk::pubkey!("ACvL73V4GNnxPVfZ7K89jCrYurLyzpEuE9qirjvh2Xmi"),
    solana_sdk::pubkey!("8tjFeSApQ85ThoQXT28acfF2KUfQr3TvTdirSkzNnYC7"),
    solana_sdk::pubkey!("HH5dA42XF1HxNk1TRpG6LuKfLViMYNdAz5iWrFM4hWFi"),
    solana_sdk::pubkey!("Gv9gguvrAkgQtB5g5a3Un7trcHCxLYsk8vSojLmQMsWV"),
    solana_sdk::pubkey!("H8fHToVcZPi5bupGZohGPX2SWs8NHzgFKQ31wi5n6oux"),
    solana_sdk::pubkey!("ChorusmmK7i1AxXeiTtQgQZhQNiXYU84ULeaYF1EH15n"),
];

/// 走 FIFO（先到先得、不参与 tip/cu_price 竞价）的 (leader vote account, client_type_id) 列表。
/// 定义见 `fifo.rs`（硬编码自 config/FIFO-Leader.json）。命中时发送强制 tip=None、cu_price=None。
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

// ── CostConfig ────────────────────────────────────────────────────────────────

/// 单预算竞价配置：把「tip 高度 + cu_price」二维参数合并成单个 cost 预算，
/// 由内部根据平台性质决定走 tip 通道还是 cu_price 通道（或同一平台双发两笔）。
///
/// 语义：`cost_amount` 是这笔交易愿意付出的**单笔竞价总预算**（lamports）。
/// 因为 ctx 送 nonce，同平台双发/全平台广播只会有一笔成功上链、只付一次费，
/// 所以带 price 那笔和纯 tip 那笔各自都能尽力花到 `cost_amount`，无需拆预算。
///
/// 内部通道推导：
/// - **cu_price 通道**（带 price 那笔 / 只收 gas 的平台）：
///   `cu_price = cost_amount × 1e6 / cu_limit`，tip 用 `tip_rate` 保底（比平台默认高一点点）。
/// - **tip 通道**（纯 tip 那笔 / jito 等）：tip = `Absolute(cost_amount)` 全额。
#[derive(Debug, Clone, Copy)]
pub struct CostConfig {
    /// 单笔竞价总预算（lamports）。走 tip 通道即全额 tip；走 cu_price 通道经 cu_limit 换算。
    pub cost_amount: u64,
    /// compute units 上限。用于把 `cost_amount` 换算成 `cu_price`（micro-lamports/CU）。
    pub cu_limit: u32,
    /// “高 gas”那笔的 tip 倍率（相对平台最低 tip，如 1.01 / 1.05），
    /// 保证带 cu_price 的交易不至于因 tip 不足被平台丢弃，但不过度抬高。
    pub tip_rate: f64,
}

// ── CostTxConfig（V1）─────────────────────────────────────────────────────────

/// **V1 的单预算竞价配置** —— 结构与 [`V1TxConfig`] 完全一致，
/// 只把 `priority_fee` 改名为 `cost`，并额外带一个 `tip_rate`。
///
/// # 为什么要单独一个类型
///
/// [`V1TxConfig::priority_fee`] 是**已落地的竞价值**（确定走 priority_fee 通道）。
/// 而 cost 语义表达的是**意图**：「这笔我愿意花多少，具体落在哪个通道由平台性质决定」。
///
/// 两者的区别**必须体现在类型上**，否则：
///
/// - `dispatch_v1` 只能接受一个 config，无法表达「带 price 笔 fee=cost / 纯 tip 笔 fee=None」
/// - `send_with_cost_v1` 只能硬把 `cost` 塞进 `priority_fee`，`tip_rate` 无处安放
///
/// # cost 怎么落地（由各 `*_cost_mode_v1` 自行决定）
///
/// | 平台 | 落成 |
/// |---|---|
/// | Harmonic | `priority_fee = cost`（`uses_tip_transfer()=false`，它本就是 gas 竞价） |
/// | Jito / TipOnly | `tip = Absolute(cost)` |
/// | Fallback 带 price 笔 | `priority_fee = cost` + `tip = Ratio(tip_rate)` 保底 |
/// | Fallback 纯 tip 笔 | `tip = Absolute(cost)` |
///
/// 这正是 V0 [`CostConfig`] 分流行为的 V1 对应物。
#[derive(Debug, Clone, Copy, Default)]
pub struct CostTxConfig {
    /// 单笔竞价总预算（lamports）。**由各 mode 决定落地为 tip 还是 priority_fee。**
    pub cost: u64,
    /// compute units 上限。
    pub cu_limit: u32,
    /// 「带 price 那笔」的 tip 倍率（相对平台最低 tip，如 1.05）。
    ///
    /// 与 [`CostConfig::tip_rate`] 同义：保证带 priority_fee 的那笔不至于
    /// 因 tip 不足被平台丢弃，但不过度抬高。
    pub tip_rate: f64,
}

impl CostTxConfig {
    /// 最小构造：只给 cost / cu_limit，`tip_rate` 默认 1.05。
    pub fn new(cost: u64, cu_limit: u32) -> Self {
        Self {
            cost,
            cu_limit,
            tip_rate: 1.05,
        }
    }

    /// 覆盖 tip_rate。
    pub fn with_tip_rate(mut self, tip_rate: f64) -> Self {
        self.tip_rate = tip_rate;
        self
    }

    /// 转成 [`V1TxConfig`]，`cost` 落地到 **`priority_fee`**。
    ///
    /// `..Default::default()` 保证 `loaded_accounts_data_size_limit` / `heap_size`
    /// 拿到默认值（V1 下若为 `None` 链上按 0 处理 → 交易失败）。
    pub fn as_fee_config(&self) -> sol_tx_send::platform_clients::V1TxConfig {
        sol_tx_send::platform_clients::V1TxConfig {
            priority_fee: Some(self.cost),
            compute_unit_limit: Some(self.cu_limit),
            ..Default::default()
        }
    }

    /// 转成 [`V1TxConfig`]，**不给 `priority_fee`**（`cost` 落地到 tip 通道）。
    ///
    /// ⚠️ 同样必须走 `..Default::default()`。
    pub fn as_tip_channel_config(&self) -> sol_tx_send::platform_clients::V1TxConfig {
        sol_tx_send::platform_clients::V1TxConfig {
            priority_fee: None,
            compute_unit_limit: Some(self.cu_limit),
            ..Default::default()
        }
    }
}

/// 默认「带 price 那笔」的 tip 倍率（相对平台最低 tip）。
///
/// 1.05 = 比平台最低价高 5%：保证带 `priority_fee` 的那笔不被平台以 tip 不足丢弃，
/// 又不至于浪费。与 V0 `fallback_mode` 里写死的 `Ratio(1.05)` 一致。
pub const DEFAULT_TIP_RATE: f64 = 1.05;

// ── SpendMode / SpendConfig ───────────────────────────────────────────────────

/// **V1 发送的竞价模式** —— 明确区分「我只要这个 tip」和「我总共花这么多」。
///
/// 这两种语义**不可混用**，所以在类型层面分开：
///
/// | 变体 | 含义 | 底层接口 |
/// |---|---|---|
/// | [`SpendMode::FixedTip`] | 「tip = 这个值」（+ 可选保底 gas） | [`TxDispacher::send_tip_only_v1`] |
/// | [`SpendMode::Cost`] | 「这笔总共花这么多」，落地通道由平台性质决定 | [`TxDispacher::send_with_cost_v1`] |
///
/// # 为什么需要区分
///
/// 历史上有调用方把两者混着设（`tip = X` + `gas = Y`），导致：
///
/// - 换计价腿 / 换 CU 上限时 tip 与 gas 的配比悄悄失衡
/// - 无法表达「不要 tip」（只能靠传 `None`，容易被误当默认值）
///
/// 拆成 enum 后，每个调用点**必须显式选一种**，语义不会再混。
#[derive(Debug, Clone, Copy)]
pub enum SpendMode {
    /// **固定 tip**：`tip` 原样交给底层；`gas` 是调用方**显式声明**的保底 gas。
    ///
    /// - `tip = None` = **不给 tip**（平台默认，等价 V0 的 `tip = None`）
    /// - `tip = Some(n)` = 精确 `n` lamports（`n` 可为 `0`）
    /// - `gas = None` = **不带 `priority_fee` 字段**（不是 0 —— 「不表态」与「给 0」在类型上分开）
    /// - `gas = Some(n)` = 精确 `n` lamports，**原样落地**
    ///
    /// # `gas` 的落地范围（刻意收窄到一个点）
    ///
    /// 只在 **`SendRoute::Fallback` 双发平台的「tip 竞价腿」**上生效 —— 即
    /// `fire_v1_both!` 里 `no_price` 那一笔（那笔原本 `priority_fee = None`）。
    /// 效果 = 「SOL tip 主竞价 + 一点点 gas 保底」。
    ///
    /// 其余通道**一律忽略**，保持原状：
    ///
    /// - Fallback 的「带价腿」（`with_price` 侧）→ 仍旧只用 `Ratio(1.05)` 的 tip 保底
    /// - Fallback 的单发平台、Jito → 不动
    /// - Harmonic → 它只认 `priority_fee`（`uses_tip_transfer() = false`），本就不走 tip 腿
    ///
    /// # 为什么不给 `gas` 兜底默认值
    ///
    /// dispatcher **不替调用方决定 gas 值**：任何「`None` → 某个默认值」「给低了就抬到下限」
    /// 的写法都会让调用方无法表达「我只想给这么点」。所以这里只做**原样透传** ——
    /// `Some(1)` 就是 1 lamports，没人拦；不想要 gas 就填 `None`（与旧行为逐字节相同）。
    FixedTip {
        /// SOL tip 转账额（`None` = 平台默认）
        tip: Option<u64>,
        /// 保底 gas（V1 `priority_fee`，lamports 总额）。`None` = 不带该字段
        gas: Option<u64>,
    },

    /// **单笔竞价总预算（lamports）**：落地为 tip 还是 `priority_fee` 由平台性质决定。
    ///
    /// 走 cost 语义（V0 `send_with_cost` 的 V1 对应物）。
    Cost(u64),
}

impl SpendMode {
    /// 便捷：精确 tip（不带 gas）。
    pub const fn tip(n: u64) -> Self {
        Self::FixedTip { tip: Some(n), gas: None }
    }

    /// 便捷：不给 tip（平台默认，也不带 gas）。
    pub const fn no_tip() -> Self {
        Self::FixedTip { tip: None, gas: None }
    }

    /// 便捷：单笔总预算。
    pub const fn cost(n: u64) -> Self {
        Self::Cost(n)
    }

    /// 便捷：固定 tip + 可选保底 gas（落地范围见 [`Self::FixedTip`] 的文档）。
    pub const fn fixed_tip(tip: Option<u64>, gas: Option<u64>) -> Self {
        Self::FixedTip { tip, gas }
    }

    /// 挂上保底 gas（仅 `FixedTip` 有意义；`Cost` 原样返回）。
    ///
    /// 读起来就是「tip = 这个值，再补一点点 gas」：
    /// `SpendMode::tip(cost).with_gas(100_000)`。
    pub const fn with_gas(self, gas: u64) -> Self {
        match self {
            Self::FixedTip { tip, .. } => Self::FixedTip { tip, gas: Some(gas) },
            other => other,
        }
    }

    /// 保底 gas（仅 `FixedTip` 有；`Cost` 恒为 `None`）。
    pub const fn gas_opt(&self) -> Option<u64> {
        match self {
            Self::FixedTip { gas, .. } => *gas,
            Self::Cost(_) => None,
        }
    }
}

/// **V1 发送配置** —— 统一入口 [`TxDispacher::send`] 的唯一入参。
///
/// 把「竞价意图」（[`SpendMode`]）与「构建参数」（`cu_limit` / 账户数据上限 / heap）
/// 收在一个类型里，避免调用方各自拼 `tip_strategy` + `V1TxConfig` 时
/// **把两种语义混起来**。
///
/// # 用法
///
/// ```ignore
/// // 固定 tip
/// dispacher().send(ixs, ctx, slot, SpendConfig::tip(1_000_000, 135_000), 60).await;
///
/// // 不给 tip（平台默认）
/// dispacher().send(ixs, ctx, slot, SpendConfig::no_tip(135_000), 60).await;
///
/// // 单笔总预算
/// dispacher().send(ixs, ctx, slot, SpendConfig::cost(170_000_000, 135_000), 60).await;
/// ```
#[derive(Debug, Clone, Copy)]
pub struct SpendConfig {
    /// 竞价模式。
    pub mode: SpendMode,
    /// compute units 上限。
    pub cu_limit: u32,
    /// 最大可加载账户数据字节数。`None` = 走 [`V1TxConfig::default()`]（64 MiB）。
    pub loaded_accounts_data_size_limit: Option<u32>,
    /// 堆大小（字节）。`None` = 走 [`V1TxConfig::default()`]（32 KB）。
    pub heap_size: Option<u32>,
    /// 「带 price 那笔」的 tip 倍率（相对平台最低 tip）。
    ///
    /// 仅 [`SpendMode::Cost`] 有意义：保证带 `priority_fee` 的那笔
    /// 不致因 tip 不足被平台丢弃，又不过度抬高。
    /// 默认 [`DEFAULT_TIP_RATE`]。
    pub tip_rate: f64,
}

impl SpendConfig {
    /// 通用构造。
    pub fn new(mode: SpendMode, cu_limit: u32) -> Self {
        Self {
            mode,
            cu_limit,
            loaded_accounts_data_size_limit: None,
            heap_size: None,
            tip_rate: DEFAULT_TIP_RATE,
        }
    }

    /// 便捷：固定 tip（`None` = 平台默认）+ 可选保底 gas。
    ///
    /// `gas` 的落地范围见 [`SpendMode::FixedTip`] 的文档（**只在 Fallback 的 tip 竞价腿**）。
    pub fn fixed_tip(tip: Option<u64>, gas: Option<u64>, cu_limit: u32) -> Self {
        Self::new(SpendMode::fixed_tip(tip, gas), cu_limit)
    }

    /// 便捷：精确 tip。
    pub fn tip(n: u64, cu_limit: u32) -> Self {
        Self::new(SpendMode::tip(n), cu_limit)
    }

    /// 便捷：不给 tip。
    pub fn no_tip(cu_limit: u32) -> Self {
        Self::new(SpendMode::no_tip(), cu_limit)
    }

    /// 便捷：单笔总预算。
    pub fn cost(n: u64, cu_limit: u32) -> Self {
        Self::new(SpendMode::cost(n), cu_limit)
    }

    /// 覆盖 `cu_limit`。
    pub fn with_cu_limit(mut self, cu_limit: u32) -> Self {
        self.cu_limit = cu_limit;
        self
    }

    /// 覆盖账户数据上限。
    pub fn with_loaded_accounts_data_size_limit(mut self, limit: u32) -> Self {
        self.loaded_accounts_data_size_limit = Some(limit);
        self
    }

    /// 覆盖 `tip_rate`（仅 [`SpendMode::Cost`] 有意义）。
    ///
    /// 控制「带 price 那笔」的 tip 倍率（相对平台最低 tip）。
    pub fn with_tip_rate(mut self, tip_rate: f64) -> Self {
        self.tip_rate = tip_rate;
        self
    }

    /// 覆盖堆大小。
    pub fn with_heap_size(mut self, size: u32) -> Self {
        self.heap_size = Some(size);
        self
    }

    /// **仅给 tip 值**的 `V1TxConfig`（`priority_fee = None`）。
    ///
    /// 用于「走 SOL tip 竞价」的通道。⚠️ **必须走 `..Default::default()`**：
    /// V1 下 `loaded_accounts_data_size_limit` / `heap_size` 若为 `None`，
    /// 链上按 0 处理 → 交易直接失败（gas 白烧）。
    ///
    /// # ⚠️ 这里**故意**不塞 `FixedTip.gas`
    ///
    /// `gas` 只该落在 Fallback 的 tip 竞价腿上。若塞进这个通用 config，
    /// Harmonic / Jito / Fallback 的**带价腿**都会跟着带上 —— 那是过度放开。
    /// 所以 `gas` 由 [`Self::gas_opt`] 单独取出、以独立参数传进
    /// [`TxDispacher::send_v1`] 的 `tip_leg_gas`。改这里之前先看那段文档。
    pub fn as_tip_config(&self) -> sol_tx_send::platform_clients::V1TxConfig {
        sol_tx_send::platform_clients::V1TxConfig {
            priority_fee: None,
            compute_unit_limit: Some(self.cu_limit),
            ..self.defaults()
        }
    }

    /// **把 `cost` 落地为 `priority_fee`** 的 `V1TxConfig`。
    ///
    /// ⚠️ 只有 [`SpendMode::Cost`] 有意义；`FixedTip` 时 `priority_fee = None`。
    pub fn as_fee_config(&self) -> sol_tx_send::platform_clients::V1TxConfig {
        sol_tx_send::platform_clients::V1TxConfig {
            priority_fee: self.cost_opt(),
            compute_unit_limit: Some(self.cu_limit),
            ..self.defaults()
        }
    }

    /// 转成 [`TipStrategy`]（仅 [`SpendMode::FixedTip`] 有意义）。
    ///
    /// - `FixedTip { tip: None, .. }` → `None`（平台默认）
    /// - `FixedTip { tip: Some(n), .. }` → `Some(Absolute(n))`
    /// - `Cost(_)` → `None`（cost 模式的 tip 由 cost 语义自行推）
    pub fn as_tip_strategy(&self) -> Option<TipStrategy> {
        match self.mode {
            SpendMode::FixedTip { tip: Some(n), .. } => Some(TipStrategy::Absolute(n)),
            SpendMode::FixedTip { tip: None, .. } | SpendMode::Cost(_) => None,
        }
    }

    /// `cost` 值（仅 [`SpendMode::Cost`] 有）。
    pub fn cost_opt(&self) -> Option<u64> {
        match self.mode {
            SpendMode::Cost(n) => Some(n),
            SpendMode::FixedTip { .. } => None,
        }
    }

    /// 保底 gas（仅 [`SpendMode::FixedTip`] 的 `gas` 字段有）。
    ///
    /// 落地范围见 [`SpendMode::FixedTip`] 的文档 —— **只在 Fallback 的 tip 竞价腿**。
    pub fn gas_opt(&self) -> Option<u64> {
        self.mode.gas_opt()
    }

    /// 组装 `V1TxConfig` 的「账户数据上限 / heap」部分（都走 `Default` 兜底）。
    fn defaults(&self) -> sol_tx_send::platform_clients::V1TxConfig {
        let d = sol_tx_send::platform_clients::V1TxConfig::default();
        sol_tx_send::platform_clients::V1TxConfig {
            priority_fee: None,
            compute_unit_limit: None,
            loaded_accounts_data_size_limit: Some(
                self.loaded_accounts_data_size_limit
                    .or(d.loaded_accounts_data_size_limit)
                    .unwrap_or(64 * 1024 * 1024),
            ),
            heap_size: self.heap_size.or(d.heap_size),
        }
    }

    /// 转成旧的 [`CostTxConfig`]（供 cost 语义的内部实现使用）。
    pub fn as_cost_tx_config(&self) -> CostTxConfig {
        CostTxConfig {
            cost: self.cost_opt().unwrap_or(0),
            cu_limit: self.cu_limit,
            tip_rate: self.tip_rate,
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
use sol_tx_send::platform_clients::helius_max::HeliusMax;
#[cfg(feature = "helius")]
use sol_tx_send::platform_clients::helius_swqos::HeliusSwqos;
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
    /// Tip-only 模式：只发 tip 竞价（不发 cu_price），全平台一发
    TipOnly,
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
    pub(crate) helius_max: Option<Arc<HeliusMax>>,
    #[cfg(feature = "helius")]
    pub(crate) helius_swqos: Option<Arc<HeliusSwqos>>,
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

    /// 构造默认注入 4 个 bundle 平台（Jito / Astralane / FlashBlock / Helius Max）的
    /// 多平台 bundle 发送器，用于把同一个原子 bundle 并发发到这些平台。
    ///
    /// 各平台按 feature 开关存在，未启用的自动跳过。可直接链式 `append(...)` 后 `send(timeout)`。
    /// 注：SWQOS-only 档位不支持 bundle，故不在此列。
    pub fn bundle_sender(&self) -> MultiBundleSender {
        let mut senders: Vec<Box<dyn BundleSender>> = Vec::new();
        #[cfg(feature = "jito")]
        if let Some(c) = &self.jito {
            senders.push(Box::new(c.as_ref().clone()));
        }
        #[cfg(feature = "astralane")]
        if let Some(c) = &self.astralane {
            senders.push(Box::new(c.as_ref().clone()));
        }
        #[cfg(feature = "flash_block")]
        if let Some(c) = &self.flash_block {
            senders.push(Box::new(c.as_ref().clone()));
        }
        #[cfg(feature = "helius")]
        if let Some(c) = &self.helius_max {
            senders.push(Box::new(c.as_ref().clone()));
        }
        MultiBundleSender::new(senders)
    }

    /// **[`Self::bundle_sender`] 的 V1 版**：同样是多平台并发 bundle，
    /// 只是每笔交易用 V1 格式构建（`V1TxConfig` 取代 `cu`，无 ALT）。
    ///
    /// # 什么时候要用它
    ///
    /// V1 只提高**账户数**（64 inline）与**字节数**（4096），**没有**提高单笔能锁的
    /// 账户上限。所以「migrate + sell」这类指令条数多、去重后仍超 64 个账户的场景，
    /// 还是必须拆成多笔 bundle —— 只是每笔内部改用 V1 编码。
    pub fn bundle_sender_v1(&self) -> MultiBundleSenderV1 {
        let mut senders: Vec<Box<dyn BundleSender>> = Vec::new();
        #[cfg(feature = "jito")]
        if let Some(c) = &self.jito {
            senders.push(Box::new(c.as_ref().clone()));
        }
        #[cfg(feature = "astralane")]
        if let Some(c) = &self.astralane {
            senders.push(Box::new(c.as_ref().clone()));
        }
        #[cfg(feature = "flash_block")]
        if let Some(c) = &self.flash_block {
            senders.push(Box::new(c.as_ref().clone()));
        }
        #[cfg(feature = "helius")]
        if let Some(c) = &self.helius_max {
            senders.push(Box::new(c.as_ref().clone()));
        }
        MultiBundleSenderV1::new(senders)
    }

    /// 只用 **FlashBlock** 发一笔（不挑 leader、不参与 tip/cu_price 竞价）。
    ///
    /// tip 走平台最低、cu 按传入；fire-and-forget，不等确认 ——
    /// 适合补 ATA 这类"发出去就行"的维护交易。返回 None 表示没配 FlashBlock。
    #[cfg(feature = "flash_block")]
    pub fn send_flashblock_only(
        &self,
        ixs: &[solana_sdk::instruction::Instruction],
        ctx: &SendContext,
        cu: (Option<u32>, Option<u64>),
    ) -> Option<solana_sdk::signature::Signature> {
        let client = self.flash_block.as_ref()?;
        let mut sigs = ahash::AHashSet::new();
        fire::fire_client(client, ixs, &ctx.payer, None, &ctx.hash_param, &cu, &ctx.alt, None, &mut sigs);
        sigs.into_iter().next()
    }

    /// 查询当前 slot 的路由决策（不发送）。
    /// 查询 `target_slot` 的 leader 类型并返回路由决策。
    /// 调用方自行决定传当前 slot 还是 current_slot + N。
    pub fn resolve_route(&self, target_slot: u64) -> SendRoute {
        let info = self.oracle.leader_at(target_slot);
        // 1. 先按具体 leader pubkey 匹配 tip-only 白名单
        if let Some(ref info) = info {
            if let Some(pk) = info.leader_pubkey() {
                if TIP_ONLY_LEADERS.contains(pk) {
                    return SendRoute::TipOnly;
                }
            }
        }
        // 2. 再按客户端类型匹配
        match info {
            Some(info) if info.is_harmonic() => SendRoute::Harmonic,
            Some(info) if info.is_jito() => SendRoute::Jito,
            _ => SendRoute::Fallback,
        }
    }

    /// 目标 slot 的 leader 是否命中 FIFO 表（leader vote account + client_type_id 都匹配）。
    /// FIFO leader 命中时：tip=None（下游折成平台最低 tip）、cu_limit 保留、cu_price=None（不参与竞价）。
    fn is_fifo_leader(&self, slot: u64) -> bool {
        if let Some(info) = self.oracle.leader_at(slot) {
            if let (Some(pk), Some(ctid)) = (info.leader_pubkey().copied(), info.client_type_id) {
                return FIFO_LEADERS.contains(&(pk, ctid));
            }
        }
        false
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
        // FIFO leader：tip=None（下游折成平台最低 tip）、cu_limit 保留、cu_price=None（不参与 cu_price 竞价）。
        if self.is_fifo_leader(target_slot) {
            log::info!(
                "[TxDispacher] slot={} route={:?} 命中 FIFO leader → tip=None cu=(limit, None)",
                target_slot,
                route
            );
            return strategy::dispatch(self, ixs, ctx, route, None, (cu.0, None), confirm_timeout_secs)
                .await
                .map_err(into_tx_confirm_err);
        }
        log::info!("[TxDispacher] slot={} route={:?}", target_slot, route);
        strategy::dispatch(self, ixs, ctx, route, tip_strategy, cu, confirm_timeout_secs)
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
        strategy::dispatch_cheap(self, ixs, ctx, tip_strategy, cu, confirm_timeout_secs)
            .await
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
        strategy::dispatch_tip_only(self, ixs, ctx, route, min_tip_floor, cu_limit, confirm_timeout_secs)
            .await
            .map_err(into_tx_confirm_err)
    }

    /// 单预算竞价发送——把 tip/cu_price 二维参数合并为单个 cost 预算。
    ///
    /// 语义：`config.cost_amount` 是这笔交易愿意付出的单笔竞价总预算（lamports）。
    /// 内部根据平台性质推导通道：
    /// - 走 **cu_price 通道** 的平台/那笔：`cu_price = cost × 1e6 / cu_limit`，
    ///   tip 用 `config.tip_rate` 保底（保证带 cu_price 的交易不被平台以 tip 不足丢弃）；
    /// - 走 **tip 通道** 的平台/那笔（jito、纯 tip 那笔）：tip = 全额 `cost_amount`。
    ///
    /// 因为 ctx 送 single nonce，同平台 `fire_both` 双发 / 全平台广播只会有一笔
    /// 成功上链、只付一次费，故两笔各自都能尽力花到 `cost_amount`，不拆预算。
    /// 路由逻辑与 [`send`] 一致（Harmonic / Jito / Fallback / TipOnly / FIFO）。
    pub async fn send_with_cost(
        &self,
        ixs: &[solana_sdk::instruction::Instruction],
        ctx: &SendContext,
        target_slot: u64,
        config: CostConfig,
        confirm_timeout_secs: u64,
    ) -> Result<(solana_sdk::signature::Signature, grpc_client::TransactionFormat), TxConfirmError> {
        let route = self.resolve_route(target_slot);
        // FIFO leader：不参与 tip/cu_price 竞价（靠先到先得），cost 不生效，
        // 只保留 cu_limit，tip=None、cu_price=None。
        if self.is_fifo_leader(target_slot) {
            log::info!(
                "[TxDispacher::send_with_cost] slot={} route={:?} 命中 FIFO leader → cost 不生效 tip=None cu=(limit, None)",
                target_slot,
                route
            );
            return strategy::dispatch(
                self,
                ixs,
                ctx,
                route,
                None,
                (Some(config.cu_limit), None),
                confirm_timeout_secs,
            )
            .await
            .map_err(into_tx_confirm_err);
        }
        log::info!(
            "[TxDispacher::send_with_cost] slot={} route={:?} cost={} cu_limit={} tip_rate={}",
            target_slot,
            route,
            config.cost_amount,
            config.cu_limit,
            config.tip_rate
        );
        strategy::dispatch_with_cost(self, ixs, ctx, route, config, confirm_timeout_secs)
            .await
            .map_err(into_tx_confirm_err)
    }

    // ══════════════════════════════════════════════════════════════════════════
    // V1 接口
    //
    // 与 V0 一一对应，行为保持不变，只把构建参数换成 V1：
    //   - `cu: (Option<u32>, Option<u64>)` → `config: V1TxConfig`
    //   - 无 `alt` 参数（V1 不支持地址查找表，账户全部内联）
    //
    // **注意 `priority_fee` 语义**：它是 **lamports 总额**，不是 micro-lamports/CU 单价。
    // V0 里所有 `tip × 1_000_000 / cu_limit` 的换算在 V1 下都不做。
    // ══════════════════════════════════════════════════════════════════════════

    /// V1 版 [`send_cheap`]。
    pub async fn send_cheap_v1(
        &self,
        ixs: &[solana_sdk::instruction::Instruction],
        ctx: &SendContext,
        tip_strategy: Option<TipStrategy>,
        config: V1TxConfig,
        confirm_timeout_secs: u64,
    ) -> Result<(solana_sdk::signature::Signature, grpc_client::TransactionFormat), TxConfirmError> {
        strategy::dispatch_cheap_v1(self, ixs, ctx, tip_strategy, config, confirm_timeout_secs)
            .await
            .map_err(into_tx_confirm_err)
    }

    /// **V1 统一发送入口** —— V1 侧唯一对外 API（与 V0 的 [`Self::send`] 对称）。
    ///
    /// 按 [`SpendConfig::mode`] 分派：
    ///
    /// | 模式 | 行为 | tip 语义 |
    /// |---|---|---|
    /// | [`SpendMode::FixedTip`] | 走 `dispatch_v1` | `tip`：`None` → 平台默认；`Some(n)` → 精确 n；`gas` 只在 Fallback 的 tip 竞价腿落地 |
    /// | [`SpendMode::Cost`] | 走 `dispatch_with_cost_v1` | 落地通道由平台性质决定 |
    ///
    /// 两条路径**都**做 FIFO leader 判断与路由。
    ///
    /// ⚠️ `FixedTip` **不走 tip-only 路径**：那条会把 tip 当 `min_tip_floor`
    /// 并强制抬到平台最低价（`Some`），无法表达「不给 tip」（V0 的 `tip = None`）。
    pub async fn send_v1(
        &self,
        ixs: &[solana_sdk::instruction::Instruction],
        ctx: &SendContext,
        target_slot: u64,
        cfg: SpendConfig,
        confirm_timeout_secs: u64,
    ) -> Result<(solana_sdk::signature::Signature, grpc_client::TransactionFormat), TxConfirmError> {
        let route = self.resolve_route(target_slot);
        match cfg.mode {
            SpendMode::FixedTip { .. } => {
                let tip_strategy = cfg.as_tip_strategy();
                // 保底 gas：只在 Fallback 的 tip 竞价腿落地（其余 mode 忽略）
                let tip_leg_gas = cfg.gas_opt();
                // FIFO leader：不参与竞价（tip=None），只保留 cu_limit 等构建参数
                let is_fifo = self.is_fifo_leader(target_slot);
                if is_fifo {
                    log::info!(
                        "[TxDispacher::send_v1] slot={} route={route:?} 命中 FIFO leader → tip=None",
                        target_slot
                    );
                }
                strategy::dispatch_v1(
                    self,
                    ixs,
                    ctx,
                    route,
                    if is_fifo { None } else { tip_strategy },
                    cfg.as_tip_config(),
                    tip_leg_gas,
                    confirm_timeout_secs,
                )
                .await
                .map_err(into_tx_confirm_err)
            }
            SpendMode::Cost(cost) => {
                log::info!(
                    "[TxDispacher::send_v1] slot={} route={route:?} mode=Cost cost={cost} cu_limit={}",
                    target_slot,
                    cfg.cu_limit
                );
                self.send_with_cost_v1(ixs, ctx, target_slot, cfg.as_cost_tx_config(), confirm_timeout_secs)
                    .await
            }
        }
    }

    /// V1 版 cost 语义发送（**内部用**，外部统一走 [`Self::send_v1`]）。
    ///
    /// 单预算 `cost`，按 route 分派到各 `*_cost_mode_v1`，由后者决定落地通道。
    pub(crate) async fn send_with_cost_v1(
        &self,
        ixs: &[solana_sdk::instruction::Instruction],
        ctx: &SendContext,
        target_slot: u64,
        config: CostTxConfig,
        confirm_timeout_secs: u64,
    ) -> Result<(solana_sdk::signature::Signature, grpc_client::TransactionFormat), TxConfirmError> {
        let route = self.resolve_route(target_slot);
        // FIFO leader：不参与竞价，cost 不生效（priority_fee=None 且不给 tip）
        if self.is_fifo_leader(target_slot) {
            log::info!(
                "[TxDispacher::send_with_cost_v1] slot={} route={:?} 命中 FIFO leader → cost 不生效",
                target_slot,
                route
            );
            return strategy::dispatch_v1(
                self,
                ixs,
                ctx,
                route,
                None,
                // FIFO 下只需 cu_limit，priority_fee 置 None
                sol_tx_send::platform_clients::V1TxConfig {
                    priority_fee: None,
                    compute_unit_limit: Some(config.cu_limit),
                    ..Default::default()
                },
                None, // 保底 gas：FIFO 下不参与竞价
                confirm_timeout_secs,
            )
            .await
            .map_err(into_tx_confirm_err);
        }
        log::info!(
            "[TxDispacher::send_with_cost_v1] slot={} route={:?} cost={} cu_limit={} tip_rate={}",
            target_slot,
            route,
            config.cost,
            config.cu_limit,
            config.tip_rate
        );
        strategy::dispatch_with_cost_v1(self, ixs, ctx, route, config, confirm_timeout_secs)
            .await
            .map_err(into_tx_confirm_err)
    }

    /// V1 版 [`send_flashblock_only`]。只用 FlashBlock 发一笔，fire-and-forget。
    #[cfg(feature = "flash_block")]
    pub fn send_flashblock_only_v1(
        &self,
        ixs: &[solana_sdk::instruction::Instruction],
        ctx: &SendContext,
        config: V1TxConfig,
    ) -> Option<solana_sdk::signature::Signature> {
        let client = self.flash_block.as_ref()?;
        let mut sigs = ahash::AHashSet::new();
        fire::fire_v1_client(client, ixs, ctx, None, config, None, &mut sigs);
        sigs.into_iter().next()
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
                leader: None,
                client_type_id: None,
            })
        }
    }

    // ── resolve_route 单元测试 ────────────────────────────────────────────────

    #[test]
    fn noop_oracle_always_fallback() {
        let d = TxDispacher::builder(NoopOracle).build();
        assert_eq!(d.resolve_route(422_000_000), SendRoute::Fallback);
    }

    #[test]
    fn harmonic_client_type_routes_to_harmonic() {
        let d = TxDispacher::builder(MockOracle {
            harmonic: true,
            name: None,
        })
        .build();
        assert_eq!(d.resolve_route(100), SendRoute::Harmonic);
    }

    #[test]
    fn non_harmonic_client_type_routes_to_fallback() {
        let d = TxDispacher::builder(MockOracle {
            harmonic: false,
            name: None,
        })
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
            .astralane(sol_tx_send::platform_clients::astralane::Astralane::init_with(
                "ASTRALANE_API_KEY",
                Region::Amsterdam,
            ))
            // feature = "temporal"
            .temporal(sol_tx_send::platform_clients::temporal::Temporal::init_with(
                "TEMPORAL_KEY",
                Region::Amsterdam,
            ))
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
