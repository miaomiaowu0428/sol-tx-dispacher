//! sol-tx-dispacher
//!
//! 根据 `SlotOracle` 提供的 leader 信息，自适应地选择发送策略：
//!
//! - `Harmonic` leader → Harmonic 直发 + Astralane / Temporal 按 90% tip
//! - `P2P.org Turbo` leader → fallback 全平台广播 + 追加 P2P.org Syncro Sender 通道
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
mod config;
mod bundle;
mod context;
mod fifo;
mod fire;
mod region_plan;
mod region_pool;
mod strategy;

pub use builder::TxDispacherBuilder;
pub use region_plan::{
    DEFAULT_PLAN, PLAN_PATH_ENV, Policy, RegionPlan, RegionProfile, SUPPORTED_VERSION,
};
pub use region_pool::{RegionPool, SEND_REGION, with_region};
pub use bundle::{MultiBundleError, MultiBundleErrorV1, MultiBundleSender, MultiBundleSenderV1};
pub use context::{SendContext, merge_alts};

use fifo::FIFO_LEADERS;
use nonce_cache::TxConfirmError;
use sol_slot_leader::{LeaderInfo, SlotOracle};
use sol_tx_send::platform_clients::{BundleSender, KeepAliveTarget, V1TxConfig, run_keep_alive};
use sol_tx_send::platform_clients::Region;
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
    /// 绝对数额（lamports）—— **竞价意图**：这是「我想给这么多」，但落地时
    /// 仍会被抬到平台最低 tip（见 [`TipStrategy::Exact`] 的对比）。
    Absolute(u64),
    /// 相对于各平台最低 tip 的比例（例如 0.9 = 90%，1.1 = 110%）
    Ratio(f64),
    /// **严格值（lamports）**：给定多少就是多少，**任何情况下都不抬、不缩**
    /// —— 即使低于平台最低 tip 也原样落地。
    ///
    /// 语义来自 [`SpendMode::FixedTip`]：[`SpendConfig::as_tip_strategy`] 把
    /// `FixedTip { tip: Some(n) }` 转成 `Exact(n)`，使「固定 tip」真的固定。
    ///
    /// ⚠️ 低于平台门槛的交易可能被平台直接丢弃 —— 这是调用方**明确要求**的取舍：
    /// 要么按我给的值发，要么不发，而不是偷偷帮我抬价。
    Exact(u64),
}

impl TipStrategy {
    /// 根据平台最低 tip 计算实际 tip 数额。
    pub fn compute(&self, platform_min: u64) -> u64 {
        match self {
            TipStrategy::Absolute(amt) => *amt,
            TipStrategy::Ratio(r) => (platform_min as f64 * r) as u64,
            // 严格值：平台最低 tip 完全不参与计算
            TipStrategy::Exact(amt) => *amt,
        }
    }

    /// 在当前策略基础上再乘以缩放系数。
    ///
    /// `Absolute(n).scaled(f)` = `Absolute(n × f)`
    /// `Ratio(r).scaled(f)`    = `Ratio(r × f)`
    /// `Exact(n).scaled(f)`    = `Exact(n)` —— **忽略缩放**，严格值就是严格值
    pub fn scaled(self, factor: f64) -> Self {
        match self {
            TipStrategy::Absolute(n) => TipStrategy::Absolute((n as f64 * factor) as u64),
            TipStrategy::Ratio(r) => TipStrategy::Ratio(r * factor),
            TipStrategy::Exact(n) => TipStrategy::Exact(n),
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
    /// # `tip = Some(n)` 是**真的**精确 `n`
    ///
    /// 本模式落的 [`TipStrategy::Exact`] 在 dispatcher 内**不做任何抬价**：
    /// 即使 `n` 低于目标平台的最低 tip，也原样写入/发送，绝不偷偷抬到
    /// 「平台最低价 × 1.02」或「× 1.05」。低于门槛时平台可能直接丢弃交易 ——
    /// 这是本模式刻意的取舍（「要么按我给的值发，要么不发」）。
    ///
    /// ⚠️ 只有本模式如此。`Cost` 语义仍走 `Absolute`/`Ratio`，会按平台最低 tip 兜底。
    ///
    /// # `gas` 的落地范围（刻意收窄到一个点）
    ///
    /// 只在 **`SendRoute::Fallback` 双发平台的「tip 竞价腿」**上生效 —— 即
    /// `fire_v1_both!` 里 `no_price` 那一笔（那笔原本 `priority_fee = None`）。
    /// 效果 = 「SOL tip 主竞价 + 一点点 gas 保底」。
    ///
    /// 其余通道**一律忽略**，保持原状：
    ///
    /// - Fallback 的「带价腿」（`with_price` 侧）→ `Ratio(1.05)` 的 tip 保底；
    ///   **但 `FixedTip`（`Exact`）时两条腿都用调用方的严格值**，不再保底
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
    /// - `FixedTip { tip: Some(n), .. }` → `Some(Exact(n))` —— **严格 n**，
    ///   不会被抬到平台最低 tip（见 [`TipStrategy::Exact`]）
    /// - `Cost(_)` → `None`（cost 模式的 tip 由 cost 语义自行推）
    pub fn as_tip_strategy(&self) -> Option<TipStrategy> {
        match self.mode {
            SpendMode::FixedTip { tip: Some(n), .. } => Some(TipStrategy::Exact(n)),
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
#[cfg(feature = "landx")]
use sol_tx_send::platform_clients::landx::LandX;
#[cfg(feature = "nextblock")]
use sol_tx_send::platform_clients::nextblock::NextBlock;
#[cfg(feature = "nodeone")]
use sol_tx_send::platform_clients::nodeone::NodeOne;
#[cfg(feature = "stellium")]
use sol_tx_send::platform_clients::stellium::Stellium;
#[cfg(feature = "syncro")]
use sol_tx_send::platform_clients::syncro::Syncro;
#[cfg(feature = "temporal")]
use sol_tx_send::platform_clients::temporal::Temporal;
#[cfg(feature = "zeroslot")]
use sol_tx_send::platform_clients::zeroslot::ZeroSlot;

// ── 发送路由决策 ──────────────────────────────────────────────────────────────

/// 根据 oracle 查询结果得出的路由决策。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendRoute {
    /// Harmonic 系节点出块：走 Harmonic 直发 + Astralane/Temporal 90% tip
    ///
    /// Temporal 仅在**本路由**（Harmonic 场景）吃指定 tip；其余路由只给平台最低。
    Harmonic,
    /// Jito 节点出块：只发带 tip 的版本，跳过纯 cu_price 的交易
    Jito,
    /// Tip-only 模式：只发 tip 竞价（不发 cu_price），全平台一发
    TipOnly,
    /// **P2P.org Turbo** 节点出块：在 Fallback 全平台广播的**基础上追加**
    /// P2P.org Syncro Sender 通道（带 API key 的私有端点）。
    ///
    /// 命中条件：`slot_leader.name == "P2P.org Turbo"`（见
    /// [`sol_slot_leader::LeaderInfo::is_p2p_turbo`]）。tip / fee 语义与
    /// [`SendRoute::Fallback`] 一致，只是多一路 P2P 自家 SWQoS 直发。
    P2pTurbo,
    /// 其他所有节点（含 DB 无记录 / NoopOracle）：退化到 send_fast
    Fallback,
}

/// 一次发送所需的全部 leader 决策 —— **只读一次 oracle 缓存**。
///
/// # 为什么要有这个类型
///
/// `route` / FIFO / ingress 都来自同一条 `LeaderInfo`。历史上它们是三个独立
/// 方法、各自调一次 `oracle.leader_at()`：
///
/// - 一次发送要读 3 次缓存（Cost 路径甚至 6 次）；
/// - 如果后台刷新任务恰好在两次读取之间更新了该 slot，route 和 ingress
///   可能来自不同快照。
///
/// 现在由 [`TxDispacher::plan`] 一次取出 `LeaderInfo` 并派生全部字段，发送路径
/// 只使用这一个 `SendPlan`。
///
/// # 数据来源
///
/// `oracle.leader_at()` 必须是**纯内存读**（`SlotLeaderCache` 的 DashMap）。
/// DB 拉取只发生在启动时的 `refresh_once()` 和后台每 10 分钟的刷新任务里，
/// **发送路径不查库**。
#[derive(Debug, Clone)]
pub struct SendPlan {
    /// 本次查询的 slot。
    pub slot: u64,
    /// 路由决策。
    pub route: SendRoute,
    /// 是否命中 FIFO leader（`(vote account, client_type_id)` 白名单）。
    pub fifo: bool,
    /// 本次发送读到的 leader 快照（`None` = 缓存无该 slot / NoopOracle）。
    pub leader: Option<LeaderInfo>,
    /// leader 所在地区（由 DB 的 `region` 文本经 `geo_to_region` 得到）。
    pub leader_region: Region,
    /// 实际使用的入口地区（leader 地区在 fanout 内 → 该地区；否则 own）。
    pub ingress: Region,
}

/// 一个已构造的发送通道（启动日志 / 运维自检用）。
#[derive(Debug, Clone)]
pub struct ChannelInfo {
    /// 平台名（如 `Astralane` / `AstralaneQuic`）。
    pub platform: &'static str,
    /// 传输方式（`HTTP` / `QUIC` / `gRPC`）。
    pub transport: &'static str,
    /// 该通道实际开了哪些入口地区；空表示不按地区池发送（如 Harmonic）。
    pub regions: Vec<Region>,
}

impl ChannelInfo {
    fn pool<T>(platform: &'static str, transport: &'static str, pool: &RegionPool<T>) -> Self {
        let mut regions: Vec<Region> = pool.regions().collect();
        // HashMap 无序；按 Region::ALL 的固定顺序输出，保证日志可对比。
        regions.sort_by_key(|r| Region::ALL.iter().position(|x| x == r).unwrap_or(usize::MAX));
        Self {
            platform,
            transport,
            regions,
        }
    }

    fn single(platform: &'static str, transport: &'static str) -> Self {
        Self {
            platform,
            transport,
            regions: vec![*sol_tx_send::constants::REGION],
        }
    }

    fn multi_endpoint(platform: &'static str, transport: &'static str) -> Self {
        Self {
            platform,
            transport,
            regions: Vec::new(),
        }
    }
}

// ── TxDispacher ───────────────────────────────────────────────────────────────

/// slot-aware 交易分发器。
///
/// 通过 [`TxDispacherBuilder`] 构造，各平台客户端按 feature 开关。
pub struct TxDispacher<O: SlotOracle> {
    oracle: O,
    /// 本机地区计划：leader 地区 -> 用哪个入口地区的 client。
    region_profile: RegionProfile,

    #[cfg(feature = "astralane")]
    pub(crate) astralane: Option<Arc<RegionPool<Astralane>>>,
    #[cfg(feature = "astralane_quic")]
    pub(crate) astralane_quic: Option<Arc<AstralaneQuic>>,
    #[cfg(feature = "everstake")]
    pub(crate) everstake: Option<Arc<RegionPool<EverStake>>>,
    #[cfg(feature = "everstake_quic")]
    pub(crate) everstake_quic: Option<Arc<EverStakeQuic>>,
    #[cfg(feature = "flash_block")]
    pub(crate) flash_block: Option<Arc<RegionPool<FlashBlock>>>,
    #[cfg(feature = "nodeone")]
    pub(crate) nodeone: Option<Arc<RegionPool<NodeOne>>>,
    #[cfg(feature = "blockrazor")]
    pub(crate) blockrazor: Option<Arc<RegionPool<Blockrazor>>>,
    #[cfg(feature = "temporal")]
    pub(crate) temporal: Option<Arc<RegionPool<Temporal>>>,
    #[cfg(feature = "helius")]
    pub(crate) helius_max: Option<Arc<RegionPool<HeliusMax>>>,
    #[cfg(feature = "helius")]
    pub(crate) helius_swqos: Option<Arc<RegionPool<HeliusSwqos>>>,
    #[cfg(feature = "zeroslot")]
    pub(crate) zeroslot: Option<Arc<RegionPool<ZeroSlot>>>,
    #[cfg(feature = "nextblock")]
    pub(crate) nextblock: Option<Arc<RegionPool<NextBlock>>>,
    #[cfg(feature = "stellium")]
    pub(crate) stellium: Option<Arc<RegionPool<Stellium>>>,
    #[cfg(feature = "syncro")]
    pub(crate) syncro: Option<Arc<RegionPool<Syncro>>>,
    #[cfg(feature = "jito")]
    pub(crate) jito: Option<Arc<RegionPool<Jito>>>,
    #[cfg(feature = "landx")]
    pub(crate) landx: Option<Arc<RegionPool<LandX>>>,
    #[cfg(feature = "harmonic")]
    pub(crate) harmonic: Option<Arc<HarmonicBlockEngine>>,
}

/// 将 anyhow::Error 转为 TxConfirmError（downcast 根因，downcast 不出当 Other）
fn into_tx_confirm_err(e: anyhow::Error) -> TxConfirmError {
    e.downcast::<TxConfirmError>()
        .unwrap_or_else(|e| TxConfirmError::Other(format!("{:#}", e)))
}

/// 打印一次发送的 leader 决策快照。
///
/// `leader` 是本次**唯一一次**缓存读取得到的完整 `LeaderInfo`，方便直接对照
/// DB 里的 `client_type / name / leader / client_type_id / region`。
fn log_send_plan(prefix: &str, plan: &SendPlan) {
    log::info!(
        "[TxDispacher] {prefix} slot={} route={:?} fifo={} leader_region={} ingress={} leader={:?}",
        plan.slot,
        plan.route,
        plan.fifo,
        plan.leader_region,
        plan.ingress,
        plan.leader
    );
}

/// 在 `plan` 的 region + slot 上下文下执行发送 future。
///
/// - region 给 `RegionPool` 选实例；
/// - slot 只给逐平台日志关联用，不参与路由。
async fn with_plan_ctx<F: std::future::Future>(plan: &SendPlan, fut: F) -> F::Output {
    crate::region_pool::with_region(
        plan.ingress,
        crate::region_pool::with_send_slot(plan.slot, fut),
    )
    .await
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

    /// 当前实际注册的发送通道（平台 + 传输 + 地区）。
    ///
    /// 只包含对应 `Option<...>` 为 `Some` 的 client；未配 key / 初始化失败的
    /// 平台不会出现。供启动日志与运维自检使用。
    pub fn channels(&self) -> Vec<ChannelInfo> {
        let mut out = Vec::new();

        macro_rules! push_pool {
            ($opt:expr, $name:literal, $transport:literal) => {
                if let Some(pool) = &$opt {
                    out.push(ChannelInfo::pool($name, $transport, pool));
                }
            };
        }

        #[cfg(feature = "astralane")]
        push_pool!(self.astralane, "Astralane", "HTTP");
        #[cfg(feature = "astralane_quic")]
        if self.astralane_quic.is_some() {
            out.push(ChannelInfo::single("AstralaneQuic", "QUIC"));
        }
        #[cfg(feature = "everstake")]
        push_pool!(self.everstake, "EverStake", "HTTP");
        #[cfg(feature = "everstake_quic")]
        if self.everstake_quic.is_some() {
            out.push(ChannelInfo::single("EverStakeQuic", "QUIC"));
        }
        #[cfg(feature = "flash_block")]
        push_pool!(self.flash_block, "FlashBlock", "HTTP");
        #[cfg(feature = "nodeone")]
        push_pool!(self.nodeone, "NodeOne", "HTTP");
        #[cfg(feature = "blockrazor")]
        push_pool!(self.blockrazor, "Blockrazor", "HTTP");
        #[cfg(feature = "temporal")]
        push_pool!(self.temporal, "Temporal", "HTTP");
        #[cfg(feature = "helius")]
        push_pool!(self.helius_max, "HeliusMax", "HTTP");
        #[cfg(feature = "helius")]
        push_pool!(self.helius_swqos, "HeliusSwqos", "HTTP");
        #[cfg(feature = "zeroslot")]
        push_pool!(self.zeroslot, "ZeroSlot", "HTTP");
        #[cfg(feature = "nextblock")]
        push_pool!(self.nextblock, "NextBlock", "HTTP");
        #[cfg(feature = "stellium")]
        push_pool!(self.stellium, "Stellium", "HTTP");
        #[cfg(feature = "syncro")]
        push_pool!(self.syncro, "Syncro", "HTTP");
        #[cfg(feature = "jito")]
        push_pool!(self.jito, "Jito", "HTTP");
        #[cfg(feature = "landx")]
        push_pool!(self.landx, "LandX", "HTTP");
        #[cfg(feature = "harmonic")]
        if self.harmonic.is_some() {
            out.push(ChannelInfo::multi_endpoint("Harmonic", "gRPC"));
        }

        out
    }

    /// 打印启动时的通道清单。建议在 `init()` 里 `from_config` 之后调用一次。
    pub fn log_channels(&self) {
        let channels = self.channels();
        if channels.is_empty() {
            log::warn!("[TxDispacher] 没有注册任何发送通道！请检查 feature 与各平台环境变量");
            return;
        }
        log::info!("[TxDispacher] 已构造 {} 个发送通道:", channels.len());
        for c in &channels {
            log::info!(
                "[TxDispacher]   channel platform={} transport={} regions={:?}",
                c.platform,
                c.transport,
                c.regions
            );
        }
    }

    /// 已注册 HTTP 平台的保活目标：**只包含真正注入的客户端，且覆盖池里每个地区**。
    ///
    /// 为什么放在 dispatcher 而不是 `sol-tx-send::endpoint_keep_alive()`：
    /// 后者会把**所有**平台的 endpoint 都探一遍，不管本进程有没有注册那些通道
    /// （客户端初始化失败 / 没配 key 时是 `None`）。既然交易的发送路由就在这个
    /// dispatcher 里，保活也只该覆盖它实际会用的通道。
    ///
    /// - endpoint 逐地区取自各平台的 `endpoint_for(region)`（多地区池 → 多条目标）；
    /// - 退化到同一 URL 的目标按 URL 去重（平台在那一带没机房时很常见）；
    /// - gRPC（Harmonic）与 QUIC（AstralaneQuic / EverStakeQuic）不在列：
    ///   它们在连接层各自保活（见对应客户端）。
    pub fn keep_alive_targets(&self) -> Vec<KeepAliveTarget> {
        let mut targets: Vec<KeepAliveTarget> = Vec::new();

        // 每个平台按池里**实际注册的地区**逐个探测。多地区部署时只保 own 的连接，
        // 其它入口在轮到该 leader 时才首次建连（HTTP 握手 + TLS ≈ 好几个 RTT），
        // 正好落在最不能慢的那 400ms 里。
        #[cfg(feature = "astralane")]
        if let Some(pool) = &self.astralane {
            for r in pool.regions() {
                targets.push(KeepAliveTarget::new("Astralane", Astralane::endpoint_for(r).to_string()));
            }
        }
        #[cfg(feature = "blockrazor")]
        if let Some(pool) = &self.blockrazor {
            for r in pool.regions() {
                targets.push(KeepAliveTarget::new("Blockrazor", Blockrazor::endpoint_for(r).to_string()));
            }
        }
        #[cfg(feature = "everstake")]
        if let Some(pool) = &self.everstake {
            for r in pool.regions() {
                targets.push(KeepAliveTarget::new("EverStake", EverStake::endpoint_for(r).to_string()));
            }
        }
        #[cfg(feature = "flash_block")]
        if let Some(pool) = &self.flash_block {
            for r in pool.regions() {
                targets.push(KeepAliveTarget::new("FlashBlock", FlashBlock::endpoint_for(r).to_string()));
            }
        }
        // Helius Max / SWQOS 共用同一组区域 origin。官方保活端点是
        // `GET <origin>/ping`，**不是**提交端点 `.../fast`；两者按 URL 去重后
        // 每个地区只会剩下一条探测。
        #[cfg(feature = "helius")]
        if let Some(pool) = &self.helius_max {
            for r in pool.regions() {
                targets.push(KeepAliveTarget::new("Helius", HeliusMax::ping_endpoint_for(r)));
            }
        }
        #[cfg(feature = "helius")]
        if let Some(pool) = &self.helius_swqos {
            for r in pool.regions() {
                targets.push(KeepAliveTarget::new("Helius", HeliusSwqos::ping_endpoint_for(r)));
            }
        }
        #[cfg(feature = "jito")]
        if let Some(pool) = &self.jito {
            for r in pool.regions() {
                targets.push(KeepAliveTarget::new("Jito", Jito::endpoint_for(r).to_string()));
            }
        }
        #[cfg(feature = "landx")]
        if let Some(pool) = &self.landx {
            for r in pool.regions() {
                targets.push(KeepAliveTarget::new("LandX", format!("{}/ping", LandX::endpoint_for_region(r))));
            }
        }
        #[cfg(feature = "nodeone")]
        if let Some(pool) = &self.nodeone {
            for r in pool.regions() {
                targets.push(KeepAliveTarget::new("NodeOne", NodeOne::endpoint_for(r).to_string()));
            }
        }
        #[cfg(feature = "temporal")]
        if let Some(pool) = &self.temporal {
            for r in pool.regions() {
                targets.push(KeepAliveTarget::new("Temporal", Temporal::endpoint_for(r).to_string()));
            }
        }
        #[cfg(feature = "zeroslot")]
        if let Some(pool) = &self.zeroslot {
            for r in pool.regions() {
                targets.push(KeepAliveTarget::new("ZeroSlot", ZeroSlot::endpoint_for(r).to_string()));
            }
        }
        #[cfg(feature = "nextblock")]
        if let Some(pool) = &self.nextblock {
            for r in pool.regions() {
                targets.push(KeepAliveTarget::new("NextBlock", NextBlock::endpoint_for(r).to_string()));
            }
        }
        #[cfg(feature = "stellium")]
        if let Some(pool) = &self.stellium {
            for r in pool.regions() {
                targets.push(KeepAliveTarget::new("Stellium", Stellium::endpoint_for(r).to_string()));
            }
        }
        #[cfg(feature = "syncro")]
        if let Some(pool) = &self.syncro {
            for r in pool.regions() {
                targets.push(KeepAliveTarget::new("Syncro", Syncro::regional_base(r).to_string()));
            }
        }

        // 不同目标地区可能退化到同一个 endpoint（该平台在那一带没有机房），
        // 按 URL 去重，避免同一个 origin 被重复 ping。
        targets.retain(|t| !t.url.trim().is_empty());
        let mut seen = std::collections::HashSet::new();
        targets.retain(|t| seen.insert(t.url.clone()));
        targets
    }

    /// 启动后台保活任务：每个已注册 HTTP 端点一个独立循环（间隔 / 错峰逻辑在
    /// `sol_tx_send::platform_clients::run_keep_alive`）。
    ///
    /// 用法：在全局 `init()` 里 `builder.build()` 之后调一次即可。返回值可忽略；
    /// 想停掉保活就 `handle.abort()`。
    pub fn spawn_keep_alive(&self) -> tokio::task::JoinHandle<()> {
        let targets = self.keep_alive_targets();
        log::info!("[TxDispacher] keep-alive 覆盖 {} 个 HTTP endpoint", targets.len());
        tokio::spawn(run_keep_alive(targets))
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

    /// 只读一次 leader 缓存，派生本次发送的全部决策。
    ///
    /// 这是发送路径唯一允许调用 `oracle.leader_at()` 的地方；[`Self::resolve_route`] /
    /// [`Self::ingress_for`] 等公开查询也复用它，避免同一 slot 多次读取。
    ///
    /// `leader_at` 是同步内存缓存读（见 [`sol_slot_leader::SlotLeaderCache`]），
    /// 不会在这里查 DB；DB 数据由 oracle 的后台刷新任务预先载入。
    pub fn plan(&self, target_slot: u64) -> SendPlan {
        let info = self.oracle.leader_at(target_slot);
        let leader_region = info
            .as_ref()
            .and_then(|i| i.region.as_deref())
            .map(region_plan::geo_to_region)
            .unwrap_or(Region::Unknown);
        SendPlan {
            slot: target_slot,
            route: Self::route_for(info.as_ref()),
            fifo: Self::fifo_for(info.as_ref()),
            leader_region,
            ingress: self.region_profile.ingress_for(leader_region),
            leader: info,
        }
    }

    /// 纯函数：`LeaderInfo` → route（不查 oracle）。
    fn route_for(info: Option<&LeaderInfo>) -> SendRoute {
        if let Some(info) = info {
            // 1. 先按具体 leader pubkey 匹配 tip-only 白名单
            if let Some(pk) = info.leader_pubkey() {
                if TIP_ONLY_LEADERS.contains(pk) {
                    return SendRoute::TipOnly;
                }
            }
            // 2. P2P.org Turbo：在 fallback 广播之外追加 Syncro 通道
            if info.is_p2p_turbo() {
                return SendRoute::P2pTurbo;
            }
            // 3. 再按客户端类型匹配
            if info.is_harmonic() {
                return SendRoute::Harmonic;
            }
            if info.is_jito() {
                return SendRoute::Jito;
            }
        }
        SendRoute::Fallback
    }

    /// 纯函数：`LeaderInfo` → 是否 FIFO（不查 oracle）。
    fn fifo_for(info: Option<&LeaderInfo>) -> bool {
        if let Some(info) = info {
            if let (Some(pk), Some(ctid)) = (info.leader_pubkey().copied(), info.client_type_id) {
                return FIFO_LEADERS.contains(&(pk, ctid));
            }
        }
        false
    }

    /// 本次发送应使用哪个入口地区的 client。
    ///
    /// `leader 地区 ∈ fanout` → 用该地区（就近）；否则回退 `own_region`
    /// （跨洲 leader 由平台内部路由）。
    ///
    /// ⚠️ 这是一次**独立**的缓存读取（`plan` 会顺带算 route/fifo）。发送路径
    /// 内部不再调它，而是复用同一个 [`SendPlan`]。
    pub fn ingress_for(&self, slot: u64) -> Region {
        self.plan(slot).ingress
    }

    /// 查询当前 slot 的路由决策（不发送）。
    /// 查询 `target_slot` 的 leader 类型并返回路由决策。
    /// 调用方自行决定传当前 slot 还是 current_slot + N。
    ///
    /// ⚠️ 与 `ingress_for` 相同：这是独立查询；发送路径内部复用 [`SendPlan`]。
    pub fn resolve_route(&self, target_slot: u64) -> SendRoute {
        self.plan(target_slot).route
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
    ///
    /// ⚠️ **Temporal 只在 Harmonic 场景吃指定 tip**（×0.9）；Jito / Fallback /
    /// P2pTurbo / TipOnly / cheap 等场景 Temporal 一律只给平台最低 tip
    /// —— 见 `strategy::TEMPORAL_TIP_NON_HARMONIC`。
    pub async fn send(
        &self,
        ixs: &[solana_sdk::instruction::Instruction],
        ctx: &SendContext,
        target_slot: u64,
        tip_strategy: Option<TipStrategy>,
        cu: (Option<u32>, Option<u64>),
        confirm_timeout_secs: u64,
    ) -> Result<(solana_sdk::signature::Signature, grpc_client::TransactionFormat), TxConfirmError> {
        let plan = self.plan(target_slot);
        log_send_plan("send", &plan);
        log::info!(
            "[TxDispacher] send mode=v0 tip_strategy={:?} cu={:?} timeout={}s",
            tip_strategy,
            cu,
            confirm_timeout_secs
        );
        // FIFO leader：tip=None（下游折成平台最低 tip）、cu_limit 保留、cu_price=None（不参与 cu_price 竞价）。
        if plan.fifo {
            return with_plan_ctx(&plan, strategy::dispatch(self, ixs, ctx, plan.route, None, (cu.0, None), confirm_timeout_secs))
                .await
                .map_err(into_tx_confirm_err);
        }
        with_plan_ctx(&plan, strategy::dispatch(self, ixs, ctx, plan.route, tip_strategy, cu, confirm_timeout_secs))
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
        log::info!(
            "[TxDispacher] send_cheap mode=cheap ingress={} tip_strategy={:?} cu={:?} timeout={}s",
            self.region_profile.own_region,
            tip_strategy,
            cu,
            confirm_timeout_secs
        );
        crate::region_pool::with_region(self.region_profile.own_region, strategy::dispatch_cheap(self, ixs, ctx, tip_strategy, cu, confirm_timeout_secs))
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
        let plan = self.plan(target_slot);
        log_send_plan("send_tip_only", &plan);
        log::info!(
            "[TxDispacher] send_tip_only mode=tip_only tip_floor={} cu_limit={} timeout={}s",
            min_tip_floor,
            cu_limit,
            confirm_timeout_secs
        );
        with_plan_ctx(&plan, strategy::dispatch_tip_only(self, ixs, ctx, plan.route, min_tip_floor, cu_limit, confirm_timeout_secs))
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
        let plan = self.plan(target_slot);
        log_send_plan("send_with_cost", &plan);
        log::info!(
            "[TxDispacher] send_with_cost mode=cost cost={} cu_limit={} tip_rate={} timeout={}s",
            config.cost_amount,
            config.cu_limit,
            config.tip_rate,
            confirm_timeout_secs
        );
        // FIFO leader：不参与 tip/cu_price 竞价（靠先到先得），cost 不生效，
        // 只保留 cu_limit，tip=None、cu_price=None。
        if plan.fifo {
            return with_plan_ctx(
                &plan,
                strategy::dispatch(
                    self,
                    ixs,
                    ctx,
                    plan.route,
                    None,
                    (Some(config.cu_limit), None),
                    confirm_timeout_secs,
                ),
            )
            .await
            .map_err(into_tx_confirm_err);
        }
        with_plan_ctx(&plan, strategy::dispatch_with_cost(self, ixs, ctx, plan.route, config, confirm_timeout_secs))
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
        log::info!(
            "[TxDispacher] send_cheap_v1 mode=cheap_v1 ingress={} tip_strategy={:?} config={:?} timeout={}s",
            self.region_profile.own_region,
            tip_strategy,
            config,
            confirm_timeout_secs
        );
        crate::region_pool::with_region(self.region_profile.own_region, strategy::dispatch_cheap_v1(self, ixs, ctx, tip_strategy, config, confirm_timeout_secs))
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
        let plan = self.plan(target_slot);
        log_send_plan("send_v1", &plan);
        match cfg.mode {
            SpendMode::FixedTip { .. } => {
                let tip_strategy = cfg.as_tip_strategy();
                // 保底 gas：只在 Fallback 的 tip 竞价腿落地（其余 mode 忽略）
                let tip_leg_gas = cfg.gas_opt();
                log::info!(
                    "[TxDispacher] send_v1 mode=fixed_tip tip_strategy={:?} gas={:?} fifo={} cu_limit={} loaded_accounts_data_size_limit={:?} heap_size={:?} timeout={}s",
                    tip_strategy,
                    tip_leg_gas,
                    plan.fifo,
                    cfg.cu_limit,
                    cfg.loaded_accounts_data_size_limit,
                    cfg.heap_size,
                    confirm_timeout_secs
                );
                // FIFO leader：**tip 原样给，但不给 gas**（gas 固定 0）。
                //
                // ⚠️ 2026-09-23 策略组改了 FIFO 的定义 —— 旧口径是「不参与竞价」
                //    （tip=None），现在改成「照常给 tip，只是不额外付 gas」。
                with_plan_ctx(
                    &plan,
                    strategy::dispatch_v1(
                        self,
                        ixs,
                        ctx,
                        plan.route,
                        tip_strategy,
                        cfg.as_tip_config(),
                        if plan.fifo { None } else { tip_leg_gas },
                        confirm_timeout_secs,
                    ),
                )
                .await
                .map_err(into_tx_confirm_err)
            }
            SpendMode::Cost(cost) => {
                log::info!(
                    "[TxDispacher] send_v1 mode=cost cost={cost} cu_limit={} tip_rate={} timeout={}s",
                    cfg.cu_limit,
                    cfg.tip_rate,
                    confirm_timeout_secs
                );
                self.send_with_cost_v1(ixs, ctx, plan, cfg.as_cost_tx_config(), confirm_timeout_secs)
                    .await
            }
        }
    }

    /// V1 版 cost 语义发送（**内部用**，外部统一走 [`Self::send_v1`]）。
    ///
    /// 单预算 `cost`，按 route 分派到各 `*_cost_mode_v1`，由后者决定落地通道。
    ///
    /// `plan` 由 `send_v1` 传入 —— 这里**不再**重新查 oracle / 重算 route/FIFO/ingress，
    /// 保证同一次发送只读一次 leader 缓存。
    pub(crate) async fn send_with_cost_v1(
        &self,
        ixs: &[solana_sdk::instruction::Instruction],
        ctx: &SendContext,
        plan: SendPlan,
        config: CostTxConfig,
        confirm_timeout_secs: u64,
    ) -> Result<(solana_sdk::signature::Signature, grpc_client::TransactionFormat), TxConfirmError> {
        // FIFO leader：不参与竞价，cost 不生效（priority_fee=None 且不给 tip）
        if plan.fifo {
            return with_plan_ctx(
                &plan,
                strategy::dispatch_v1(
                    self,
                    ixs,
                    ctx,
                    plan.route,
                    None,
                    // FIFO 下只需 cu_limit，priority_fee 置 None
                    sol_tx_send::platform_clients::V1TxConfig {
                        priority_fee: None,
                        compute_unit_limit: Some(config.cu_limit),
                        ..Default::default()
                    },
                    None, // 保底 gas：FIFO 下不给
                    confirm_timeout_secs,
                ),
            )
            .await
            .map_err(into_tx_confirm_err);
        }
        with_plan_ctx(&plan, strategy::dispatch_with_cost_v1(self, ixs, ctx, plan.route, config, confirm_timeout_secs))
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
    use solana_sdk::{hash::Hash, pubkey, signature::Keypair};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // ── 测试用 MockOracle ─────────────────────────────────────────────────────

    /// 可以指定 is_harmonic 返回值的 mock oracle。
    struct MockOracle {
        harmonic: bool,
        name: Option<&'static str>,
        region: Option<&'static str>,
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
                region: self.region.map(str::to_string),
            })
        }
    }

    /// 记录 `leader_at` 调用次数的 oracle，用来锁住「一次发送只读一次缓存」。
    struct CountingOracle {
        calls: AtomicUsize,
        info: Option<LeaderInfo>,
    }

    impl CountingOracle {
        fn new(info: Option<LeaderInfo>) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                info,
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::Relaxed)
        }
    }

    impl SlotOracle for CountingOracle {
        fn leader_at(&self, _slot: u64) -> Option<LeaderInfo> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.info.clone()
        }
    }

    /// 每次调用返回不同快照；用来证明 `plan` 的 route/FIFO/ingress 全部来自同一次读取。
    struct SequenceOracle {
        calls: AtomicUsize,
        first: LeaderInfo,
        second: LeaderInfo,
    }

    impl SlotOracle for SequenceOracle {
        fn leader_at(&self, _slot: u64) -> Option<LeaderInfo> {
            let n = self.calls.fetch_add(1, Ordering::Relaxed);
            Some(if n == 0 { self.first.clone() } else { self.second.clone() })
        }
    }

    fn info(
        client_type: ClientType,
        name: Option<&str>,
        leader: Option<solana_sdk::pubkey::Pubkey>,
        client_type_id: Option<u16>,
        region: Option<&str>,
    ) -> LeaderInfo {
        LeaderInfo {
            client_type,
            name: name.map(str::to_string),
            leader,
            client_type_id,
            region: region.map(str::to_string),
        }
    }

    // ── SendPlan：每次发送只读一次 leader 缓存 ────────────────────────────────

    /// `plan` 只调一次 `leader_at`，并一次性派生 route / FIFO / leader_region / ingress。
    #[test]
    fn plan_reads_leader_cache_once() {
        let oracle = CountingOracle::new(Some(info(
            ClientType::HarmonicAgave,
            Some("Harmonic-SG"),
            None,
            None,
            Some("The Netherlands / Amsterdam"),
        )));
        let profile = RegionProfile {
            own_region: Region::Frankfurt,
            fanout: vec![Region::Frankfurt, Region::Amsterdam],
        };
        let d = TxDispacher::builder(oracle).region_profile(profile).build();

        let plan = d.plan(123);
        assert_eq!(plan.slot, 123);
        assert_eq!(plan.route, SendRoute::Harmonic);
        assert_eq!(plan.leader_region, Region::Amsterdam);
        assert_eq!(plan.ingress, Region::Amsterdam);
        assert!(!plan.fifo);
        assert_eq!(d.oracle.calls(), 1, "一次 plan 只能读一次 oracle");
    }

    /// route 与 ingress 必须来自**同一条** LeaderInfo；不能各查一次。
    #[test]
    fn plan_uses_one_snapshot_for_route_and_ingress() {
        let oracle = SequenceOracle {
            calls: AtomicUsize::new(0),
            first: info(
                ClientType::HarmonicAgave,
                Some("Harmonic-SG"),
                None,
                None,
                Some("The Netherlands / Amsterdam"),
            ),
            second: info(ClientType::JitoLabs, None, None, None, Some("Japan / Tokyo")),
        };
        let profile = RegionProfile {
            own_region: Region::Frankfurt,
            fanout: vec![Region::Frankfurt, Region::Amsterdam],
        };
        let d = TxDispacher::builder(oracle).region_profile(profile).build();

        let plan = d.plan(1);
        // 如果 route / ingress 分开查，第二次会读到 Jito/Tokyo，ingress 会变成 own。
        assert_eq!(plan.route, SendRoute::Harmonic);
        assert_eq!(plan.leader_region, Region::Amsterdam);
        assert_eq!(plan.ingress, Region::Amsterdam);
        assert_eq!(d.oracle.calls.load(Ordering::Relaxed), 1);
    }

    /// FIFO 与 route 也来自同一个快照。
    #[test]
    fn plan_detects_fifo_from_the_same_snapshot() {
        let oracle = CountingOracle::new(Some(info(
            ClientType::Agave,
            None,
            Some(pubkey!("q9XWcZ7T1wP4bW9SB4XgNNwjnFEJ982nE8aVbbNuwot")),
            Some(1),
            Some("Germany / Frankfurt am Main"),
        )));
        let profile = RegionProfile {
            own_region: Region::Frankfurt,
            fanout: vec![Region::Frankfurt, Region::Amsterdam],
        };
        let d = TxDispacher::builder(oracle).region_profile(profile).build();

        let plan = d.plan(1);
        assert!(plan.fifo, "q9... + client_type_id=1 应命中 FIFO");
        assert_eq!(plan.route, SendRoute::Fallback);
        assert_eq!(plan.ingress, Region::Frankfurt);
        assert_eq!(d.oracle.calls(), 1);
    }

    /// 公开查询 `resolve_route` / `ingress_for` 各自也只读一次缓存。
    #[test]
    fn public_queries_each_read_cache_once() {
        let d = TxDispacher::builder(CountingOracle::new(None)).build();
        let _ = d.resolve_route(1);
        assert_eq!(d.oracle.calls(), 1);
        let _ = d.ingress_for(1);
        assert_eq!(d.oracle.calls(), 2);
    }

    fn empty_ctx() -> SendContext {
        SendContext::new(
            Arc::new(Keypair::new()),
            HashParam::Blockhash(Hash::new_unique()),
            Arc::new(Vec::new()),
        )
    }

    /// V0 发送：一次 send 只读一次 leader 缓存。
    #[tokio::test]
    async fn send_reads_leader_cache_once() {
        let d = TxDispacher::builder(CountingOracle::new(None)).build();
        let _ = d
            .send(&[], &empty_ctx(), 1, None, (Some(100_000), None), 0)
            .await;
        assert_eq!(d.oracle.calls(), 1);
    }

    /// V1 Cost 路径历史上会 `send_v1` + `send_with_cost_v1` 各查一轮；现在必须只查一次。
    #[tokio::test]
    async fn cost_send_reads_leader_cache_once() {
        let d = TxDispacher::builder(CountingOracle::new(None)).build();
        let _ = d
            .send_v1(&[], &empty_ctx(), 1, SpendConfig::cost(1_000, 100_000), 0)
            .await;
        assert_eq!(d.oracle.calls(), 1, "Cost 路径不能二次查 oracle");
    }

    /// TipOnly 路径同样只读一次。
    #[tokio::test]
    async fn tip_only_send_reads_leader_cache_once() {
        let d = TxDispacher::builder(CountingOracle::new(None)).build();
        let _ = d
            .send_tip_only(&[], &empty_ctx(), 1, 5_000, 100_000, 0)
            .await;
        assert_eq!(d.oracle.calls(), 1);
    }

    // ── resolve_route 单元测试 ────────────────────────────────────────────────

    #[test]
    fn noop_oracle_always_fallback() {
        let d = TxDispacher::builder(NoopOracle).build();
        assert_eq!(d.resolve_route(422_000_000), SendRoute::Fallback);
    }

    // ── leader 地区 -> 入口地区 ────────────────────────────────────────────────

    /// leader 在 fanout 内 -> 用该地区；跨洲 -> 回退本机。
    #[test]
    fn ingress_follows_leader_region_within_fanout() {
        let profile = RegionProfile {
            own_region: Region::Frankfurt,
            fanout: vec![Region::Frankfurt, Region::Amsterdam],
        };
        let d = TxDispacher::builder(MockOracle {
            harmonic: false,
            name: None,
            region: Some("The Netherlands / Amsterdam"),
        })
        .region_profile(profile.clone())
        .build();
        assert_eq!(d.ingress_for(1), Region::Amsterdam);

        // 跨洲 leader（东京不在 fanout 里）-> 本机
        let d = TxDispacher::builder(MockOracle {
            harmonic: false,
            name: None,
            region: Some("Japan / Tokyo"),
        })
        .region_profile(profile)
        .build();
        assert_eq!(d.ingress_for(1), Region::Frankfurt);
    }

    /// 没有地区数据 / 无法识别 -> 回退本机，绝不乱猜。
    #[test]
    fn ingress_falls_back_to_own_without_region_data() {
        let profile = RegionProfile {
            own_region: Region::Frankfurt,
            fanout: vec![Region::Frankfurt, Region::Amsterdam],
        };
        let d = TxDispacher::builder(MockOracle {
            harmonic: false,
            name: None,
            region: None,
        })
        .region_profile(profile)
        .build();
        assert_eq!(d.ingress_for(1), Region::Frankfurt);
    }

    // ── keep-alive 目标收集 ───────────────────────────────────────────────────

    /// 没有注入任何客户端时，保活列表必须为空（不能去探没注册的通道）。
    #[test]
    fn no_clients_means_no_keep_alive_targets() {
        let d = TxDispacher::builder(NoopOracle).build();
        assert!(d.keep_alive_targets().is_empty());
    }

    /// 注入了某个平台，它就必须出现在保活列表里。
    #[cfg(feature = "astralane")]
    #[test]
    fn injected_platform_appears_in_keep_alive_targets() {
        use sol_tx_send::platform_clients::astralane::Astralane;

        let d = TxDispacher::builder(NoopOracle)
            .astralane(Astralane::init_with("test-key", Region::Frankfurt))
            .build();
        let targets = d.keep_alive_targets();
        assert!(
            targets.iter().any(|t| t.name == "Astralane"),
            "注入 Astralane 后保活列表应包含它，实际: {targets:?}"
        );
        // 没注入的平台不该出现（这里只断言一个代表性平台）
        #[cfg(feature = "jito")]
        assert!(!targets.iter().any(|t| t.name == "Jito"));
    }

    /// 多地区池：保活必须覆盖**所有**已注册地区，而不只是 own。
    #[cfg(feature = "astralane")]
    #[test]
    fn keep_alive_covers_every_pooled_region() {
        use sol_tx_send::platform_clients::astralane::Astralane;

        let profile = RegionProfile {
            own_region: Region::Frankfurt,
            fanout: vec![Region::Frankfurt, Region::Amsterdam, Region::London],
        };
        let d = TxDispacher::builder(NoopOracle)
            .astralane_pool(RegionPool::build(&profile, |r| Astralane::init_with("k", r)))
            .build();

        let targets = d.keep_alive_targets();
        assert_eq!(targets.len(), 3, "三个地区三个 URL: {targets:?}");
        for needle in ["fr.gateway", "ams.gateway", "lon.gateway"] {
            assert!(
                targets.iter().any(|t| t.url.contains(needle)),
                "缺少 {needle}: {targets:?}"
            );
        }
    }

    /// 启动日志的数据源：`channels()` 必须报告平台、传输方式和实际地区。
    #[cfg(feature = "astralane")]
    #[test]
    fn channels_reports_platform_transport_and_regions() {
        use sol_tx_send::platform_clients::astralane::Astralane;

        let profile = RegionProfile {
            own_region: Region::Frankfurt,
            fanout: vec![Region::Frankfurt, Region::Amsterdam],
        };
        let d = TxDispacher::builder(NoopOracle)
            .region_profile(profile.clone())
            .astralane_pool(RegionPool::build(&profile, |r| Astralane::init_with("k", r)))
            .build();

        let channels = d.channels();
        let a = channels.iter().find(|c| c.platform == "Astralane").expect("应有 Astralane");
        assert_eq!(a.transport, "HTTP");
        assert_eq!(a.regions, vec![Region::Frankfurt, Region::Amsterdam]);
    }

    /// 退化到同一 endpoint 的目标地区必须去重，否则同一个 origin 被 ping 多次。
    #[cfg(feature = "temporal")]
    #[test]
    fn keep_alive_dedups_degraded_regions() {
        use sol_tx_send::platform_clients::temporal::Temporal;

        let profile = RegionProfile {
            own_region: Region::Frankfurt,
            fanout: vec![Region::Frankfurt, Region::Limburg, Region::Lithuania],
        };
        let d = TxDispacher::builder(NoopOracle)
            .temporal_pool(RegionPool::build(&profile, |r| Temporal::init_with("k", r)))
            .build();

        let targets = d.keep_alive_targets();
        // Temporal 没有 Limburg / Lithuania 机房，两者都退化到 Amsterdam
        let temporal: Vec<_> = targets.iter().filter(|t| t.name == "Temporal").collect();
        assert_eq!(temporal.len(), 2, "应为 FRA + AMS 两条: {targets:?}");
    }

    /// LandX 注入后：出现在保活列表，且用官方 `/ping` 健康检查。
    #[cfg(feature = "landx")]
    #[test]
    fn injected_landx_uses_ping_endpoint() {
        use sol_tx_send::platform_clients::landx::LandX;

        let d = TxDispacher::builder(NoopOracle)
            .landx(LandX::init_with("test-key", Region::Frankfurt))
            .build();
        let targets = d.keep_alive_targets();
        assert!(
            targets.iter().any(|t| t.name == "LandX" && t.url.ends_with("/ping")),
            "LandX 保活应走 /ping，实际: {targets:?}"
        );
    }

    /// Helius 注入后必须进入保活列表，且探测官方 `GET <origin>/ping`，
    /// 不能拿提交端点 `.../fast` 去探。
    #[cfg(feature = "helius")]
    #[test]
    fn injected_helius_uses_official_ping_endpoint() {
        use sol_tx_send::platform_clients::helius_max::HeliusMax;

        let d = TxDispacher::builder(NoopOracle)
            .helius_max(HeliusMax::init_with("test-key", Region::Frankfurt))
            .build();
        let targets = d.keep_alive_targets();
        let helius: Vec<_> = targets.iter().filter(|t| t.name == "Helius").collect();
        assert_eq!(helius.len(), 1, "应只有一条 FRA 探测: {targets:?}");
        assert_eq!(helius[0].url, "http://fra-sender.helius-rpc.com/ping");
    }

    /// Max 与 SWQOS 共用 origin：同一地区只保留一条 `/ping` 探测；
    /// 不同地区仍各有一条。
    #[cfg(feature = "helius")]
    #[test]
    fn helius_max_and_swqos_share_one_ping_per_region() {
        use sol_tx_send::platform_clients::helius_max::HeliusMax;
        use sol_tx_send::platform_clients::helius_swqos::HeliusSwqos;

        let profile = RegionProfile {
            own_region: Region::Frankfurt,
            fanout: vec![Region::Frankfurt, Region::Amsterdam],
        };
        let d = TxDispacher::builder(NoopOracle)
            .region_profile(profile.clone())
            .helius_max_pool(RegionPool::build(&profile, |r| HeliusMax::init_with("k", r)))
            .helius_swqos_pool(RegionPool::build(&profile, |r| HeliusSwqos::init_with("k", r)))
            .build();

        let helius: Vec<_> = d
            .keep_alive_targets()
            .into_iter()
            .filter(|t| t.name == "Helius")
            .collect();
        assert_eq!(helius.len(), 2, "FRA + AMS 各一条: {helius:?}");
        for t in &helius {
            assert!(t.url.ends_with("/ping"), "{t:?}");
            assert!(!t.url.contains("/fast"), "{t:?}");
            assert!(!t.url.contains("swqos_only"), "{t:?}");
        }
    }

    #[test]
    fn harmonic_client_type_routes_to_harmonic() {
        let d = TxDispacher::builder(MockOracle {
            harmonic: true,
            name: None,
            region: None,
        })
        .build();
        assert_eq!(d.resolve_route(100), SendRoute::Harmonic);
    }

    #[test]
    fn non_harmonic_client_type_routes_to_fallback() {
        let d = TxDispacher::builder(MockOracle {
            harmonic: false,
            name: None,
            region: None,
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
            region: None,
        };
        let d = TxDispacher::builder(oracle).build();
        assert_eq!(d.resolve_route(100), SendRoute::Harmonic);
    }

    #[test]
    fn p2p_turbo_name_routes_to_p2p_turbo() {
        let oracle = MockOracle {
            harmonic: false,
            name: Some("P2P.org Turbo"),
            region: None,
        };
        let d = TxDispacher::builder(oracle).build();
        assert_eq!(d.resolve_route(100), SendRoute::P2pTurbo);
    }

    #[test]
    fn p2p_turbo_has_priority_over_harmonic() {
        // 即使是 Harmonic 客户端，只要名字是 P2P.org Turbo 就走 P2pTurbo
        let oracle = MockOracle {
            harmonic: true,
            name: Some("P2P.org Turbo"),
            region: None,
        };
        let d = TxDispacher::builder(oracle).build();
        assert_eq!(d.resolve_route(100), SendRoute::P2pTurbo);
    }

    #[test]
    fn p2p_without_turbo_stays_fallback() {
        let oracle = MockOracle {
            harmonic: false,
            name: Some("P2P.org"),
            region: None,
        };
        let d = TxDispacher::builder(oracle).build();
        assert_eq!(d.resolve_route(100), SendRoute::Fallback);
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

    /// `Exact` 是**严格值**：平台最低 tip 完全不参与，缩放也不参与。
    #[test]
    fn tip_strategy_exact_ignores_platform_min_and_scaling() {
        // 低于平台最低 → 原样，不被抬高
        assert_eq!(TipStrategy::Exact(100_000).compute(1_000_000), 100_000);
        // 高于平台最低 → 原样，也不被压缩
        assert_eq!(TipStrategy::Exact(5_000_000).compute(1_000_000), 5_000_000);
        assert_eq!(TipStrategy::Exact(0).compute(1_000_000), 0);
        // scaled 忽略缩放（Harmonic 路由的 ×0.9 不会改严格值）
        assert_eq!(TipStrategy::Exact(100_000).scaled(0.9).compute(1_000_000), 100_000);
    }

    /// `FixedTip { tip: Some(n) }` 必须落成 `Exact(n)`；`None` / `Cost` 不产生 tip 策略。
    #[test]
    fn fixed_tip_maps_to_exact() {
        let cfg = SpendConfig::fixed_tip(Some(100_000), Some(100_000), 135_000);
        assert!(matches!(cfg.as_tip_strategy(), Some(TipStrategy::Exact(100_000))));

        let none = SpendConfig::fixed_tip(None, None, 135_000);
        assert!(none.as_tip_strategy().is_none());

        let cost = SpendConfig::cost(1_000_000, 135_000);
        assert!(cost.as_tip_strategy().is_none());
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
            // feature = "syncro"（P2P.org Turbo 出块时追加的通道，带 key 走私有端点）
            .syncro(sol_tx_send::platform_clients::syncro::Syncro::init_with(
                std::env::var("SYNCRO_API_KEY").unwrap_or_default(),
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
