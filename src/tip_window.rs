//! P2：`send_tip_window` —— 一段「时段窗口」内的**均匀铺量**发送。
//!
//! # 模型（v2：按配额铺满时长，不再按「固定 rounds × interval」）
//!
//! 输入是三个数：**总笔数** `count`、**持续时长** `duration`、**每平台配额权重**（默认表）。
//! 输出是一条扁平的发送计划：
//!
//! 1. 配额：把 `count` 笔分给各平台，**优先保重点名的重平台**（默认 nodeone / astralane / landx 各 5 笔）；
//!    预算不够时先砍填充平台，够的时候才给填充平台各 1 笔（详见 `allocate`）；
//! 2. 轮次：配额最大的平台决定轮数，第 k 轮发"配额 > k"的那些平台；
//! 3. 时刻：所有笔**均匀铺开**，第 i 笔 = `t0 + step * i`，`step = duration / count`；
//!    `count` 太多导致 `step < min_gap` 时**不报错**：用 `min_gap` 拉长窗口并 warn；
//! 4. 换签名：第 k 轮的 tip = `base + k`（用户定的做法，见下）；
//! 5. 通道去重：同一个 provider 注册了多条通道时**只发优先那条** —— 支持 QUIC 的走 QUIC
//!    （astralane / nodeone / everstake / flash_block），LandX 走 HTTP（它的 HTTP 覆盖更全）；
//! 6. 白名单：真正会发的只有 **3 个重家族 + helius\* / flashblock\* / jito / harmonic**，
//!    其余注册了也不发（配额 0）。默认合计 = 15 + 5 = 20 笔。
//!
//! 例：nodeone / astralane / landx 各 5 笔，helius…harmonic 各 1 笔 = 20 笔；
//! count=20 → 5 轮，前 8 笔把 8 个平台各发一遍，之后 4 轮只有 3 个重平台。
//!
//! # 换签名：tip = base + 轮次（不靠 memo）
//!
//! 同一平台相邻两轮的 tip 差 1 lamport ⇒ 字节不同 ⇒ **签名不同**。跨轮换签的意义见
//! 计划文档 `3.4：某一笔上链失败后，它的签名就永久作废，后续轮次必须换签才有效。
//! memo 仍会写 `{tag}-r{k}`（如果给了 `memo_tag`），那只是**兜底**（平台 tip 上限可能吃掉
//! 这 1 lamport 的差异，比如 HeliusSwqos 的 0.0002 SOL 封顶），且卖出侧靠
//! `contains(MEMO_TAG)` 判"本机"，加后缀不影响。
//!
//! # 与现有入口的差别
//!
//! | 入口 | 问题 |
//! |---|---|
//! | `send_v1(FixedTip)` | 走 route 分流；both 腿平台双发；`confirm_tx` 遇首个失败即终止 |
//! | `tip_only_auto_v1` | 强制注入 ~80k lamports 的 `priority_fee`，与「只给 tip」口径冲突 |
//! | `fire_all_parallel_v1` | build + send 揉在一个 spawn 里，做不到「先全部 build、注册 confirm、再按节奏发」 |
//!
//! # 事件顺序（顺序本身就是正确性）
//!
//! 全部 **build** 完拿到签名 -> `tx_result_channel::subscribe()` -> `spawn confirm_set` -> 按计划发送。
//! 签名在发送前就全部已知，不存在「先发出去、再挂订阅」的丢事件窗口。
//!
//! # 刻意不做的事
//!
//! - **不做 route 分流**：窗口跨 2~3 个 slot，逐 slot 换 route 收益小于复杂度；
//!   入口地区固定 `own_region`（与 `send_cheap_v1` 一致），不读 leader 缓存。
//! - **不给 priority_fee**（恒为 `None`），只给 tip。
//! - **不做 429 重试**：失败只记日志（由 deadline 兜底）。
//!
//! # 安全契约（必读）
//!
//! **本接口不提供任何「只成交一次」保证。**
//! 调用方必须自己在指令里放链上互斥闸（本场景 = 非幂等 `create_ata`）和落地窗口闸
//! （本场景 = `check_slot`）。去掉这两条就会重复买入。
//!
//! 同时**要求 `SendContext` 是 blockhash 模式**：nonce 模式下同一账户的多轮 build
//! 会互相冲突（同一 nonce 只能 advance 一次），直接返回 `TipWindowError::NonceContext`。

use ahash::AHashSet as HashSet;
use nonce_cache::{
    ConfirmSetOutcome, SigErr, TxConfirmError, confirm_set,
    tx_result_channel::{self, TxResultEvent},
};
use sol_slot_leader::SlotOracle;
use sol_tx_send::platform_clients::{BuildTx, BuildV1Tx, HashParam, Region, SendTx, V1TxConfig};
use solana_sdk::{
    instruction::Instruction,
    signature::{Keypair, Signature},
    transaction::VersionedTransaction,
};
use std::{
    collections::HashMap,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use crate::{
    SendContext, TxDispacher,
    channel_pool::{ChannelPool, KeyProfile, with_send_scope},
};

/// 日志前缀：`[tw]` = tip window（与业务层的 `[ll]` 区分）。
const LOG: &str = "[tw]";

// -- 窗口计划 ------------------------------------------------------------------

/// 一个「时段窗口」内的均匀铺量计划。
#[derive(Debug, Clone)]
pub struct TipWindowPlan {
    /// 基础 tip（lamports）。第 k 轮实际 tip = `base + k`（用来换签名）。
    ///
    /// - `Some(n)`：base 是 n，**不抬不缩**（平台自己的 `max_tip_amount()` 上限仍生效）；
    /// - `None`：base 是该平台最低 tip（`+k` 照加）。
    pub cost: Option<u64>,
    /// CU 上限（必填：V1 下 `None`/0 会被链上按 0 处理 -> 交易直接失败）。
    pub cu_limit: u32,
    /// **目标总笔数**。调用方指定更少就按权重均匀裁剪，更多就按权重均匀追加。
    pub count: usize,
    /// **期望持续时长**：所有笔均匀铺在这段时间里（`step = count / duration`）。
    pub duration: Duration,
    /// 相邻两笔的最小间隔。`duration` 装不下 `count` 笔时用把它拉长窗口（不报错，只 warn）。
    pub min_gap: Duration,
    /// 最后一笔发出后，再等确认多久（`deadline = span + tail`）。
    pub tail: Duration,
    /// memo 基础标签；每轮写成 `{tag}-r{k}`。`None` 就不写 memo。
    pub memo_tag: Option<String>,
    /// 每平台配额**覆盖**（平台名，大小写不敏感的子串匹配），优先级高于 `default_quota`。
    pub quota_overrides: Vec<(String, usize)>,
}

impl TipWindowPlan {
    /// 默认总笔数（计划文档 `7 `HIGHFEE_LL_COUNT`）。
    pub const DEFAULT_COUNT: usize = 20;
    /// 默认持续时长（`7 `HIGHFEE_LL_DURATION_MS`）。
    pub const DEFAULT_DURATION: Duration = Duration::from_millis(1200);
    /// 默认最小间隔（`7 `HIGHFEE_LL_MIN_GAP_MS`）。
    pub const DEFAULT_MIN_GAP: Duration = Duration::from_millis(20);
    /// 默认确认尾巴（`7 `HIGHFEE_LL_TAIL_MS`）。
    pub const DEFAULT_TAIL: Duration = Duration::from_millis(300);
    /// 重平台（nodeone / astralane / landx 三系）的默认配额（`7 `HIGHFEE_LL_HEAVY_QUOTA`）。
    pub const DEFAULT_HEAVY_QUOTA: usize = 5;

    /// 最小构造：`cost` + `cu_limit`，其余取默认值。
    pub fn new(cost: Option<u64>, cu_limit: u32) -> Self {
        Self {
            cost,
            cu_limit,
            count: Self::DEFAULT_COUNT,
            duration: Self::DEFAULT_DURATION,
            min_gap: Self::DEFAULT_MIN_GAP,
            tail: Self::DEFAULT_TAIL,
            memo_tag: None,
            quota_overrides: Vec::new(),
        }
    }

    pub fn with_count(mut self, count: usize) -> Self {
        self.count = count;
        self
    }

    pub fn with_duration(mut self, duration: Duration) -> Self {
        self.duration = duration;
        self
    }

    pub fn with_min_gap(mut self, gap: Duration) -> Self {
        self.min_gap = gap;
        self
    }

    pub fn with_tail(mut self, tail: Duration) -> Self {
        self.tail = tail;
        self
    }

    pub fn with_memo_tag(mut self, tag: impl Into<String>) -> Self {
        self.memo_tag = Some(tag.into());
        self
    }

    /// 覆盖某个平台的配额（子串匹配，大小写不敏感）。
    pub fn with_quota(mut self, platform: impl Into<String>, quota: usize) -> Self {
        self.quota_overrides.push((platform.into(), quota));
        self
    }

    /// 第 k 轮写进交易的 memo 片段（换签的兜底手段）。
    fn memo_for(&self, round: u32) -> Option<Vec<String>> {
        self.memo_tag.as_ref().map(|t| vec![t.clone(), format!("r{round}")])
    }

    /// 一组已注册平台的**权重**：默认表 + 覆盖表。
    ///
    /// 默认表按**家族**算：nodeone / astralane / landx 三系各 5 笔（家族总配额，
    /// 家族注册了几个通道就均分几个，见 [`default_weights`]），其余通道各 1 笔。
    fn weights_for(&self, platforms: &[String]) -> Vec<usize> {
        let mut w = default_weights(platforms);
        for (pat, q) in &self.quota_overrides {
            let lower = pat.to_ascii_lowercase();
            for (i, p) in platforms.iter().enumerate() {
                if p.to_ascii_lowercase().contains(&lower) {
                    w[i] = *q;
                }
            }
        }
        w
    }
}

/// **重平台**家族：每家族默认 5 笔（家族内按已注册通道均分）。
pub const HEAVY_FAMILIES: &[&str] = &["nodeone", "astralane", "landx"];

/// **填充平台**家族：每通道默认 1 笔。
///
/// **只有这些通道真的会发，其余注册了也不发**（配额 0）——线上有些通道 key 过期 /
/// 限地区 / 根本不想用，注册着只是没删。要临时开一条就 `with_quota("temporal", 1)`。
///
/// - `"helius"` 同时命中 `HeliusMax` 与 `HeliusSwqos`（两条独立通道，各 1 笔）；
/// - `"flashblock"` 命中 `FlashBlock` / `FlashBlockQuic`（通道去重后只留一条）。
pub const FILLER_FAMILIES: &[&str] = &["helius", "flashblock", "jito", "harmonic"];

/// 平台属于哪个重家族（大小写不敏感的子串匹配）。
pub fn family_of(platform: &str) -> Option<&'static str> {
    let p = platform.to_ascii_lowercase();
    HEAVY_FAMILIES.iter().copied().find(|f| p.contains(f))
}

/// 平台属于哪个填充家族（不在名单里的返回 `None` = 不发）。
pub fn filler_family(platform: &str) -> Option<&'static str> {
    let p = platform.to_ascii_lowercase();
    FILLER_FAMILIES.iter().copied().find(|f| p.contains(f))
}

/// 默认权重表：
///
/// | 平台 | 权重 |
/// |---|---|
/// | nodeone / astralane / landx（重家族） | 5（家族总配额，在已注册通道间均分） |
/// | helius* / flashblock* / jito / harmonic（填充家族） | 1 |
/// | **其他**（everstake / temporal / zeroslot / blockrazor / nextblock / stellium / syncro …） | **0（不发）** |
///
/// # 为什么重平台要按家族算
///
/// 同一个 provider 往往同时注册了 HTTP 与 QUIC（如 `Astralane` + `AstralaneQuic`，
/// 或 `LandX` + `LandXUdp`），它们共用同一份 key/额度。如果**按通道**各给 5 笔，
/// 3 个家族 6 个通道就是 30 笔，"总共 20 笔"的形状直接崩掉。
///
/// 所以家族配额 5 笔在**已注册的通道之间均分**（余数给排在前面的）：
/// 1 个通道 -> 5；2 个通道 -> 3 + 2；3 个通道 -> 2 + 2 + 1。
/// 正常情况下 `prefer_channels` 已经先把同 provider 去重成 1 条，这里只是兜底。
pub fn default_weights(platforms: &[String]) -> Vec<usize> {
    let mut w = vec![0usize; platforms.len()];

    // 填充家族：白名单外的一律 0
    for (i, p) in platforms.iter().enumerate() {
        if filler_family(p).is_some() {
            w[i] = 1;
        }
    }

    // 重家族：配额在已注册通道间均分
    for fam in HEAVY_FAMILIES {
        let idx: Vec<usize> = platforms
            .iter()
            .enumerate()
            .filter(|(_, p)| family_of(p) == Some(*fam))
            .map(|(i, _)| i)
            .collect();
        if idx.is_empty() {
            continue;
        }
        let total = TipWindowPlan::DEFAULT_HEAVY_QUOTA;
        let base = total / idx.len();
        let rem = total % idx.len();
        for (k, &i) in idx.iter().enumerate() {
            w[i] = base + usize::from(k < rem);
        }
    }
    w
}

// -- 调度（纯函数，可单测）------------------------------------------------------

/// 一次窗口的发送计划。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schedule {
    /// 每平台配额（与入参平台列表等长，顺序一致）。
    pub quotas: Vec<usize>,
    /// 轮数 = max(quota)。
    pub rounds: u32,
    /// 总笔数 = Σ quota。
    pub total: usize,
    /// 相邻两笔的间隔。
    pub step: Duration,
    /// 窗口总跨度 = step × total。
    pub span: Duration,
    /// 确认时限 = span + tail。
    pub deadline: Duration,
    /// 是否因为 `step < min_gap` 被拉长（true 时实际 span > duration）。
    pub stretched: bool,
}

/// 把 `count` 笔分配到各平台，**优先保住高配额平台**。
///
/// 规则（"先保我点名的 5 笔"）：
///
/// 1. `w > 1` 的平台是**优先平台**（默认表里 nodeone / astralane / landx，或 `with_quota` 抬高的）；
///    `w == 1` 的是**填空平台**，`w == 0` 的平台**永远不发**；
/// 2. `count <= Σ优先平台权重`：只在优先平台之间按权重分（[`hamilton`]），填空平台一个不发 ——
///    预算不够时宁可让三个重平台各发 3 笔，也不摊成"20 个平台各 1 笔"；
/// 3. `count > Σ优先平台权重`：优先平台先拿满自己的 w，剩下的按注册顺序给填空平台各 1；
/// 4. 还有剩（count 超过自然总数）：在优先平台之间**轮转**加（重平台先吃量）。
///
/// 结果**完全确定**（同样的输入永远同样的配额）。
pub fn allocate(count: usize, weights: &[usize]) -> Vec<usize> {
    let n = weights.len();
    let mut out = vec![0usize; n];
    if n == 0 || count == 0 {
        return out;
    }

    let priority: Vec<usize> = (0..n).filter(|&i| weights[i] > 1).collect();
    let normals: Vec<usize> = (0..n).filter(|&i| weights[i] == 1).collect();
    let demand: usize = priority.iter().map(|&i| weights[i]).sum();

    // (2) 预算喂不满优先平台：只在它们之间按权重分
    if !priority.is_empty() && count <= demand {
        let pw: Vec<usize> = priority.iter().map(|&i| weights[i]).collect();
        let q = hamilton(count, &pw);
        for (k, &i) in priority.iter().enumerate() {
            out[i] = q[k];
        }
        return out;
    }

    // (3) 优先平台拿满
    for &i in &priority {
        out[i] = weights[i];
    }
    let mut left = count.saturating_sub(demand);

    // 填空平台各 1（按注册顺序）
    for &i in &normals {
        if left == 0 {
            break;
        }
        out[i] = 1;
        left -= 1;
    }

    // (4) 还有剩：在"能发量"的平台之间轮转（优先平台在环的前面）
    let ring: Vec<usize> = if priority.is_empty() { normals } else { priority };
    if !ring.is_empty() {
        let mut k = 0usize;
        while left > 0 {
            out[ring[k % ring.len()]] += 1;
            left -= 1;
            k += 1;
        }
    }
    out
}

/// Hamilton 最大余数法：按权重把 `count` 份按比例分给 `weights`，余数按
/// (小数部分 desc, 权重 desc, 下标 asc) 分配，结果**完全确定**。
fn hamilton(count: usize, weights: &[usize]) -> Vec<usize> {
    let n = weights.len();
    let mut out = vec![0usize; n];
    if n == 0 || count == 0 {
        return out;
    }
    let sum: usize = weights.iter().sum();
    if sum == 0 {
        // 全 0 权重：均分（前面几个吃余数），避免除以 0
        let base = count / n;
        let mut left = count;
        for q in out.iter_mut() {
            *q = base.min(left);
            left -= *q;
        }
        return out;
    }

    // floor(w_i * count / sum)，用 u128 防止大数溢出
    let mut assigned = 0usize;
    for (i, w) in weights.iter().enumerate() {
        out[i] = (count as u128 * *w as u128 / sum as u128) as usize;
        assigned += out[i];
    }

    let mut rem: Vec<(u128, usize, usize)> = weights
        .iter()
        .enumerate()
        .map(|(i, w)| {
            let frac = count as u128 * *w as u128 % sum as u128;
            (frac, *w, i)
        })
        .collect();
    rem.sort_by(|a, b| b.0.cmp(&a.0).then(b.1.cmp(&a.1)).then(a.2.cmp(&b.2)));

    let mut left = count - assigned;
    for (_, _, i) in rem {
        if left == 0 {
            break;
        }
        out[i] += 1;
        left -= 1;
    }
    out
}

/// 由「平台列表 + 计划」算出 `Schedule`（纯函数，不碰网络）。
pub fn plan_schedule(platforms: &[String], plan: &TipWindowPlan) -> Result<Schedule, TipWindowError> {
    if platforms.is_empty() {
        return Err(TipWindowError::NoPlatform);
    }
    if plan.count == 0 {
        return Err(TipWindowError::InvalidPlan {
            reason: "count == 0（不会发任何一笔）",
        });
    }
    let weights = plan.weights_for(platforms);
    let quotas = allocate(plan.count, &weights);
    let total: usize = quotas.iter().sum();
    if total == 0 {
        // 所有平台都被 with_quota(.., 0) 关掉了
        return Err(TipWindowError::InvalidPlan {
            reason: "所有平台的配额都是 0（检查 with_quota / 默认权重）",
        });
    }
    let rounds = quotas.iter().copied().max().unwrap_or(0) as u32;

    // 均匀铺开：step = duration / total；太密就用 min_gap 拉长窗口（不报错）
    let ideal = plan.duration / total as u32;
    let stretched = ideal < plan.min_gap;
    let step = if stretched { plan.min_gap } else { ideal };
    let span = step * total as u32;

    Ok(Schedule {
        quotas,
        rounds,
        total,
        step,
        span,
        deadline: span + plan.tail,
        stretched,
    })
}

// -- 结果 ----------------------------------------------------------------------

/// 窗口发送的结论。
#[derive(Debug)]
pub enum TipWindowOutcome {
    /// 确实成交：`sig` 是首胜副本，`round` / `platform` 是它能 join 回 build 表的位置
    Landed {
        sig: Signature,
        tx: grpc_client::TransactionFormat,
        round: u32,
        platform: String,
    },
    /// 到点了还没等齐；已收到的失败仍全部带出来
    Timeout { pending: Vec<Signature>, failed: Vec<SigErr> },
    /// 全部拿到终态失败（一笔没成）
    AllFailed { failed: Vec<SigErr> },
}

/// `send_tip_window` 的错误：**这里只表示「根本没发出去」**，
/// 发出去之后的结果一律走 [`TipWindowOutcome`]（三态），不混用。
#[derive(Debug, Clone)]
pub enum TipWindowError {
    /// `SendContext` 不是 blockhash 模式（安全契约，见模块文档）
    NonceContext,
    /// 一个平台都没注册（feature / 环境变量没配）
    NoPlatform,
    /// 全部平台都 build 失败（含平台名 + 原因）
    ///
    /// **每个平台一条**：同一平台跨轮基本是同一个原因，只保留首次。
    AllBuildFailed { failures: Vec<(String, String)> },
    /// 计划本身非法（`count == 0` / `cu_limit == 0`）
    InvalidPlan { reason: &'static str },
}

impl fmt::Display for TipWindowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NonceContext => write!(f, "send_tip_window 只接受 blockhash 模式的 SendContext"),
            Self::NoPlatform => write!(f, "没有任何已注册的发送平台"),
            Self::AllBuildFailed { failures } => {
                write!(f, "全部 {} 个平台 build 失败: {:?}", failures.len(), failures)
            }
            Self::InvalidPlan { reason } => write!(f, "窗口计划非法: {reason}"),
        }
    }
}

impl std::error::Error for TipWindowError {}

// -- 平台句柄（类型擦除）--------------------------------------------------------

/// 构建一笔：返回 `(sig, tx)`（`TxEnvelope` 的借用在这个闭包里就结束）。
type BuildFn = Box<
    dyn Fn(
            &[Instruction],
            &Arc<Keypair>,
            &Option<u64>,
            &HashParam,
            V1TxConfig,
            Option<Vec<String>>,
        ) -> Result<(Signature, VersionedTransaction), String>
        + Send
        + Sync,
>;

/// 一个已注册平台的发送句柄。
///
/// `build` 用闭包擦除类型（`build_v1_tx` 带 `Self: Sized`，没法放进 `dyn`），
/// `sender` 用 [`SendTx`] 对象擦除（`async_trait` 的对象安全接口）。
pub(crate) struct ClientRef {
    /// 日志名（与 [`crate::TxDispacher::channels`] 的平台名一致）
    display: String,
    /// 平台最低 tip（lamports）
    min_tip: u64,
    /// 平台自己的 tip 上限（如 HeliusSwqos 0.0002 SOL）
    max_tip: Option<u64>,
    /// `false` = 这个平台不写 tip 指令（目前只有 Harmonic），`cost` 对它无意义
    uses_tip_transfer: bool,
    build: BuildFn,
    sender: Arc<dyn SendTx>,
}

impl ClientRef {
    fn from_pool<C>(display_name: &'static str, pool: Arc<ChannelPool<C>>) -> Self
    where
        C: BuildV1Tx + BuildTx + SendTx + fmt::Display + Sync + Send + 'static,
    {
        // `Display` 命中 alt key 时会带 `(profile)`；两个名字都给，排障才不误导。
        let shown = pool.to_string();
        let display = if shown == display_name {
            display_name.to_string()
        } else {
            format!("{display_name}/{shown}")
        };
        let build_pool = Arc::clone(&pool);
        let build: BuildFn = Box::new(move |ixs, payer, tip, hash, config, memo| {
            let memo_refs: Option<Vec<&str>> = memo.as_ref().map(|m| m.iter().map(String::as_str).collect());
            match build_pool.build_v1_tx(ixs, payer, tip, hash, config, memo_refs) {
                Ok(env) => Ok((env.sig(), env.inner_tx().clone())),
                Err(e) => Err(e.to_string()),
            }
        });
        Self {
            display,
            min_tip: pool.get_min_tip_amount(),
            max_tip: pool.max_tip_amount(),
            uses_tip_transfer: pool.uses_tip_transfer(),
            build,
            sender: pool,
        }
    }

    /// 第 `round` 轮这一笔**实际会写进交易的 tip**（`None` = 该平台不收 tip，日志标 `n/a`）。
    ///
    /// `base = cost.unwrap_or(min_tip)`，再 `+ round` 换签名；平台上限只影响日志显示，
    /// 真正的钳位在 `build_v1_tx` 里做。
    fn effective_tip(&self, cost: Option<u64>, round: u32) -> Option<u64> {
        if !self.uses_tip_transfer {
            return None;
        }
        let base = cost.unwrap_or(self.min_tip);
        let tip = base.saturating_add(round as u64);
        Some(match self.max_tip {
            Some(cap) => tip.min(cap),
            None => tip,
        })
    }

    /// 第 `round` 轮真正传给 `build_v1_tx` 的 tip（`+ round` 是换签手段）。
    fn tip_for_round(&self, cost: Option<u64>, round: u32) -> Option<u64> {
        let base = cost.unwrap_or(self.min_tip);
        Some(base.saturating_add(round as u64))
    }
}

/// 一笔「已 build、待发」的变体。
pub(crate) struct PlannedSend {
    pub(crate) round: u32,
    pub(crate) platform: String,
    pub(crate) sig: Signature,
    pub(crate) tx: Arc<VersionedTransaction>,
    pub(crate) sender: Arc<dyn SendTx>,
    /// 日志用：这笔实际带的 tip（`None` = 平台不收 tip）
    pub(crate) tip: Option<u64>,
}

// -- 入口 ----------------------------------------------------------------------

impl<O: SlotOracle> TxDispacher<O> {
    /// 当前**所有已注册平台**（不做 route 分组）。
    ///
    /// 这是 P2 唯一的重构点：平台清单此前散在 `strategy.rs` 的一堆宏里，每个 mode 各自挑一组。
    /// 本接口刻意**不改任何现有分组**，只是另开一份全量清单。
    pub(crate) fn all_clients(&self) -> Vec<ClientRef> {
        let mut out = Vec::new();
        macro_rules! push_client {
            ($opt:expr, $name:literal) => {
                if let Some(pool) = &$opt {
                    out.push(ClientRef::from_pool($name, Arc::clone(pool)));
                }
            };
        }

        #[cfg(feature = "everstake_quic")]
        push_client!(self.everstake_quic, "EverStakeQuic");
        #[cfg(feature = "everstake")]
        push_client!(self.everstake, "EverStake");
        #[cfg(feature = "astralane_quic")]
        push_client!(self.astralane_quic, "AstralaneQuic");
        #[cfg(feature = "nodeone_quic")]
        push_client!(self.nodeone_quic, "NodeOneQuic");
        #[cfg(feature = "flash_block_quic")]
        push_client!(self.flash_block_quic, "FlashBlockQuic");
        #[cfg(feature = "astralane")]
        push_client!(self.astralane, "Astralane");
        #[cfg(feature = "flash_block")]
        push_client!(self.flash_block, "FlashBlock");
        #[cfg(feature = "temporal")]
        push_client!(self.temporal, "Temporal");
        #[cfg(feature = "zeroslot")]
        push_client!(self.zeroslot, "ZeroSlot");
        #[cfg(feature = "nodeone")]
        push_client!(self.nodeone, "NodeOne");
        #[cfg(feature = "blockrazor")]
        push_client!(self.blockrazor, "Blockrazor");
        #[cfg(feature = "helius")]
        push_client!(self.helius_max, "HeliusMax");
        #[cfg(feature = "helius")]
        push_client!(self.helius_swqos, "HeliusSwqos");
        #[cfg(feature = "nextblock")]
        push_client!(self.nextblock, "NextBlock");
        #[cfg(feature = "stellium")]
        push_client!(self.stellium, "Stellium");
        #[cfg(feature = "landx")]
        push_client!(self.landx, "LandX");
        #[cfg(feature = "landx_udp")]
        push_client!(self.landx_udp, "LandXUdp");
        // 下面三家不在 TipOnly 的广播面里，但都实现了 SendTx + BuildV1Tx：
        // Jito 走自己的 tip 竞价、Syncro 是 P2P 专用通道、Harmonic 只认 priority_fee
        // （`uses_tip_transfer() = false`，日志标 tip=n/a，照发）。
        #[cfg(feature = "jito")]
        push_client!(self.jito, "Jito");
        #[cfg(feature = "syncro")]
        push_client!(self.syncro, "Syncro");
        #[cfg(feature = "harmonic")]
        push_client!(self.harmonic, "Harmonic");

        out
    }

    /// **时段窗口均匀铺量发送**：全平台广播，任意一笔成交即成功。
    ///
    /// - `ixs`：业务指令（**不含** tip / memo；tip 与 `{tag}-r{k}` memo 由本函数加）；
    /// - `ctx`：必须是 blockhash 模式（运行期检查）；
    /// - `plan`：见 [`TipWindowPlan`]。
    ///
    /// 返回三态见 [`TipWindowOutcome`]；`Err` 表示**根本没发出去**。
    pub async fn send_tip_window(
        &self,
        ixs: &[Instruction],
        ctx: &SendContext,
        plan: TipWindowPlan,
    ) -> Result<TipWindowOutcome, TipWindowError> {
        // (1) 安全契约：nonce 模式下多轮 build 会互相冲突，直接拒绝
        if !matches!(ctx.hash_param, HashParam::Blockhash(_)) {
            log::error!("{LOG} 拒绝执行：SendContext 必须是 blockhash 模式（nonce 下多轮 build 会互相冲突）");
            return Err(TipWindowError::NonceContext);
        }
        if plan.cu_limit == 0 {
            return Err(TipWindowError::InvalidPlan {
                reason: "cu_limit == 0（V1 下会被链上按 0 处理，交易必失败）",
            });
        }

        // (2) 整段（build + pacing）都在同一个 region / key profile 作用域里。
        //     入口地区固定 own_region：本接口不做 route 分流，理由见模块文档。
        let region = self.region_profile.own_region;
        with_send_scope(Some(region), ctx.key_profile.clone(), async {
            run_with_clients(&self.all_clients(), ixs, ctx, plan, region, ctx.key_profile.clone()).await
        })
        .await
    }
}

/// 用一组平台句柄跑完整个窗口 —— `send_tip_window` 的真正主体。
///
/// 与「取平台列表」分开是为了能用**假平台**测完整链路（配额 / 时刻 / tip 递增 /
/// 无平台 / 全 build 失败 / 三态返回），不必真的连平台；生产路径只是 `&self.all_clients()`。
pub(crate) async fn run_with_clients(
    clients: &[ClientRef],
    ixs: &[Instruction],
    ctx: &SendContext,
    plan: TipWindowPlan,
    region: Region,
    key_profile: Option<KeyProfile>,
) -> Result<TipWindowOutcome, TipWindowError> {
    // (3) 通道去重：同一个 provider 只发优先那条（QUIC 优先 / LandX 走 HTTP），
    //     被丢掉的那条**不占配额**，这样重家族的 5 笔全部落在真正会发的通道上。
    let (clients, shadowed) = prefer_channels(clients);
    if !shadowed.is_empty() {
        let names: Vec<&str> = shadowed.iter().map(|c| c.display.as_str()).collect();
        log::info!("{LOG} 通道去重：跳过 {names:?}（同 provider 已用优先通道）");
    }

    // (4) 纯计算：配额 -> 轮次 -> 时刻
    let platforms: Vec<String> = clients.iter().map(|c| c.display.clone()).collect();
    let sched = plan_schedule(&platforms, &plan)?;

    log::info!(
        "{LOG} schedule count={} rounds={} step={:?} span={:?} deadline={:?} duration={:?} min_gap={:?} cost={:?} cu_limit={} ingress={} key_profile={:?} memo_tag={:?}",
        sched.total,
        sched.rounds,
        sched.step,
        sched.span,
        sched.deadline,
        plan.duration,
        plan.min_gap,
        plan.cost,
        plan.cu_limit,
        region,
        key_profile.as_ref().map(|p| p.as_str()),
        plan.memo_tag
    );
    if sched.stretched {
        log::warn!(
            "{LOG} 装不下：{total} 笔 / {duration:?} 的理论间隔 {ideal:?} < min_gap {min_gap:?}；已把间隔拉长到 {step:?}，实际窗口 {span:?}",
            total = sched.total,
            duration = plan.duration,
            ideal = plan.duration / sched.total.max(1) as u32,
            min_gap = plan.min_gap,
            step = sched.step,
            span = sched.span
        );
    }
    // 配额分布日志（裁剪/追加后各平台几笔）
    let quota_desc: Vec<String> = platforms
        .iter()
        .zip(&sched.quotas)
        .filter(|(_, q)| **q > 0)
        .map(|(p, q)| format!("{p}:{q}"))
        .collect();
    let dropped: Vec<&str> = platforms
        .iter()
        .zip(&sched.quotas)
        .filter(|(_, q)| **q == 0)
        .map(|(p, _)| p.as_str())
        .collect();
    log::info!("{LOG} quotas {quota_desc:?}");
    if !dropped.is_empty() {
        log::warn!("{LOG} 裁剪后不发（配额 0）: {dropped:?}");
    }

    // (4) 先全部 build（纯本地，无网络）：priority_fee 恒为 None，只给 tip。
    let config = V1TxConfig {
        priority_fee: None,
        compute_unit_limit: Some(plan.cu_limit),
        ..Default::default()
    };
    let mut variants: Vec<PlannedSend> = Vec::with_capacity(sched.total);
    let mut build_failures: Vec<(String, String)> = Vec::new();
    // sig -> (round, platform)：成功事件回来时靠它 join 回 build 表
    let mut by_sig: HashMap<Signature, (u32, String)> = HashMap::new();

    for round in 0..sched.rounds {
        let memo = plan.memo_for(round);
        let t_round = std::time::Instant::now();
        let mut built = 0usize;
        for (c, quota) in clients.iter().zip(&sched.quotas) {
            if *quota <= round as usize {
                continue;
            }
            // 第 k 轮：tip = base + k（换签手段）
            let tip_in = c.tip_for_round(plan.cost, round);
            match (c.build)(ixs, &ctx.payer, &tip_in, &ctx.hash_param, config, memo.clone()) {
                Ok((sig, tx)) => {
                    built += 1;
                    by_sig.entry(sig).or_insert_with(|| (round, c.display.clone()));
                    variants.push(PlannedSend {
                        round,
                        platform: c.display.clone(),
                        sig,
                        tx: Arc::new(tx),
                        sender: Arc::clone(&c.sender),
                        tip: c.effective_tip(plan.cost, round),
                    });
                }
                // 同一平台跨轮通常是同一个原因，只保留首次，避免刷屏
                Err(e) => {
                    if !build_failures.iter().any(|(p, _)| p == &c.display) {
                        build_failures.push((c.display.clone(), e));
                    }
                }
            }
        }
        log::info!("{LOG} r{round} built {built} build={:?}", t_round.elapsed());
    }

    if variants.is_empty() {
        log::error!("{LOG} 全部 {} 个平台 build 失败: {:?}", build_failures.len(), build_failures);
        return Err(TipWindowError::AllBuildFailed {
            failures: build_failures,
        });
    }
    if !build_failures.is_empty() {
        log::warn!(
            "{LOG} {} 个平台 build 失败（其余照发）: {:?}",
            build_failures.len(),
            build_failures
        );
    }

    let sigs: HashSet<Signature> = by_sig.keys().copied().collect();
    if sigs.len() < variants.len() {
        log::warn!(
            "{LOG} 签名重复：{} 笔只有 {} 个唯一签名（tip 递增被平台上限吃掉 / 没写 memo）。重复的那几笔只能视为重传",
            variants.len(),
            sigs.len()
        );
    }

    // (5) 订阅必须在任何发送之前；(6) confirm 先注册，再按节奏发。
    let rx = tx_result_channel::subscribe();
    let outcome = run_window(variants, sched.step, sigs, rx, sched.deadline, region, key_profile).await;

    Ok(match outcome {
        ConfirmSetOutcome::Success { sig, tx } => {
            let (round, platform) = by_sig.get(&sig).cloned().unwrap_or_else(|| {
                log::warn!("{LOG} 成交签名不在 build 表里（不该发生）: {sig}");
                (0, String::from("unknown"))
            });
            log::info!("{LOG} LANDED round={round} platform={platform} sig={sig}");
            TipWindowOutcome::Landed {
                sig,
                tx,
                round,
                platform,
            }
        }
        ConfirmSetOutcome::AllFailed { failed } => {
            log::warn!("{LOG} ALL-FAILED failed={}", failed.len());
            log_failed_breakdown(&failed, &by_sig);
            TipWindowOutcome::AllFailed { failed }
        }
        ConfirmSetOutcome::Timeout { pending, failed } => {
            log::warn!("{LOG} TIMEOUT pending={} failed={}", pending.len(), failed.len());
            log_failed_breakdown(&failed, &by_sig);
            TipWindowOutcome::Timeout { pending, failed }
        }
    })
}

// -- 通道去重（同一 provider 只发一条）------------------------------------------

/// 同一 provider 的多条通道（HTTP / QUIC / UDP）归到哪个家族键。
///
/// 只对"确实有多条通道"的 provider 有意义；单通道的返回 `None`（永不参与去重）。
fn channel_family(platform: &str) -> Option<&'static str> {
    let p = platform.to_ascii_lowercase();
    if p.contains("astralane") {
        Some("astralane")
    } else if p.contains("nodeone") {
        Some("nodeone")
    } else if p.contains("everstake") {
        Some("everstake")
    } else if p.contains("flashblock") {
        Some("flashblock")
    } else if p.contains("landx") {
        Some("landx")
    } else {
        None
    }
}

/// 家族内通道的优先级（**数字越小越优先**）。
///
/// 规则（2026-10-29 定）：
/// - 支持 QUIC 的一律**只发 QUIC**：astralane / nodeone / everstake / flash_block；
/// - **LandX 的 HTTP 覆盖更全**，只发 HTTP（不发 UDP）。
fn channel_rank(platform: &str) -> u8 {
    let p = platform.to_ascii_lowercase();
    let is_quic = p.contains("quic");
    let is_udp = p.contains("udp");
    match channel_family(platform) {
        // LandX：HTTP 优先于 UDP
        Some("landx") => u8::from(is_udp),
        // 其余有 QUIC 的家族：QUIC 优先于 HTTP
        Some(_) => u8::from(!is_quic),
        None => 0,
    }
}

/// 同一 provider 只保留优先的那条通道（其余丢弃），返回 `(保留, 丢弃)`。
///
/// 丢掉的通道**连配额都不占**：所以 3 个重家族的配额永远落在真正会发的那条上
/// （如 `AstralaneQuic` 拿满 5 笔，而不是被 `Astralane` 分走一半）。
pub(crate) fn prefer_channels(clients: &[ClientRef]) -> (Vec<&ClientRef>, Vec<&ClientRef>) {
    let mut best: HashMap<&'static str, usize> = HashMap::new();
    for (i, c) in clients.iter().enumerate() {
        if let Some(fam) = channel_family(&c.display) {
            let better = match best.get(fam) {
                None => true,
                Some(&j) => channel_rank(&c.display) < channel_rank(&clients[j].display),
            };
            if better {
                best.insert(fam, i);
            }
        }
    }

    let mut kept = Vec::with_capacity(clients.len());
    let mut dropped = Vec::new();
    for (i, c) in clients.iter().enumerate() {
        let shadowed = channel_family(&c.display).is_some_and(|fam| best.get(fam) != Some(&i));
        if shadowed {
            dropped.push(c);
        } else {
            kept.push(c);
        }
    }
    (kept, dropped)
}

// -- 发送编排 ------------------------------------------------------------------

/// 注册 confirm + 按 `step` 均匀发送，谁先结束就处理谁。
///
/// 单独抽出来是为了**能单测**：测试可以直接喂 `PlannedSend`（假 `SendTx`）和
/// 本地 broadcast channel，不需要真的连平台。
pub(crate) async fn run_window(
    variants: Vec<PlannedSend>,
    step: Duration,
    sigs: HashSet<Signature>,
    rx: tokio::sync::broadcast::Receiver<TxResultEvent>,
    deadline: Duration,
    region: Region,
    key_profile: Option<KeyProfile>,
) -> ConfirmSetOutcome {
    let stop = Arc::new(AtomicBool::new(false));
    let pending_on_panic = sigs.clone();

    // 先注册（spawn 在 pace 之前；订阅本身是同步完成的，事件不会漏）
    let mut confirm_task = tokio::spawn(confirm_set(rx, sigs, deadline));
    let mut pace_task = tokio::spawn(pace_and_fire(variants, step, Arc::clone(&stop), region, key_profile));

    tokio::select! {
        res = &mut confirm_task => {
            // 有结论了：立刻打断后续发送
            stop.store(true, Ordering::Relaxed);
            pace_task.abort();
            match res {
                Ok(outcome) => outcome,
                Err(e) => {
                    log::error!("{LOG} confirm_set 任务异常退出: {e}");
                    timeout_fallback(pending_on_panic)
                }
            }
        }
        _ = &mut pace_task => {
            // 全部发完：继续等 confirm 到 deadline
            match confirm_task.await {
                Ok(outcome) => outcome,
                Err(e) => {
                    log::error!("{LOG} confirm_set 任务异常退出: {e}");
                    timeout_fallback(pending_on_panic)
                }
            }
        }
    }
}

/// confirm 任务异常（panic / 取消）时的兜底：拿不到终态的签名全部按 pending 交回。
fn timeout_fallback(sigs: HashSet<Signature>) -> ConfirmSetOutcome {
    let mut pending: Vec<Signature> = sigs.into_iter().collect();
    pending.sort_by(|a, b| a.as_ref().cmp(b.as_ref()));
    ConfirmSetOutcome::Timeout {
        pending,
        failed: Vec::new(),
    }
}

/// 按 `t0 + step * i` 逐笔发送（fire-and-forget，i 是扁平计划里的序号）。
///
/// - 每笔单独 `tokio::spawn`：单平台网络慢不能拖慢后面的平台/笔；
/// - `stop` 置位后立刻返回（成功/全失败事件打断剩余发送）；
/// - region / key profile 显式带进每个 spawn 的 task（task-local 不跨 spawn）。
async fn pace_and_fire(
    variants: Vec<PlannedSend>,
    step: Duration,
    stop: Arc<AtomicBool>,
    region: Region,
    key_profile: Option<KeyProfile>,
) {
    let t0 = tokio::time::Instant::now();

    for (i, v) in variants.iter().enumerate() {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        let due = t0 + step * i as u32;
        if due > tokio::time::Instant::now() {
            tokio::time::sleep_until(due).await;
        }
        if stop.load(Ordering::Relaxed) {
            return;
        }

        let sender = Arc::clone(&v.sender);
        let tx = Arc::clone(&v.tx);
        let platform = v.platform.clone();
        let sig = v.sig;
        let tip = v.tip;
        let round = v.round;
        let kp = key_profile.clone();
        tokio::spawn(async move {
            let res = with_send_scope(Some(region), kp, sender.send_tx(&tx)).await;
            let at = t0.elapsed().as_secs_f64() * 1000.0;
            match res {
                Ok(()) => log::info!(
                    "{LOG} #{i} r{round} send {platform} tip={} gas=0 sig={sig} t=+{at:.1}ms",
                    tip.map(|t| t.to_string()).unwrap_or_else(|| "n/a".to_string())
                ),
                // 429 / 限流 / 网络错误都只记日志，不重试
                Err(e) => log::error!("{LOG} #{i} r{round} send {platform} failed: {e} sig={sig} t=+{at:.1}ms"),
            }
        });
    }
}

// -- 观测（计划文档 8）---------------------------------------------------------

/// 失败按「平台」「错误码」两个维度聚合 —— 这是 P6 标定最常用的两张表。
fn log_failed_breakdown(failed: &[SigErr], by_sig: &HashMap<Signature, (u32, String)>) {
    if failed.is_empty() {
        return;
    }
    let mut by_platform: HashMap<&str, usize> = HashMap::new();
    let mut codes: HashMap<String, usize> = HashMap::new();
    for (sig, err) in failed {
        let platform = by_sig.get(sig).map(|(_, p)| p.as_str()).unwrap_or("unknown");
        *by_platform.entry(platform).or_default() += 1;
        *codes.entry(error_code(err)).or_default() += 1;
    }

    let mut p: Vec<(&str, usize)> = by_platform.into_iter().collect();
    p.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
    let mut c: Vec<(String, usize)> = codes.into_iter().collect();
    c.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    log::warn!("{LOG}   failed_by_platform={p:?}");
    log::warn!("{LOG}   codes={c:?}");
}

/// 失败原因的可读标签（自定义错误码优先，其次错误类型名）。
fn error_code(err: &TxConfirmError) -> String {
    match err {
        TxConfirmError::Failed { detail, .. } => match detail.custom_error_code() {
            Some(code) => format!("custom({code})"),
            None => detail.error_type().to_string(),
        },
        TxConfirmError::MetaMissing { .. } => "meta_missing".to_string(),
        TxConfirmError::Timeout { .. } => "timeout".to_string(),
        TxConfirmError::Other(msg) => msg.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nonce_cache::{TradeStatus, TxFailureDetail, tx_result_channel::TxResultEvent};
    use sol_slot_leader::NoopOracle;
    use solana_sdk::{
        hash::Hash,
        pubkey::Pubkey,
        transaction::{Transaction, TransactionError, VersionedTransaction},
    };
    use std::sync::Mutex;
    use tokio::sync::broadcast;

    // -- 假平台 ------------------------------------------------------------------

    struct Sent {
        platform: String,
        round: u32,
        at: std::time::Instant,
    }

    #[derive(Default)]
    struct Recorder {
        sends: Mutex<Vec<Sent>>,
        /// 每次 build 时收到的 tip（按 build 顺序，即轮为主序、平台为次序）
        tips: Mutex<Vec<Option<u64>>>,
    }

    impl Recorder {
        fn sends(&self) -> Vec<(String, u32, std::time::Instant)> {
            self.sends
                .lock()
                .unwrap()
                .iter()
                .map(|s| (s.platform.clone(), s.round, s.at))
                .collect()
        }
        fn tips(&self) -> Vec<Option<u64>> {
            self.tips.lock().unwrap().clone()
        }
    }

    struct FakeSender {
        name: &'static str,
        round: u32,
        rec: Arc<Recorder>,
    }

    #[async_trait::async_trait]
    impl SendTx for FakeSender {
        async fn send_tx(&self, _tx: &VersionedTransaction) -> Result<(), String> {
            self.rec.sends.lock().unwrap().push(Sent {
                platform: self.name.to_string(),
                round: self.round,
                at: std::time::Instant::now(),
            });
            Ok(())
        }
    }

    struct NoopSender;

    #[async_trait::async_trait]
    impl SendTx for NoopSender {
        async fn send_tx(&self, _tx: &VersionedTransaction) -> Result<(), String> {
            Ok(())
        }
    }

    fn plain_client(name: &'static str, min_tip: u64, max_tip: Option<u64>, uses_tip: bool) -> ClientRef {
        ClientRef {
            display: name.to_string(),
            min_tip,
            max_tip,
            uses_tip_transfer: uses_tip,
            build: Box::new(|_, _, _, _, _, _| Err("unused".to_string())),
            sender: Arc::new(NoopSender),
        }
    }

    fn failing_client(name: &'static str) -> ClientRef {
        ClientRef {
            display: name.to_string(),
            min_tip: 1_000,
            max_tip: None,
            uses_tip_transfer: true,
            build: Box::new(|_, _, _, _, _, _| Err("build boom".to_string())),
            sender: Arc::new(NoopSender),
        }
    }

    /// 能 build 成功，并记录 tip（build 时）与发送时刻（send 时）。
    fn sending_client(name: &'static str, rec: Arc<Recorder>) -> ClientRef {
        let build_rec = Arc::clone(&rec);
        ClientRef {
            display: name.to_string(),
            min_tip: 1_000,
            max_tip: None,
            uses_tip_transfer: true,
            build: Box::new(move |_, _, tip, _, _, _| {
                build_rec.tips.lock().unwrap().push(*tip);
                Ok((Signature::new_unique(), VersionedTransaction::from(Transaction::default())))
            }),
            sender: Arc::new(FakeSender { name, round: 0, rec }),
        }
    }

    fn dummy_tx() -> Arc<VersionedTransaction> {
        Arc::new(VersionedTransaction::from(Transaction::default()))
    }

    /// 造 `rounds x platforms` 个变体（顺序 = 轮为主序、平台为次序）
    fn make_variants(rounds: u32, platforms: &[&'static str], rec: Arc<Recorder>) -> Vec<PlannedSend> {
        let mut out = Vec::new();
        for round in 0..rounds {
            for p in platforms {
                out.push(PlannedSend {
                    round,
                    platform: p.to_string(),
                    sig: Signature::new_unique(),
                    tx: dummy_tx(),
                    sender: Arc::new(FakeSender {
                        name: p,
                        round,
                        rec: Arc::clone(&rec),
                    }),
                    tip: Some(1_000),
                });
            }
        }
        out
    }

    fn sigs_of(variants: &[PlannedSend]) -> HashSet<Signature> {
        variants.iter().map(|v| v.sig).collect()
    }

    fn names(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    // -- 事件注入 ----------------------------------------------------------------

    fn format_of(sig: Signature) -> grpc_client::TransactionFormat {
        let mut tx = Transaction::default();
        tx.signatures = vec![sig];
        grpc_client::TransactionFormat {
            slot: 0,
            index: 0,
            meta: None,
            transaction: VersionedTransaction::from(tx),
        }
    }

    fn ev_success(sig: Signature) -> TxResultEvent {
        TxResultEvent {
            signature: sig,
            tx: format_of(sig),
            status: TradeStatus::Success {
                signature: sig,
                tx: format_of(sig),
            },
        }
    }

    fn ev_failed(sig: Signature) -> TxResultEvent {
        TxResultEvent {
            signature: sig,
            tx: format_of(sig),
            status: TradeStatus::Failed {
                signature: sig,
                tx: format_of(sig),
                detail: TxFailureDetail::new(TransactionError::AlreadyProcessed),
            },
        }
    }

    fn channel(cap: usize) -> (broadcast::Sender<TxResultEvent>, broadcast::Receiver<TxResultEvent>) {
        broadcast::channel(cap)
    }

    // -- 配额 / 调度（纯函数）----------------------------------------------------

    /// 默认权重：重家族按家族均分；填充家族各 1；**其余一律 0（不发）**。
    #[test]
    fn default_weights_split_by_family() {
        // 只有一个通道的家族：满额 5
        assert_eq!(default_weights(&names(&["NodeOne"])), vec![5]);
        // 同家族注册了 HTTP + QUIC：5 笔在两条通道间分（3 + 2），不是各 5
        assert_eq!(default_weights(&names(&["Astralane", "AstralaneQuic"])), vec![3, 2]);
        assert_eq!(
            default_weights(&names(&["NodeOne", "NodeOneQuic", "LandX", "LandXUdp"])),
            vec![3, 2, 3, 2]
        );
        // 填充家族的每个通道各 1（helius 的两条是独立通道，都发）
        assert_eq!(
            default_weights(&names(&["HeliusMax", "HeliusSwqos", "FlashBlock", "Jito", "Harmonic"])),
            vec![1, 1, 1, 1, 1]
        );
        // 不在名单里的通道：配额 0，一条都不发
        assert_eq!(
            default_weights(&names(&[
                "EverStakeQuic",
                "Temporal",
                "ZeroSlot",
                "Blockrazor",
                "NextBlock",
                "Stellium",
                "Syncro"
            ])),
            vec![0; 7]
        );
        // 家族识别
        assert_eq!(family_of("LandXUdp"), Some("landx"));
        assert_eq!(family_of("HeliusMax"), None);
        assert_eq!(filler_family("HeliusSwqos"), Some("helius"));
        assert_eq!(filler_family("Temporal"), None);
    }

    /// 用户举的例子：3 个重平台 ×5 + 5 个普通平台 ×1 = 20 笔，count 正好 20 时原样保留。
    #[test]
    fn allocate_keeps_shape_when_count_matches() {
        let w = [5, 5, 5, 1, 1, 1, 1, 1];
        assert_eq!(allocate(20, &w), vec![5, 5, 5, 1, 1, 1, 1, 1]);
    }

    /// **先保重平台的 5 笔**：预算够 15 笔时普通平台一个不发。
    #[test]
    fn allocate_protects_heavy_quota() {
        let w = [5, 5, 5, 1, 1, 1, 1, 1];
        // 15 = 三个重平台的满额：普通平台全砍
        assert_eq!(allocate(15, &w), vec![5, 5, 5, 0, 0, 0, 0, 0]);
        // 10 笔：只在重平台之间按权重分（4/3/3），普通平台依然不发
        assert_eq!(allocate(10, &w), vec![4, 3, 3, 0, 0, 0, 0, 0]);
        // 3 笔：重平台各 1
        assert_eq!(allocate(3, &w), vec![1, 1, 1, 0, 0, 0, 0, 0]);
    }

    /// 平台数比 count 多（线上常见：20 个平台都注册了，但有些 key 过期）：
    /// 重平台仍然拿满 5 笔，剩下的名额才给普通平台。
    #[test]
    fn allocate_beats_expired_keys_with_heavy_first() {
        // 3 个重平台 + 17 个普通平台
        let mut w = vec![5, 5, 5];
        w.extend(std::iter::repeat_n(1, 17));

        let q = allocate(20, &w);
        assert_eq!(&q[..3], &[5, 5, 5], "重平台必须各 5 笔: {q:?}");
        assert_eq!(q[3..].iter().filter(|x| **x == 1).count(), 5, "剩 5 个名额给普通平台: {q:?}");
        assert_eq!(q.iter().sum::<usize>(), 20);

        // count 降到 15：普通平台全部砍掉，重平台保满
        let q = allocate(15, &w);
        assert_eq!(&q[..3], &[5, 5, 5]);
        assert!(q[3..].iter().all(|x| *x == 0));
    }

    /// 调用方要更多 -> 多出来的量加在重平台上（普通平台先各 1，之后轮转重平台）。
    #[test]
    fn allocate_grows_onto_heavy() {
        assert_eq!(allocate(5, &[1, 1]), vec![3, 2]);
        let w = [5, 5, 5, 1, 1, 1, 1, 1];
        let q = allocate(40, &w);
        assert_eq!(q.iter().sum::<usize>(), 40);
        assert_eq!(&q[3..], &[1, 1, 1, 1, 1], "普通平台最多 1 笔");
        assert!(q[0] >= 7, "多出来的量应该加在重平台上: {q:?}");
    }

    /// 所有平台都被 with_quota(.., 0) 关掉 -> 计划非法。
    #[test]
    fn allocate_all_zero_quota_is_invalid() {
        let plan = TipWindowPlan::new(Some(1), 100_000)
            .with_quota("NodeOne", 0)
            .with_quota("Astralane", 0)
            .with_quota("LandX", 0);
        let r = plan_schedule(&names(&["NodeOne", "Astralane", "LandX"]), &plan);
        assert!(matches!(r, Err(TipWindowError::InvalidPlan { .. })), "实际: {r:?}");
    }

    /// 配额覆盖（with_quota）优先于默认表，且支持子串匹配（一个模式可以命中多条通道）。
    #[test]
    fn quota_override_wins() {
        let plan = TipWindowPlan::new(Some(1), 100_000).with_quota("helius", 4);
        let w = plan.weights_for(&names(&["HeliusMax", "HeliusSwqos", "Astralane", "Jito"]));
        assert_eq!(w, vec![4, 4, 5, 1], "helius 两条都被覆盖成 4");

        // 关掉某条通道（配额 0 = 不发）
        let plan = TipWindowPlan::new(Some(1), 100_000).with_quota("AstralaneQuic", 0);
        assert_eq!(
            plan.weights_for(&names(&["Astralane", "AstralaneQuic"])),
            vec![3, 0],
            "显式置 0 的通道不参与分配"
        );
    }

    /// 时长为 duration 时：step = duration / count，span 就是 duration。
    #[test]
    fn schedule_spreads_over_duration() {
        let plan = TipWindowPlan::new(Some(1), 100_000)
            .with_count(20)
            .with_duration(Duration::from_millis(1200));
        let s = plan_schedule(&names(&["NodeOne", "Astralane", "LandX", "Jito"]), &plan).unwrap();
        assert_eq!(s.total, 20);
        assert_eq!(s.step, Duration::from_millis(60));
        assert_eq!(s.span, Duration::from_millis(1200));
        assert_eq!(s.deadline, Duration::from_millis(1500), "span + tail(300ms)");
        assert!(!s.stretched);
        // 3 个重平台分 20 笔：至少各 4 笔，且总数对得上
        assert!(s.quotas[0] >= 4 && s.quotas[1] >= 4 && s.quotas[2] >= 4, "{:?}", s.quotas);
        assert_eq!(s.rounds, *s.quotas.iter().max().unwrap() as u32);
    }

    /// 笔数多到 duration 装不下 -> 拉长间隔（不报错），span > duration。
    #[test]
    fn schedule_stretches_when_gap_too_small() {
        let plan = TipWindowPlan::new(Some(1), 100_000)
            .with_count(100)
            .with_duration(Duration::from_millis(1000))
            .with_min_gap(Duration::from_millis(20));
        let s = plan_schedule(&names(&["NodeOne"]), &plan).unwrap();
        assert!(s.stretched, "10ms < min_gap 20ms 应触发拉长");
        assert_eq!(s.step, Duration::from_millis(20));
        assert_eq!(s.total, 100);
        assert_eq!(s.span, Duration::from_millis(2000), "span 被拉长到 count x min_gap");
    }

    /// count = 0 直接报错；没有平台报 NoPlatform。
    #[test]
    fn schedule_rejects_zero() {
        let list = names(&["NodeOne"]);
        let zero = TipWindowPlan::new(Some(1), 100_000).with_count(0);
        assert!(matches!(plan_schedule(&list, &zero), Err(TipWindowError::InvalidPlan { .. })));
        assert!(matches!(
            plan_schedule(&[], &TipWindowPlan::new(Some(1), 100_000)),
            Err(TipWindowError::NoPlatform)
        ));
    }

    /// 第 k 轮传进 build 的 tip = base + k（换签名的手段）。
    #[tokio::test]
    async fn tip_grows_with_round() {
        let rec = Arc::new(Recorder::default());
        let clients = vec![sending_client("Solo", Arc::clone(&rec))];
        // "Solo" 不在默认名单里，用 with_quota 打开（顺便测覆盖能力）
        let plan = TipWindowPlan::new(Some(1_000_000), 200_000)
            .with_count(3)
            .with_quota("Solo", 1)
            .with_duration(Duration::from_millis(60))
            .with_tail(Duration::from_millis(30));

        let _ = run_with_clients(&clients, &[], &blockhash_ctx(), plan, Region::Frankfurt, None).await;
        assert_eq!(
            rec.tips(),
            vec![Some(1_000_000), Some(1_000_001), Some(1_000_002)],
            "tip 必须逐轮 +1，否则同一平台的跨轮签名会重复"
        );
    }

    /// cost = None 时 base 退化为平台最低 tip，且照样 +k。
    #[test]
    fn tip_base_is_platform_min_when_cost_is_none() {
        let c = plain_client("T", 1_000_000, None, true);
        assert_eq!(c.tip_for_round(None, 0), Some(1_000_000));
        assert_eq!(c.tip_for_round(None, 2), Some(1_000_002));
        assert_eq!(c.effective_tip(None, 2), Some(1_000_002));
    }

    /// cost = Some(n) 是严格值；平台上限只影响日志里的生效值。
    #[test]
    fn effective_tip_is_exact_and_capped() {
        assert_eq!(
            plain_client("T", 1_000_000, None, true).effective_tip(Some(100_000), 0),
            Some(100_000),
            "低于平台门槛也不抬"
        );
        assert_eq!(
            plain_client("T", 1_000, Some(200_000), true).effective_tip(Some(9_999_999), 0),
            Some(200_000),
            "平台 max_tip_amount 生效"
        );
        assert_eq!(
            plain_client("Harmonic", 1, None, false).effective_tip(Some(1), 0),
            None,
            "不收 tip 的平台 -> n/a"
        );
    }

    /// memo 每轮带 r{k}（tip 递增的兜底）。
    #[test]
    fn memo_has_round_suffix() {
        let p = TipWindowPlan::new(Some(1), 100_000).with_memo_tag("TEST");
        assert_eq!(p.memo_for(0), Some(vec!["TEST".to_string(), "r0".to_string()]));
        assert_eq!(p.memo_for(3), Some(vec!["TEST".to_string(), "r3".to_string()]));
        assert_eq!(TipWindowPlan::new(Some(1), 100_000).memo_for(0), None);
    }

    // -- 时序：发送时刻必须贴合计划 ----------------------------------------------

    /// 扁平计划里第 i 笔的实际发送时刻与 t0 + step*i 的偏差 p99 < 15ms。
    #[tokio::test]
    async fn pacing_matches_plan_within_15ms() {
        let rec = Arc::new(Recorder::default());
        let platforms = ["A", "B", "C"];
        let (rounds, step) = (3u32, Duration::from_millis(25));
        let variants = make_variants(rounds, &platforms, Arc::clone(&rec));
        let sigs = sigs_of(&variants);
        // 保住 sender：drop 会让 channel 变 Closed，confirm_set 会立刻返回
        let (_keep_sender, rx) = channel(64);

        let t0 = std::time::Instant::now();
        let out = run_window(variants, step, sigs, rx, Duration::from_millis(200), Region::Frankfurt, None).await;

        let sends = rec.sends();
        assert_eq!(sends.len(), (rounds as usize) * platforms.len(), "所有变体都要发出");
        assert!(matches!(out, ConfirmSetOutcome::Timeout { .. }), "没有事件应超时");

        let mut late: Vec<Duration> = sends
            .iter()
            .map(|(platform, round, at)| {
                let idx = platforms.iter().position(|p| p == platform).expect("未知平台") as u32;
                // 轮为主序、平台为次序 -> 扁平序号
                let seq = *round * platforms.len() as u32 + idx;
                let due = t0 + step * seq;
                at.saturating_duration_since(due)
            })
            .collect();
        late.sort();
        let p99 = late[(late.len() * 99 / 100).min(late.len() - 1)];
        assert!(
            p99 < Duration::from_millis(15),
            "发送时刻偏差 p99={p99:?} 超过 15ms: {late:?}"
        );
    }

    /// 收到成功事件后：立刻返回，且剩余发送全部停掉。
    #[tokio::test]
    async fn success_stops_remaining_sends() {
        let rec = Arc::new(Recorder::default());
        let variants = make_variants(6, &["A", "B"], Arc::clone(&rec));
        let first_sig = variants[0].sig;
        let sigs = sigs_of(&variants);
        let (ev, rx) = channel(64);

        // 40ms 后注入首笔成功（step=30ms，此时已发到第 1 笔）
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(40)).await;
            let _ = ev.send(ev_success(first_sig));
        });

        let t0 = std::time::Instant::now();
        let out = run_window(
            variants,
            Duration::from_millis(30),
            sigs,
            rx,
            Duration::from_millis(800),
            Region::Frankfurt,
            None,
        )
        .await;

        match out {
            ConfirmSetOutcome::Success { sig, .. } => assert_eq!(sig, first_sig),
            other => panic!("期望 Success，实际 {other:?}"),
        }
        let sends = rec.sends();
        let after = t0 + Duration::from_millis(50);
        let late: Vec<_> = sends.iter().filter(|(_, _, at)| *at > after).collect();
        assert!(late.is_empty(), "成功后不该再有发送: {late:?}");
        assert!(sends.len() <= 4, "12 笔不该发完，实际发了 {} 笔", sends.len());
    }

    /// 全部失败：立刻 AllFailed，不等满 deadline，也不再继续发。
    #[tokio::test]
    async fn all_failed_returns_early() {
        let rec = Arc::new(Recorder::default());
        let variants = make_variants(3, &["A", "B"], Arc::clone(&rec));
        let sigs = sigs_of(&variants);
        let (ev, rx) = channel(64);
        for sig in &sigs {
            ev.send(ev_failed(*sig)).unwrap();
        }

        let t0 = std::time::Instant::now();
        let out = run_window(
            variants,
            Duration::from_millis(20),
            sigs,
            rx,
            Duration::from_secs(5),
            Region::Frankfurt,
            None,
        )
        .await;

        let elapsed = t0.elapsed();
        match out {
            ConfirmSetOutcome::AllFailed { failed } => assert_eq!(failed.len(), 6),
            other => panic!("期望 AllFailed，实际 {other:?}"),
        }
        assert!(elapsed < Duration::from_millis(500), "不该等满 5s deadline，实际 {elapsed:?}");
        assert!(rec.sends().len() <= 2, "全失败后不该继续发，实际 {}", rec.sends().len());
    }

    /// 超时：没等到事件 -> pending 覆盖全部签名，failed 为空；所有笔都发过。
    #[tokio::test]
    async fn timeout_splits_pending_and_failed() {
        let rec = Arc::new(Recorder::default());
        let variants = make_variants(2, &["A"], Arc::clone(&rec));
        let sigs = sigs_of(&variants);
        // 不注入事件，但要保住 sender
        let (_keep_sender, rx) = channel(64);

        let out = run_window(
            variants,
            Duration::from_millis(15),
            sigs,
            rx,
            Duration::from_millis(120),
            Region::Frankfurt,
            None,
        )
        .await;

        match out {
            ConfirmSetOutcome::Timeout { pending, failed } => {
                assert_eq!(pending.len(), 2);
                assert!(failed.is_empty());
            }
            other => panic!("期望 Timeout，实际 {other:?}"),
        }
        assert_eq!(rec.sends().len(), 2, "两笔都要发出去");
    }

    // -- 契约：这些情况必须「根本没发出去」----------------------------------------

    fn blockhash_ctx() -> SendContext {
        SendContext::new(
            Arc::new(Keypair::new()),
            HashParam::Blockhash(Hash::new_unique()),
            Arc::new(Vec::new()),
        )
    }

    fn plan() -> TipWindowPlan {
        TipWindowPlan::new(Some(1_000_000), 200_000)
            .with_count(2)
            .with_duration(Duration::from_millis(40))
            .with_min_gap(Duration::from_millis(10))
            .with_tail(Duration::from_millis(40))
            .with_memo_tag("TEST")
    }

    /// 传 nonce ctx 必须直接报错（安全契约：nonce 下多轮 build 会互相冲突）。
    #[tokio::test]
    async fn nonce_context_is_rejected() {
        let d = TxDispacher::builder(NoopOracle).build();
        let ctx = SendContext::new(
            Arc::new(Keypair::new()),
            HashParam::NonceAccount {
                account: Pubkey::new_unique(),
                authority: Pubkey::new_unique(),
                hash: Hash::new_unique(),
            },
            Arc::new(Vec::new()),
        );
        let err = d.send_tip_window(&[], &ctx, plan()).await.unwrap_err();
        assert!(matches!(err, TipWindowError::NonceContext), "实际: {err:?}");
    }

    /// 一个平台都没注册 -> Err(NoPlatform)。
    #[tokio::test]
    async fn no_platform_is_rejected() {
        let d = TxDispacher::builder(NoopOracle).build();
        assert!(d.all_clients().is_empty(), "裸 dispatcher 不该有平台");
        let err = d.send_tip_window(&[], &blockhash_ctx(), plan()).await.unwrap_err();
        assert!(matches!(err, TipWindowError::NoPlatform), "实际: {err:?}");
    }

    /// 计划非法（cu_limit = 0）必须在碰平台之前就报错
    /// （count = 0 由 `schedule_rejects_zero` 覆盖：dp 上没有平台时会先报 NoPlatform）。
    #[tokio::test]
    async fn invalid_plan_is_rejected() {
        let d = TxDispacher::builder(NoopOracle).build();
        let zero_cu = TipWindowPlan::new(Some(1), 0);
        let err = d.send_tip_window(&[], &blockhash_ctx(), zero_cu).await.unwrap_err();
        assert!(matches!(err, TipWindowError::InvalidPlan { .. }), "实际: {err:?}");
    }

    /// 全部平台 build 失败 -> Err(AllBuildFailed)，而不是「发出去了在等」。
    #[tokio::test]
    async fn all_build_failed_is_err() {
        // 用名单里的真名，配额才不为 0
        let clients = vec![failing_client("Jito"), failing_client("Harmonic")];
        let err = run_with_clients(&clients, &[], &blockhash_ctx(), plan(), Region::Frankfurt, None)
            .await
            .unwrap_err();
        match err {
            TipWindowError::AllBuildFailed { failures } => {
                assert_eq!(failures.len(), 2, "两个平台都要报出来: {failures:?}");
                assert_eq!(failures[0].0, "Jito");
            }
            other => panic!("期望 AllBuildFailed，实际 {other:?}"),
        }
    }

    /// 只有部分平台 build 失败 -> 其余照发，不进 AllBuildFailed。
    #[tokio::test]
    async fn partial_build_failure_still_sends() {
        let rec = Arc::new(Recorder::default());
        let clients = vec![failing_client("bad"), sending_client("good", Arc::clone(&rec))];
        // bad 权重 1、good 权重 2 => 3 笔里 good 拿 2 笔，bad 那 1 笔 build 失败
        // （假名字不在默认名单里，必须显式 with_quota 打开）
        let window = plan()
            .with_count(3)
            .with_quota("bad", 1)
            .with_quota("good", 2)
            .with_duration(Duration::from_millis(20));

        let out = run_with_clients(&clients, &[], &blockhash_ctx(), window, Region::Frankfurt, None).await;
        match out {
            Ok(TipWindowOutcome::Timeout { pending, failed }) => {
                assert_eq!(pending.len(), 2, "only good 的 2 笔应该真的发出去");
                assert!(failed.is_empty());
            }
            other => panic!("期望 Timeout，实际 {other:?}"),
        }
        assert_eq!(rec.sends().len(), 2, "build 失败平台的配额不能挤掉别的平台");
    }

    /// 注入的真实平台必须被 `all_clients()` 收进来。
    #[cfg(feature = "astralane")]
    #[test]
    fn all_clients_picks_injected_platform() {
        use sol_tx_send::platform_clients::astralane::Astralane;

        let d = TxDispacher::builder(NoopOracle)
            .astralane(Astralane::init_with("test-key", Region::Frankfurt))
            .build();
        let clients = d.all_clients();
        assert_eq!(clients.len(), 1, "只注入了 1 个平台");
        assert_eq!(clients[0].display, "Astralane");
        assert!(clients[0].uses_tip_transfer, "Astralane 走 tip 转账");
        assert!(clients[0].min_tip > 0, "平台最低 tip 应该是真实值");
    }

    // -- 通道去重：同一 provider 只发优先那条 --------------------------------

    fn plain_clients(names: &[&'static str]) -> Vec<ClientRef> {
        names.iter().map(|n| plain_client(n, 1_000, None, true)).collect()
    }

    fn displays(v: &[&ClientRef]) -> Vec<String> {
        v.iter().map(|c| c.display.clone()).collect()
    }

    /// 有 QUIC 的家族只发 QUIC（astralane / nodeone / everstake / flash_block）。
    #[test]
    fn prefer_channels_keeps_quic() {
        let all = plain_clients(&[
            "EverStake",
            "EverStakeQuic",
            "Astralane",
            "AstralaneQuic",
            "NodeOne",
            "NodeOneQuic",
            "FlashBlock",
            "FlashBlockQuic",
        ]);
        let (kept, dropped) = prefer_channels(&all);
        assert_eq!(
            displays(&kept),
            vec!["EverStakeQuic", "AstralaneQuic", "NodeOneQuic", "FlashBlockQuic"]
        );
        assert_eq!(displays(&dropped), vec!["EverStake", "Astralane", "NodeOne", "FlashBlock"]);
    }

    /// LandX 反过来：HTTP 覆盖更全，只发 HTTP（不发 UDP）。
    #[test]
    fn prefer_channels_keeps_landx_http() {
        let all = plain_clients(&["LandX", "LandXUdp"]);
        let (kept, dropped) = prefer_channels(&all);
        assert_eq!(displays(&kept), vec!["LandX"]);
        assert_eq!(displays(&dropped), vec!["LandXUdp"]);
    }

    /// 只注册了一条的家族保持原样；没有兄弟通道的平台之间互不影响
    /// （HeliusMax / HeliusSwqos 是两条独立通道，都保留）。
    #[test]
    fn prefer_channels_keeps_singletons() {
        let all = plain_clients(&["AstralaneQuic", "Jito", "HeliusMax", "HeliusSwqos", "Temporal"]);
        let (kept, dropped) = prefer_channels(&all);
        assert_eq!(
            displays(&kept),
            vec!["AstralaneQuic", "Jito", "HeliusMax", "HeliusSwqos", "Temporal"]
        );
        assert!(dropped.is_empty());
    }

    /// **线上形状的验收测试**：把默认 feature 下能注册的 20 条通道全打开，
    /// count=20 / duration=1s 时必须正好是
    ///
    /// ```text
    /// AstralaneQuic 5 + NodeOneQuic 5 + LandX 5 = 15   (只发 QUIC / LandX 走 HTTP)
    /// FlashBlock 1 + HeliusMax 1 + HeliusSwqos 1 + Jito 1 + Harmonic 1 = 5
    /// 合计 20，其余 12 条（EverStake* / Temporal / ZeroSlot / Blockrazor /
    /// NextBlock / Stellium / LandXUdp / Syncro / 以及被去重掉的 HTTP 通道）一条都不发
    /// ```
    #[tokio::test]
    async fn production_shape_is_exactly_20_sends() {
        let rec = Arc::new(Recorder::default());
        // 注册顺序 = all_clients() 的顺序
        let registered = [
            "EverStake",
            "EverStakeQuic",
            "Astralane",
            "AstralaneQuic",
            "NodeOne",
            "NodeOneQuic",
            "FlashBlock",
            "Temporal",
            "ZeroSlot",
            "Blockrazor",
            "HeliusMax",
            "HeliusSwqos",
            "NextBlock",
            "Stellium",
            "LandX",
            "LandXUdp",
            "Jito",
            "Syncro",
            "Harmonic",
        ];
        let clients: Vec<ClientRef> = registered.iter().map(|n| sending_client(n, Arc::clone(&rec))).collect();
        let window = TipWindowPlan::new(Some(10_000_000), 200_000)
            .with_count(20)
            .with_duration(Duration::from_millis(1000))
            .with_min_gap(Duration::from_millis(1)) // 让测试跑快点，不影响分配
            .with_tail(Duration::from_millis(30))
            .with_memo_tag("LL");

        let _ = run_with_clients(&clients, &[], &blockhash_ctx(), window, Region::Frankfurt, None).await;

        let mut got: std::collections::BTreeMap<String, usize> = Default::default();
        for (p, _, _) in rec.sends() {
            *got.entry(p).or_default() += 1;
        }
        let want: std::collections::BTreeMap<String, usize> = [
            ("AstralaneQuic", 5),
            ("NodeOneQuic", 5),
            ("LandX", 5),
            ("FlashBlock", 1),
            ("HeliusMax", 1),
            ("HeliusSwqos", 1),
            ("Jito", 1),
            ("Harmonic", 1),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
        assert_eq!(got, want, "只允许这 8 条通道，且笔数必须精确");
        assert_eq!(rec.sends().len(), 20, "总共 20 笔");
    }

    /// 端到端：注册了 HTTP + QUIC 两条，实际只发 QUIC，且**配额不缩水**
    /// （5 笔全落在 AstralaneQuic，而不是被 Astralane 分走一半）。
    #[tokio::test]
    async fn dedupe_gives_full_quota_to_preferred_channel() {
        let rec = Arc::new(Recorder::default());
        let clients = vec![
            sending_client("Astralane", Arc::clone(&rec)),
            sending_client("AstralaneQuic", Arc::clone(&rec)),
            sending_client("Jito", Arc::clone(&rec)),
        ];
        let window = plan()
            .with_count(6)
            .with_duration(Duration::from_millis(100))
            .with_tail(Duration::from_millis(40));

        let _ = run_with_clients(&clients, &[], &blockhash_ctx(), window, Region::Frankfurt, None).await;

        let mut by_platform: std::collections::BTreeMap<String, usize> = Default::default();
        for (p, _, _) in rec.sends() {
            *by_platform.entry(p).or_default() += 1;
        }
        assert_eq!(by_platform.get("AstralaneQuic"), Some(&5), "5 笔全给 QUIC: {by_platform:?}");
        assert_eq!(by_platform.get("Jito"), Some(&1));
        assert!(!by_platform.contains_key("Astralane"), "HTTP 通道不该再发: {by_platform:?}");
    }
}
