//! 同一平台的「多 key × 多地区」客户端池。
//!
//! # 要解决什么
//!
//! 某些平台平时用 A key，个别场景想临时换成 B key（额度/限额/灰度账号不同），
//! 但**默认 key 的通道必须一直可用**。所以这里不搞「换 key 就重建 client」，
//! 而是启动时把每个 key 各建一组 client，发送时按 **key profile** 选。
//!
//! 这个机制对**所有带凭据的通道**一视同仁（字符串 key / token / keypair），
//! 命名约定都是「平台原本的环境变量名 + `_<PROFILE>`」，见 `config` 模块的表。
//!
//! ```text
//! 默认：ASTRALANE_KEY            -> profile ""（默认）
//! 额外：TX_KEY_PROFILES=alt
//!       ASTRALANE_KEY_ALT=...    -> profile "alt"
//! ```
//!
//! 这条环境变量约定由 [`crate::TxDispacher::from_config`] 自动落地；手工用
//! `TxDispacherBuilder` 注入的调用方可以自己 `ChannelPool::new(..).with_alt(..)`。
//!
//! 发送时只影响这一次调用：
//!
//! ```rust,ignore
//! let ctx = ctx.with_key_profile("alt");       // 这次发送用 alt 的 key
//! dispacher.send_v1(&ixs, &ctx, slot, cfg, 60).await?;
//! // 其余调用照旧走默认 key
//! ```
//!
//! # 稀疏覆盖（重要）
//!
//! profile 是**按平台稀疏覆盖**的：`alt` 只配了 `ASTRALANE_KEY_ALT` 时，
//! 用 `alt` 发交易只有 Astralane 换 key，其它平台（Jito / Temporal / Helius…）
//! 继续用默认 key。这样才能「只换一个平台的 key，广播面不变」。
//!
//! # 为什么用 task-local 选 profile
//!
//! 和 [`crate::region_pool`] 选地区是同一个原因：各平台 `BuildTx` / `SendTx`
//! 的 trait 签名不带上下文，而 `fire_client` 还要把发送 `tokio::spawn` 出去。
//! 这里由发送入口 scope 一个 `SEND_KEY_PROFILE`，池子在自己的 trait 实现里
//! 读它选实例 —— 上层策略代码（`strategy.rs` 里几十个 `fire_no_price!`）
//! **一行都不用改**。
//!
//! # 与保活的关系（刻意的设计约束）
//!
//! key 只是鉴权凭据，**endpoint 不随 key 变化**：
//!
//! - Astralane / Node1 / Temporal / …：key 在请求头或请求体里，URL 一样；
//! - LandX：key 在提交 URL 的 path 里，但保活探的是 `<base>/ping`，不含 key；
//! - 所有 HTTP client 共用 `sol-tx-send` 的全局 `HTTP_CLIENT` 连接池（按 origin 复用）。
//!
//! 所以 alt key 的实例与默认实例命中**同一批 endpoint / 同一条 TCP+TLS 连接**，
//! 保活目标列表不因 profile 增加，也不需要为 alt key 多 ping 一次
//! （那只会重复打同一个 origin）。[`crate::TxDispacher::keep_alive_targets`]
//! 仍然只按 `(平台, 地区)` 展开，改动前后逐条一致 —— 单测
//! `alt_key_profile_does_not_change_keep_alive_targets` 锁住这一点。

use crate::region_pool::RegionPool;
use sol_tx_send::platform_clients::{
    BuildTx, BuildV0Tx, BuildV1Tx, BundleSender, PlatformName, Region, SendTx,
};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_sdk::transaction::VersionedTransaction;
use std::collections::HashMap;
use std::fmt::{self, Display};
use std::future::Future;
use std::sync::Arc;

tokio::task_local! {
    /// 当前发送任务使用的 key profile；未设置 = 默认 key。
    ///
    /// 由发送入口（[`crate::TxDispacher`] 的 `send*`）按
    /// [`crate::SendContext::key_profile`] scope，`fire_client` 在 spawn 时再带进新 task。
    pub static SEND_KEY_PROFILE: KeyProfile;
}

/// 一个具名的备用 key 组（如 `"alt"` / `"backup"`）。
///
/// 名字同时决定环境变量后缀：`KeyProfile::new("alt")` 对应
/// `ASTRALANE_KEY_ALT` / `HELIUS_KEY_ALT` / …（大写、`-` 换成 `_`）。
///
/// 默认 key 不表示成 `KeyProfile`，而是「作用域里没有 `SEND_KEY_PROFILE`」，
/// 这样默认路径一次 task-local 读取都没有命中 alt，行为与加这个功能之前完全一致。
#[derive(Clone, PartialEq, Eq, Hash, Debug, PartialOrd, Ord)]
pub struct KeyProfile(Arc<str>);

impl KeyProfile {
    /// 用名字构造（自动 trim）。空名字没有意义，调用方应过滤掉。
    pub fn new(name: impl Into<String>) -> Self {
        Self(Arc::from(name.into().trim()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// 拼进环境变量名用的后缀（大写、`-` → `_`）。
    pub fn env_suffix(&self) -> String {
        self.0.to_ascii_uppercase().replace('-', "_")
    }
}

impl From<&str> for KeyProfile {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for KeyProfile {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

impl From<&String> for KeyProfile {
    fn from(s: &String) -> Self {
        Self::new(s.clone())
    }
}

impl AsRef<str> for KeyProfile {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for KeyProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 当前任务的 key profile（默认 key 时返回 `None`）。
pub fn current_key_profile() -> Option<KeyProfile> {
    SEND_KEY_PROFILE.try_with(Clone::clone).ok()
}

/// 在 `profile` 的作用域里执行 `fut`。
///
/// 一般不用直接调：给 [`crate::SendContext::with_key_profile`] 设好 profile，
/// `send*` 入口会自动 scope。这个函数留给需要自己组合发送逻辑的地方
/// （例如先 `bundle_sender()` 再 `send_bundle`）。
pub async fn with_key_profile<F: Future>(profile: impl Into<KeyProfile>, fut: F) -> F::Output {
    SEND_KEY_PROFILE.scope(profile.into(), fut).await
}

/// `Some(profile)` 时进作用域，`None` 时直接执行 —— 默认 key 路径不付出任何
/// task-local 开销，也是 `tokio::spawn` 前后传递 profile 的统一入口。
pub async fn with_optional_key_profile<F: Future>(profile: Option<KeyProfile>, fut: F) -> F::Output {
    match profile {
        Some(p) => with_key_profile(p, fut).await,
        None => fut.await,
    }
}

/// 同步版的 [`with_key_profile`]：给同步路径（如 `send_flashblock_only`）用。
pub fn sync_with_key_profile<R>(profile: impl Into<KeyProfile>, f: impl FnOnce() -> R) -> R {
    SEND_KEY_PROFILE.sync_scope(profile.into(), f)
}

/// 一次性把「地区 + key profile」两个 hint 都带进 `fut`。
///
/// `fire_client` 在 `tokio::spawn` 出来的 task 里用它：task-local 不会跨
/// `tokio::spawn` 继承，必须显式重新 scope。
pub async fn with_send_scope<F: Future>(region: Option<Region>, profile: Option<KeyProfile>, fut: F) -> F::Output {
    match (region, profile) {
        (Some(r), Some(p)) => crate::region_pool::with_region(r, with_key_profile(p, fut)).await,
        (Some(r), None) => crate::region_pool::with_region(r, fut).await,
        (None, Some(p)) => with_key_profile(p, fut).await,
        (None, None) => fut.await,
    }
}

/// 同一平台的多 key 客户端池：默认 key 一组 + 每个 profile 一组。
///
/// 对下层 trait 的可见行为与 [`RegionPool`] 完全一致，只是在选地区之前**先选 key**。
pub struct ChannelPool<C> {
    /// 默认 key 的实例池（没有 profile hint 时用它）。
    primary: Arc<RegionPool<C>>,
    /// profile 名 -> 该 profile 下这个平台的实例池。**稀疏**：没配 key 的平台不在表里。
    alts: HashMap<KeyProfile, Arc<RegionPool<C>>>,
}

impl<C> ChannelPool<C> {
    pub fn new(primary: RegionPool<C>) -> Self {
        Self {
            primary: Arc::new(primary),
            alts: HashMap::new(),
        }
    }

    /// 单地区 + 单 client（QUIC / UDP / gRPC 这类不参与地区扇出的通道）。
    pub fn single(own: Region, client: C) -> Self {
        Self::new(RegionPool::single(own, client))
    }

    /// 挂一个 profile 的实例池（builder 风格）。
    pub fn with_alt(mut self, profile: impl Into<KeyProfile>, pool: RegionPool<C>) -> Self {
        self.insert_alt(profile, pool);
        self
    }

    /// 挂一个 profile 的实例池（原地）。
    pub fn insert_alt(&mut self, profile: impl Into<KeyProfile>, pool: RegionPool<C>) {
        self.alts.insert(profile.into(), Arc::new(pool));
    }

    /// 默认 key 的实例池。
    pub fn primary(&self) -> &Arc<RegionPool<C>> {
        &self.primary
    }

    /// 当前任务该用的实例池：命中的 profile 没有这个平台的 key 时**回退默认**。
    ///
    /// 回退（而不是跳过该平台）是刻意的：profile 只覆盖想换 key 的平台，
    /// 其余平台的广播面必须与默认完全一致。
    pub fn current(&self) -> &Arc<RegionPool<C>> {
        match current_key_profile() {
            Some(p) => self.alts.get(&p).unwrap_or(&self.primary),
            None => &self.primary,
        }
    }

    /// 已注册的 profile 名（有序，便于日志比对）。
    pub fn alt_profiles(&self) -> Vec<KeyProfile> {
        let mut names: Vec<KeyProfile> = self.alts.keys().cloned().collect();
        names.sort();
        names
    }

    /// 这个平台实际注册了几个 profile 的 key。
    pub fn has_alts(&self) -> bool {
        !self.alts.is_empty()
    }

    /// 默认池开了哪些地区（保活 / 启动日志用；profile 与地区无关，不重复列）。
    pub fn regions(&self) -> impl Iterator<Item = Region> + '_ {
        self.primary.regions()
    }
}

impl<C> Clone for ChannelPool<C> {
    fn clone(&self) -> Self {
        Self {
            primary: Arc::clone(&self.primary),
            alts: self.alts.clone(),
        }
    }
}

impl<C> From<RegionPool<C>> for ChannelPool<C> {
    fn from(pool: RegionPool<C>) -> Self {
        Self::new(pool)
    }
}

// ── 平台 trait 委托：全部交给当前 profile 的实例池（未命中则默认池）───────────

impl<C: BuildTx> BuildTx for ChannelPool<C> {
    fn get_tip_address(&self) -> Pubkey {
        self.current().get_tip_address()
    }
    fn get_min_tip_amount(&self) -> u64 {
        self.current().get_min_tip_amount()
    }
    fn platform(&self) -> PlatformName {
        self.current().platform()
    }
    fn tip_recvs(&self) -> Vec<Pubkey> {
        self.current().tip_recvs()
    }
    fn uses_tip_transfer(&self) -> bool {
        self.current().uses_tip_transfer()
    }
    fn uses_cu_price(&self) -> bool {
        self.current().uses_cu_price()
    }
    fn max_tip_amount(&self) -> Option<u64> {
        self.current().max_tip_amount()
    }
}

#[async_trait::async_trait]
impl<C: SendTx> SendTx for ChannelPool<C> {
    async fn send_tx(&self, tx: &VersionedTransaction) -> Result<(), String> {
        // 热路径：一次 task-local 读取 + 一次 HashMap 查找（未命中再走默认池）。
        self.current().send_tx(tx).await
    }
}

impl<C: Display> Display for ChannelPool<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let pool = self.current();
        // 只有「profile 真的命中了这个平台的 alt key」才标出来；稀疏回退时
        // 日志必须显示成默认平台名，否则会误导排障（看着像换了 key，其实没有）。
        match current_key_profile().filter(|p| self.alts.contains_key(p)) {
            Some(p) => write!(f, "{}({})", pool, p),
            None => pool.fmt(f),
        }
    }
}

#[async_trait::async_trait]
impl<C: BundleSender> BundleSender for ChannelPool<C> {
    fn tip_address(&self) -> Pubkey {
        self.current().tip_address()
    }
    fn max_tx_size(&self) -> usize {
        self.current().max_tx_size()
    }
    async fn send_bundle(&self, txs: &[VersionedTransaction]) -> Result<Vec<Signature>, String> {
        self.current().send_bundle(txs).await
    }
}

impl<C> BuildV0Tx for ChannelPool<C> where
    C: BuildV0Tx + BuildTx + SendTx + Display + Sync + Send + 'static
{
}

impl<C> BuildV1Tx for ChannelPool<C> where
    C: BuildV1Tx + BuildTx + SendTx + Display + Sync + Send + 'static
{
}

#[cfg(test)]
mod tests {
    use super::*;
    use sol_tx_send::platform_clients::Region;

    fn pool() -> ChannelPool<u8> {
        ChannelPool::new(RegionPool::single(Region::Frankfurt, 1u8)).with_alt("alt", RegionPool::single(Region::Frankfurt, 2u8))
    }

    #[test]
    fn no_scope_uses_primary() {
        let p = pool();
        assert_eq!(**p.current().get(Region::Frankfurt), 1);
        assert!(current_key_profile().is_none());
    }

    #[tokio::test]
    async fn alt_profile_selects_alt_pool() {
        let p = pool();
        let got = with_key_profile("alt", async { **p.current().get(Region::Frankfurt) }).await;
        assert_eq!(got, 2);
    }

    /// 稀疏覆盖：profile 在这个平台没有 key 时必须回退默认池，而不是「跳过平台」。
    #[tokio::test]
    async fn unknown_platform_in_profile_falls_back_to_primary() {
        let p = pool();
        let got = with_key_profile("backup", async { **p.current().get(Region::Frankfurt) }).await;
        assert_eq!(got, 1);
    }

    #[tokio::test]
    async fn with_send_scope_carries_both_hints() {
        let p = pool();
        let (region, key) = with_send_scope(Some(Region::NewYork), Some(KeyProfile::new("alt")), async {
            (
                crate::region_pool::current_region(),
                current_key_profile().map(|k| k.as_str().to_string()),
            )
        })
        .await;
        assert_eq!(region, Some(Region::NewYork));
        assert_eq!(key.as_deref(), Some("alt"));
        // 同步路径同样生效
        let v = sync_with_key_profile("alt", || **p.current().get(Region::Frankfurt));
        assert_eq!(v, 2);
    }

    #[tokio::test]
    async fn display_only_marks_profile_when_alt_is_used() {
        let p = pool();
        assert_eq!(p.to_string(), "1");
        let hit = with_key_profile("alt", async { p.to_string() }).await;
        assert_eq!(hit, "2(alt)");
        // 该 profile 没有这个平台 -> 显示默认，不加误导性的后缀
        let miss = with_key_profile("backup", async { p.to_string() }).await;
        assert_eq!(miss, "1");
    }

    #[test]
    fn env_suffix_is_normalized() {
        assert_eq!(KeyProfile::new("alt").env_suffix(), "ALT");
        assert_eq!(KeyProfile::new(" Hot-Backup ").env_suffix(), "HOT_BACKUP");
        assert_eq!(KeyProfile::new("x").as_str(), "x");
    }
}
