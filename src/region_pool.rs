//! 多地区 client 池。
//!
//! 一个 [`RegionPool`] 装同一平台的**多个地区实例**，发送时按「当前目标入口地区」
//! 选一个；查询不到就回退 `own`。
//!
//! # 为什么用 task-local 传地区
//!
//! 各平台的 `SendTx::send_tx(&self, tx)` 签名不带上下文，而 dispatcher 在
//! `fire_client` 里 `tokio::spawn` 一个独立 task 去发送——正好是 task-local 的
//! 标准场景：
//!
//! ```text
//! dispatcher: route = resolve_route(slot) -> ingress 地区
//!   fire_client(.., region)  ->  tokio::spawn(SEND_REGION.scope(region, send_tx()))
//!                                              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//! RegionPool::send_tx -> SEND_REGION.try_with(..) -> 选对应地区的 client
//! ```
//!
//! 没设置 hint 的场景（单元测试、直调）一律走 `own`，行为与单实例完全一致。
//!
//! # 为什么不 dedup
//!
//! 两个目标地区可能退化成同一个 endpoint（比如某平台没有立陶宛机房，退回
//! 阿姆斯特丹）。这里**不做去重**：多开一个 client 的代价可以忽略，而去重会引入
//! 「一个实例要服务多个目标地区」的映射表，热路径查表更复杂、也更容易出错。

use sol_tx_send::platform_clients::{
    BuildTx, BuildV0Tx, BuildV1Tx, BundleSender, PlatformName, Region, SendTx,
};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_sdk::transaction::VersionedTransaction;
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;

use crate::region_plan::RegionProfile;

tokio::task_local! {
    /// 当前发送任务的目标入口地区，由 `fire_client` 在 spawn 时设置。
    pub static SEND_REGION: Region;
    /// 当前发送的 target slot，仅用于日志关联（不参与路由）。
    pub static SEND_SLOT: u64;
}

/// 当前任务的目标入口地区（没有设置 hint 时返回 `None`）。
pub fn current_region() -> Option<Region> {
    SEND_REGION.try_with(|r| *r).ok()
}

/// 当前发送的 target slot（没有设置时返回 `None`）。
pub fn current_slot() -> Option<u64> {
    SEND_SLOT.try_with(|s| *s).ok()
}

/// 在 `region` 的 hint 下执行 `fut`。
pub async fn with_region<F: std::future::Future>(region: Region, fut: F) -> F::Output {
    SEND_REGION.scope(region, fut).await
}

/// 在 `slot` 的日志上下文下执行 `fut`（可与 [`with_region`] 嵌套）。
pub async fn with_send_slot<F: std::future::Future>(slot: u64, fut: F) -> F::Output {
    SEND_SLOT.scope(slot, fut).await
}

/// 同一平台的多地区实例池。
pub struct RegionPool<T> {
    own: Region,
    /// 目标地区 -> 实例。直接以「配置里的目标地区」为 key，退化重复也无所谓。
    clients: HashMap<Region, Arc<T>>,
}

impl<T> RegionPool<T> {
    /// 按 profile 构造：`make` 负责为一个地区建实例（内部做就近回退）。
    ///
    /// `own_region` 一定在池子里——即使配置漏写也会被补上，因为它是所有兜底的
    /// 落点（`profile.validate()` 已经拦过一遍，这里只是防御）。
    pub fn build(profile: &RegionProfile, mut make: impl FnMut(Region) -> T) -> Self {
        let own = profile.own_region;
        let mut clients: HashMap<Region, Arc<T>> = HashMap::new();
        clients.insert(own, Arc::new(make(own)));
        for region in &profile.fanout {
            clients
                .entry(*region)
                .or_insert_with(|| Arc::new(make(*region)));
        }
        Self { own, clients }
    }

    /// 只有单个地区的池（兼容旧的注入路径）。
    pub fn single(own: Region, client: T) -> Self {
        let mut clients = HashMap::new();
        clients.insert(own, Arc::new(client));
        Self { own, clients }
    }

    /// 本机地区（兜底落点）。
    pub fn own_region(&self) -> Region {
        self.own
    }

    /// 池里实际开了哪些地区（无序）。
    pub fn regions(&self) -> impl Iterator<Item = Region> + '_ {
        self.clients.keys().copied()
    }

    /// 池里的实例数量。
    pub fn len(&self) -> usize {
        self.clients.len()
    }

    pub fn is_empty(&self) -> bool {
        self.clients.is_empty()
    }

    /// 取指定地区的实例；没有则回退 `own`。
    pub fn get(&self, region: Region) -> &Arc<T> {
        self.clients
            .get(&region)
            .or_else(|| self.clients.get(&self.own))
            .expect("RegionPool 必须包含 own_region")
    }

    /// 取指定地区的实例（克隆 Arc），用于 `spawn` 后持有。
    pub fn get_owned(&self, region: Region) -> Arc<T> {
        Arc::clone(self.get(region))
    }

    /// 当前任务的 hint 地区（没有 hint 时返回 `own`）。
    pub fn current_region(&self) -> Region {
        SEND_REGION.try_with(|r| *r).unwrap_or(self.own)
    }
}

// `Clone` 不要求 `T: Clone`：池里存的是 `Arc<T>`，克隆只加引用计数。
impl<T> Clone for RegionPool<T> {
    fn clone(&self) -> Self {
        Self {
            own: self.own,
            clients: self.clients.clone(),
        }
    }
}

// ── 平台 trait 委托：默认全部交给 own 实例（与地区无关的语义） ────────────────

impl<T: BuildTx> BuildTx for RegionPool<T> {
    fn get_tip_address(&self) -> Pubkey {
        self.get(self.own).get_tip_address()
    }
    fn get_min_tip_amount(&self) -> u64 {
        self.get(self.own).get_min_tip_amount()
    }
    fn platform(&self) -> PlatformName {
        self.get(self.own).platform()
    }
    fn tip_recvs(&self) -> Vec<Pubkey> {
        self.get(self.own).tip_recvs()
    }
    fn uses_tip_transfer(&self) -> bool {
        self.get(self.own).uses_tip_transfer()
    }
    fn uses_cu_price(&self) -> bool {
        self.get(self.own).uses_cu_price()
    }
    fn max_tip_amount(&self) -> Option<u64> {
        self.get(self.own).max_tip_amount()
    }
}

#[async_trait::async_trait]
impl<T: SendTx> SendTx for RegionPool<T> {
    async fn send_tx(&self, tx: &VersionedTransaction) -> Result<(), String> {
        // 热路径：只有一次 task-local 读取 + 一次 HashMap 查找，无 I/O。
        let region = self.current_region();
        self.get(region).send_tx(tx).await
    }
}

impl<T: Display> Display for RegionPool<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 单地区时输出与旧实现**逐字节一致**，避免日志格式变化影响既有告警/抓取。
        if self.clients.len() <= 1 {
            return self.get(self.own).fmt(f);
        }
        write!(f, "{}[{}x]", self.get(self.own), self.clients.len())
    }
}

#[async_trait::async_trait]
impl<T: BundleSender> BundleSender for RegionPool<T> {
    fn tip_address(&self) -> Pubkey {
        self.get(self.own).tip_address()
    }
    fn max_tx_size(&self) -> usize {
        self.get(self.own).max_tx_size()
    }
    async fn send_bundle(&self, txs: &[VersionedTransaction]) -> Result<Vec<Signature>, String> {
        let region = self.current_region();
        self.get(region).send_bundle(txs).await
    }
}

// `BuildV0Tx` / `BuildV1Tx` 的方法体都是默认实现，这里只是把 trait 接上去。
impl<T> BuildV0Tx for RegionPool<T> where
    T: BuildV0Tx + BuildTx + SendTx + Display + Sync + Send + 'static
{
}

impl<T> BuildV1Tx for RegionPool<T> where
    T: BuildV1Tx + BuildTx + SendTx + Display + Sync + Send + 'static
{
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile(own: Region, fanout: &[Region]) -> RegionProfile {
        RegionProfile {
            own_region: own,
            fanout: fanout.to_vec(),
        }
    }

    #[test]
    fn build_always_contains_own_region() {
        // 配置里漏了 own（validate 会拦，但构造层也要兜住）
        let p = profile(Region::Frankfurt, &[Region::Amsterdam, Region::London]);
        let pool = RegionPool::build(&p, |r| r);
        assert_eq!(pool.len(), 3);
        assert_eq!(**pool.get(Region::Frankfurt), Region::Frankfurt);
    }

    #[test]
    fn missing_region_falls_back_to_own() {
        let p = profile(Region::Frankfurt, &[Region::Frankfurt, Region::Amsterdam]);
        let pool = RegionPool::build(&p, |r| r);
        // 东京没配 -> 回退本机
        assert_eq!(**pool.get(Region::Tokyo), Region::Frankfurt);
        assert_eq!(**pool.get(Region::Amsterdam), Region::Amsterdam);
    }

    #[test]
    fn duplicate_target_regions_are_tolerated() {
        // 两个目标地区退化到同一个 endpoint 也无所谓：各自一个实例
        let p = profile(Region::Frankfurt, &[Region::Frankfurt, Region::Limburg, Region::Lithuania]);
        let pool = RegionPool::build(&p, |_| 0u8);
        assert_eq!(pool.len(), 3);
    }

    #[test]
    fn single_pool_keeps_behaviour() {
        let pool = RegionPool::single(Region::NewYork, 7u8);
        assert_eq!(pool.len(), 1);
        assert_eq!(**pool.get(Region::Tokyo), 7);
        assert_eq!(pool.own_region(), Region::NewYork);
    }
}
