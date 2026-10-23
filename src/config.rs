//! 由 `RegionPlan` 直接构造 `TxDispacher`（dispatcher 自己负责建 client）。
//!
//! 调用方只需给出 oracle（slot leader）与计划文件，不需要再逐个平台注入：
//!
//! ```rust,ignore
//! let plan = RegionPlan::load()?;
//! let dispacher = TxDispacher::from_config(oracle, &plan).await;
//! let _keep_alive = dispacher.spawn_keep_alive();
//! ```
//!
//! # 每个平台开哪些地区
//!
//! `profile.fanout ∩ 平台自身能力`：每平台的 `init_with(.., region)` 内部已经
//! 穷举了所有地区并做了就近回退（缺机房 → 同洲 → Frankfurt），所以这里无脑按
//! `fanout` 逐个地区构造即可，构造失败只会跳过该平台。
//!
//! # 凭据
//!
//! 凭据仍然是**部署级**的（同一个 key 服务所有地区），从环境变量读一次，
//! 复核后再展开到每个地区。缺 key 的平台直接不注册（与旧的注入路径行为一致）。
//!
//! # 不参与 fanout 的通道
//!
//! - **QUIC**（AstralaneQuic / EverStakeQuic）：v1 保持单地区（本机），
//!   多地区会引入重连/预热问题，等 HTTP 版跑出数据再说；
//! - **Harmonic**：gRPC 多端点广播，天然没有「选一个入口」的概念。

use crate::region_plan::RegionPlan;
use crate::region_pool::RegionPool;
use crate::TxDispacher;
use sol_slot_leader::SlotOracle;
use sol_tx_send::constants::REGION;

/// 读一个非空环境变量；空串视为未配置（避免把 `""` 当成合法 key 发出去）。
fn env_key(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

impl<O: SlotOracle> TxDispacher<O> {
    /// 加载 `REGION_PLAN_PATH` / 内嵌计划，然后 [`Self::from_config`]。
    pub async fn from_env(oracle: O) -> anyhow::Result<Self> {
        let plan = RegionPlan::load()?;
        Ok(Self::from_config(oracle, &plan).await)
    }

    /// 按计划构造：每个平台按本机 profile 的 `fanout` 展开成 [`RegionPool`]。
    pub async fn from_config(oracle: O, plan: &RegionPlan) -> Self {
        let own = *REGION;
        let profile = plan.profile_or_single(own);

        log::info!(
            "[TxDispacher] 地区计划：own={} fanout={:?}",
            profile.own_region,
            profile.fanout
        );

        let mut b = TxDispacher::builder(oracle).region_profile(profile.clone());

        // ── HTTP 平台（参与 fanout）────────────────────────────────────────
        #[cfg(feature = "astralane")]
        if let Some(key) = env_key("ASTRALANE_KEY") {
            b = b.astralane_pool(RegionPool::build(&profile, |r| {
                sol_tx_send::platform_clients::astralane::Astralane::init_with(key.clone(), r)
            }));
        }

        #[cfg(feature = "temporal")]
        if let Some(key) = env_key("TEMPORAL_KEY") {
            b = b.temporal_pool(RegionPool::build(&profile, |r| {
                sol_tx_send::platform_clients::temporal::Temporal::init_with(key.clone(), r)
            }));
        }

        #[cfg(feature = "nodeone")]
        if let Some(key) = env_key("NODEONE_KEY") {
            b = b.nodeone_pool(RegionPool::build(&profile, |r| {
                sol_tx_send::platform_clients::nodeone::NodeOne::init_with(key.clone(), r)
            }));
        }

        #[cfg(feature = "blockrazor")]
        if let Some(key) = env_key("BLOCKRAZOR_KEY") {
            b = b.blockrazor_pool(RegionPool::build(&profile, |r| {
                sol_tx_send::platform_clients::blockrazor::Blockrazor::init_with(key.clone(), r)
            }));
        }

        #[cfg(feature = "flash_block")]
        if let Some(key) = env_key("FLASHBLOCK_KEY") {
            b = b.flash_block_pool(RegionPool::build(&profile, |r| {
                sol_tx_send::platform_clients::flash_block::FlashBlock::init_with(key.clone(), r)
            }));
        }

        #[cfg(feature = "zeroslot")]
        if let Some(key) = env_key("ZEROSLOT_KEY") {
            b = b.zeroslot_pool(RegionPool::build(&profile, |r| {
                sol_tx_send::platform_clients::zeroslot::ZeroSlot::init_with(key.clone(), r)
            }));
        }

        #[cfg(feature = "nextblock")]
        if let Some(token) = env_key("NEXTBLOCK_TOKEN") {
            b = b.nextblock_pool(RegionPool::build(&profile, |r| {
                sol_tx_send::platform_clients::nextblock::NextBlock::init_with(token.clone(), r)
            }));
        }

        #[cfg(feature = "stellium")]
        if let Some(key) = env_key("STELLIUM_API_KEY") {
            b = b.stellium_pool(RegionPool::build(&profile, |r| {
                sol_tx_send::platform_clients::stellium::Stellium::init_with(key.clone(), r)
            }));
        }

        // Helius Max / SWQOS 共用同一个 key
        #[cfg(feature = "helius")]
        if let Some(key) = env_key("HELIUS_KEY") {
            b = b.helius_max_pool(RegionPool::build(&profile, |r| {
                sol_tx_send::platform_clients::helius_max::HeliusMax::init_with(key.clone(), r)
            }));
            b = b.helius_swqos_pool(RegionPool::build(&profile, |r| {
                sol_tx_send::platform_clients::helius_swqos::HeliusSwqos::init_with(key.clone(), r)
            }));
        }

        #[cfg(feature = "jito")]
        if let Some(uuid) = env_key("JITO_UUID") {
            b = b.jito_pool(RegionPool::build(&profile, |r| {
                sol_tx_send::platform_clients::jito::Jito::init_with(uuid.clone(), r)
            }));
        }

        #[cfg(feature = "landx")]
        if let Some(key) = env_key("LANDX_API_KEY") {
            b = b.landx_pool(RegionPool::build(&profile, |r| {
                sol_tx_send::platform_clients::landx::LandX::init_with(key.clone(), r)
            }));
        }

        #[cfg(feature = "syncro")]
        if let Some(key) = env_key("SYNCRO_API_KEY") {
            b = b.syncro_pool(RegionPool::build(&profile, |r| {
                sol_tx_send::platform_clients::syncro::Syncro::init_with(key.clone(), r)
            }));
        }

        // Everstake 不需要 key（走 JSON-RPC），永远注册
        #[cfg(feature = "everstake")]
        {
            b = b.everstake_pool(RegionPool::build(&profile, |r| {
                sol_tx_send::platform_clients::ever_stake::EverStake::from_region(r)
            }));
        }

        // ── 单地区通道（不参与 fanout）─────────────────────────────────────
        // LandX UDP：无连接状态、不需要保活/重连，一个 socket 打天下。
        // 同时解锁 fra2/fra3/slc1/dub1 四个"仅 UDP"机房（HTTP 只能回退到别的机房）。
        #[cfg(feature = "landx_udp")]
        if let Some(key) = env_key("LANDX_API_KEY") {
            match sol_tx_send::platform_clients::landx_udp::LandXUdp::init_with(key, own).await {
                Ok(c) => b = b.landx_udp(c),
                Err(e) => log::warn!("[TxDispacher] LandXUdp 初始化失败，跳过: {e}"),
            }
        }

        // FlashBlock QUIC（FLQ1）：有状态的已认证长连接，只能单地区注册。
        // ⚠️ 与 HTTP 通道共用同一个 key 和**同一个 TPS 限额**，两条同时开会互相抢。
        #[cfg(feature = "flash_block_quic")]
        if let Some(key) = env_key("FLASHBLOCK_KEY") {
            match sol_tx_send::platform_clients::flash_block_quic::FlashBlockQuic::init_with(key, own).await {
                Ok(c) => b = b.flash_block_quic(c),
                Err(e) => log::warn!("[TxDispacher] FlashBlockQuic 初始化失败，跳过: {e}"),
            }
        }

        // Node1 QUIC：启动时连一条长连接 + 认证一次，之后所有交易复用（官方明确要求
        // 不要每笔重新 connect/auth）。因此只能单地区注册，不能用 RegionPool 懒切换。
        #[cfg(feature = "nodeone_quic")]
        if let Some(key) = env_key("NODEONE_KEY") {
            match sol_tx_send::platform_clients::nodeone_quic::NodeOneQuic::init_with(key, own).await {
                Ok(c) => b = b.nodeone_quic(c),
                Err(e) => log::warn!("[TxDispacher] NodeOneQuic 初始化失败，跳过: {e}"),
            }
        }

        #[cfg(feature = "astralane_quic")]
        if let Some(key) = env_key("ASTRALANE_KEY") {
            match sol_tx_send::platform_clients::astralane_quic::client::AstralaneQuic::init_with(key, own).await {
                Ok(c) => b = b.astralane_quic(c),
                Err(e) => log::warn!("[TxDispacher] AstralaneQuic 初始化失败，跳过: {e}"),
            }
        }

        #[cfg(feature = "everstake_quic")]
        if let Some(keypair_b58) = env_key("EVER_STAKE_QUIC_KEYPAIR") {
            match solana_sdk::signature::Keypair::try_from_base58_string(&keypair_b58) {
                Ok(keypair) => match sol_tx_send::platform_clients::ever_stake_quic::EverStakeQuic::init_with(&keypair, own).await {
                    Ok(c) => b = b.everstake_quic(c),
                    Err(e) => log::warn!("[TxDispacher] EverStakeQuic 初始化失败，跳过: {e}"),
                },
                Err(e) => log::warn!("[TxDispacher] EVER_STAKE_QUIC_KEYPAIR 解析失败，跳过: {e}"),
            }
        }

        #[cfg(feature = "harmonic")]
        if let Some(sk) = env_key("HARMONIC_SEARCHER_PRIVATE_KEY_BASE58") {
            match solana_sdk::signature::Keypair::try_from_base58_string(&sk) {
                Ok(kp) => {
                    let searcher = std::sync::Arc::new(kp);
                    log::info!(
                        "[TxDispacher] Harmonic searcher pubkey={}",
                        solana_sdk::signer::Signer::pubkey(&*searcher)
                    );
                    b = b.harmonic(
                        sol_tx_send::platform_clients::harmonic::HarmonicBlockEngine::init_with(searcher),
                    );
                }
                Err(e) => log::warn!("[TxDispacher] HARMONIC_SEARCHER_PRIVATE_KEY_BASE58 解析失败，跳过: {e}"),
            }
        }

        let d = b.build();
        // 启动时把“实际构造了哪些平台 / 哪些地区”打出来，方便核对 feature 与 key。
        d.log_channels();
        d
    }
}
