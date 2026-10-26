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
//! # 凭据：默认 key + 可选的具名 key profile
//!
//! 默认凭据仍然是**部署级**的（同一个 key 服务所有地区），从环境变量读一次，
//! 复核后再展开到每个地区。缺 key 的平台直接不注册（与旧的注入路径行为一致）。
//!
//! 在此之上支持**备用 key**：先声明 profile 名，再给对应平台配 `<原环境变量>_<PROFILE>`：
//!
//! ```text
//! TX_KEY_PROFILES=alt
//! ASTRALANE_KEY=默认 astralane key
//! ASTRALANE_KEY_ALT=备用 astralane key      # 只有 astralane 换，其它平台继续用默认
//! ```
//!
//! - profile 是**按平台稀疏覆盖**的：没配 `_<PROFILE>` 的平台继续用默认 key；
//! - 每个 profile 会按同样的 `fanout` 展开成一份独立的 `RegionPool`（启动时建好，
//!   发送时零构造开销）；
//! - 发送时用 `SendContext::with_key_profile("alt")` 选择，**只影响那一次发送**；
//! - 保活**不受影响**：key 只是鉴权凭据，endpoint 不随 key 变化（详见
//!   [`crate::channel_pool`] 模块文档），保活目标仍然只按 `(平台, 地区)` 去重。
//!
//! 覆盖**全部**带凭据的通道，base 环境变量就是各平台原本的那一个（新增的只是
//! `_<PROFILE>` 后缀，原变量不用改）：
//!
//! | 通道 | base 环境变量 |
//! | --- | --- |
//! | Astralane HTTP / QUIC | `ASTRALANE_KEY` |
//! | Temporal | `TEMPORAL_KEY` |
//! | Node1 HTTP / QUIC | `NODEONE_KEY` |
//! | Blockrazor | `BLOCKRAZOR_KEY` |
//! | FlashBlock HTTP / QUIC | `FLASHBLOCK_KEY` |
//! | ZeroSlot | `ZEROSLOT_KEY` |
//! | NextBlock | `NEXTBLOCK_TOKEN` |
//! | Stellium | `STELLIUM_API_KEY` |
//! | Helius Max + SWQOS | `HELIUS_KEY` |
//! | Jito | `JITO_UUID` |
//! | LandX HTTP / UDP | `LANDX_API_KEY` |
//! | Syncro | `SYNCRO_API_KEY` |
//! | EverStake QUIC | `EVER_STAKE_QUIC_KEYPAIR` |
//! | Harmonic | `HARMONIC_SEARCHER_PRIVATE_KEY_BASE58` |
//!
//! 前 12 个是「API key / token」，直接当凭据用；后 2 个是 **keypair**：
//! - `EVER_STAKE_QUIC_KEYPAIR` 只用来生成自签 client cert（TLS 身份，不碰资金）；
//! - Harmonic 的 searcher keypair 是发给对方签名用的身份，备用 key 必须是**对方
//!   已登记的 searcher**，否则换了也发不进去。
//!
//! 唯一没有凭据的通道是 `EverStake` HTTP（走公开 JSON-RPC），它永远用默认路径。
//!
//! # 不参与 fanout 的通道
//!
//! - **QUIC**（AstralaneQuic / EverStakeQuic / NodeOneQuic / FlashBlockQuic）：
//!   单地区（本机），多地区会引入重连/预热问题；
//! - **Harmonic**：gRPC 多端点广播，天然没有「选一个入口」的概念。

use crate::channel_pool::{ChannelPool, KeyProfile};
use crate::region_plan::RegionPlan;
use crate::region_plan::RegionProfile;
use crate::region_pool::RegionPool;
use crate::TxDispacher;
use sol_slot_leader::SlotOracle;
use sol_tx_send::constants::REGION;

/// 声明备用 key profile 的环境变量（逗号分隔，如 `TX_KEY_PROFILES=alt,backup`）。
///
/// 必须显式声明：光看环境变量名做通配（`XXX_*`）会把 `ASTRALANE_KEY_URL` 之类
/// 误当 profile，出错时很难看出来。
pub const KEY_PROFILES_ENV: &str = "TX_KEY_PROFILES";

/// 读一个非空环境变量；空串视为未配置（避免把 `""` 当成合法 key 发出去）。
fn env_key(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// `ASTRALANE_KEY` + `alt` → `ASTRALANE_KEY_ALT`。
fn profile_env(base: &str, profile: &KeyProfile) -> String {
    format!("{base}_{}", profile.env_suffix())
}

/// 读取 `TX_KEY_PROFILES`。非法名字直接跳过并告警（它要拼进环境变量名）。
fn declared_key_profiles() -> Vec<KeyProfile> {
    let Ok(raw) = std::env::var(KEY_PROFILES_ENV) else {
        return Vec::new();
    };
    let mut out: Vec<KeyProfile> = Vec::new();
    for name in raw.split(',') {
        let name = name.trim();
        if name.is_empty() {
            continue;
        }
        if name.eq_ignore_ascii_case("default") {
            log::warn!("[TxDispacher] {KEY_PROFILES_ENV} 里的 \"default\" 没有意义（默认 key 不走 profile），已忽略");
            continue;
        }
        if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-') {
            log::warn!("[TxDispacher] {KEY_PROFILES_ENV} 里的 profile 名 {name:?} 非法（只允许字母/数字/_/-），已忽略");
            continue;
        }
        let p = KeyProfile::new(name);
        if out.contains(&p) {
            continue;
        }
        out.push(p);
    }
    out
}

/// 构造一个「多地区 + 多 key」的平台通道池（HTTP 类平台）。
///
/// - 默认 key 缺失 → 返回 `None`（该平台整体不注册，保持旧行为）；
/// - 某 profile 的 `<ENV>_<PROFILE>` 缺失 → 该 profile 在这个平台上回退默认 key；
/// - 某 profile 的构造失败由 `make` 自己决定（HTTP 客户端构造不会失败）。
fn pooled_channels<T>(
    profile: &RegionProfile,
    base_env: &str,
    key_profiles: &[KeyProfile],
    mut make: impl FnMut(&str, sol_tx_send::platform_clients::Region) -> T,
) -> Option<ChannelPool<T>> {
    let key = env_key(base_env)?;
    let mut channels = ChannelPool::new(RegionPool::build(profile, |r| make(&key, r)));
    for p in key_profiles {
        if let Some(alt) = env_key(&profile_env(base_env, p)) {
            channels.insert_alt(p.clone(), RegionPool::build(profile, |r| make(&alt, r)));
        }
    }
    Some(channels)
}

/// QUIC / UDP 这类**单实例有状态**通道：每个 key 一条连接，不按地区展开。
///
/// init 是异步且可能失败（连不上/认证失败）：默认 key 失败 → 整个平台跳过；
/// 某个 profile 失败 → 只跳过该 profile（不拖累默认 key，也不影响保活）。
async fn single_channels<T, F, Fut>(
    own: sol_tx_send::platform_clients::Region,
    base_env: &str,
    key_profiles: &[KeyProfile],
    init: F,
) -> Option<ChannelPool<T>>
where
    F: Fn(String) -> Fut,
    Fut: std::future::Future<Output = Result<T, String>>,
{
    let key = env_key(base_env)?;
    let mut channels = match init(key).await {
        Ok(c) => ChannelPool::single(own, c),
        Err(e) => {
            log::warn!("[TxDispacher] {base_env} 初始化失败，跳过: {e}");
            return None;
        }
    };
    for p in key_profiles {
        let Some(alt) = env_key(&profile_env(base_env, p)) else {
            continue;
        };
        match init(alt).await {
            Ok(c) => channels.insert_alt(p.clone(), RegionPool::single(own, c)),
            Err(e) => log::warn!(
                "[TxDispacher] key profile {} 的 {base_env} 初始化失败，跳过该 profile: {e}",
                p
            ),
        }
    }
    Some(channels)
}

impl<O: SlotOracle> TxDispacher<O> {
    /// 加载 `REGION_PLAN_PATH` / 内嵌计划，然后 [`Self::from_config`]。
    pub async fn from_env(oracle: O) -> anyhow::Result<Self> {
        let plan = RegionPlan::load()?;
        Ok(Self::from_config(oracle, &plan).await)
    }

    /// 按计划构造：每个平台按本机 profile 的 `fanout` 展开成 [`RegionPool`]；
    /// 若声明了 `TX_KEY_PROFILES`，每个平台再按备用 key 各展开一份（[`ChannelPool`]）。
    pub async fn from_config(oracle: O, plan: &RegionPlan) -> Self {
        let own = *REGION;
        let profile = plan.profile_or_single(own);
        let key_profiles = declared_key_profiles();

        log::info!(
            "[TxDispacher] 地区计划：own={} fanout={:?}",
            profile.own_region,
            profile.fanout
        );
        if key_profiles.is_empty() {
            log::info!("[TxDispacher] key profiles：无（只用默认 key）");
        } else {
            log::info!(
                "[TxDispacher] key profiles={:?}（每平台按 <ENV>_<PROFILE> 覆盖，如 ASTRALANE_KEY_ALT；未配的平台回退默认 key）",
                key_profiles.iter().map(|p| p.as_str()).collect::<Vec<_>>()
            );
        }

        let mut b = TxDispacher::builder(oracle).region_profile(profile.clone());

        // ── HTTP 平台（参与 fanout）────────────────────────────────────────
        #[cfg(feature = "astralane")]
        if let Some(ch) = pooled_channels(&profile, "ASTRALANE_KEY", &key_profiles, |key, r| {
            sol_tx_send::platform_clients::astralane::Astralane::init_with(key, r)
        }) {
            b = b.astralane_pool(ch);
        }

        #[cfg(feature = "temporal")]
        if let Some(ch) = pooled_channels(&profile, "TEMPORAL_KEY", &key_profiles, |key, r| {
            sol_tx_send::platform_clients::temporal::Temporal::init_with(key, r)
        }) {
            b = b.temporal_pool(ch);
        }

        #[cfg(feature = "nodeone")]
        if let Some(ch) = pooled_channels(&profile, "NODEONE_KEY", &key_profiles, |key, r| {
            sol_tx_send::platform_clients::nodeone::NodeOne::init_with(key, r)
        }) {
            b = b.nodeone_pool(ch);
        }

        #[cfg(feature = "blockrazor")]
        if let Some(ch) = pooled_channels(&profile, "BLOCKRAZOR_KEY", &key_profiles, |key, r| {
            sol_tx_send::platform_clients::blockrazor::Blockrazor::init_with(key, r)
        }) {
            b = b.blockrazor_pool(ch);
        }

        #[cfg(feature = "flash_block")]
        if let Some(ch) = pooled_channels(&profile, "FLASHBLOCK_KEY", &key_profiles, |key, r| {
            sol_tx_send::platform_clients::flash_block::FlashBlock::init_with(key, r)
        }) {
            b = b.flash_block_pool(ch);
        }

        #[cfg(feature = "zeroslot")]
        if let Some(ch) = pooled_channels(&profile, "ZEROSLOT_KEY", &key_profiles, |key, r| {
            sol_tx_send::platform_clients::zeroslot::ZeroSlot::init_with(key, r)
        }) {
            b = b.zeroslot_pool(ch);
        }

        #[cfg(feature = "nextblock")]
        if let Some(ch) = pooled_channels(&profile, "NEXTBLOCK_TOKEN", &key_profiles, |token, r| {
            sol_tx_send::platform_clients::nextblock::NextBlock::init_with(token, r)
        }) {
            b = b.nextblock_pool(ch);
        }

        #[cfg(feature = "stellium")]
        if let Some(ch) = pooled_channels(&profile, "STELLIUM_API_KEY", &key_profiles, |key, r| {
            sol_tx_send::platform_clients::stellium::Stellium::init_with(key, r)
        }) {
            b = b.stellium_pool(ch);
        }

        // Helius Max / SWQOS 共用同一个 key（同一份 key_profile 覆盖两者）
        #[cfg(feature = "helius")]
        if let Some(ch) = pooled_channels(&profile, "HELIUS_KEY", &key_profiles, |key, r| {
            sol_tx_send::platform_clients::helius_max::HeliusMax::init_with(key, r)
        }) {
            b = b.helius_max_pool(ch);
            if let Some(swqos) = pooled_channels(&profile, "HELIUS_KEY", &key_profiles, |key, r| {
                sol_tx_send::platform_clients::helius_swqos::HeliusSwqos::init_with(key, r)
            }) {
                b = b.helius_swqos_pool(swqos);
            }
        }

        #[cfg(feature = "jito")]
        if let Some(ch) = pooled_channels(&profile, "JITO_UUID", &key_profiles, |uuid, r| {
            sol_tx_send::platform_clients::jito::Jito::init_with(uuid, r)
        }) {
            b = b.jito_pool(ch);
        }

        #[cfg(feature = "landx")]
        if let Some(ch) = pooled_channels(&profile, "LANDX_API_KEY", &key_profiles, |key, r| {
            sol_tx_send::platform_clients::landx::LandX::init_with(key, r)
        }) {
            b = b.landx_pool(ch);
        }

        #[cfg(feature = "syncro")]
        if let Some(ch) = pooled_channels(&profile, "SYNCRO_API_KEY", &key_profiles, |key, r| {
            sol_tx_send::platform_clients::syncro::Syncro::init_with(key, r)
        }) {
            b = b.syncro_pool(ch);
        }

        // Everstake 不需要 key（走 JSON-RPC），永远注册
        #[cfg(feature = "everstake")]
        {
            b = b.everstake_pool(RegionPool::build(&profile, |r| {
                sol_tx_send::platform_clients::ever_stake::EverStake::from_region(r)
            }));
        }

        // ── 单地区通道（不参与 fanout）─────────────────────────────────────
        // LandX UDP：无连接状态、不需要保活/重连，一个 socket 打天下；
        // 同时解锁 fra2/fra3/slc1/dub1 四个"仅 UDP"机房（HTTP 只能回退到别的机房）。
        #[cfg(feature = "landx_udp")]
        if let Some(ch) = single_channels(own, "LANDX_API_KEY", &key_profiles, |key| {
            sol_tx_send::platform_clients::landx_udp::LandXUdp::init_with(key, own)
        })
        .await
        {
            b = b.landx_udp_pool(ch);
        }

        // FlashBlock QUIC（FLQ1）：有状态的已认证长连接，只能单地区注册。
        // ⚠️ 与 HTTP 通道共用同一个 key 和**同一个 TPS 限额**，两条同时开会互相抢。
        #[cfg(feature = "flash_block_quic")]
        if let Some(ch) = single_channels(own, "FLASHBLOCK_KEY", &key_profiles, |key| {
            sol_tx_send::platform_clients::flash_block_quic::FlashBlockQuic::init_with(key, own)
        })
        .await
        {
            b = b.flash_block_quic_pool(ch);
        }

        // Node1 QUIC：启动时连一条长连接 + 认证一次，之后所有交易复用（官方明确要求
        // 不要每笔重新 connect/auth）。因此只能单地区注册，不能用 RegionPool 懒切换。
        #[cfg(feature = "nodeone_quic")]
        if let Some(ch) = single_channels(own, "NODEONE_KEY", &key_profiles, |key| {
            sol_tx_send::platform_clients::nodeone_quic::NodeOneQuic::init_with(key, own)
        })
        .await
        {
            b = b.nodeone_quic_pool(ch);
        }

        #[cfg(feature = "astralane_quic")]
        if let Some(ch) = single_channels(own, "ASTRALANE_KEY", &key_profiles, |key| {
            sol_tx_send::platform_clients::astralane_quic::client::AstralaneQuic::init_with(key, own)
        })
        .await
        {
            b = b.astralane_quic_pool(ch);
        }

        // EverStakeQuic：keypair 只用于自签 client cert（TLS 身份，不碰资金），
        // 所以备用 keypair 是安全的 —— 仅换一条连接的身份。
        #[cfg(feature = "everstake_quic")]
        if let Some(keypair_b58) = env_key("EVER_STAKE_QUIC_KEYPAIR") {
            match solana_sdk::signature::Keypair::try_from_base58_string(&keypair_b58) {
                Ok(keypair) => match sol_tx_send::platform_clients::ever_stake_quic::EverStakeQuic::init_with(&keypair, own).await {
                    Ok(c) => {
                        let mut channels = ChannelPool::single(own, c);
                        for p in &key_profiles {
                            let Some(alt_b58) = env_key(&profile_env("EVER_STAKE_QUIC_KEYPAIR", p)) else {
                                continue;
                            };
                            let alt_kp = match solana_sdk::signature::Keypair::try_from_base58_string(&alt_b58) {
                                Ok(kp) => kp,
                                Err(e) => {
                                    log::warn!(
                                        "[TxDispacher] key profile {} 的 EVER_STAKE_QUIC_KEYPAIR 解析失败，跳过该 profile: {e}",
                                        p
                                    );
                                    continue;
                                }
                            };
                            match sol_tx_send::platform_clients::ever_stake_quic::EverStakeQuic::init_with(&alt_kp, own).await {
                                Ok(ac) => channels.insert_alt(p.clone(), RegionPool::single(own, ac)),
                                Err(e) => log::warn!(
                                    "[TxDispacher] key profile {} 的 EverStakeQuic 初始化失败，跳过该 profile: {e}",
                                    p
                                ),
                            }
                        }
                        b = b.everstake_quic_pool(channels);
                    }
                    Err(e) => log::warn!("[TxDispacher] EverStakeQuic 初始化失败，跳过: {e}"),
                },
                Err(e) => log::warn!("[TxDispacher] EVER_STAKE_QUIC_KEYPAIR 解析失败，跳过: {e}"),
            }
        }

        // Harmonic：searcher keypair 是「发给对方签名用的身份」。备用 key 必须
        // 是对方已登记的 searcher，否则换了也发不进去 —— 机制上仍然按 profile 选。
        #[cfg(feature = "harmonic")]
        if let Some(sk) = env_key("HARMONIC_SEARCHER_PRIVATE_KEY_BASE58") {
            match solana_sdk::signature::Keypair::try_from_base58_string(&sk) {
                Ok(kp) => {
                    let searcher = std::sync::Arc::new(kp);
                    log::info!(
                        "[TxDispacher] Harmonic searcher pubkey={}",
                        solana_sdk::signer::Signer::pubkey(&*searcher)
                    );
                    let mut channels = ChannelPool::single(
                        own,
                        sol_tx_send::platform_clients::harmonic::HarmonicBlockEngine::init_with(searcher),
                    );
                    for p in &key_profiles {
                        let Some(alt_sk) = env_key(&profile_env("HARMONIC_SEARCHER_PRIVATE_KEY_BASE58", p)) else {
                            continue;
                        };
                        match solana_sdk::signature::Keypair::try_from_base58_string(&alt_sk) {
                            Ok(alt_kp) => {
                                let alt_searcher = std::sync::Arc::new(alt_kp);
                                log::info!(
                                    "[TxDispacher] key profile {} 的 Harmonic searcher pubkey={}",
                                    p,
                                    solana_sdk::signer::Signer::pubkey(&*alt_searcher)
                                );
                                channels.insert_alt(
                                    p.clone(),
                                    RegionPool::single(
                                        own,
                                        sol_tx_send::platform_clients::harmonic::HarmonicBlockEngine::init_with(alt_searcher),
                                    ),
                                );
                            }
                            Err(e) => log::warn!(
                                "[TxDispacher] key profile {} 的 HARMONIC_SEARCHER_PRIVATE_KEY_BASE58 解析失败，跳过该 profile: {e}",
                                p
                            ),
                        }
                    }
                    b = b.harmonic_pool(channels);
                }
                Err(e) => log::warn!("[TxDispacher] HARMONIC_SEARCHER_PRIVATE_KEY_BASE58 解析失败，跳过: {e}"),
            }
        }

        let d = b.build();
        // 启动时把“实际构造了哪些平台 / 哪些地区 / 哪些 key profile”打出来，
        // 方便核对 feature、key 与 TX_KEY_PROFILES。
        d.log_channels();

        // 声明了 profile、但一个 `<ENV>_<PROFILE>` 都没配到 → 运行时用它会**静默回退
        // 默认 key**（稀疏覆盖的代价）。这里提前告警，避免「以为换了 key 其实没换」。
        if !key_profiles.is_empty() {
            let registered: std::collections::HashSet<String> = d
                .channels()
                .iter()
                .flat_map(|c| c.key_profiles.iter().cloned())
                .collect();
            for p in &key_profiles {
                if !registered.contains(p.as_str()) {
                    log::warn!(
                        "[TxDispacher] key profile {p} 没有匹配到任何凭据（未配置任何 <ENV>_{}），用它会静默回退默认 key",
                        p.env_suffix()
                    );
                }
            }
        }

        d
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 备用凭据的环境变量命名 = 原变量名 + `_<大写PROFILE>`（`-` → `_`）。
    ///
    /// 这条约定对所有平台一致：字符串 key（ASTRALANE_KEY…）与 keypair
    /// （EVER_STAKE_QUIC_KEYPAIR / HARMONIC_SEARCHER_PRIVATE_KEY_BASE58…）都一样。
    #[test]
    fn alt_env_names_follow_the_platform_convention() {
        let alt = KeyProfile::new("alt");
        let hot = KeyProfile::new("hot-backup");
        for base in [
            "ASTRALANE_KEY",
            "TEMPORAL_KEY",
            "NODEONE_KEY",
            "BLOCKRAZOR_KEY",
            "FLASHBLOCK_KEY",
            "ZEROSLOT_KEY",
            "NEXTBLOCK_TOKEN",
            "STELLIUM_API_KEY",
            "HELIUS_KEY",
            "JITO_UUID",
            "LANDX_API_KEY",
            "SYNCRO_API_KEY",
            "EVER_STAKE_QUIC_KEYPAIR",
            "HARMONIC_SEARCHER_PRIVATE_KEY_BASE58",
        ] {
            assert_eq!(profile_env(base, &alt), format!("{base}_ALT"), "{base}");
            assert_eq!(profile_env(base, &hot), format!("{base}_HOT_BACKUP"), "{base}");
        }
    }
}
