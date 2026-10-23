//! TxDispacherBuilder — 逐个注入平台客户端，build() 得到 TxDispacher。

use crate::TxDispacher;
use crate::RegionProfile;
use crate::region_pool::RegionPool;
use sol_tx_send::constants::REGION;
use sol_slot_leader::SlotOracle;
use std::sync::Arc;

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
#[cfg(feature = "nodeone_quic")]
use sol_tx_send::platform_clients::nodeone_quic::NodeOneQuic;
#[cfg(feature = "stellium")]
use sol_tx_send::platform_clients::stellium::Stellium;
#[cfg(feature = "syncro")]
use sol_tx_send::platform_clients::syncro::Syncro;
#[cfg(feature = "temporal")]
use sol_tx_send::platform_clients::temporal::Temporal;
#[cfg(feature = "zeroslot")]
use sol_tx_send::platform_clients::zeroslot::ZeroSlot;

/// TxDispacher 构建器。
pub struct TxDispacherBuilder<O: SlotOracle> {
    oracle: O,
    /// 本机的地区计划（决定 leader 地区 -> 入口地区）。
    region_profile: RegionProfile,

    #[cfg(feature = "astralane")]
    astralane: Option<Arc<RegionPool<Astralane>>>,
    #[cfg(feature = "astralane_quic")]
    astralane_quic: Option<Arc<AstralaneQuic>>,
    #[cfg(feature = "everstake")]
    everstake: Option<Arc<RegionPool<EverStake>>>,
    #[cfg(feature = "everstake_quic")]
    everstake_quic: Option<Arc<EverStakeQuic>>,
    #[cfg(feature = "flash_block")]
    flash_block: Option<Arc<RegionPool<FlashBlock>>>,
    #[cfg(feature = "nodeone")]
    nodeone: Option<Arc<RegionPool<NodeOne>>>,
    #[cfg(feature = "nodeone_quic")]
    nodeone_quic: Option<Arc<NodeOneQuic>>,
    #[cfg(feature = "blockrazor")]
    blockrazor: Option<Arc<RegionPool<Blockrazor>>>,
    #[cfg(feature = "temporal")]
    temporal: Option<Arc<RegionPool<Temporal>>>,
    #[cfg(feature = "helius")]
    helius_max: Option<Arc<RegionPool<HeliusMax>>>,
    #[cfg(feature = "helius")]
    helius_swqos: Option<Arc<RegionPool<HeliusSwqos>>>,
    #[cfg(feature = "zeroslot")]
    zeroslot: Option<Arc<RegionPool<ZeroSlot>>>,
    #[cfg(feature = "nextblock")]
    nextblock: Option<Arc<RegionPool<NextBlock>>>,
    #[cfg(feature = "stellium")]
    stellium: Option<Arc<RegionPool<Stellium>>>,
    #[cfg(feature = "syncro")]
    syncro: Option<Arc<RegionPool<Syncro>>>,
    #[cfg(feature = "jito")]
    jito: Option<Arc<RegionPool<Jito>>>,
    #[cfg(feature = "landx")]
    landx: Option<Arc<RegionPool<LandX>>>,
    #[cfg(feature = "harmonic")]
    harmonic: Option<Arc<HarmonicBlockEngine>>,
}

impl<O: SlotOracle> TxDispacherBuilder<O> {
    pub(crate) fn new(oracle: O) -> Self {
        Self {
            oracle,
            region_profile: RegionProfile::single(*REGION),
            #[cfg(feature = "astralane")]
            astralane: None,
            #[cfg(feature = "astralane_quic")]
            astralane_quic: None,
            #[cfg(feature = "everstake")]
            everstake: None,
            #[cfg(feature = "everstake_quic")]
            everstake_quic: None,
            #[cfg(feature = "flash_block")]
            flash_block: None,
            #[cfg(feature = "nodeone")]
            nodeone: None,
            #[cfg(feature = "nodeone_quic")]
            nodeone_quic: None,
            #[cfg(feature = "blockrazor")]
            blockrazor: None,
            #[cfg(feature = "temporal")]
            temporal: None,
            #[cfg(feature = "helius")]
            helius_max: None,
            #[cfg(feature = "helius")]
            helius_swqos: None,
            #[cfg(feature = "zeroslot")]
            zeroslot: None,
            #[cfg(feature = "nextblock")]
            nextblock: None,
            #[cfg(feature = "stellium")]
            stellium: None,
            #[cfg(feature = "syncro")]
            syncro: None,
            #[cfg(feature = "jito")]
            jito: None,
            #[cfg(feature = "landx")]
            landx: None,
            #[cfg(feature = "harmonic")]
            harmonic: None,
        }
    }

    /// 注入地区计划（决定 leader 所在地区 -> 用哪个入口地区的 client）。
    ///
    /// 不设置时退化为「只开本机地区」，行为与旧版本一致。
    pub fn region_profile(mut self, profile: RegionProfile) -> Self {
        self.region_profile = profile;
        self
    }

    // ── setter 方法（feature-gated）────────────────────────────────────────

    #[cfg(feature = "astralane")]
    pub fn astralane(mut self, c: Astralane) -> Self {
        self.astralane = Some(Arc::new(RegionPool::single(*REGION, c)));
        self
    }

    #[cfg(feature = "astralane_quic")]
    pub fn astralane_quic(mut self, c: AstralaneQuic) -> Self {
        self.astralane_quic = Some(Arc::new(c));
        self
    }

    #[cfg(feature = "everstake")]
    pub fn everstake(mut self, c: EverStake) -> Self {
        self.everstake = Some(Arc::new(RegionPool::single(*REGION, c)));
        self
    }

    #[cfg(feature = "everstake_quic")]
    pub fn everstake_quic(mut self, c: EverStakeQuic) -> Self {
        self.everstake_quic = Some(Arc::new(c));
        self
    }

    #[cfg(feature = "flash_block")]
    pub fn flash_block(mut self, c: FlashBlock) -> Self {
        self.flash_block = Some(Arc::new(RegionPool::single(*REGION, c)));
        self
    }

    #[cfg(feature = "nodeone_quic")]
    pub fn nodeone_quic(mut self, c: NodeOneQuic) -> Self {
        self.nodeone_quic = Some(Arc::new(c));
        self
    }

    #[cfg(feature = "nodeone")]
    pub fn nodeone(mut self, c: NodeOne) -> Self {
        self.nodeone = Some(Arc::new(RegionPool::single(*REGION, c)));
        self
    }

    #[cfg(feature = "blockrazor")]
    pub fn blockrazor(mut self, c: Blockrazor) -> Self {
        self.blockrazor = Some(Arc::new(RegionPool::single(*REGION, c)));
        self
    }

    #[cfg(feature = "temporal")]
    pub fn temporal(mut self, c: Temporal) -> Self {
        self.temporal = Some(Arc::new(RegionPool::single(*REGION, c)));
        self
    }

    #[cfg(feature = "helius")]
    pub fn helius_max(mut self, c: HeliusMax) -> Self {
        self.helius_max = Some(Arc::new(RegionPool::single(*REGION, c)));
        self
    }

    #[cfg(feature = "helius")]
    pub fn helius_swqos(mut self, c: HeliusSwqos) -> Self {
        self.helius_swqos = Some(Arc::new(RegionPool::single(*REGION, c)));
        self
    }

    #[cfg(feature = "zeroslot")]
    pub fn zeroslot(mut self, c: ZeroSlot) -> Self {
        self.zeroslot = Some(Arc::new(RegionPool::single(*REGION, c)));
        self
    }

    #[cfg(feature = "nextblock")]
    pub fn nextblock(mut self, c: NextBlock) -> Self {
        self.nextblock = Some(Arc::new(RegionPool::single(*REGION, c)));
        self
    }

    #[cfg(feature = "stellium")]
    pub fn stellium(mut self, c: Stellium) -> Self {
        self.stellium = Some(Arc::new(RegionPool::single(*REGION, c)));
        self
    }

    /// 注入 **P2P.org Syncro Sender**（带 API key 的私有端点）。
    ///
    /// 只在 `slot_leader.name == "P2P.org Turbo"` 时作为额外通道参与发送，
    /// 见 `SendRoute::P2pTurbo`。
    #[cfg(feature = "syncro")]
    pub fn syncro(mut self, c: Syncro) -> Self {
        self.syncro = Some(Arc::new(RegionPool::single(*REGION, c)));
        self
    }

    #[cfg(feature = "jito")]
    pub fn jito(mut self, c: Jito) -> Self {
        self.jito = Some(Arc::new(RegionPool::single(*REGION, c)));
        self
    }

    /// 注入 **LandX**（HTTP JSON-RPC，API key 放 URL path）。
    #[cfg(feature = "landx")]
    pub fn landx(mut self, c: LandX) -> Self {
        self.landx = Some(Arc::new(RegionPool::single(*REGION, c)));
        self
    }

    #[cfg(feature = "harmonic")]
    pub fn harmonic(mut self, c: HarmonicBlockEngine) -> Self {
        self.harmonic = Some(Arc::new(c));
        self
    }


    // ── 多地区 setter（接收已按 fanout 构造好的 RegionPool）──────────────
    #[cfg(feature = "astralane")]
    pub fn astralane_pool(mut self, p: RegionPool<Astralane>) -> Self {
        self.astralane = Some(Arc::new(p));
        self
    }

    #[cfg(feature = "everstake")]
    pub fn everstake_pool(mut self, p: RegionPool<EverStake>) -> Self {
        self.everstake = Some(Arc::new(p));
        self
    }

    #[cfg(feature = "flash_block")]
    pub fn flash_block_pool(mut self, p: RegionPool<FlashBlock>) -> Self {
        self.flash_block = Some(Arc::new(p));
        self
    }

    #[cfg(feature = "nodeone")]
    pub fn nodeone_pool(mut self, p: RegionPool<NodeOne>) -> Self {
        self.nodeone = Some(Arc::new(p));
        self
    }

    #[cfg(feature = "blockrazor")]
    pub fn blockrazor_pool(mut self, p: RegionPool<Blockrazor>) -> Self {
        self.blockrazor = Some(Arc::new(p));
        self
    }

    #[cfg(feature = "temporal")]
    pub fn temporal_pool(mut self, p: RegionPool<Temporal>) -> Self {
        self.temporal = Some(Arc::new(p));
        self
    }

    #[cfg(feature = "helius")]
    pub fn helius_max_pool(mut self, p: RegionPool<HeliusMax>) -> Self {
        self.helius_max = Some(Arc::new(p));
        self
    }

    #[cfg(feature = "helius")]
    pub fn helius_swqos_pool(mut self, p: RegionPool<HeliusSwqos>) -> Self {
        self.helius_swqos = Some(Arc::new(p));
        self
    }

    #[cfg(feature = "zeroslot")]
    pub fn zeroslot_pool(mut self, p: RegionPool<ZeroSlot>) -> Self {
        self.zeroslot = Some(Arc::new(p));
        self
    }

    #[cfg(feature = "nextblock")]
    pub fn nextblock_pool(mut self, p: RegionPool<NextBlock>) -> Self {
        self.nextblock = Some(Arc::new(p));
        self
    }

    #[cfg(feature = "stellium")]
    pub fn stellium_pool(mut self, p: RegionPool<Stellium>) -> Self {
        self.stellium = Some(Arc::new(p));
        self
    }

    #[cfg(feature = "syncro")]
    pub fn syncro_pool(mut self, p: RegionPool<Syncro>) -> Self {
        self.syncro = Some(Arc::new(p));
        self
    }

    #[cfg(feature = "jito")]
    pub fn jito_pool(mut self, p: RegionPool<Jito>) -> Self {
        self.jito = Some(Arc::new(p));
        self
    }

    #[cfg(feature = "landx")]
    pub fn landx_pool(mut self, p: RegionPool<LandX>) -> Self {
        self.landx = Some(Arc::new(p));
        self
    }

    // ── build ─────────────────────────────────────────────────────────────

    pub fn build(self) -> TxDispacher<O> {
        TxDispacher {
            oracle: self.oracle,
            region_profile: self.region_profile,
            #[cfg(feature = "astralane")]
            astralane: self.astralane,
            #[cfg(feature = "astralane_quic")]
            astralane_quic: self.astralane_quic,
            #[cfg(feature = "everstake")]
            everstake: self.everstake,
            #[cfg(feature = "everstake_quic")]
            everstake_quic: self.everstake_quic,
            #[cfg(feature = "flash_block")]
            flash_block: self.flash_block,
            #[cfg(feature = "nodeone")]
            nodeone: self.nodeone,
            #[cfg(feature = "nodeone_quic")]
            nodeone_quic: self.nodeone_quic,
            #[cfg(feature = "blockrazor")]
            blockrazor: self.blockrazor,
            #[cfg(feature = "temporal")]
            temporal: self.temporal,
            #[cfg(feature = "helius")]
            helius_max: self.helius_max,
            #[cfg(feature = "helius")]
            helius_swqos: self.helius_swqos,
            #[cfg(feature = "zeroslot")]
            zeroslot: self.zeroslot,
            #[cfg(feature = "nextblock")]
            nextblock: self.nextblock,
            #[cfg(feature = "stellium")]
            stellium: self.stellium,
            #[cfg(feature = "syncro")]
            syncro: self.syncro,
            #[cfg(feature = "jito")]
            jito: self.jito,
            #[cfg(feature = "landx")]
            landx: self.landx,
            #[cfg(feature = "harmonic")]
            harmonic: self.harmonic,
        }
    }
}
