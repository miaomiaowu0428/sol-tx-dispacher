//! TxDispacherBuilder — 逐个注入平台客户端，build() 得到 TxDispacher。

use crate::TxDispacher;
use crate::RegionProfile;
use crate::channel_pool::ChannelPool;
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
#[cfg(feature = "flash_block_quic")]
use sol_tx_send::platform_clients::flash_block_quic::FlashBlockQuic;
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
#[cfg(feature = "landx_udp")]
use sol_tx_send::platform_clients::landx_udp::LandXUdp;
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
    astralane: Option<Arc<ChannelPool<Astralane>>>,
    #[cfg(feature = "astralane_quic")]
    astralane_quic: Option<Arc<ChannelPool<AstralaneQuic>>>,
    #[cfg(feature = "everstake")]
    everstake: Option<Arc<ChannelPool<EverStake>>>,
    #[cfg(feature = "everstake_quic")]
    everstake_quic: Option<Arc<ChannelPool<EverStakeQuic>>>,
    #[cfg(feature = "flash_block")]
    flash_block: Option<Arc<ChannelPool<FlashBlock>>>,
    #[cfg(feature = "flash_block_quic")]
    flash_block_quic: Option<Arc<ChannelPool<FlashBlockQuic>>>,
    #[cfg(feature = "nodeone")]
    nodeone: Option<Arc<ChannelPool<NodeOne>>>,
    #[cfg(feature = "nodeone_quic")]
    nodeone_quic: Option<Arc<ChannelPool<NodeOneQuic>>>,
    #[cfg(feature = "blockrazor")]
    blockrazor: Option<Arc<ChannelPool<Blockrazor>>>,
    #[cfg(feature = "temporal")]
    temporal: Option<Arc<ChannelPool<Temporal>>>,
    #[cfg(feature = "helius")]
    helius_max: Option<Arc<ChannelPool<HeliusMax>>>,
    #[cfg(feature = "helius")]
    helius_swqos: Option<Arc<ChannelPool<HeliusSwqos>>>,
    #[cfg(feature = "zeroslot")]
    zeroslot: Option<Arc<ChannelPool<ZeroSlot>>>,
    #[cfg(feature = "nextblock")]
    nextblock: Option<Arc<ChannelPool<NextBlock>>>,
    #[cfg(feature = "stellium")]
    stellium: Option<Arc<ChannelPool<Stellium>>>,
    #[cfg(feature = "syncro")]
    syncro: Option<Arc<ChannelPool<Syncro>>>,
    #[cfg(feature = "jito")]
    jito: Option<Arc<ChannelPool<Jito>>>,
    #[cfg(feature = "landx")]
    landx: Option<Arc<ChannelPool<LandX>>>,
    #[cfg(feature = "landx_udp")]
    landx_udp: Option<Arc<ChannelPool<LandXUdp>>>,
    #[cfg(feature = "harmonic")]
    harmonic: Option<Arc<ChannelPool<HarmonicBlockEngine>>>,
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
            #[cfg(feature = "flash_block_quic")]
            flash_block_quic: None,
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
            #[cfg(feature = "landx_udp")]
            landx_udp: None,
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
        self.astralane = Some(Arc::new(ChannelPool::single(*REGION, c)));
        self
    }

    #[cfg(feature = "astralane_quic")]
    pub fn astralane_quic(mut self, c: AstralaneQuic) -> Self {
        self.astralane_quic = Some(Arc::new(ChannelPool::single(*REGION, c)));
        self
    }

    #[cfg(feature = "everstake")]
    pub fn everstake(mut self, c: EverStake) -> Self {
        self.everstake = Some(Arc::new(ChannelPool::single(*REGION, c)));
        self
    }

    #[cfg(feature = "everstake_quic")]
    pub fn everstake_quic(mut self, c: EverStakeQuic) -> Self {
        self.everstake_quic = Some(Arc::new(ChannelPool::single(*REGION, c)));
        self
    }

    #[cfg(feature = "flash_block_quic")]
    pub fn flash_block_quic(mut self, c: FlashBlockQuic) -> Self {
        self.flash_block_quic = Some(Arc::new(ChannelPool::single(*REGION, c)));
        self
    }

    #[cfg(feature = "flash_block")]
    pub fn flash_block(mut self, c: FlashBlock) -> Self {
        self.flash_block = Some(Arc::new(ChannelPool::single(*REGION, c)));
        self
    }

    #[cfg(feature = "nodeone_quic")]
    pub fn nodeone_quic(mut self, c: NodeOneQuic) -> Self {
        self.nodeone_quic = Some(Arc::new(ChannelPool::single(*REGION, c)));
        self
    }

    #[cfg(feature = "nodeone")]
    pub fn nodeone(mut self, c: NodeOne) -> Self {
        self.nodeone = Some(Arc::new(ChannelPool::single(*REGION, c)));
        self
    }

    #[cfg(feature = "blockrazor")]
    pub fn blockrazor(mut self, c: Blockrazor) -> Self {
        self.blockrazor = Some(Arc::new(ChannelPool::single(*REGION, c)));
        self
    }

    #[cfg(feature = "temporal")]
    pub fn temporal(mut self, c: Temporal) -> Self {
        self.temporal = Some(Arc::new(ChannelPool::single(*REGION, c)));
        self
    }

    #[cfg(feature = "helius")]
    pub fn helius_max(mut self, c: HeliusMax) -> Self {
        self.helius_max = Some(Arc::new(ChannelPool::single(*REGION, c)));
        self
    }

    #[cfg(feature = "helius")]
    pub fn helius_swqos(mut self, c: HeliusSwqos) -> Self {
        self.helius_swqos = Some(Arc::new(ChannelPool::single(*REGION, c)));
        self
    }

    #[cfg(feature = "zeroslot")]
    pub fn zeroslot(mut self, c: ZeroSlot) -> Self {
        self.zeroslot = Some(Arc::new(ChannelPool::single(*REGION, c)));
        self
    }

    #[cfg(feature = "nextblock")]
    pub fn nextblock(mut self, c: NextBlock) -> Self {
        self.nextblock = Some(Arc::new(ChannelPool::single(*REGION, c)));
        self
    }

    #[cfg(feature = "stellium")]
    pub fn stellium(mut self, c: Stellium) -> Self {
        self.stellium = Some(Arc::new(ChannelPool::single(*REGION, c)));
        self
    }

    /// 注入 **P2P.org Syncro Sender**（带 API key 的私有端点）。
    ///
    /// 只在 `slot_leader.name == "P2P.org Turbo"` 时作为额外通道参与发送，
    /// 见 `SendRoute::P2pTurbo`。
    #[cfg(feature = "syncro")]
    pub fn syncro(mut self, c: Syncro) -> Self {
        self.syncro = Some(Arc::new(ChannelPool::single(*REGION, c)));
        self
    }

    #[cfg(feature = "jito")]
    pub fn jito(mut self, c: Jito) -> Self {
        self.jito = Some(Arc::new(ChannelPool::single(*REGION, c)));
        self
    }

    /// 注入 **LandX**（HTTP JSON-RPC，API key 放 URL path）。
    #[cfg(feature = "landx_udp")]
    pub fn landx_udp(mut self, c: LandXUdp) -> Self {
        self.landx_udp = Some(Arc::new(ChannelPool::single(*REGION, c)));
        self
    }

    #[cfg(feature = "landx")]
    pub fn landx(mut self, c: LandX) -> Self {
        self.landx = Some(Arc::new(ChannelPool::single(*REGION, c)));
        self
    }

    #[cfg(feature = "harmonic")]
    pub fn harmonic(mut self, c: HarmonicBlockEngine) -> Self {
        self.harmonic = Some(Arc::new(ChannelPool::single(*REGION, c)));
        self
    }

    /// 注入 **Harmonic** 的多 searcher 池（备用 searcher key 必须是对方已登记的）。
    #[cfg(feature = "harmonic")]
    pub fn harmonic_pool(mut self, p: impl Into<ChannelPool<HarmonicBlockEngine>>) -> Self {
        self.harmonic = Some(Arc::new(p.into()));
        self
    }


    // ── 多地区 setter（接收已按 fanout 构造好的 RegionPool，或带 alt key 的 ChannelPool）
    #[cfg(feature = "astralane")]
    pub fn astralane_pool(mut self, p: impl Into<ChannelPool<Astralane>>) -> Self {
        self.astralane = Some(Arc::new(p.into()));
        self
    }

    #[cfg(feature = "everstake")]
    pub fn everstake_pool(mut self, p: impl Into<ChannelPool<EverStake>>) -> Self {
        self.everstake = Some(Arc::new(p.into()));
        self
    }

    #[cfg(feature = "flash_block")]
    pub fn flash_block_pool(mut self, p: impl Into<ChannelPool<FlashBlock>>) -> Self {
        self.flash_block = Some(Arc::new(p.into()));
        self
    }

    #[cfg(feature = "nodeone")]
    pub fn nodeone_pool(mut self, p: impl Into<ChannelPool<NodeOne>>) -> Self {
        self.nodeone = Some(Arc::new(p.into()));
        self
    }

    #[cfg(feature = "blockrazor")]
    pub fn blockrazor_pool(mut self, p: impl Into<ChannelPool<Blockrazor>>) -> Self {
        self.blockrazor = Some(Arc::new(p.into()));
        self
    }

    #[cfg(feature = "temporal")]
    pub fn temporal_pool(mut self, p: impl Into<ChannelPool<Temporal>>) -> Self {
        self.temporal = Some(Arc::new(p.into()));
        self
    }

    #[cfg(feature = "helius")]
    pub fn helius_max_pool(mut self, p: impl Into<ChannelPool<HeliusMax>>) -> Self {
        self.helius_max = Some(Arc::new(p.into()));
        self
    }

    #[cfg(feature = "helius")]
    pub fn helius_swqos_pool(mut self, p: impl Into<ChannelPool<HeliusSwqos>>) -> Self {
        self.helius_swqos = Some(Arc::new(p.into()));
        self
    }

    #[cfg(feature = "zeroslot")]
    pub fn zeroslot_pool(mut self, p: impl Into<ChannelPool<ZeroSlot>>) -> Self {
        self.zeroslot = Some(Arc::new(p.into()));
        self
    }

    #[cfg(feature = "nextblock")]
    pub fn nextblock_pool(mut self, p: impl Into<ChannelPool<NextBlock>>) -> Self {
        self.nextblock = Some(Arc::new(p.into()));
        self
    }

    #[cfg(feature = "stellium")]
    pub fn stellium_pool(mut self, p: impl Into<ChannelPool<Stellium>>) -> Self {
        self.stellium = Some(Arc::new(p.into()));
        self
    }

    #[cfg(feature = "syncro")]
    pub fn syncro_pool(mut self, p: impl Into<ChannelPool<Syncro>>) -> Self {
        self.syncro = Some(Arc::new(p.into()));
        self
    }

    #[cfg(feature = "jito")]
    pub fn jito_pool(mut self, p: impl Into<ChannelPool<Jito>>) -> Self {
        self.jito = Some(Arc::new(p.into()));
        self
    }

    #[cfg(feature = "landx")]
    pub fn landx_pool(mut self, p: impl Into<ChannelPool<LandX>>) -> Self {
        self.landx = Some(Arc::new(p.into()));
        self
    }

    // ── 单实例通道的池化 setter（QUIC / UDP：单地区，不做地区扇出）─────────
    //
    // 这些通道本身「一个 socket / 一条长连接」，多地区会引入重连/预热问题，
    // 所以池里每个 key 只放一个实例；key 维度仍然成立（alt key = 第二条连接）。

    #[cfg(feature = "astralane_quic")]
    pub fn astralane_quic_pool(mut self, p: impl Into<ChannelPool<AstralaneQuic>>) -> Self {
        self.astralane_quic = Some(Arc::new(p.into()));
        self
    }

    #[cfg(feature = "everstake_quic")]
    pub fn everstake_quic_pool(mut self, p: impl Into<ChannelPool<EverStakeQuic>>) -> Self {
        self.everstake_quic = Some(Arc::new(p.into()));
        self
    }

    #[cfg(feature = "flash_block_quic")]
    pub fn flash_block_quic_pool(mut self, p: impl Into<ChannelPool<FlashBlockQuic>>) -> Self {
        self.flash_block_quic = Some(Arc::new(p.into()));
        self
    }

    #[cfg(feature = "nodeone_quic")]
    pub fn nodeone_quic_pool(mut self, p: impl Into<ChannelPool<NodeOneQuic>>) -> Self {
        self.nodeone_quic = Some(Arc::new(p.into()));
        self
    }

    #[cfg(feature = "landx_udp")]
    pub fn landx_udp_pool(mut self, p: impl Into<ChannelPool<LandXUdp>>) -> Self {
        self.landx_udp = Some(Arc::new(p.into()));
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
            #[cfg(feature = "flash_block_quic")]
            flash_block_quic: self.flash_block_quic,
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
            #[cfg(feature = "landx_udp")]
            landx_udp: self.landx_udp,
            #[cfg(feature = "harmonic")]
            harmonic: self.harmonic,
        }
    }
}
