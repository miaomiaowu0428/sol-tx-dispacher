//! TxDispacherBuilder — 逐个注入平台客户端，build() 得到 TxDispacher。

use crate::TxDispacher;
use sol_slot_leader::SlotOracle;
use std::sync::Arc;

#[cfg(feature = "astralane")]
use sol_tx_send::platform_clients::astralane::Astralane;
#[cfg(feature = "astralane_quic")]
use sol_tx_send::platform_clients::astralane_quic::client::AstralaneQuic;
#[cfg(feature = "everstake")]
use sol_tx_send::platform_clients::ever_stake::EverStake;
#[cfg(feature = "everstake_quic")]
use sol_tx_send::platform_clients::ever_stake_quic::EverStakeQuic;
#[cfg(feature = "flash_block")]
use sol_tx_send::platform_clients::flash_block::FlashBlock;
#[cfg(feature = "nodeone")]
use sol_tx_send::platform_clients::nodeone::NodeOne;
#[cfg(feature = "blockrazor")]
use sol_tx_send::platform_clients::blockrazor::Blockrazor;
#[cfg(feature = "temporal")]
use sol_tx_send::platform_clients::temporal::Temporal;
#[cfg(feature = "helius")]
use sol_tx_send::platform_clients::helius::Helius;
#[cfg(feature = "zeroslot")]
use sol_tx_send::platform_clients::zeroslot::ZeroSlot;
#[cfg(feature = "nextblock")]
use sol_tx_send::platform_clients::nextblock::NextBlock;
#[cfg(feature = "stellium")]
use sol_tx_send::platform_clients::stellium::Stellium;
#[cfg(feature = "jito")]
use sol_tx_send::platform_clients::jito::Jito;
#[cfg(feature = "harmonic")]
use sol_tx_send::platform_clients::harmonic::HarmonicBlockEngine;

/// TxDispacher 构建器。
pub struct TxDispacherBuilder {
    oracle: Arc<dyn SlotOracle>,

    #[cfg(feature = "astralane")]
    astralane: Option<Arc<Astralane>>,
    #[cfg(feature = "astralane_quic")]
    astralane_quic: Option<Arc<AstralaneQuic>>,
    #[cfg(feature = "everstake")]
    everstake: Option<Arc<EverStake>>,
    #[cfg(feature = "everstake_quic")]
    everstake_quic: Option<Arc<EverStakeQuic>>,
    #[cfg(feature = "flash_block")]
    flash_block: Option<Arc<FlashBlock>>,
    #[cfg(feature = "nodeone")]
    nodeone: Option<Arc<NodeOne>>,
    #[cfg(feature = "blockrazor")]
    blockrazor: Option<Arc<Blockrazor>>,
    #[cfg(feature = "temporal")]
    temporal: Option<Arc<Temporal>>,
    #[cfg(feature = "helius")]
    helius: Option<Arc<Helius>>,
    #[cfg(feature = "zeroslot")]
    zeroslot: Option<Arc<ZeroSlot>>,
    #[cfg(feature = "nextblock")]
    nextblock: Option<Arc<NextBlock>>,
    #[cfg(feature = "stellium")]
    stellium: Option<Arc<Stellium>>,
    #[cfg(feature = "jito")]
    jito: Option<Arc<Jito>>,
    #[cfg(feature = "harmonic")]
    harmonic: Option<Arc<HarmonicBlockEngine>>,
}

impl TxDispacherBuilder {
    pub(crate) fn new(oracle: Arc<dyn SlotOracle>) -> Self {
        Self {
            oracle,
            #[cfg(feature = "astralane")]      astralane:      None,
            #[cfg(feature = "astralane_quic")]  astralane_quic: None,
            #[cfg(feature = "everstake")]       everstake:      None,
            #[cfg(feature = "everstake_quic")]  everstake_quic: None,
            #[cfg(feature = "flash_block")]     flash_block:    None,
            #[cfg(feature = "nodeone")]         nodeone:        None,
            #[cfg(feature = "blockrazor")]      blockrazor:     None,
            #[cfg(feature = "temporal")]        temporal:       None,
            #[cfg(feature = "helius")]          helius:         None,
            #[cfg(feature = "zeroslot")]        zeroslot:       None,
            #[cfg(feature = "nextblock")]       nextblock:      None,
            #[cfg(feature = "stellium")]        stellium:       None,
            #[cfg(feature = "jito")]            jito:           None,
            #[cfg(feature = "harmonic")]        harmonic:       None,
        }
    }

    // ── setter 方法（feature-gated）────────────────────────────────────────

    #[cfg(feature = "astralane")]
    pub fn astralane(mut self, c: Astralane) -> Self { self.astralane = Some(Arc::new(c)); self }

    #[cfg(feature = "astralane_quic")]
    pub fn astralane_quic(mut self, c: AstralaneQuic) -> Self { self.astralane_quic = Some(Arc::new(c)); self }

    #[cfg(feature = "everstake")]
    pub fn everstake(mut self, c: EverStake) -> Self { self.everstake = Some(Arc::new(c)); self }

    #[cfg(feature = "everstake_quic")]
    pub fn everstake_quic(mut self, c: EverStakeQuic) -> Self { self.everstake_quic = Some(Arc::new(c)); self }

    #[cfg(feature = "flash_block")]
    pub fn flash_block(mut self, c: FlashBlock) -> Self { self.flash_block = Some(Arc::new(c)); self }

    #[cfg(feature = "nodeone")]
    pub fn nodeone(mut self, c: NodeOne) -> Self { self.nodeone = Some(Arc::new(c)); self }

    #[cfg(feature = "blockrazor")]
    pub fn blockrazor(mut self, c: Blockrazor) -> Self { self.blockrazor = Some(Arc::new(c)); self }

    #[cfg(feature = "temporal")]
    pub fn temporal(mut self, c: Temporal) -> Self { self.temporal = Some(Arc::new(c)); self }

    #[cfg(feature = "helius")]
    pub fn helius(mut self, c: Helius) -> Self { self.helius = Some(Arc::new(c)); self }

    #[cfg(feature = "zeroslot")]
    pub fn zeroslot(mut self, c: ZeroSlot) -> Self { self.zeroslot = Some(Arc::new(c)); self }

    #[cfg(feature = "nextblock")]
    pub fn nextblock(mut self, c: NextBlock) -> Self { self.nextblock = Some(Arc::new(c)); self }

    #[cfg(feature = "stellium")]
    pub fn stellium(mut self, c: Stellium) -> Self { self.stellium = Some(Arc::new(c)); self }

    #[cfg(feature = "jito")]
    pub fn jito(mut self, c: Jito) -> Self { self.jito = Some(Arc::new(c)); self }

    #[cfg(feature = "harmonic")]
    pub fn harmonic(mut self, c: HarmonicBlockEngine) -> Self { self.harmonic = Some(Arc::new(c)); self }

    // ── build ─────────────────────────────────────────────────────────────

    pub fn build(self) -> TxDispacher {
        TxDispacher {
            oracle: self.oracle,
            #[cfg(feature = "astralane")]      astralane:      self.astralane,
            #[cfg(feature = "astralane_quic")]  astralane_quic: self.astralane_quic,
            #[cfg(feature = "everstake")]       everstake:      self.everstake,
            #[cfg(feature = "everstake_quic")]  everstake_quic: self.everstake_quic,
            #[cfg(feature = "flash_block")]     flash_block:    self.flash_block,
            #[cfg(feature = "nodeone")]         nodeone:        self.nodeone,
            #[cfg(feature = "blockrazor")]      blockrazor:     self.blockrazor,
            #[cfg(feature = "temporal")]        temporal:       self.temporal,
            #[cfg(feature = "helius")]          helius:         self.helius,
            #[cfg(feature = "zeroslot")]        zeroslot:       self.zeroslot,
            #[cfg(feature = "nextblock")]       nextblock:      self.nextblock,
            #[cfg(feature = "stellium")]        stellium:       self.stellium,
            #[cfg(feature = "jito")]            jito:           self.jito,
            #[cfg(feature = "harmonic")]        harmonic:       self.harmonic,
        }
    }
}
