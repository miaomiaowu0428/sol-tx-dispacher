//! FIFO leader 配置。
//!
//! 这些 leader 采用先到先得（FIFO）出块、不参与 tip / cu_price 竞价。
//! 命中时发送强制 tip=None、cu_price=None（不管上游传什么）。
//!
//! 数据来源：`config/FIFO-Leader.json`（(leader vote account, client_type_id) 快照），硬编码于此。

/// 走 FIFO（不参与 tip/cu_price 竞价）的 (leader vote account, client_type_id) 列表。
pub const FIFO_LEADERS: &[(solana_sdk::pubkey::Pubkey, u16)] = &[
    (solana_sdk::pubkey!("Fd7btgySsrjuo25CJCj7oE7VPMyezDhnx7pZkj2v69Nk"), 8),
    (solana_sdk::pubkey!("Fd7btgySsrjuo25CJCj7oE7VPMyezDhnx7pZkj2v69Nk"), 1),
    (solana_sdk::pubkey!("q9XWcZ7T1wP4bW9SB4XgNNwjnFEJ982nE8aVbbNuwot"), 1),
    (solana_sdk::pubkey!("q9XWcZ7T1wP4bW9SB4XgNNwjnFEJ982nE8aVbbNuwot"), 8),
    (solana_sdk::pubkey!("Awes4Tr6TX8JDzEhCZY2QVNimT6iD1zWHzf1vNyGvpLM"), 2),
    (solana_sdk::pubkey!("9jxgosAfHgHzwnxsHw4RAZYaLVokMbnYtmiZBreynGFP"), 5),
    (solana_sdk::pubkey!("E1r4Psq84tHfQ6aPTvvDka4U3u8zPVD7gEUrH25RdxHL"), 1),
    (solana_sdk::pubkey!("JupmVLmA8RoyTUbTMMuTtoPWHEiNQobxgTeGTrPNkzT"), 1),
    (solana_sdk::pubkey!("JD549HsbJHeEKKUrKgg4Fj2iyv2RGjsV7NTZjZUrHybB"), 2),
    (solana_sdk::pubkey!("9rkJMARqK6VBkcxGfKBAwnA44gPAfGxPbPsfsggFNDSQ"), 1),
    (solana_sdk::pubkey!("5Cchr1XGEg7dbBXByV5NY2ad8jfxAM7HA3x8D56rq9Ux"), 3),
    (solana_sdk::pubkey!("5pPRHniefFjkiaArbGX3Y8NUysJmQ9tMZg3FrFGwHzSm"), 1),
    (solana_sdk::pubkey!("BkoS26vBuaXnSowACdChi4WKid8UwmuPNhEJWa8KsLHd"), 1),
    (solana_sdk::pubkey!("Aw5wEMXhbygFLR7jHtHpih8QvxVBGAMTqsQ2SjWPk1ex"), 1),
    (solana_sdk::pubkey!("AEHqTB2RtJjegsR2ePjvoJSm6AA5pnYKWVbcsn6kqTBD"), 1),
    (solana_sdk::pubkey!("DNVZMSqeRH18Xa4MCTrb1MndNf3Npg4MEwqswo23eWkf"), 5),
    (solana_sdk::pubkey!("FBbqKvwLfKGZrKrfSbPJz4ymQ7zMarhRyZtu1RBkSe89"), 5),
    (solana_sdk::pubkey!("FBKFWadXZJahGtFitAsBvbqh5968gLY7dMBBJUoUjeNi"), 1),
    (solana_sdk::pubkey!("FBKFWadXZJahGtFitAsBvbqh5968gLY7dMBBJUoUjeNi"), 10),
    (solana_sdk::pubkey!("EUcJwf7jXskRE6NZBtFPVH2EedNvNYko8LL2WT62XctB"), 1),
    (solana_sdk::pubkey!("anza1rXDVhy1NfVNtsbT3kSBh2jgB1BGZUKuUibSAJd"), 1),
    (solana_sdk::pubkey!("UPSCQNqdbiaqrQou9X9y8mr43ZHzvoNpKC26Mo7GubF"), 1),
    (solana_sdk::pubkey!("5Us18hLZPXJTS4QVuGSsUw137Dyd2tgBaem24Xsf5nBS"), 3),
    (solana_sdk::pubkey!("8tjFeSApQ85ThoQXT28acfF2KUfQr3TvTdirSkzNnYC7"), 8),
    (solana_sdk::pubkey!("7PdKhpKz7T39vZHFL1UfcYNDsLvay6hp4KPQq1aUckFf"), 2),
    (solana_sdk::pubkey!("7PdKhpKz7T39vZHFL1UfcYNDsLvay6hp4KPQq1aUckFf"), 8),
    (solana_sdk::pubkey!("2Wf9V9rPeVRUTfmWdPedCJuWVr6MFfyLuigEq42DuMDc"), 1),
    (solana_sdk::pubkey!("HH5dA42XF1HxNk1TRpG6LuKfLViMYNdAz5iWrFM4hWFi"), 1),
    (solana_sdk::pubkey!("Gv9gguvrAkgQtB5g5a3Un7trcHCxLYsk8vSojLmQMsWV"), 1),
    (solana_sdk::pubkey!("H8fHToVcZPi5bupGZohGPX2SWs8NHzgFKQ31wi5n6oux"), 8),
    (solana_sdk::pubkey!("ChorusmmK7i1AxXeiTtQgQZhQNiXYU84ULeaYF1EH15n"), 1),
    (solana_sdk::pubkey!("HEL1USMZKAL2odpNBj2oCjffnFGaYwmbGmyewGv1e2TU"), 1),
    (solana_sdk::pubkey!("GQzMeEMwAR44ugoNCifTb5NdRKos1GduDUPeNh6AgV46"), 1),
    (solana_sdk::pubkey!("ACvL73V4GNnxPVfZ7K89jCrYurLyzpEuE9qirjvh2Xmi"), 1),
];
