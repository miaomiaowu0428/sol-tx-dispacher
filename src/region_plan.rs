//! 地区路由计划（RegionPlan）。
//!
//! 一份 TOML 全机队通用：启动时按本机 `REGION` 选 profile，得到「要为本机开哪些
//! 入口地区的 client」。
//!
//! - 默认值 **内嵌进二进制**（[`DEFAULT_PLAN`] + `include_str!`），部署时不依赖
//!   额外的配置目录 / CWD；
//! - 设置 `REGION_PLAN_PATH` 可在运行时覆盖，调 fanout 不必重新编译。
//!
//! # 为什么配置里只有「地区列表」
//!
//! 每个地区开哪些平台由**代码**决定（各 client 的 `endpoint_for(region)` 已经
//! 穷举了所有地区并做了就近回退），配置里再写一遍平台名只会制造两份真相。
//!
//! # 与发送路径的关系
//!
//! [`RegionProfile::ingress_for`] 给出「leader 地区 → 用哪个入口地区」，供
//! dispatcher 在 `resolve_route` 之后挑选 `RegionPool` 里的 client 实例。

use anyhow::{Context, Result};
use sol_tx_send::platform_clients::Region;
use std::collections::HashSet;

/// 内嵌的默认计划（`config/RegionPlan.toml`，编译期打进二进制）。
pub const DEFAULT_PLAN: &str = include_str!("../config/RegionPlan.toml");

/// 运行时覆盖计划文件的环境变量。
pub const PLAN_PATH_ENV: &str = "REGION_PLAN_PATH";

/// 支持的配置格式版本；不匹配直接报错，避免旧字段被静默忽略。
pub const SUPPORTED_VERSION: u32 = 1;

/// 整个机队的地区计划。
#[derive(Debug, Clone, serde::Deserialize)]
pub struct RegionPlan {
    #[serde(default = "default_version")]
    pub version: u32,
    #[serde(default)]
    pub profiles: Vec<RegionProfile>,
    #[serde(default)]
    pub policy: Policy,
}

/// 单个「本机地区」的配置。
#[derive(Debug, Clone, serde::Deserialize)]
pub struct RegionProfile {
    /// 本机所在地，与 `REGION` 环境变量对应。
    pub own_region: Region,
    /// 本机要开的入口地区，**顺序即优先级**（近 → 远）。
    ///
    /// 必须包含 `own_region`：它是所有兜底的落点。
    #[serde(default)]
    pub fanout: Vec<Region>,
}

/// 路由策略。
#[derive(Debug, Clone, serde::Deserialize)]
pub struct Policy {
    /// leader 地区在 `fanout` 内时直接走该地区入口，否则回退 `own_region`。
    ///
    /// 跨洲的 leader 一律走本机入口，由平台自己内部路由——这是刻意的：
    /// 我们不跨洲开 client。
    #[serde(default = "default_true")]
    pub leader_local: bool,
}

impl Default for Policy {
    fn default() -> Self {
        Self { leader_local: true }
    }
}

fn default_version() -> u32 {
    SUPPORTED_VERSION
}

fn default_true() -> bool {
    true
}

impl Default for RegionPlan {
    /// 空计划：没有任何 profile。`profile_or_single` 会把本机地区当成单地区
    /// profile，因此这是「配置加载失败」时的安全兜底。
    fn default() -> Self {
        Self {
            version: SUPPORTED_VERSION,
            profiles: Vec::new(),
            policy: Policy::default(),
        }
    }
}

impl RegionPlan {
    /// 从 TOML 文本解析并校验。
    pub fn parse(src: &str) -> Result<Self> {
        let plan: Self = toml::from_str(src).context("解析 RegionPlan TOML 失败")?;
        plan.validate()?;
        Ok(plan)
    }

    /// 加载计划：`REGION_PLAN_PATH` 存在则读文件，否则用内嵌默认值。
    pub fn load() -> Result<Self> {
        match std::env::var(PLAN_PATH_ENV) {
            Ok(path) if !path.trim().is_empty() => {
                let src = std::fs::read_to_string(&path)
                    .with_context(|| format!("读取 {PLAN_PATH_ENV}={path} 失败"))?;
                Self::parse(&src).with_context(|| format!("解析 {path} 失败"))
            }
            _ => Self::parse(DEFAULT_PLAN).context("解析内嵌 RegionPlan 失败"),
        }
    }

    /// 校验配置自身的一致性（越早失败越好，别等运行到一半才发现）。
    pub fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            self.version == SUPPORTED_VERSION,
            "RegionPlan 版本 {} 不受支持（当前只支持 {}）",
            self.version,
            SUPPORTED_VERSION
        );
        anyhow::ensure!(!self.profiles.is_empty(), "RegionPlan 没有任何 profiles");

        let mut own_regions = HashSet::new();
        for profile in &self.profiles {
            let own = profile.own_region;
            anyhow::ensure!(own != Region::Unknown, "profile 的 own_region 不能是 Unknown");
            anyhow::ensure!(
                own_regions.insert(own),
                "own_region {own} 出现了多个 profile"
            );
            anyhow::ensure!(!profile.fanout.is_empty(), "profile {own} 的 fanout 为空");
            anyhow::ensure!(
                profile.fanout.contains(&own),
                "profile {own} 的 fanout 必须包含 own_region（兜底落点）"
            );

            let mut seen = HashSet::new();
            for region in &profile.fanout {
                anyhow::ensure!(*region != Region::Unknown, "profile {own} 的 fanout 不能含 Unknown");
                anyhow::ensure!(seen.insert(*region), "profile {own} 的 fanout 重复地区 {region}");
            }
        }
        Ok(())
    }

    /// 取本机地区对应的 profile。
    pub fn profile_for(&self, own: Region) -> Option<&RegionProfile> {
        self.profiles.iter().find(|p| p.own_region == own)
    }

    /// 取 profile；没配就退回「只开本机地区」的安全默认（并告警）。
    pub fn profile_or_single(&self, own: Region) -> RegionProfile {
        match self.profile_for(own) {
            Some(p) => p.clone(),
            None => {
                log::warn!(
                    "[RegionPlan] 没有 own_region={own} 的 profile，退化为只开本机地区"
                );
                RegionProfile::single(own)
            }
        }
    }
}

impl RegionProfile {
    /// 只开本机地区的兜底 profile。
    pub fn single(own: Region) -> Self {
        Self {
            own_region: own,
            fanout: vec![own],
        }
    }

    /// leader 地区 → 实际使用的入口地区。
    ///
    /// `leader` 在 fanout 内就用它（leader 就近），否则回退 `own_region`
    /// （跨洲 leader 由平台内部路由）。
    pub fn ingress_for(&self, leader: Region) -> Region {
        if self.fanout.contains(&leader) {
            leader
        } else {
            self.own_region
        }
    }
}

/// 把 `slot_leader.region` 的原始文本映射到发送侧的地区枚举。
///
/// DB 里的格式是 `"<国家> / <城市>"`（例如 `"Germany / Frankfurt am Main"`），
/// 城市数量远多于我们关心的机房，所以按「城市关键词 → 国家」两级匹配：
///
/// 1. 先认城市（同一国家里可能有多個我们关心的地区，比如美国的东西部）；
/// 2. 认不出城市时按国家归类（德国→法兰克福、荷兰→阿姆斯特丹…）；
/// 3. 都不命中返回 [`Region::Unknown`]，由调用方回退 `own_region`。
///
/// 刻意保守：宁可返回 `Unknown`（不参与 leader 就近路由），也不要把欧洲的
/// leader 猜成大西洋对岸。
pub fn geo_to_region(geo: &str) -> Region {
    let s = geo.trim().to_ascii_lowercase();
    if s.is_empty() {
        return Region::Unknown;
    }

    // ── 城市级（同国家内需要区分地区）──────────────────────────────────
    // 欧洲
    if s.contains("amsterdam") || s.contains("haarlem") || s.contains("schiphol") {
        return Region::Amsterdam;
    }
    if s.contains("frankfurt") || s.contains("eschborn") || s.contains("offenbach") || s.contains("hanau") {
        return Region::Frankfurt;
    }
    if s.contains("london") || s.contains("bexley") || s.contains("kent") {
        return Region::London;
    }
    if s.contains("dublin") || s.contains("crumlin") {
        return Region::Dublin;
    }
    if s.contains("limburg") {
        return Region::Limburg;
    }
    if s.contains("lithuania") || s.contains("šiauliai") || s.contains("siauliai") {
        return Region::Lithuania;
    }
    // 亚太
    if s.contains("tokyo") {
        return Region::Tokyo;
    }
    if s.contains("singapore") {
        return Region::Singapore;
    }
    // 北美
    if s.contains("newark") || s.contains("new york") || s.contains("sterling") || s.contains("leesburg") || s.contains("boardman") || s.contains("secaucus") {
        return Region::NewYork;
    }
    if s.contains("salt lake") || s.contains("bluffdale") {
        return Region::SaltLakeCity;
    }
    if s.contains("los angeles") || s.contains("san jose") || s.contains("corona") || s.contains("fremont") {
        return Region::LosAngeles;
    }
    if s.contains("pittsburgh") {
        return Region::Pittsburgh;
    }

    // ── 国家级（城市不认识，但国家能定位到大区）────────────────────────
    let country = s.split('/').next().unwrap_or("").trim().to_string();
    match country.as_str() {
        "germany" => Region::Frankfurt,
        "the netherlands" | "netherlands" => Region::Amsterdam,
        "united kingdom" => Region::London,
        "ireland" => Region::Dublin,
        "lithuania" => Region::Lithuania,
        "japan" => Region::Tokyo,
        "singapore" => Region::Singapore,
        // 美国/加拿大/南美跨度太大，城市认不出就不猜
        _ => Region::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn geo_maps_major_cities() {
        use Region::*;
        let cases = [
            ("Germany / Frankfurt am Main", Frankfurt),
            ("The Netherlands / Amsterdam", Amsterdam),
            ("United Kingdom / London", London),
            ("Ireland / Crumlin", Dublin),
            ("Lithuania / Šiauliai", Lithuania),
            ("Japan / Tokyo", Tokyo),
            ("Singapore / Singapore", Singapore),
            ("United States / Newark", NewYork),
            ("United States / Bluffdale", SaltLakeCity),
            ("United States / San Jose", LosAngeles),
            ("United States / Pittsburgh", Pittsburgh),
        ];
        for (geo, expected) in cases {
            assert_eq!(super::geo_to_region(geo), expected, "geo={geo}");
        }
    }

    #[test]
    fn geo_falls_back_to_country_then_unknown() {
        use Region::*;
        // 城市不认识，但国家能定性
        assert_eq!(super::geo_to_region("Germany / Eschborn"), Frankfurt);
        assert_eq!(super::geo_to_region("The Netherlands / Lelystad"), Amsterdam);
        assert_eq!(super::geo_to_region("Japan / Monzen-nakachō"), Tokyo);
        // 美国城市不认识时不猜（跨度太大）
        assert_eq!(super::geo_to_region("United States / Nowhere"), Unknown);
        assert_eq!(super::geo_to_region("Brazil / São Paulo"), Unknown);
        // 空值 / 脏数据
        assert_eq!(super::geo_to_region(""), Unknown);
        assert_eq!(super::geo_to_region("   "), Unknown);
    }

    #[test]
    fn default_plan_is_valid() {
        let plan = RegionPlan::parse(DEFAULT_PLAN).expect("内嵌的 config/RegionPlan.toml 必须合法");
        assert_eq!(plan.version, SUPPORTED_VERSION);
        assert!(!plan.profiles.is_empty());
        // 配置文件里出现的每个 own_region 都必须能反查
        for p in &plan.profiles {
            assert!(plan.profile_for(p.own_region).is_some());
        }
    }

    #[test]
    fn ingress_prefers_leader_region() {
        let p = RegionProfile {
            own_region: Region::Frankfurt,
            fanout: vec![Region::Frankfurt, Region::Amsterdam],
        };
        // fanout 内 -> 用 leader 所在地区
        assert_eq!(p.ingress_for(Region::Amsterdam), Region::Amsterdam);
        // 跨洲 -> 回退本机
        assert_eq!(p.ingress_for(Region::Tokyo), Region::Frankfurt);
        // Unknown -> 回退本机
        assert_eq!(p.ingress_for(Region::Unknown), Region::Frankfurt);
    }

    #[test]
    fn missing_profile_falls_back_to_single() {
        let plan = RegionPlan::parse(DEFAULT_PLAN).unwrap();
        // 配置里没有立陶宛的 profile
        let p = plan.profile_or_single(Region::Lithuania);
        assert_eq!(p.own_region, Region::Lithuania);
        assert_eq!(p.fanout, vec![Region::Lithuania]);
    }

    #[test]
    fn rejects_fanout_without_own_region() {
        let bad = r#"
            version = 1
            [[profiles]]
            own_region = "Frankfurt"
            fanout = ["Amsterdam", "London"]
        "#;
        let err = RegionPlan::parse(bad).unwrap_err().to_string();
        assert!(err.contains("必须包含 own_region"), "unexpected: {err}");
    }

    #[test]
    fn rejects_duplicate_region_in_fanout() {
        let bad = r#"
            version = 1
            [[profiles]]
            own_region = "Frankfurt"
            fanout = ["Frankfurt", "Amsterdam", "Amsterdam"]
        "#;
        let err = RegionPlan::parse(bad).unwrap_err().to_string();
        assert!(err.contains("重复地区"), "unexpected: {err}");
    }

    #[test]
    fn rejects_unknown_region_name() {
        // 地区名写错必须硬失败，不能静默变成 Unknown
        let bad = r#"
            version = 1
            [[profiles]]
            own_region = "Frankfurt"
            fanout = ["Frankfurt", "Mars"]
        "#;
        assert!(RegionPlan::parse(bad).is_err());
    }
}
