//! 交易构建/签名的「保温」后台线程（**实验性，默认关闭**）。
//!
//! # 为什么会有这个模块
//!
//! 批量发交易时**第一笔**明显比后续慢（实测 40µs vs 20µs）。这不是玄学：L1/L2 是
//! per-core 的，空闲约 1ms 之后 V0Message::try_compile + ed25519 签名这条路径的
//! 代码/数据就凉了，冷态要多花 20µs 左右。实测（i5-13400 / Hyper-V，taskset 单核）：
//!
//! | 空闲时长 | 首笔 p50 |
//! |---|---|
//! | 0（连续） | 14.9µs |
//! | 1ms | 26.8µs |
//! | 3ms | 36.0µs |
//! | >=10ms | ~38µs（饱和） |
//!
//! 所以「每 1ms 签一笔丢着」只有在**和业务线程同一个物理核**上才有意义。实测不同核
//! 只能靠共享 L3 收回来一部分（42µs -> 27µs），同核才能回到 20µs 左右。
//!
//! 因此这里默认给**每个可用核各起一个**保温线程并各自钉死，业务线程无论被调度到哪个
//! 核都是热的——不需要去改 tokio 的 runtime / 给业务线程做绑核。
//!
//! # 开关（环境变量）
//!
//! - TX_WARM=1   ：常开
//! - TX_WARM=ab  ：**A/B 自测**，按 TX_WARM_AB_SECS（默认 300）在 on/off 之间轮换，
//!   每次切换打一行日志；配合 fire 日志里新增的 warm=on|off 字段，可以直接比较
//!   两组的 build= 分布。跑一晚上就能拿到自己的数据，不用换机器、不用换时段。
//! - 不设或 0    ：关闭（默认）
//!
//! 其它：TX_WARM_PERIOD_US（默认 1000）、TX_WARM_CORES=all|0,1,2（默认全部）。
//!
//! # 注意
//!
//! - 保温只覆盖 try_compile + try_new（签名）这两段，也就是冷态代价的 ~95%；
//!   平台 tip 地址查表只有几十 ns，不值得为它做泛型实例化的保温。
//! - 每核约 2% CPU（1ms 周期 × ~20µs）。别用 sleep 去凑 500µs 以下——sleep(1ms)
//!   实测落在 1.07ms（p99 1.35ms），更短的周期会被内核直接吞掉。
//! - 这是实验件：先 dry-run 一晚看清楚再决定要不要常开。

use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use solana_sdk::{
    hash::Hash,
    instruction::{AccountMeta, Instruction},
    message::{v0::Message as V0Message, VersionedMessage},
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    transaction::VersionedTransaction,
};

const PHASE_OFF: u8 = 0;
const PHASE_ON: u8 = 1;

/// 当前相位。fire 日志会读它（见 phase_tag）。
static PHASE: AtomicU8 = AtomicU8::new(PHASE_OFF);
/// 是否装过保温线程；没装时日志里打 n/a，避免看起来像"开了但没效果"。
static INSTALLED: AtomicBool = AtomicBool::new(false);

/// fire 日志里的 warm= 字段：n/a（没开）/ on / off（A/B 的对照组相位）。
pub fn phase_tag() -> &'static str {
    if !INSTALLED.load(Ordering::Relaxed) {
        "n/a"
    } else if PHASE.load(Ordering::Relaxed) == PHASE_ON {
        "on"
    } else {
        "off"
    }
}

/// 保温线程集合。**没有 Drop**：句柄被丢掉后线程继续跑到进程退出。
/// 想中途停就显式调 Warmer::stop。
pub struct Warmer {
    stop: Arc<AtomicBool>,
    handles: Vec<std::thread::JoinHandle<()>>,
}

impl Warmer {
    pub fn stop(self) {
        self.stop.store(true, Ordering::Relaxed);
        for h in self.handles {
            let _ = h.join();
        }
    }
}

struct WarmCfg {
    mode: &'static str, // "on" | "ab"
    period: Duration,
    ab_secs: u64,
    cores: Vec<usize>,
}

fn cfg_from_env() -> Option<WarmCfg> {
    let raw = std::env::var("TX_WARM").unwrap_or_default();
    let mode = match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "on" | "yes" => "on",
        "ab" => "ab",
        _ => return None,
    };
    let period_us = std::env::var("TX_WARM_PERIOD_US")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(1_000)
        .max(200);
    let ab_secs = std::env::var("TX_WARM_AB_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(300)
        .max(10);
    let allowed = allowed_cpus();
    if allowed.is_empty() {
        return None;
    }
    let cores = match std::env::var("TX_WARM_CORES") {
        Ok(v) if !v.trim().is_empty() && !v.eq_ignore_ascii_case("all") => v
            .split(',')
            .filter_map(|s| s.trim().parse::<usize>().ok())
            .filter(|c| allowed.contains(c))
            .collect::<Vec<_>>(),
        _ => allowed,
    };
    if cores.is_empty() {
        return None;
    }
    Some(WarmCfg { mode, period: Duration::from_micros(period_us), ab_secs, cores })
}

/// 生成一笔「形状和真实交易差不多」的 dummy 交易并签名。
///
/// 直接用 solana-sdk 而非某个平台 client：try_compile / ed25519 都是非泛型共享代码，
/// 换平台实例化不会变快也不会变慢。
fn warm_once(payer: &Keypair, ixs: &[Instruction]) {
    let Ok(msg) = V0Message::try_compile(&payer.pubkey(), ixs, &[], Hash::new_unique()) else {
        return;
    };
    let Ok(tx) = VersionedTransaction::try_new(VersionedMessage::V0(msg), &[payer]) else {
        return;
    };
    std::hint::black_box(&tx);
}

/// 按环境变量决定是否启动保温线程；返回 None 表示没开。
///
/// 在 TxDispacher::init 之后调一次即可：
///
///   let _warm = sol_tx_dispacher::warm::spawn_from_env();
pub fn spawn_from_env() -> Option<Warmer> {
    let cfg = cfg_from_env()?;

    // 固定一笔"样板交易"：payer 常驻，指令形状固定，避免每次构造 ix 的噪声。
    let payer = Keypair::new();
    let mut transfer_data = vec![2u8, 0, 0, 0];
    transfer_data.extend_from_slice(&1_000u64.to_le_bytes());
    let ixs: Vec<Instruction> = vec![
        Instruction {
            program_id: Pubkey::new_unique(),
            accounts: vec![AccountMeta::new_readonly(Pubkey::new_unique(), false)],
            data: vec![1; 8],
        },
        Instruction {
            program_id: Pubkey::new_unique(),
            accounts: vec![AccountMeta::new_readonly(Pubkey::new_unique(), false)],
            data: vec![1; 8],
        },
        Instruction {
            program_id: Pubkey::from_str_const("11111111111111111111111111111111"),
            accounts: vec![
                AccountMeta::new(payer.pubkey(), true),
                AccountMeta::new(Pubkey::new_unique(), false),
            ],
            data: transfer_data,
        },
    ];
    let ixs = Arc::new(ixs);
    let payer = Arc::new(payer);

    PHASE.store(if cfg.mode == "ab" { PHASE_OFF } else { PHASE_ON }, Ordering::Relaxed);
    INSTALLED.store(true, Ordering::Relaxed);

    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::with_capacity(cfg.cores.len() + 1);

    for cpu in &cfg.cores {
        let stop = Arc::clone(&stop);
        let period = cfg.period;
        let payer = Arc::clone(&payer);
        let ixs = Arc::clone(&ixs);
        let cpu = *cpu;
        handles.push(
            std::thread::Builder::new()
                .name(format!("tx-warm-{cpu}"))
                .spawn(move || {
                    let pinned = pin_to_cpu(cpu);
                    log::info!("[warm] 线程启动 core={cpu} pinned={pinned} period={:?}", period);
                    let mut next = Instant::now();
                    let mut window = Window::new(Duration::from_secs(60));
                    while !stop.load(Ordering::Relaxed) {
                        if PHASE.load(Ordering::Relaxed) == PHASE_ON {
                            let t = Instant::now();
                            warm_once(&payer, &ixs);
                            window.push(t.elapsed());
                        }
                        next += period;
                        let now = Instant::now();
                        if next > now {
                            std::thread::sleep(next - now);
                        } else {
                            // 落后了（被抢占 / sleep 超时）：不补跑，直接对齐到当下。
                            next = now;
                        }
                        if let Some((n, p50, max)) = window.maybe_report() {
                            log::info!(
                                "[warm] core={cpu} n={n} build_p50={:.1}us build_max={:.1}us",
                                p50,
                                max
                            );
                        }
                    }
                })
                .expect("spawn tx-warm thread"),
        );
    }

    if cfg.mode == "ab" {
        let stop = Arc::clone(&stop);
        let ab = Duration::from_secs(cfg.ab_secs);
        handles.push(
            std::thread::Builder::new()
                .name("tx-warm-ab".into())
                .spawn(move || {
                    log::info!("[warm] A/B 模式：每 {ab:?} 切换 on/off（对比 fire 日志里的 warm= 字段）");
                    while !stop.load(Ordering::Relaxed) {
                        std::thread::sleep(ab);
                        if stop.load(Ordering::Relaxed) {
                            break;
                        }
                        PHASE.store(PHASE_ON, Ordering::Relaxed);
                        log::info!("[warm] phase=on  （实验组）");
                        std::thread::sleep(ab);
                        if stop.load(Ordering::Relaxed) {
                            break;
                        }
                        PHASE.store(PHASE_OFF, Ordering::Relaxed);
                        log::info!("[warm] phase=off （对照组）");
                    }
                })
                .expect("spawn tx-warm-ab thread"),
        );
    }

    log::info!(
        "[warm] 已启动 {} 个保温线程 cores={:?} period={:?} mode={}（实验件）",
        cfg.cores.len(),
        cfg.cores,
        cfg.period,
        cfg.mode
    );
    Some(Warmer { stop, handles })
}

/// 60s 一个窗口，统计保温线程自己的「构建+签名」耗时——用来确认它真的在跑、
/// 没有被业务线程挤到失速。失速的话 A/B 实验是无效的。
struct Window {
    span: Duration,
    start: Instant,
    samples: Vec<Duration>,
}

impl Window {
    fn new(span: Duration) -> Self {
        Self { span, start: Instant::now(), samples: Vec::with_capacity(70_000) }
    }
    fn push(&mut self, d: Duration) {
        self.samples.push(d);
    }
    fn maybe_report(&mut self) -> Option<(usize, f64, f64)> {
        if self.start.elapsed() < self.span {
            return None;
        }
        let n = self.samples.len();
        let mut vals: Vec<f64> = self.samples.drain(..).map(|d| d.as_secs_f64() * 1e6).collect();
        self.start = Instant::now();
        if vals.is_empty() {
            return Some((0, 0.0, 0.0));
        }
        vals.sort_by(|a, b| a.partial_cmp(b).unwrap());
        Some((n, vals[vals.len() / 2], *vals.last().unwrap()))
    }
}

// ── CPU 亲和 ────────────────────────────────────────────────────────────────

#[cfg(target_os = "linux")]
fn allowed_cpus() -> Vec<usize> {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        let size = std::mem::size_of::<libc::cpu_set_t>();
        if libc::sched_getaffinity(0, size, &mut set) != 0 {
            // 拿不到就退回 0..n，交给 sched_setaffinity 自己挑错。
            return (0..std::thread::available_parallelism().map(|v| v.get()).unwrap_or(1)).collect();
        }
        (0..libc::CPU_SETSIZE as usize)
            .filter(|c| libc::CPU_ISSET(*c, &set))
            .collect()
    }
}

#[cfg(target_os = "linux")]
fn pin_to_cpu(cpu: usize) -> bool {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(cpu, &mut set);
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) == 0
    }
}

#[cfg(not(target_os = "linux"))]
fn allowed_cpus() -> Vec<usize> {
    (0..std::thread::available_parallelism().map(|v| v.get()).unwrap_or(1)).collect()
}

#[cfg(not(target_os = "linux"))]
fn pin_to_cpu(_cpu: usize) -> bool {
    false
}
