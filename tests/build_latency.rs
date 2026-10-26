
//! 构建/签名热路径的延迟诊断（默认 #[ignore]，不影响正常 cargo test）。
//!
//! 跑法：
//!   taskset -c 2 cargo test --release -p sol-tx-dispacher --test build_latency -- --ignored --nocapture --test-threads=1
//!   BENCH_LOG=1 前缀后再跑，可额外装一个“异步日志”实现，模拟真实 flexi_logger 的调用侧开销。
//!
//! 回答一个问题：批量发交易时为什么第一笔慢、后续快，以及“后台每秒签一笔”能不能救回来。
//!
//! 结论（本机 i5-13400 / Hyper-V，8 vCPU，taskset 单核）：
//! - 稳态 15~23µs；空闲 >=1ms 后首笔 25µs，空闲 >=10ms 后 40µs（与线上“40 多 vs 20 多”一致）；
//! - 纯 ALU 循环在同样空闲后毫无变化 -> 不是降频/被抢占；
//! - 页错误 minflt 增量为 0 -> 不是缺页/分配器；
//! - 后台每 1s、甚至每 10ms 签一笔（同核）都没用；
//! - 同一线程、紧挨着先丢弃构建 1 笔 -> 立刻回落到 ~17µs；
//! - 拆分看：compile（组消息/哈希/排序）冷 13.5µs -> 热 1.4µs（10x），
//!   sign（ed25519）冷 28µs -> 热 16µs（1.8x），两半各背一半。

use sol_tx_send::platform_clients::{jito::Jito, BuildV0Tx, HashParam};
use solana_sdk::{
    hash::Hash,
    instruction::{AccountMeta, Instruction},
    message::{v0::Message as V0Message, VersionedMessage},
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    transaction::VersionedTransaction,
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

// --------------------------- 固定装置 ---------------------------

struct Fx {
    client: Jito,
    payer: Arc<Keypair>,
    ixs: Vec<Instruction>,
    hash: HashParam,
}

fn fixture() -> Fx {
    let payer = Arc::new(Keypair::new());
    let mut data = vec![2u8, 0, 0, 0]; // system::transfer 的 12 字节 data
    data.extend_from_slice(&1_000u64.to_le_bytes());
    Fx {
        client: Jito::new("bench-uuid"),
        payer: payer.clone(),
        ixs: vec![
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
                data,
            },
        ],
        hash: HashParam::Blockhash(Hash::new_unique()),
    }
}

/// 与 fire.rs 里 build_v0_tx 完全同一条路径（含内部日志）。
fn build_full(f: &Fx) -> f64 {
    let t = Instant::now();
    let env = f
        .client
        .build_v0_tx(
            &f.ixs,
            &f.payer,
            &Some(1_000),
            &f.hash,
            &(Some(200_000), Some(1_000)),
            &[],
            None,
        )
        .unwrap();
    let d = t.elapsed().as_secs_f64() * 1e6;
    std::hint::black_box(env.sig());
    d
}

/// 把 build 拆成 compile / sign 两段。
fn build_phases(f: &Fx) -> (f64, f64) {
    let payer = f.payer.pubkey();
    let t0 = Instant::now();
    let msg = V0Message::try_compile(&payer, &f.ixs, &[], Hash::new_unique()).unwrap();
    let compile = t0.elapsed().as_secs_f64() * 1e6;
    let t1 = Instant::now();
    let tx = VersionedTransaction::try_new(VersionedMessage::V0(msg), &[f.payer.as_ref()]).unwrap();
    let sign = t1.elapsed().as_secs_f64() * 1e6;
    std::hint::black_box(&tx);
    (compile, sign)
}

fn med(v: &mut [f64]) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn p90(v: &mut [f64]) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[((v.len() as f64 * 0.9) as usize).min(v.len() - 1)]
}

/// 空闲 idle 后构建一笔，重复 rounds 次；pre 是“预热动作”。
fn idle_rounds(label: &str, idle: Duration, rounds: usize, mut pre: impl FnMut(&Fx), logged: bool) -> (f64, f64, f64) {
    let f = fixture();
    for _ in 0..30 {
        build_full(&f);
    }
    let mut v = Vec::new();
    for _ in 0..rounds {
        std::thread::sleep(idle);
        pre(&f);
        v.push(build_full(&f));
    }
    let (p50, p90v, mx) = (
        med(&mut v.clone()),
        p90(&mut v.clone()),
        v.iter().cloned().fold(0.0, f64::max),
    );
    println!(
        "  {label:<30} idle={idle:>7?}  p50={p50:>6.1}  p90={p90v:>6.1}  max={mx:>7.1}{}",
        if logged { "  [log]" } else { "" }
    );
    (p50, p90v, mx)
}

/// 模拟 flexi_logger WriteMode::Async：调用方 format + 入队，写盘在别的线程。
struct AsyncLogger {
    tx: std::sync::mpsc::SyncSender<String>,
}
impl log::Log for AsyncLogger {
    fn enabled(&self, m: &log::Metadata) -> bool {
        m.level() <= log::Level::Info
    }
    fn log(&self, r: &log::Record) {
        if !self.enabled(r.metadata()) {
            return;
        }
        let _ = self.tx.try_send(format!("[{} {}] {}", r.level(), r.target(), r.args()));
    }
    fn flush(&self) {}
}

fn install_logger() {
    let (tx, rx) = std::sync::mpsc::sync_channel::<String>(1 << 16);
    log::set_boxed_logger(Box::new(AsyncLogger { tx })).unwrap();
    log::set_max_level(log::LevelFilter::Info);
    std::thread::spawn(move || {
        while let Ok(s) = rx.recv() {
            std::hint::black_box(s.len()); // 假装 write(2)
        }
    });
}

// --------------------------- 主测试 ---------------------------

#[test]
#[ignore = "延迟基准，需要独占 CPU：taskset -c 2 ... -- --ignored --nocapture"]
fn build_latency() {
    let logged = std::env::var("BENCH_LOG").is_ok();
    if logged {
        install_logger();
        println!("\n# 已安装异步日志（每笔 2~3 条 info，与线上 flexi_logger 同侧开销）");
    }

    println!("\n=== A. 空闲多久之后，首笔开始变慢 ===");
    for ms in [0u64, 1, 3, 10, 30, 100] {
        idle_rounds("直接构建", Duration::from_millis(ms), 21, |_| {}, logged);
    }

    println!("\n=== B. 空闲 100ms 后，各种“预热”能否救回来 ===");
    idle_rounds("(1) 不做任何预热", Duration::from_millis(100), 21, |_| {}, logged);
    idle_rounds("(2) 同线程先丢弃构建 1 笔", Duration::from_millis(100), 21, |f| {
        build_full(f);
    }, logged);
    idle_rounds("(3) 同线程只做一次小分配", Duration::from_millis(100), 21, |_| {
        std::hint::black_box(vec![0u8; 1024]);
    }, logged);
    idle_rounds("(4) 同线程空转 ~100µs", Duration::from_millis(100), 21, |_| {
        let t = Instant::now();
        while t.elapsed() < Duration::from_micros(100) {
            std::hint::spin_loop();
        }
    }, logged);

    println!("\n=== C. 后台签名线程（就是“每秒签一笔不发”的方案；整进程已 taskset 到 1 核）===");
    for period in [Duration::from_secs(1), Duration::from_millis(10)] {
        let f = fixture();
        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = Arc::clone(&stop);
        let (client, payer, ixs, hash) = (
            f.client.clone(),
            Arc::clone(&f.payer),
            f.ixs.clone(),
            f.hash.clone(),
        );
        let h = std::thread::spawn(move || {
            let g = Fx { client, payer, ixs, hash };
            while !stop2.load(Ordering::Relaxed) {
                build_full(&g);
                std::thread::sleep(period);
            }
        });
        std::thread::sleep(Duration::from_millis(50));
        idle_rounds(
            &format!("后台每 {period:?} 签一笔中的首笔"),
            Duration::from_millis(100),
            21,
            |_| {},
            logged,
        );
        stop.store(true, Ordering::Relaxed);
        let _ = h.join();
    }

    println!("\n=== D. 冷/热拆分：compile 还是 sign ===");
    let f = fixture();
    for _ in 0..30 {
        build_full(&f);
    }
    let (mut cc, mut cs, mut wc, mut ws) = (vec![], vec![], vec![], vec![]);
    for _ in 0..21 {
        std::thread::sleep(Duration::from_millis(100));
        let (c, s) = build_phases(&f); // 冷
        cc.push(c);
        cs.push(s);
        let (c, s) = build_phases(&f); // 紧接着再来一次 = 热
        wc.push(c);
        ws.push(s);
    }
    println!(
        "  冷(空闲100ms): compile={:>5.1}  sign={:>5.1}    热(连续): compile={:>5.1}  sign={:>5.1}   (µs, p50)",
        med(&mut cc),
        med(&mut cs),
        med(&mut wc),
        med(&mut ws)
    );

    println!("\n=== E. 纯 ALU 循环（排除降频/被宿主抢占）===");
    let n = 200_000u64;
    let alu = |n: u64| {
        let mut x = 0u64;
        for i in 0..n {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(i) | 1;
        }
        std::hint::black_box(x)
    };
    let (mut warm, mut cold) = (vec![], vec![]);
    for _ in 0..21 {
        let t = Instant::now();
        alu(n);
        warm.push(t.elapsed().as_secs_f64() * 1e6);
        std::thread::sleep(Duration::from_millis(100));
        let t = Instant::now();
        alu(n);
        cold.push(t.elapsed().as_secs_f64() * 1e6);
    }
    println!(
        "  ALU 热 p50={:.1}µs  空闲100ms 后 p50={:.1}µs  -> {:.2}x",
        med(&mut warm.clone()),
        med(&mut cold.clone()),
        med(&mut cold) / med(&mut warm)
    );
}

// --------------------------- 1ms 级 warmer 的真实效果 ---------------------------

/// 当前线程跑在哪个 CPU 上（/proc/thread-self/stat 第 39 字段）。
fn my_cpu() -> u32 {
    let s = std::fs::read_to_string("/proc/thread-self/stat").unwrap_or_default();
    s.rsplit(')')
        .next()
        .and_then(|r| r.split_whitespace().nth(36))
        .and_then(|v| v.parse().ok())
        .unwrap_or(999)
}

/// 空转到某个时刻（不睡眠，避免 nanosleep 的 50~100µs 抖动）。
fn spin_until(deadline: Instant) {
    while Instant::now() < deadline {
        std::hint::spin_loop();
    }
}

fn med_of(v: &mut Vec<f64>) -> f64 {
    if v.is_empty() {
        return f64::NAN;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

/// 后台 warmer（周期可配）对“空闲 100ms 后那一笔”到底有没有帮助。
///
///   WARM_PERIOD_US=1000 taskset -c 2   cargo test ... warm_period -- --ignored --nocapture
///   WARM_PERIOD_US=1000 taskset -c 2,3 cargo test ... warm_period -- --ignored --nocapture
///
/// 同时打印每次测量**实际的 gap**（距离 warmer 上一次签名多久），因为真正决定冷热的
/// 就是这个 gap，而不是 warmer 的设定周期。
#[test]
#[ignore = "延迟基准：WARM_PERIOD_US=... taskset -c ... -- --ignored --nocapture"]
fn warm_period() {
    let period_us: u64 = std::env::var("WARM_PERIOD_US")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000);
    let period = Duration::from_micros(period_us);

    let f = fixture();
    for _ in 0..30 {
        build_full(&f);
    }

    // 基线：没有 warmer
    let mut base = Vec::new();
    for _ in 0..21 {
        std::thread::sleep(Duration::from_millis(100));
        base.push(build_full(&f));
    }

    let epoch = Instant::now();
    let last = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let warmer_cpu = Arc::new(std::sync::atomic::AtomicU32::new(999));

    let (last2, stop2, cpu2) = (Arc::clone(&last), Arc::clone(&stop), Arc::clone(&warmer_cpu));
    let w = fixture();
    let h = std::thread::spawn(move || {
        cpu2.store(my_cpu(), Ordering::Relaxed);
        let mut next = Instant::now();
        while !stop2.load(Ordering::Relaxed) {
            build_full(&w);
            last2.store(epoch.elapsed().as_nanos() as u64, Ordering::Relaxed);
            next += period;
            spin_until(next);
        }
    });
    std::thread::sleep(Duration::from_millis(200));
    warmer_cpu.store({ let c = warmer_cpu.load(Ordering::Relaxed); c }, Ordering::Relaxed);

    let mut rows: Vec<(f64, f64)> = Vec::new();
    for _ in 0..41 {
        std::thread::sleep(Duration::from_millis(100));
        let gap_us = (epoch.elapsed().as_nanos() as u64).saturating_sub(last.load(Ordering::Relaxed)) as f64 / 1000.0;
        let d = build_full(&f);
        rows.push((gap_us, d));
    }
    stop.store(true, Ordering::Relaxed);
    let _ = h.join();

    println!(
        "\nwarmer 周期={period:?}  builder 核={}  warmer 核={}",
        my_cpu(),
        warmer_cpu.load(Ordering::Relaxed)
    );
    println!("  无 warmer（空闲100ms）p50 = {:.1}µs", med_of(&mut base));
    let mut buckets: [(&str, f64, f64); 4] = [
        ("gap < 0.3ms", 0.0, 300.0),
        ("gap 0.3~0.8ms", 300.0, 800.0),
        ("gap 0.8~1.5ms", 800.0, 1500.0),
        ("gap > 1.5ms", 1500.0, 1e9),
    ];
    for (name, lo, hi) in buckets.iter_mut() {
        let mut v: Vec<f64> = rows.iter().filter(|(g, _)| *g >= *lo && *g < *hi).map(|(_, d)| *d).collect();
        let n = v.len();
        println!("  有 warmer: {name:<15} n={n:>2}  p50={:>6.1}µs", med_of(&mut v));
    }
}

