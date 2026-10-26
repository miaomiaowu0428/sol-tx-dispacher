
//! 保温线程的冒烟测试：只验证「能起、能绑核、能打日志」，不验证性能。
//! 真性能要跑 build_latency 那套。

use std::sync::{Mutex, OnceLock};

struct Capture;
static LINES: OnceLock<Mutex<Vec<String>>> = OnceLock::new();

fn lines() -> &'static Mutex<Vec<String>> {
    LINES.get_or_init(|| Mutex::new(Vec::new()))
}

impl log::Log for Capture {
    fn enabled(&self, _: &log::Metadata) -> bool {
        true
    }
    fn log(&self, r: &log::Record) {
        lines().lock().unwrap().push(format!("{}", r.args()));
    }
    fn flush(&self) {}
}

#[test]
fn warmer_starts_and_pins() {
    let _ = log::set_boxed_logger(Box::new(Capture));
    log::set_max_level(log::LevelFilter::Info);

    // SAFETY: 单测进程内没有并发读写这几个变量；本用例独占该环境变量。
    unsafe {
        std::env::set_var("TX_WARM", "1");
        std::env::set_var("TX_WARM_PERIOD_US", "1000");
        std::env::set_var("TX_WARM_CORES", "0");
    }

    let w = sol_tx_dispacher::warm::spawn_from_env().expect("TX_WARM=1 应该启动保温线程");
    std::thread::sleep(std::time::Duration::from_millis(300));
    assert_eq!(sol_tx_dispacher::warm::phase_tag(), "on");

    let all = lines().lock().unwrap().join("\n");
    assert!(all.contains("线程启动 core=0"), "缺少线程启动日志:\n{all}");
    assert!(all.contains("pinned=true"), "绑核失败（pinned=false）:\n{all}");
    assert!(all.contains("已启动 1 个保温线程"), "缺少启动汇总日志:\n{all}");

    w.stop();
}
