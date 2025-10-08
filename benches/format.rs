use criterion::{black_box, criterion_group, criterion_main, Criterion};
use gzset::{fmt_f64, with_fmt_buf};
use redis_module::raw;
use std::os::raw::{c_char, c_int};
use std::sync::Once;
use std::time::Duration;

fn bench_format(c: &mut Criterion) {
    let mut group = c.benchmark_group("fmt_vs_to_string");
    configure_group(&mut group, 5.0, 3.0, 100);
    group.bench_function("fmt_f64", |b| {
        b.iter(|| {
            for _ in 0..1_000_000 {
                with_fmt_buf(|buf| {
                    let s = fmt_f64(buf, black_box(42.123456));
                    black_box(s);
                });
            }
        })
    });
    group.bench_function("to_string", |b| {
        b.iter(|| {
            for _ in 0..1_000_000 {
                black_box(black_box(42.123456).to_string());
            }
        })
    });
    group.finish();
}

fn bench_reply_methods(c: &mut Criterion) {
    init_reply_stubs();
    let raw_ctx = redis_module::Context::dummy().get_raw();
    let mut group = c.benchmark_group("reply_methods");
    configure_group(&mut group, 5.0, 3.0, 100);
    group.bench_function("string_buffer_fmt", |b| {
        b.iter(|| {
            for _ in 0..1_000_000 {
                with_fmt_buf(|buf| {
                    let score = black_box(42.123456);
                    let formatted = fmt_f64(buf, score);
                    unsafe {
                        raw::RedisModule_ReplyWithStringBuffer.unwrap()(
                            raw_ctx,
                            formatted.as_ptr().cast(),
                            formatted.len(),
                        );
                    }
                });
            }
        })
    });
    group.bench_function("reply_with_double", |b| {
        b.iter(|| {
            for _ in 0..1_000_000 {
                unsafe {
                    raw::RedisModule_ReplyWithDouble.unwrap()(raw_ctx, black_box(42.123456));
                }
            }
        })
    });
    group.finish();
}

fn init_reply_stubs() {
    static INIT: Once = Once::new();
    INIT.call_once(|| unsafe {
        raw::RedisModule_ReplyWithStringBuffer = Some(reply_with_string_buffer_stub);
        raw::RedisModule_ReplyWithDouble = Some(reply_with_double_stub);
    });
}

unsafe extern "C" fn reply_with_string_buffer_stub(
    _ctx: *mut raw::RedisModuleCtx,
    _buf: *const c_char,
    len: usize,
) -> c_int {
    std::hint::black_box(len);
    raw::Status::Ok as c_int
}

unsafe extern "C" fn reply_with_double_stub(_ctx: *mut raw::RedisModuleCtx, value: f64) -> c_int {
    std::hint::black_box(value);
    raw::Status::Ok as c_int
}

criterion_group!(benches, bench_format, bench_reply_methods);
criterion_main!(benches);

fn configure_group(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    default_measure: f64,
    default_warmup: f64,
    default_samples: usize,
) {
    let measurement = duration_env("GZSET_BENCH_MEASUREMENT_SECS", default_measure);
    let warmup = duration_env("GZSET_BENCH_WARMUP_SECS", default_warmup);
    let sample_size = sample_size_env("GZSET_BENCH_SAMPLE_SIZE", default_samples);
    group.measurement_time(measurement);
    group.warm_up_time(warmup);
    group.sample_size(sample_size);
}

fn duration_env(name: &str, default_secs: f64) -> Duration {
    let secs = std::env::var(name)
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|value| *value >= 0.0)
        .unwrap_or(default_secs);
    Duration::from_secs_f64(secs)
}

fn sample_size_env(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .map(|size| size.max(10))
        .unwrap_or(default)
}
