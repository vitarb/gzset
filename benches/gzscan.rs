use std::{cell::RefCell, time::Duration};

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use gzset::{fmt_f64, with_fmt_buf, ScoreSet};
use ordered_float::OrderedFloat;
use rand::Rng;

mod support;

fn bench_scan(c: &mut Criterion) {
    let scan_size = support::usize_env("GZSET_BENCH_SCAN_SIZE", 150_000);
    let cursor_samples = support::usize_env("GZSET_BENCH_SCAN_CURSOR_SAMPLES", 256);
    let entries = build_scan_entries(scan_size);
    let set = Box::leak(Box::new(support::build_set(&entries)));
    let cursors = build_cursors(set, cursor_samples);

    let mut group = c.benchmark_group("scan");
    group.measurement_time(Duration::from_secs(10));
    group.warm_up_time(Duration::from_secs(3));
    group.sample_size(10);
    group.sampling_mode(criterion::SamplingMode::Flat);
    for &count in &[10usize, 100, 1024] {
        group.throughput(Throughput::Elements(count as u64));
        group.bench_function(format!("count_{count}"), |b| {
            let index = RefCell::new(0usize);
            b.iter(|| {
                let mut guard = index.borrow_mut();
                let cursor = &cursors[*guard];
                let next = simulate_scan(set, cursor, count);
                *guard = (*guard + 1) % cursors.len();
                black_box(next);
            });
        });
    }
    group.finish();
}

fn build_scan_entries(n: usize) -> Vec<(f64, String)> {
    (0..n)
        .map(|i| {
            let score = i as f64 * 0.5;
            let member = format!("name|{:04X}%{:02X}", i, i % 97);
            (score, member)
        })
        .collect()
}

fn build_cursors(set: &ScoreSet, samples: usize) -> Vec<String> {
    let mut cursors = Vec::with_capacity(samples + 1);
    cursors.push("0".to_string());
    let members = set.members_with_scores();
    let mut rng = support::seeded_rng();
    for _ in 0..samples {
        let idx = rng.gen_range(0..members.len());
        let (member, score) = &members[idx];
        cursors.push(encode_cursor(*score, member));
    }
    cursors
}

fn simulate_scan(set: &ScoreSet, cursor: &str, count: usize) -> String {
    let parsed = if cursor == "0" {
        None
    } else {
        Some(decode_cursor(cursor).expect("valid cursor"))
    };
    let mut iter = match parsed {
        None => set
            .iter_from(OrderedFloat(f64::NEG_INFINITY), "", true)
            .peekable(),
        Some((score, ref member)) => set.iter_from(OrderedFloat(score), member, true).peekable(),
    };

    let mut arr = Vec::with_capacity(count * 2);
    let mut last = None;
    for _ in 0..count {
        if let Some((m, sc)) = iter.next() {
            arr.push(m.to_owned());
            with_fmt_buf(|b| arr.push(fmt_f64(b, sc).to_owned()));
            last = Some((sc, m.to_owned()));
        } else {
            break;
        }
    }
    black_box(&arr);
    match last {
        Some((sc, m)) if iter.peek().is_some() => encode_cursor(sc, &m),
        _ => "0".to_string(),
    }
}

fn encode_cursor(score: f64, member: &str) -> String {
    with_fmt_buf(|b| {
        let score_s = fmt_f64(b, score);
        let mut out = String::with_capacity(score_s.len() + 1 + member.len() * 3);
        out.push_str(score_s);
        out.push('|');
        for ch in member.chars() {
            match ch {
                '|' => out.push_str("%7C"),
                '%' => out.push_str("%25"),
                _ => out.push(ch),
            }
        }
        out
    })
}

fn decode_cursor(cur: &str) -> Option<(f64, String)> {
    let (score_s, member_s) = cur.split_once('|')?;
    let score = score_s.parse::<f64>().ok()?;
    if !score.is_finite() {
        return None;
    }
    if !with_fmt_buf(|b| fmt_f64(b, score) == score_s) {
        return None;
    }

    fn decode_hex(b: u8) -> Option<u8> {
        match b {
            b'0'..=b'9' => Some(b - b'0'),
            b'a'..=b'f' => Some(b - b'a' + 10),
            b'A'..=b'F' => Some(b - b'A' + 10),
            _ => None,
        }
    }

    let bytes = member_s.as_bytes();
    let mut member_bytes = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                return None;
            }
            let hi = decode_hex(bytes[i + 1])?;
            let lo = decode_hex(bytes[i + 2])?;
            member_bytes.push((hi << 4) | lo);
            i += 3;
        } else {
            member_bytes.push(bytes[i]);
            i += 1;
        }
    }
    let member = String::from_utf8(member_bytes).ok()?;
    Some((score, member))
}

criterion_group!(benches, bench_scan);
criterion_main!(benches);
