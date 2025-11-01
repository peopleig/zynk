use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use zynk::engine::kv::LsmEngine;
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

fn gen_kv(n: usize, vlen: usize, seed: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..n)
        .map(|i| {
            let mut key = format!("key_{:08}", i).into_bytes();
            // small randomization to avoid perfect locality
            key.push(rng.r#gen());
            let val = vec![b'x'; vlen];
            (key, val)
        })
        .collect()
}

fn bench_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("put");
    for &n in &[10_000usize, 50_000] {
        group.throughput(Throughput::Elements(n as u64));

        group.bench_with_input(BenchmarkId::new("hashmap", n), &n, |b, &n| {
            b.iter_batched(
                || (HashMap::<Vec<u8>, Vec<u8>>::with_capacity(n), gen_kv(n, 32, 1)),
                |(mut map, items)| {
                    for (k, v) in items.into_iter() {
                        map.insert(k, v);
                    }
                },
                BatchSize::LargeInput,
            )
        });

        group.bench_with_input(BenchmarkId::new("lsm", n), &n, |b, &n| {
            b.iter_batched(
                || {
                    // fresh data dir under target/bench-tmp
                    let dir = PathBuf::from("target/bench-tmp/lsm_put");
                    let _ = fs::remove_dir_all(&dir);
                    fs::create_dir_all(&dir).unwrap();
                    let eng = LsmEngine::new_with_manifest(&dir, 64 * 1024, 8 * 1024).unwrap();
                    (eng, gen_kv(n, 32, 1), dir)
                },
                |(mut eng, items, dir)| {
                    for (k, v) in items.into_iter() {
                        eng.put(&k, &v).unwrap();
                    }
                    let _ = eng.flush();
                    let _ = fs::remove_dir_all(&dir);
                },
                BatchSize::LargeInput,
            )
        });
    }
    group.finish();
}

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("get");
    for &n in &[10_000usize, 50_000] {
        group.throughput(Throughput::Elements(n as u64));

        group.bench_with_input(BenchmarkId::new("hashmap", n), &n, |b, &n| {
            let items = gen_kv(n, 32, 2);
            let mut map = HashMap::with_capacity(n);
            for (k, v) in items.iter() { map.insert(k.clone(), v.clone()); }
            b.iter(|| {
                for (k, _) in items.iter() {
                    let _ = map.get(k);
                }
            })
        });

        group.bench_with_input(BenchmarkId::new("lsm", n), &n, |b, &n| {
            let dir = PathBuf::from("target/bench-tmp/lsm_get");
            let _ = fs::remove_dir_all(&dir);
            fs::create_dir_all(&dir).unwrap();
            let mut eng = LsmEngine::new_with_manifest(&dir, 64 * 1024, 8 * 1024).unwrap();
            let items = gen_kv(n, 32, 2);
            for (k, v) in items.iter() { eng.put(k, v).unwrap(); }
            eng.flush().unwrap();

            b.iter(|| {
                for (k, _) in items.iter() {
                    let _ = eng.get(k).unwrap();
                }
            });
            let _ = fs::remove_dir_all(&dir);
        });
    }
    group.finish();
}

criterion_group!(benches, bench_put, bench_get);
criterion_main!(benches);
