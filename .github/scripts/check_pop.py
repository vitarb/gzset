import json, sys, glob

GROUP = "pop_loop_vs_baseline"

def read_mean(path):
    try:
        with open(path) as f:
            return json.load(f)["mean"]["point_estimate"]
    except json.JSONDecodeError as e:
        sys.exit(f"[bench] invalid JSON in {path}: {e}")

def sum_means(baseline):
    # Works whether Criterion organizes as group/function/baseline or group/baseline
    paths = glob.glob(f"target/criterion/{GROUP}/**/{baseline}/estimates.json", recursive=True)
    if not paths:
        sys.exit(f"[bench] no estimates for baseline '{baseline}'. Did you run with --save-baseline {baseline}?")
    return sum(read_mean(p) for p in paths)

base = sum_means("base")
new = sum_means("new")
if base <= 0:
    sys.exit("invalid baseline (base <= 0)")

impr = (base - new) / base
print(f"Improvement: {impr*100:.1f}% (base={base:.3g}, new={new:.3g})")
if impr < 0.10:
    raise SystemExit("pop loop speedup <10%")

