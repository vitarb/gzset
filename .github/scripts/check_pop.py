import json, sys
from pathlib import Path

base = Path('target/criterion/pop_loop_vs_baseline/base/estimates.json')
new = Path('target/criterion/pop_loop_vs_baseline/new/estimates.json')
with base.open() as f:
    b = json.load(f)
with new.open() as f:
    n = json.load(f)
base_mean = b['mean']['point_estimate']
new_mean = n['mean']['point_estimate']
if base_mean == 0:
    raise SystemExit('invalid baseline')
impr = (base_mean - new_mean) / base_mean
print(f"Improvement: {impr*100:.1f}%")
if impr < 0.10:
    raise SystemExit('pop loop speedup <10%')
