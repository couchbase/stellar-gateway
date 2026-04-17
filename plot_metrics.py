import sys
import json
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

PERCENTILE_MAP = {
    50: "fiftieth",
    95: "ninetyFifth",
    98: "ninetyEighth",
    99: "ninetyNinth",
}

if len(sys.argv) != 2 or not sys.argv[1].isdigit() or int(sys.argv[1]) not in PERCENTILE_MAP:
    print(f"Usage: python plot_metrics.py <percentile>")
    print(f"Valid percentiles: {sorted(PERCENTILE_MAP)}")
    sys.exit(1)

percentile = int(sys.argv[1])
percentile_key = PERCENTILE_MAP[percentile]

with open("direct.json") as f:
    direct = json.load(f)

with open("gateway.json") as f:
    gateway = json.load(f)

def extract_data(data, key):
    ops = {}
    for entry in data:
        num_users = entry["numUsers"]
        for op in entry["operations"]:
            name = op["name"]
            if name not in ops:
                ops[name] = {"numUsers": [], "latency": []}
            ops[name]["numUsers"].append(num_users)
            ops[name]["latency"].append(op["latencyPercentiles"][key])
    return ops

def smooth(x, y):
    x = np.array(x)
    y = np.array(y)
    coeffs = np.polyfit(x, y, deg=min(2, len(x) - 1))
    x_smooth = np.linspace(x.min(), x.max(), 300)
    y_smooth = np.polyval(coeffs, x_smooth)
    return x_smooth, y_smooth

direct_ops = extract_data(direct, percentile_key)
gateway_ops = extract_data(gateway, percentile_key)

operation_names = list(direct_ops.keys())

fig, axes = plt.subplots(1, len(operation_names), figsize=(5 * len(operation_names), 5))
fig.suptitle(f"{percentile}th Percentile Latency by Operation", fontsize=14, fontweight="bold", y=1.02)

for ax, name in zip(axes, operation_names):
    for ops, label, marker, color in [
        (direct_ops, "Direct", "o", "tab:blue"),
        (gateway_ops, "Gateway", "s", "tab:orange"),
    ]:
        x = ops[name]["numUsers"]
        y = ops[name]["latency"]
        x_smooth, y_smooth = smooth(x, y)
        ax.plot(x_smooth, y_smooth, color=color, linewidth=1.5)
        ax.plot(x, y, marker=marker, linestyle="None", color=color, label=label)

    ax.set_title(name)
    ax.set_xlabel("Number of Users")
    ax.set_ylabel(f"Latency p{percentile} (ms)")
    ax.legend()
    ax.grid(True)

plt.tight_layout()
output_file = f"metrics_p{percentile}.png"
plt.savefig(output_file, dpi=150, bbox_inches="tight")
print(f"Saved {output_file}")
