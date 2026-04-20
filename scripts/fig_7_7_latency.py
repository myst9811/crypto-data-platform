"""Fig 7.7 — End-to-end latency breakdown (horizontal waterfall)."""

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[1]
OUT_PATH = REPO_ROOT / "docs" / "figures" / "fig_7_7_latency.png"

STAGES = [
    "Exchange → Kafka\n(WebSocket RTT)",
    "Kafka → Bronze\n(micro-batch trigger)",
    "Bronze → Silver → Gold\n(two trigger cycles)",
    "Gold → API → Dashboard\n(cached read + HTTP)",
]
LATENCIES_MS = [50, 10_000, 20_000, 100]
COLORS = ["#2E86AB", "#457B9D", "#1D3557", "#E63946"]


def main() -> None:
    cumulative = np.cumsum([0, *LATENCIES_MS[:-1]])
    total = sum(LATENCIES_MS)

    fig, ax = plt.subplots(figsize=(11, 5))
    bars = ax.barh(STAGES, LATENCIES_MS, left=cumulative,
                   color=COLORS, edgecolor="white")

    for bar, lat in zip(bars, LATENCIES_MS):
        width = bar.get_width()
        label = f"{lat} ms" if lat < 1000 else f"{lat / 1000:.0f} s"
        ax.text(
            bar.get_x() + width / 2,
            bar.get_y() + bar.get_height() / 2,
            label,
            ha="center", va="center",
            color="white", fontweight="bold", fontsize=10,
        )

    ax.set_xlabel("Cumulative latency (milliseconds)")
    ax.set_title(f"End-to-End Latency Breakdown — Total ≈ {total / 1000:.0f} s")
    ax.invert_yaxis()
    ax.grid(axis="x", alpha=0.3)
    plt.tight_layout()
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(OUT_PATH, dpi=300, bbox_inches="tight")
    print(f"Saved: {OUT_PATH.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
