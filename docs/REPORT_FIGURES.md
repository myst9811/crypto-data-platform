# Chapter 7 Figures — Generation Guide

This document walks the author through regenerating every figure in Chapter 7 of
the project report, captures the screenshot protocol for the two dashboard
figures, and sketches further improvements that strengthen the chapter beyond
the seven mandatory figures.

All outputs land in `docs/figures/` at 300 dpi (report-quality). Scripts live in
`scripts/` and are idempotent — rerun any of them and the corresponding PNG is
overwritten in place.

---

## 1. One-time setup

Run these from the repository root:

```bash
cd /Users/shannensaikia/Projects/crypto-data-platform
source .venv/bin/activate
pip install matplotlib seaborn          # if not already installed
mkdir -p docs/figures                   # already created, kept for portability
```

The scripts assume:

- `ml/artifacts/*.pkl` and `ml/artifacts/lstm_price_direction.pt` exist (they
  do — see `ls ml/artifacts`).
- `mlruns/` contains at least one run in the `price_direction_lstm`
  experiment (needed for Fig 7.2).
- The Silver lake has rows for `ETH/USD` (needed for Fig 7.4).

---

## 2. Generate figures 7.1, 7.2, 7.3, 7.4, 7.7

| Figure | Script | Runtime |
| --- | --- | --- |
| 7.1 XGBoost feature importance | `scripts/fig_7_1_xgboost_importance.py` | ~1 s |
| 7.2 LSTM train/val loss | `scripts/fig_7_2_lstm_loss.py` | ~2 s |
| 7.3 Isolation Forest anomalies | `scripts/fig_7_3_isolation_forest.py` | ~3 s |
| 7.4 GARCH(1,1) ETH/USD volatility | `scripts/fig_7_4_garch_eth.py` | ~4 s |
| 7.7 End-to-end latency waterfall | `scripts/fig_7_7_latency.py` | ~1 s |

Run them all:

```bash
python scripts/fig_7_1_xgboost_importance.py
python scripts/fig_7_2_lstm_loss.py
python scripts/fig_7_3_isolation_forest.py
python scripts/fig_7_4_garch_eth.py
python scripts/fig_7_7_latency.py
```

Or in one line:

```bash
for f in scripts/fig_7_*.py; do python "$f" || { echo "FAILED: $f"; break; }; done
```

Each script prints the saved file path on success. Expected stdout looks like:

```
Saved: docs/figures/fig_7_1_xgboost_importance.png
Saved: docs/figures/fig_7_2_lstm_loss.png  (final train=…, val=…)
Saved: docs/figures/fig_7_3_isolation_forest.png  (anomaly rate = …)
Saved: docs/figures/fig_7_4_garch_eth.png
Saved: docs/figures/fig_7_7_latency.png
```

### What each figure shows (caption-ready prose)

- **Fig 7.1** — horizontal bar chart of `feature_importances_` gain values for
  the 10 features baked into the XGBoost arbitrage classifier
  (`train_xgboost.py:20-24`). Generated against the redesigned label defined
  in `ml/training/label_generator.py`, where a row is positive iff a
  round-trip trade opened now would clear taker + withdrawal fees on both
  legs when closed at T + 200 ms. Under this label, `spread_pct` remains the
  strongest single signal (as expected — spread is the primary arbitrage
  driver) but no longer carries the near-unit gain that the previous
  spread-threshold label produced. The earlier spread-only label is
  discussed in §7 "Further steps" as the motivating defect.
- **Fig 7.2** — train and validation binary cross-entropy per epoch pulled from
  the most recent MLflow run in the `price_direction_lstm` experiment. The gap
  between the curves is the visual evidence for the overfitting discussion.
- **Fig 7.3** — two-panel figure. (a) histogram of `decision_function` scores
  split by prediction class, with the decision boundary at 0; (b) scatter in
  `spread_deviation` × `orderbook_imbalance` space colouring anomalies. The
  anomaly-rate percentage is computed live and shown in the (a) subtitle.
- **Fig 7.4** — realised rolling-60s σ in grey, fitted GARCH(1,1) conditional
  volatility in blue, and a 30-step out-of-sample forecast in red dashed. AIC
  is read off the pickled `ARCHModelResult` so the number in the title is
  always the trained value.
- **Fig 7.7** — horizontal waterfall of the four latency stages from
  `REPORT_BRIEF.md:285-289`: WebSocket RTT (50 ms), Kafka → Bronze (10 s),
  Bronze → Silver → Gold (20 s), Gold → API → Dashboard (100 ms).

---

## 3. Dashboard screenshots (figures 7.5 and 7.6)

Scripts cannot produce these — both require the live stack.

### Bring the stack up

```bash
./start.sh
# Wait ~60s for Silver/Gold tables to populate.
```

Sanity check before capturing:

```bash
curl -s http://localhost:8000/api/v1/health          # should return 200
curl -s http://localhost:8000/api/v1/prices | head   # should show current rows
```

### Fig 7.5 — Live Prices page

1. Open `http://localhost:8501` in Chrome.
2. Navigate to **Live Prices** in the sidebar.
3. Wait until all 5 symbols show a current price AND the per-exchange pivot
   table is populated (≥30 s after start-up).
4. In Chrome DevTools: `Cmd+Shift+P` → "Capture full size screenshot".
5. Save as `docs/figures/fig_7_5_dashboard_live_prices.png`.

### Fig 7.6 — ML Insights page

1. Navigate to **ML Insights**.
2. Warm the model cache if any panel shows "no data":
   ```bash
   curl http://localhost:8000/api/v1/ml/predict?symbol=BTC/USD
   ```
   Refresh the page.
3. Confirm all four panels render: XGBoost probability card, LSTM direction
   card, anomaly flag, GARCH σ² sparkline.
4. Capture as above. Save as `docs/figures/fig_7_6_dashboard_ml_insights.png`.

### Quality tips

- Use a 1440×900 or 1920×1080 browser window at 100 % zoom.
- Use Chrome DevTools' **full size** capture, not the OS snip — it avoids
  address-bar and tab-bar chrome and exports at device-pixel resolution.
- If the report template expects consistent dimensions across 7.5 and 7.6,
  capture both at the same window size in one session.

---

## 4. Verification checklist

After running everything, confirm:

```bash
ls -la docs/figures/
```

You should see exactly:

- `fig_7_1_xgboost_importance.png`
- `fig_7_2_lstm_loss.png`
- `fig_7_3_isolation_forest.png`
- `fig_7_4_garch_eth.png`
- `fig_7_5_dashboard_live_prices.png`    (manual capture)
- `fig_7_6_dashboard_ml_insights.png`    (manual capture)
- `fig_7_7_latency.png`

Each PNG should be ≥ 200 KB (a smaller file almost certainly means matplotlib
wrote a blank canvas). Spot-check with:

```bash
file docs/figures/*.png         # should all say "PNG image data, … 8-bit/color RGBA"
```

Open each one visually before quoting numbers from the plots in the prose —
the headline figures in Table 7.X must match what the plot shows.

---

## 5. Including figures in the report

### LaTeX

```latex
\begin{figure}[H]
  \centering
  \includegraphics[width=0.9\textwidth]{docs/figures/fig_7_1_xgboost_importance.png}
  \caption[XGBoost feature importance]{XGBoost arbitrage classifier feature
  importance by gain. Spread-derived features dominate, consistent with the
  model's use as a short-horizon arbitrage detector.}
  \label{fig:xgboost-importance}
\end{figure}
```

Replace `width=0.9\textwidth` with `width=\linewidth` inside two-column layouts.

### Word (.docx)

Insert → Picture → from file; select the PNG; right-click → **Size and
Position** → set width to 16 cm and check **Lock aspect ratio**. Caption via
Insert → Caption, numbered under **Figure**.

---

## 6. Troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| `ModuleNotFoundError: ml.features…` | Running outside the repo root | `cd` to repo root before invoking the script — they inject `REPO_ROOT` into `sys.path`. |
| Fig 7.2 raises "No MLflow experiment" | Empty `mlruns/` or different tracking URI | Run `python -m ml.training.train_lstm`. If you use a non-default MLflow store, set `MLFLOW_TRACKING_URI` before running. |
| Fig 7.2 raises "missing train_loss/val_loss" | Older LSTM checkpoint from before per-epoch metric logging | Retrain: `python -m ml.training.train_lstm`. |
| Fig 7.3 produces an all-blue scatter | No points triggered the `-1` class — contamination too low for this data slice | Expand the feature-store window (`load_feature_store()`) or retrain the forest with a larger `contamination`. |
| Fig 7.4 x-axis is empty / single tick | ETH/USD Silver rows < 30 | Let the pipeline run longer; GARCH needs ≥50 observations to even fit (`train_garch.py:31`). |
| `pickle.UnpicklingError` on any artifact | Artifact produced by a different Python/sklearn/xgboost version | Recreate the artifact with the current env: `python -m ml.training.train_xgboost` (or the relevant trainer). |
| Dashboard screenshots show "no data" panels | API hasn't warmed the model cache | `curl http://localhost:8000/api/v1/ml/predict?symbol=BTC/USD` then refresh. |
| Blank/tiny PNG (<20 KB) | Matplotlib failed silently before rendering | Rerun the script from a terminal and read the traceback; check `$DISPLAY` isn't forced to an X server that doesn't exist (`MPLBACKEND=Agg` is safe). |

---

## 7. Further steps — optional improvements to the chapter

None of these are blockers for submission; they are sensible follow-ups if
there is time.

### 7.1 Regeneration hygiene

- **Commit the PNGs**: `git add docs/figures/*.png` so the version in the
  report matches the artifacts at that commit.
- **Makefile target**: add a `figures` target that runs all five scripts plus
  prints manual capture reminders for 7.5/7.6.
  ```make
  figures:
      @python scripts/fig_7_1_xgboost_importance.py
      @python scripts/fig_7_2_lstm_loss.py
      @python scripts/fig_7_3_isolation_forest.py
      @python scripts/fig_7_4_garch_eth.py
      @python scripts/fig_7_7_latency.py
      @echo "Manual: capture 7.5 and 7.6 per docs/REPORT_FIGURES.md §3"
  ```
- **Pre-commit hook**: verify that for every updated `.pkl` / `.pt` artifact,
  the corresponding figure PNG is also newer — catches stale plots.

### 7.2 Strengthening the ML evaluation (new figures worth adding)

All of these can reuse the existing scripts as templates:

| # | Figure | Data source | Why it helps |
| --- | --- | --- | --- |
| A | XGBoost ROC & PR curves | test-set scores logged in `train_xgboost.py` | Turns the single accuracy number into a threshold-sensitivity story. |
| B | XGBoost confusion matrix | same | Shows class imbalance handling visually. |
| C | LSTM per-class accuracy over epochs | add two extra scalars to `train_lstm.py` and re-log | Separates "flat" vs. "direction" errors for readers. |
| D | GARCH residual ACF / PACF (2×1 subplot) | `result.std_resid` | Demonstrates the model has de-correlated the squared returns. |
| E | Isolation Forest anomaly timeline | `df.event_time` vs. `scores` | Converts 7.3's scatter into a narrative: "these bursts are when". |
| F | Latency histogram from real telemetry | API access log or Kafka end-to-end probe | Upgrades 7.7's block diagram into a distribution plot with p50/p95/p99 lines. |
| G | Class-balance before/after label redesign | `label_generator` stdout + a short bar | Makes the leakage fix visible to the reader in numbers. |

### 7.3 Figure style consistency

Adopt one matplotlib rc block at the top of every future figure script so
colours, font sizes and grids match across the chapter:

```python
import matplotlib.pyplot as plt
plt.rcParams.update({
    "figure.dpi": 120,
    "savefig.dpi": 300,
    "axes.grid": True,
    "grid.alpha": 0.3,
    "axes.titlesize": 13,
    "axes.labelsize": 11,
    "legend.fontsize": 10,
    "font.family": "DejaVu Sans",
})
```

Refactor the five existing scripts to import a shared `scripts/_plot_style.py`
helper instead of duplicating the block.

### 7.4 Reproducibility metadata

Have every script stamp a small caption file next to the PNG:

```
docs/figures/fig_7_1_xgboost_importance.png
docs/figures/fig_7_1_xgboost_importance.caption.txt   ← new
```

containing the model hash, the MLflow run ID (if any), row count of the
feature store, and the git SHA. This makes audit-trail questions trivial and
feeds straight into the appendix.

### 7.5 Poster variants

The repo includes `30_3_26 Poster_Template.pptx`. Add a `--poster` flag to
each script that bumps font sizes (title 18→24, labels 11→14) and emits a
second PNG named `fig_7_X_poster.png`. Posters are read from further away and
the defaults used here are tuned for A4.

### 7.6 CI enforcement

Add a lightweight GitHub Actions job that runs `python scripts/fig_7_*.py` on
every PR that touches `ml/training/**`. If any script fails or produces a 0-byte
PNG, fail the check. This stops a silent drift where retraining changes a model
but the report keeps the old plot.

---

## 8. Appendix: file-by-file inventory

```
scripts/
├── fig_7_1_xgboost_importance.py   XGBoost gain bar chart
├── fig_7_2_lstm_loss.py            LSTM BCE loss from MLflow
├── fig_7_3_isolation_forest.py     Anomaly hist + 2-D scatter
├── fig_7_4_garch_eth.py            GARCH(1,1) fit + forecast (ETH/USD)
└── fig_7_7_latency.py              End-to-end latency waterfall

docs/figures/
├── fig_7_1_xgboost_importance.png
├── fig_7_2_lstm_loss.png
├── fig_7_3_isolation_forest.png
├── fig_7_4_garch_eth.png
├── fig_7_5_dashboard_live_prices.png   (manual capture)
├── fig_7_6_dashboard_ml_insights.png   (manual capture)
└── fig_7_7_latency.png

docs/REPORT_FIGURES.md              (this file)
```
