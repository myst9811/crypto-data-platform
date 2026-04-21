# Chapter 7 Figures — Generation Guide

One-stop playbook for producing every figure in Chapter 7 of the project
report (and the supporting poster) from a clean checkout. Organised in the
order the author actually performs the work, not by figure number.

All rendered figures land in `docs/figures/` at 300 dpi. Scripts live in
`scripts/` and are idempotent.

**Current as of commit `3a75d2a` (label redesign) and `eba4f1d` (Fig 7.1
regeneration under the new label).**

---

## §0 TL;DR

If everything is already trained and the feature store is warm:

```bash
source .venv/bin/activate
for f in scripts/fig_7_*.py; do python "$f" || { echo "FAILED: $f"; break; }; done
./start.sh                               # then capture 7.5, 7.6 manually
ls -la docs/figures/fig_7_*.png          # expect 7 files
```

Fig 7.1 is already current under the redesigned **fee-net profit label**
(commit `3a75d2a`). If you retrain anything upstream of the XGBoost
classifier, rerun `scripts/fig_7_1_xgboost_importance.py` before claiming
any gain value in the report prose. See §2 for the full reproduction
order.

---

## §1 Prerequisites

Run from the repository root.

```bash
cd /Users/shannensaikia/Projects/crypto-data-platform
source .venv/bin/activate
pip install matplotlib seaborn           # idempotent
mkdir -p docs/figures                    # already exists
```

Data/model readiness:

- `ml/artifacts/xgboost_arbitrage.pkl` must be from commit `3a75d2a` or
  later. If older, retrain: `python -m ml.training.train_xgboost`.
- `ml/artifacts/lstm_price_direction.pt` exists and `mlruns/` contains at
  least one run in the `price_direction_lstm` experiment (Fig 7.2).
- `ml/artifacts/isolation_forest.pkl` exists (Fig 7.3).
- `ml/artifacts/garch_ETH_USD.pkl` exists and the Silver lake has
  ETH/USD rows (Fig 7.4).
- For 7.5/7.6: `./start.sh` has been run and the dashboard is up at
  `http://localhost:8501`.

---

## §2 Reproduction order

The one canonical sequence from a clean checkout:

1. `source .venv/bin/activate`
2. *(only if the XGBoost artifact is pre-`3a75d2a`)*
   `python -m ml.training.train_xgboost` — retrains under the redesigned
   label. **Do not** retrain LSTM / Isolation Forest / GARCH; the label
   redesign does not affect them.
3. `for f in scripts/fig_7_*.py; do python "$f"; done` — renders
   figures 7.1, 7.2, 7.3, 7.4 and 7.7.
4. `./start.sh` then wait ~60 s for Silver/Gold tables to populate.
5. Capture Fig 7.5 (Live Prices) and Fig 7.6 (ML Insights) per §4.
6. `ls -la docs/figures/` — confirm seven PNGs, each ≥ 100 KB.

---

## §3 Figure-by-figure

### Fig 7.1 — XGBoost feature importance

**What it shows.** Horizontal bar chart of XGBoost `feature_importances_`
(gain) for the 10 features in `ml/training/train_xgboost.py:20-24`, under
the redesigned fee-net profit label from `ml/training/label_generator.py`.

**How to run.** `python scripts/fig_7_1_xgboost_importance.py`

**Numbers to quote.**

| Feature | Gain |
| --- | --- |
| `spread_pct` | 0.450 |
| `price_b` | 0.100 |
| `price_a` | 0.094 |
| `rolling_vol_15s` | 0.093 |
| `time_sin` | 0.090 |
| `spread_abs` | 0.089 |
| `rolling_vol_60s` | 0.085 |
| `time_cos` / `garch_forecast` / `latency_ms` | 0.000 |

**Caption notes.** Three points the prose should establish:

1. The label is now "did a round-trip trade opened now clear taker +
   withdrawal fees on both legs by T + 200 ms," with fees lifted verbatim
   from `src/processing/transformations/arbitrage.py:14-30` (Binance,
   Coinbase, Kraken).
2. `spread_pct` is still the dominant signal (spread IS the primary
   arbitrage driver) but its gain dropped from 0.982 to 0.450 once the
   label stopped being a direct thresholding of `spread_pct`.
3. On current data the fee threshold produces no natural positives, so
   `generate_labels` engages its percentile fallback — the class balance
   printed at training time is ~25% positives. This is expected under a
   short data window; the fallback thresholds *future* net profit so the
   label still does not leak through `spread_pct`.

### Fig 7.2 — LSTM train/val loss

**What it shows.** Per-epoch binary cross-entropy for the bidirectional
LSTM on the `price_direction_lstm` task, pulled from the latest MLflow
run.

**How to run.** `python scripts/fig_7_2_lstm_loss.py`

**Numbers to quote.**

- Latest run: `c9a44e95171340c39d999104d6c0490b`
- Epochs: 10
- Final train loss: **0.5773**
- Final validation loss: **0.5826**
- Train−val gap is small and positive → the model is essentially at the
  chance level for a binary direction task (BCE of `ln 2 ≈ 0.693` is the
  uniform baseline; 0.58 is meaningfully better but far from saturated).

**Caption notes.** Frame the small train/val gap as "no catastrophic
overfitting across ten epochs" rather than "the model has learned" — the
absolute loss is still high. If you retrain, re-run the harvest snippet
in §1 before quoting numbers.

### Fig 7.3 — Isolation Forest anomaly distribution

**What it shows.** Two-panel figure. (a) histogram of `decision_function`
scores, split by predicted class, with the decision boundary at 0. (b)
scatter in `spread_deviation` × `orderbook_imbalance` space, anomalies in
red.

**How to run.** `python scripts/fig_7_3_isolation_forest.py`

**Numbers to quote.**

- Anomaly rate on the full feature store: **4.77%** (≈ `contamination`
  target set at training time).
- Features used for the forest: `spread_deviation`, `volume_spike_ratio`,
  `orderbook_imbalance` — derived columns, not raw `spread_pct`.

**Caption notes.** This model is unaffected by the label redesign — it is
unsupervised and does not depend on `generate_labels`.

### Fig 7.4 — GARCH(1,1) volatility (ETH/USD)

**What it shows.** Realised rolling-60 s σ (grey), fitted GARCH(1,1)
conditional volatility (blue), and a 30-step out-of-sample forecast (red
dashed). AIC is read off the pickled `ARCHModelResult`.

**How to run.** `python scripts/fig_7_4_garch_eth.py`

**Numbers to quote.**

- AIC: **−3,236,946**
- BIC: **−3,236,902**
- Observations: **473,784** log-returns (× 100 scale)
- Forecast horizon: 30 one-second steps

**Caption notes.** Negative AIC at this magnitude is a consequence of
fitting to *percent* log-returns on a very long series. The relative
ordering of models (GARCH vs. baseline) is what matters; AIC is not
directly interpretable as a goodness-of-fit percentage.

### Fig 7.7 — End-to-end latency waterfall

**What it shows.** Horizontal waterfall of the four latency stages that
sum to the end-to-end "exchange-to-dashboard" delay.

**How to run.** `python scripts/fig_7_7_latency.py`

**Numbers to quote.** Taken from `scripts/fig_7_7_latency.py:LATENCIES_MS`
(the script is the source of truth; `REPORT_BRIEF.md:285-289` mirrors
them):

| Stage | Latency |
| --- | --- |
| Exchange → Kafka (WebSocket RTT) | 50 ms |
| Kafka → Bronze (micro-batch) | 10 s |
| Bronze → Silver → Gold (two cycles) | 20 s |
| Gold → API → Dashboard | 100 ms |
| **Total** | **≈ 30 s** |

**Caption notes.** The 30 s is dominated by the two Spark micro-batch
cycles. This is a design choice (Structured Streaming with a 10 s trigger
interval), not a bug. For sub-second latency the architecture would need
Spark Continuous Processing or a Flink migration.

---

## §4 Dashboard screenshots (Fig 7.5 and 7.6)

Scripts cannot produce these — both require the live stack.

### Bring the stack up

```bash
./start.sh
# Wait ~60s for Silver/Gold tables to populate.
```

Sanity check:

```bash
curl -s http://localhost:8000/api/v1/health           # should return 200
curl -s http://localhost:8000/api/v1/prices | head    # should show current rows
```

### Fig 7.5 — Live Prices page

1. Open `http://localhost:8501` in Chrome.
2. Navigate to **Live Prices** in the sidebar.
3. Wait until all 5 symbols show a current price AND the per-exchange
   pivot table is populated (≥ 30 s after start-up).
4. Chrome DevTools → `Cmd+Shift+P` → "Capture full size screenshot".
5. Save as `docs/figures/fig_7_5_dashboard_live_prices.png`.

### Fig 7.6 — ML Insights page

1. Warm the API's model cache so the panels hit the **retrained** pkl:
   ```bash
   curl -s "http://localhost:8000/api/v1/predictions/BTC/USD"
   curl -s "http://localhost:8000/api/v1/anomalies/recent"
   curl -s "http://localhost:8000/api/v1/volatility/BTC/USD"
   curl -s "http://localhost:8000/api/v1/arbitrage/live"
   ```
   (One call per ML panel on the Insights page. Full route list:
   `curl -s http://localhost:8000/api/v1/openapi.json | python -m json.tool | grep -A1 '"paths"'`.)
2. Navigate to **ML Insights** and refresh.
3. Confirm all four panels render: XGBoost probability card, LSTM
   direction card, anomaly flag, GARCH σ² sparkline.
4. Capture as above. Save as
   `docs/figures/fig_7_6_dashboard_ml_insights.png`.

### Quality tips

- Use a 1440 × 900 or 1920 × 1080 browser window at 100 % zoom.
- Use Chrome DevTools' **full size** capture rather than the OS snip —
  avoids address-bar chrome and exports at device-pixel resolution.
- Capture 7.5 and 7.6 in the same session so they share dimensions.

---

## §5 Caption templates

Paste directly into LaTeX or Word. Numbers are as of the current `main`.
If you retrain, rerun the §3 harvest before quoting.

### Fig 7.1 — XGBoost feature importance

**LaTeX:**

```latex
\begin{figure}[H]
  \centering
  \includegraphics[width=0.9\textwidth]{docs/figures/fig_7_1_xgboost_importance.png}
  \caption[XGBoost arbitrage classifier feature importance]{XGBoost
  arbitrage classifier feature importance (gain), generated under the
  fee-net profit label introduced in commit \texttt{3a75d2a}.
  \texttt{spread_pct} is the dominant signal at gain $\approx 0.45$,
  followed by \texttt{price_b}, \texttt{price_a},
  \texttt{rolling_vol_15s}, \texttt{time_sin} and \texttt{spread_abs}
  in the 0.08--0.10 band. This distribution replaces the earlier
  spread-threshold label, which produced a degenerate gain of 0.982
  on \texttt{spread_pct} alone.}
  \label{fig:xgboost-importance}
\end{figure}
```

**Word:** Insert → Picture → from file, 16 cm wide, locked aspect ratio.
Caption: *"Figure 7.1 — XGBoost arbitrage classifier feature importance
by gain. Generated under the fee-net profit label. spread_pct is the
dominant signal at 0.450 gain, followed by price_b (0.100), price_a
(0.094), rolling_vol_15s (0.093), time_sin (0.090) and spread_abs
(0.089). The earlier spread-threshold label produced a degenerate gain
of 0.982 on spread_pct alone — see §7.1 discussion."*

### Fig 7.2 — LSTM training curves

**LaTeX:**

```latex
\begin{figure}[H]
  \centering
  \includegraphics[width=0.9\textwidth]{docs/figures/fig_7_2_lstm_loss.png}
  \caption[Bidirectional LSTM training loss]{Bidirectional LSTM
  training and validation binary cross-entropy over 10 epochs for the
  \textit{price\_direction\_lstm} task (MLflow run
  \texttt{c9a44e95}). Final train loss 0.5773; final validation loss
  0.5826. The small positive train--val gap indicates no catastrophic
  overfitting, though both losses remain well above a trained
  binary-classifier target, consistent with the difficulty of
  directional prediction at the chosen horizon.}
  \label{fig:lstm-loss}
\end{figure}
```

**Word:** *"Figure 7.2 — Bidirectional LSTM train vs. validation loss
over 10 epochs. Final values: train 0.5773, validation 0.5826. No
overfitting, but both losses remain close to the uniform-prior baseline
of ln 2 ≈ 0.693."*

### Fig 7.3 — Isolation Forest anomalies

**LaTeX:**

```latex
\begin{figure}[H]
  \centering
  \includegraphics[width=\textwidth]{docs/figures/fig_7_3_isolation_forest.png}
  \caption[Isolation Forest anomaly distribution]{Isolation Forest
  anomaly detection on the feature store. (a) Histogram of
  \texttt{decision\_function} scores split by predicted class with the
  decision boundary at zero; (b) 2-D feature space coloured by class.
  Anomaly rate on the full feature store is 4.77\%, consistent with
  the \texttt{contamination} target set at training time.}
  \label{fig:iso-forest}
\end{figure}
```

**Word:** *"Figure 7.3 — Isolation Forest anomaly distribution.
Left: score histogram with decision boundary at zero. Right: feature
scatter, anomalies in red. 4.77% of rows flagged as anomalous."*

### Fig 7.4 — GARCH(1,1) volatility

**LaTeX:**

```latex
\begin{figure}[H]
  \centering
  \includegraphics[width=\textwidth]{docs/figures/fig_7_4_garch_eth.png}
  \caption[GARCH(1,1) volatility — ETH/USD]{GARCH(1,1) volatility
  fit and 30-step forecast for ETH/USD, trained on 473,784 log-return
  observations. Realised volatility (rolling-60\,s $\sigma$) in grey,
  fitted conditional volatility in blue, out-of-sample forecast in
  red dashed. Model AIC $= -3{,}236{,}946$.}
  \label{fig:garch-eth}
\end{figure}
```

**Word:** *"Figure 7.4 — GARCH(1,1) conditional volatility for ETH/USD
with 30-step forecast. Fitted on 473,784 log-returns; AIC = −3,236,946.
Realised σ in grey, fitted in blue, forecast in red dashed."*

### Fig 7.7 — Latency waterfall

**LaTeX:**

```latex
\begin{figure}[H]
  \centering
  \includegraphics[width=\textwidth]{docs/figures/fig_7_7_latency.png}
  \caption[End-to-end latency waterfall]{End-to-end latency from
  exchange tick to dashboard render. Components: WebSocket RTT
  (50\,ms), Kafka-to-Bronze micro-batch (10\,s), two Spark cycles
  Bronze $\to$ Silver $\to$ Gold (20\,s), Gold $\to$ API $\to$
  Dashboard (100\,ms). Total $\approx 30$\,s, dominated by Spark
  Structured Streaming trigger intervals; sub-second latency would
  require Continuous Processing or Flink.}
  \label{fig:latency}
\end{figure}
```

**Word:** *"Figure 7.7 — End-to-end latency waterfall. Total ≈ 30 s,
dominated by the two Spark micro-batch cycles."*

### Fig 7.5 & 7.6 (dashboard screenshots)

Word-style only; these are image-only figures.

- **Figure 7.5** — *"Live Prices page of the Streamlit dashboard. Top
  panel: current price per symbol across Binance, Coinbase, and Kraken.
  Bottom: per-exchange pivot showing all five tracked pairs."*
- **Figure 7.6** — *"ML Insights page. Four panels show live model
  outputs: XGBoost arbitrage probability, LSTM direction, Isolation
  Forest anomaly flag, and GARCH conditional volatility sparkline."*

---

## §6 Poster workflow

Poster template: `30_3_26 Poster_Template.pptx` at the repo root.

### Which figures to reuse

Three figures carry the most narrative weight at arm's length:

1. **Fig 7.5 — Live Prices** — "what we built" (a picture of the
   product doing its job).
2. **Fig 7.1 — Feature importance** — "why the model works" (a bar
   chart readable from 2 m away).
3. **Fig 7.7 — Latency waterfall** — "what it costs" (a single visual
   with one big number).

Avoid 7.2 (LSTM loss) and 7.4 (GARCH): both rely on axes that are
unreadable at poster scale.

### Re-export at poster dpi

The existing scripts save at 300 dpi, which is enough for A1. For A0,
monkey-patch dpi at the top of the script:

```python
import matplotlib as mpl
mpl.rcParams["savefig.dpi"] = 600
```

For a permanent poster mode, plan the §8.5 "poster variants" follow-up
from the Further-steps table — add a `--poster` flag that also bumps
title sizes (18 → 24) and label sizes (11 → 14).

### Headline paragraph (centre of poster)

60-word block, quotes this guide's current numbers:

> *A streaming crypto-arbitrage platform built on Kafka, Spark
> Structured Streaming and Delta Lake ingests ticks from Binance,
> Coinbase and Kraken. Each row is labelled by whether a round-trip
> trade opened now would clear taker + withdrawal fees by T + 200 ms.
> An XGBoost classifier trained on this fee-net label is dominated by
> current spread and short-horizon volatility (Fig 7.1); end-to-end
> latency is ≈ 30 s (Fig 7.7).*

### Layout advice

- Minimum font size when exporting from Python: 28 pt body, 36 pt
  figure titles. The current scripts default to smaller sizes tuned
  for A4.
- Legend position: force `loc="upper left"` for Fig 7.1 and 7.7 so the
  bars are never overlapped by the legend — posters are viewed from
  awkward angles.
- Template background: `30_3_26 Poster_Template.pptx` uses a light
  background, so the current `#2E86AB` / `#E63946` palette has enough
  contrast. If switching to a dark theme poster, regenerate with
  `plt.style.use("dark_background")` first.

### Placing PNGs in PowerPoint

Paste the rendered PNGs into the template's placeholder frames. **Do
not** re-plot inside PowerPoint — the figures lose dpi and the typeface
no longer matches the written report. If a figure needs cropping, do it
with matplotlib's `bbox_inches="tight"` at render time, not in
PowerPoint.

---

## §7 Troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| `ModuleNotFoundError: ml.features…` | Running outside the repo root | `cd` to repo root before invoking the script — they inject `REPO_ROOT` into `sys.path`. |
| Fig 7.2 raises "No MLflow experiment" | Empty `mlruns/` or different tracking URI | Run `python -m ml.training.train_lstm`. If you use a non-default MLflow store, set `MLFLOW_TRACKING_URI` before running. |
| Fig 7.2 raises "missing train_loss/val_loss" | Older LSTM checkpoint from before per-epoch metric logging | Retrain: `python -m ml.training.train_lstm`. |
| Fig 7.3 produces an all-blue scatter | No points triggered the `-1` class — contamination too low for this data slice | Expand the feature-store window (`load_feature_store()`) or retrain the forest with a larger `contamination`. |
| Fig 7.4 x-axis is empty / single tick | ETH/USD Silver rows < 30 | Let the pipeline run longer; GARCH needs ≥ 50 observations to even fit (`train_garch.py:31`). |
| `pickle.UnpicklingError` on any artifact | Artifact produced by a different Python / sklearn / xgboost version | Recreate the artifact with the current env: `python -m ml.training.train_xgboost` (or the relevant trainer). |
| Dashboard screenshots show "no data" panels | API hasn't warmed the model cache | `curl http://localhost:8000/api/v1/ml/predict?symbol=BTC/USD` then refresh. |
| Blank/tiny PNG (< 20 KB) | Matplotlib failed silently before rendering | Rerun the script from a terminal and read the traceback; `MPLBACKEND=Agg` is safe. |
| Stdout shows `[label_generator] No profitable rows at fee-net threshold…` | Current feature store has small gross spreads relative to fees, so the percentile fallback engages | Expected on short data windows; fallback labels the top 25 % of *future* net profit as class 1. For natural positives, let `./start.sh` run longer and rebuild the feature store. |
| XGBoost test-set shows `support=0` for class 1 | Chronological split placed all positives in the train/val slices | Not a code bug; `compute_classifier_metrics` returns 0.0 metrics deterministically in this case (`ml/evaluation/metrics.py:22-25`). For report metrics, either explain this in prose or switch to stratified CV. |

---

## §8 Further steps (optional)

None of these are blockers. Rows A, B and G are especially useful as
evidence of the label redesign: ROC/PR and confusion matrix convert the
degenerate test-set metrics into a defensible story, and the
class-balance bar visualises the fix directly.

### §8.1 Regeneration hygiene

- **Commit the PNGs**: `git add docs/figures/*.png` so the version in
  the report matches the artifacts at that commit.
- **Makefile target**:
  ```make
  figures:
      @python scripts/fig_7_1_xgboost_importance.py
      @python scripts/fig_7_2_lstm_loss.py
      @python scripts/fig_7_3_isolation_forest.py
      @python scripts/fig_7_4_garch_eth.py
      @python scripts/fig_7_7_latency.py
      @echo "Manual: capture 7.5 and 7.6 per docs/REPORT_FIGURES.md §4"
  ```
- **Pre-commit hook**: for every updated `.pkl` / `.pt` artifact, verify
  the corresponding figure PNG is also newer — catches stale plots.

### §8.2 New figures worth adding

| # | Figure | Data source | Why it helps |
| --- | --- | --- | --- |
| A | XGBoost ROC & PR curves | test-set scores logged in `train_xgboost.py` | Turns the single accuracy number into a threshold-sensitivity story. |
| B | XGBoost confusion matrix | same | Shows class imbalance handling visually. |
| C | LSTM per-class accuracy over epochs | add two extra scalars to `train_lstm.py` and re-log | Separates "flat" vs. "direction" errors for readers. |
| D | GARCH residual ACF / PACF (2×1 subplot) | `result.std_resid` | Demonstrates the model has de-correlated the squared returns. |
| E | Isolation Forest anomaly timeline | `df.event_time` vs. `scores` | Converts 7.3's scatter into a narrative: "these bursts are when". |
| F | Latency histogram from real telemetry | API access log or Kafka end-to-end probe | Upgrades 7.7's block diagram into a distribution plot with p50/p95/p99 lines. |
| G | Class-balance before/after label redesign | `label_generator` stdout + a short bar | Makes the leakage fix visible to the reader in numbers. |

### §8.3 Style consistency

Adopt one matplotlib rc block shared across every script:

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

Refactor into `scripts/_plot_style.py` and import from each figure
script.

### §8.4 Reproducibility metadata

Every script stamps a caption sidecar next to the PNG:

```
docs/figures/fig_7_1_xgboost_importance.png
docs/figures/fig_7_1_xgboost_importance.caption.txt
```

containing the model hash, MLflow run ID, row count of the feature
store, and the current git SHA. Feeds straight into the report
appendix.

### §8.5 Poster variants

Add a `--poster` flag to each script that emits a second
`fig_7_X_poster.png` with bumped font sizes (title 18 → 24, labels
11 → 14). Posters are read from 2 m away; the A4 defaults don't hold
up.

### §8.6 CI enforcement

GitHub Actions job on PRs touching `ml/training/**` that runs
`python scripts/fig_7_*.py` and fails if any produces a 0-byte PNG.
Prevents silent drift between retrained models and stale plots.

---

## §9 Appendix — file-by-file inventory

```
scripts/
├── fig_7_1_xgboost_importance.py   XGBoost gain bar chart
├── fig_7_2_lstm_loss.py            LSTM BCE loss from MLflow
├── fig_7_3_isolation_forest.py     Anomaly histogram + 2-D scatter
├── fig_7_4_garch_eth.py            GARCH(1,1) fit + 30-step forecast (ETH/USD)
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

ml/training/label_generator.py      (source of the redesigned label)
ml/training/train_xgboost.py        (retrained against the new label)
src/processing/transformations/arbitrage.py  (canonical exchange fee table)
```
