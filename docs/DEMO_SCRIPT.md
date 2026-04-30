# Demo Script — Crypto Data Platform
**Duration:** 13 minutes | **Audience:** Academic panel | **Format:** Laptop walkthrough (projected)

---

## Before You Walk In

Start both processes and leave them running:

```bash
# Terminal 1 — API
source .venv/bin/activate
uvicorn src.serving.api.main:app --host 0.0.0.0 --port 8000

# Terminal 2 — Dashboard
source .venv/bin/activate
streamlit run src/serving/dashboard/app.py
```

Open two browser tabs:
- **Tab 1:** `http://localhost:8000/api/v1/docs` — FastAPI interactive docs
- **Tab 2:** `http://localhost:8501` — Streamlit dashboard

---

## [0:00 – 1:00] The Problem

> *"Cryptocurrency prices differ slightly across exchanges at any given moment — Binance might show BTC at \$83,200 while Kraken shows \$83,320. That gap is called a spread. If you can buy on one exchange and sell on the other faster than the gap closes, that's arbitrage.*
>
> *The challenge is: these windows last milliseconds, markets are noisy, and exchange fees eat your profit. We built a system that uses four cooperating ML models to decide, in real time, whether a spread is genuinely exploitable — or just noise."*

---

## [1:00 – 2:30] Architecture in 90 Seconds

Open the architecture diagram (`diagram-export-14-04-2026.png`). Point to each layer:

| Layer | One sentence |
|---|---|
| **Exchanges → Kafka** | "Three exchange WebSocket streams feed raw price ticks into Kafka continuously." |
| **Kafka → Spark → Delta Lake** | "Spark processes these in 10-second micro-batches through a Bronze → Silver → Gold medallion pipeline — raw data, normalised prices, then analytics." |
| **Delta Lake → ML → API** | "The Gold layer feeds four ML models. Everything is served through a FastAPI REST API that the dashboard consumes." |

> One diagram. Three sentences. Move on — the ML is the story.

---

## [2:30 – 8:30] ML Deep-Dive via FastAPI Docs

Switch to **Tab 1:** `http://localhost:8000/api/v1/docs`

Hit these four endpoints in order. Say the line **before** you click.

---

### 1. `/api/v1/ml/volatility/ETH-USD` — GARCH Volatility *(~1 min)*

> *"Before we can judge whether a spread is real, we need to know how volatile the market is. A GARCH(1,1) model gives us a per-symbol variance forecast. High volatility means spreads widen naturally — we don't want false positives."*

**Do:** Expand the endpoint → click Try it out → enter `ETH-USD` → Execute.
**Point to:** the `sigma_squared` value in the response.

---

### 2. `/api/v1/ml/anomalies/recent` — Isolation Forest *(~1 min)*

> *"Next, an Isolation Forest flags whether current price behaviour looks anomalous relative to history. It found a 4.77% anomaly rate on our data — consistent with the 5% contamination prior we set. If the market is in an unusual regime, we suppress the arbitrage signal."*

**Do:** Execute.
**Point to:** the `anomaly_flag` field and `anomaly_score` in the response rows.

---

### 3. `/api/v1/ml/predictions/BTC-USD` — Bidirectional LSTM *(~1.5 min)*

> *"The Bidirectional LSTM predicts price direction 30 seconds ahead — we achieved 69.5% directional accuracy. This confirms whether the spread is moving in our favour before we act on it. We used a 2-layer BiLSTM with 60-timestep sequences, trained on z-score normalised price and volume features."*

**Do:** Expand → Try it out → enter `BTC-USD` → Execute.
**Point to:** `direction` (1 = up, 0 = down) and `confidence`.

---

### 4. `/api/v1/ml/arbitrage/live` — XGBoost Classifier *(~2.5 min)*

> *"Finally, an XGBoost classifier combines everything — spread features, GARCH volatility, LSTM direction, anomaly flag — into a single arbitrage probability score. We achieved F1 of 0.984 and AUC-ROC of 1.0 on the test set."*

**Do:** Execute. **Point to:** `arb_probability`.

**Then say the key academic point:**

> *"The label design was critical. We didn't label based on current spread — that would leak the most predictive feature directly into the target variable. Instead, we label on fee-net future profit: does the spread, after round-trip exchange fees, yield a positive return at a future timestamp? This eliminated leakage and forced the model to learn genuine market dynamics rather than memorising the threshold."*

---

## [8:30 – 11:30] ML Insights Dashboard

Switch to **Tab 2:** `http://localhost:8501` → navigate to **Page 5: ML Insights**

| What to show | What to say |
|---|---|
| **XGBoost feature importance chart** | *"Notice that `spread_abs` and `rolling_vol` dominate — not `spread_pct`. That's a direct consequence of the label redesign. The model is rewarded for learning volatility dynamics, not just threshold-crossing."* |
| **Model performance metrics** | *"F1 of 0.984 versus a rule-based baseline. The baseline simply flags any spread above 0.15% — our model is significantly more precise."* |
| **Online learner section** | *"We also have an Adaptive Random Forest for online learning — it updates continuously as new data arrives, handling concept drift as market regimes change."* |

---

## [11:30 – 13:00] Close

> *"To summarise: this is a full end-to-end data engineering and ML platform — 3.4 million price events processed through a streaming pipeline, four models operating as a sequential inference ensemble, served at under 200ms per prediction.*
>
> *The system correctly identifies that in highly efficient markets, profitable arbitrage is rare. But it's architectured to catch it the moment a dislocation occurs — exchange outage, flash crash, liquidity event. That's the value proposition."*

Navigate to the **Home page** (`http://localhost:8501`) — show the green status indicators: API online, Silver layer ready, Gold layer ready.

---

## Key Numbers to Remember

| Model | Result |
|---|---|
| XGBoost | F1 = 0.984, AUC-ROC = 1.0 |
| LSTM | Directional accuracy = 69.5% |
| Isolation Forest | Anomaly rate = 4.77% |
| GARCH | AIC = −3,236,945 (ETH/USD) |
| Pipeline | 3.4M rows processed, ~30s end-to-end latency |
| API | < 200ms per ML prediction |

---

## If They Ask About...

**"Why is the arbitrage signals table nearly empty?"**
> *"That's the system working correctly. Modern crypto markets are highly efficient — genuine dislocations above 0.15% net of fees are rare in normal conditions. The platform is designed to catch them during volatility events like exchange outages or flash crashes."*

**"How do you prevent data leakage in ML training?"**
> *"Two mechanisms: fee-net future profit labels (the target is computed from future prices, not current spread), and walk-forward cross-validation with expanding chronological windows — no future data ever enters a training fold."*

**"Why four models instead of one?"**
> *"Each model captures a different market signal: GARCH handles volatility regime, Isolation Forest handles anomalous conditions, LSTM handles directional momentum, and XGBoost combines all of these into a final probability. A single model would conflate these very different phenomena."*

**"Could this run in production?"**
> *"The architecture is production-ready in design — we'd swap local Spark for Databricks, local Kafka for Confluent Cloud, and local Delta files for S3. The code changes would be configuration, not logic."*
