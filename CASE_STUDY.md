# MarketPulseAI — Engineering Case Study

**Role Target:** Data / Machine Learning Engineer  
**Date:** 16 November 2026  
**Contract:** `mp_1m_5m_price_v1` (`1.0.0`, 1-minute UTC OHLCV bars, +5-minute directional horizon)

---

## 1. Problem Statement

Streaming market analytics prototypes frequently fail three basic engineering audits:

1. **Silent time leakage and mutable history:** SQL pipelines that compute features over raw tables with `LEAD(close)` or unguarded rolling windows allow late arrivals and future rows to alter historical features after a prediction has been issued.
2. **Train-serving skew across dual feature engines:** Maintaining one set of indicator formulas in SQL/dbt for analytics and a second set in Python/pandas for model training creates numeric drift and mismatched warm-up semantics.
3. **Unverifiable ML and operational claims:** Portfolio demos often depend on paid API quotas to run, fall back to synthetic random walks when a provider returns HTTP 403, or tune hyperparameters on the final test split until accuracy looks artificially high.

MarketPulseAI was re-architected to solve these three failure modes on a single-host Docker Compose stack using strictly free-tier resources.

---

## 2. Core Design Decisions and Tradeoffs

### Decision A: SQL Canonical Features + Write-Once Snapshots vs. Separate Python Feature Engine

- **Choice:** PostgreSQL SQL (`CANONICAL_FINAL_BAR_FEATURES_SQL` in `pipeline/features.py`, mirrored in dbt models `int_ohlcv_1min`, `int_technical_indicators`, and `stock_features`) is the single production authority for the 14 price features. The worker freezes each finalized bar's features into `marts.feature_snapshots` (`ON CONFLICT DO NOTHING`) with a SHA-256 `feature_hash`.
- **Why this won:** Both `ModelTrainer` and `Predictor` read the exact same frozen snapshot record and verify `feature_hash` before training or inference. Removing the duplicate `model/feature_engineering.py` eliminated dual-maintenance drift.
- **Measured proof:** Across 24,531 real 1-minute IBM bars (63 trading sessions), PostgreSQL computed all 14 canonical features in **394.012 ms** (`1.266 ms` planning) with **0 hash mismatches** against the offline Python reference calculator (`reports/private/kibot_ibm_20261116/sql_feature_profile.json`).
- **Tradeoff accepted:** Window functions in PostgreSQL spilled two sorts to disk on the 24,531-row scan. For a 1-minute bar cadence on four tickers, sub-400 ms batch execution makes a separate streaming feature store unnecessary complexity.

### Decision B: Availability-Ordered Deterministic Replay vs. Live Provider Dependency

- **Choice:** The default product demo (`make demo`) runs `producer/replay_producer.py` against a checksum-verified 1-minute OHLCV fixture (`fixture_smoke_v1`, 89 bars across `MSFT` and `AAPL`) ordered by `available_at`. `ReplayCoordinator` waits for PostgreSQL delivery barriers (`raw.run_delivery_progress`) on every active partition before advancing the run's `logical_as_of` clock, and injects empty decision steps at `bar_end + 15s` so the final bar's grace window drains cleanly.
- **Why this won:** Any reviewer can clone the repository and run `make demo` offline without API keys. Live ingestion (`producer/live_producer.py` on Finnhub free `/api/v1/quote` and Marketaux free `/v1/news/all`) is isolated behind the `--profile live` Compose profile and marks failed HTTP fetches as `unavailable` instead of fabricating prices.
- **Measured proof:** Two consecutive Compose replay runs (`replay-compose-1794841520` and `replay-compose-1794841520-second`) produced identical business counts (**89 final bars/run, 178 total snapshots, 10 predictions/run, 0 pending outcomes after completion**) while deduplicating global raw events and redacting poisoned secret payloads in `raw.ingestion_quarantine` (`reports/verification/repair_e2e.json`).
- **Tradeoff accepted:** Replay latency reflects local container processing rather than public exchange feed jitter. To keep reporting honest, worker compute time (`compute_duration_ms`) and 1× steady-state finalization-to-prediction latency (**p50 0.265 s, p95 0.589 s** across 20 predictions on 4 tickers in `reports/benchmarks/replay_latency_4ticker_20261116.json`) are reported separately from replay speed multipliers.

### Decision C: Purged Walk-Forward Validation and Untouched Holdout vs. Accuracy Cherry-Picking

- **Choice:** `model/evaluate.py` enforces a **315-second maturity purge** (`300 s` horizon + `15 s` grace) between training timestamps and validation/holdout cutoffs (`target_at + grace <= next_cutoff`). Three probabilistic models (`prior_baseline`, `logistic_regression`, `xgboost`) are compared using **Brier score** as the primary metric. Platt scaling and `NO_SIGNAL` abstention bands are fit strictly on out-of-fold validation predictions; the champion is locked before evaluating the final holdout once.
- **Why this won:** Time-series classifiers overfit easily when adjacent overlapping 5-minute horizons leak across train/validation boundaries.
- **Measured proof:** On the private 63-session Kibot IBM evaluation (`24,531` 1-minute bars, `17,865` pre-holdout samples across 3 expanding folds, `4,468` untouched holdout samples), `xgboost` achieved the best out-of-fold validation Brier score and was selected as champion. On the untouched holdout, however, the constant `prior_baseline` achieved a slightly lower Brier score (**0.249998**) than `xgboost` (**0.250077**) and `logistic_regression` (**0.250325**).
- **Tradeoff accepted:** We did not retune hyperparameters or swap the champion after observing the holdout. Showing that 14 short-horizon price features on a single equity do not beat a calibrated prior out-of-sample is a stronger demonstration of evaluation discipline than a leaked 70% accuracy claim.

---

## 3. Summary of Verified Evidence

| Verification Gate | Measured Result | Artifact Path |
|---|---|---|
| **GitHub Actions CI (Remote)** | 3/3 jobs green (`quality-and-tests` 47 s, `browser-e2e-ui` 43 s, `compose-integration-recovery` 9m 39s) on run `43906352303` | `reports/verification/github_ci_remote_20261116.json` |
| **Offline Unit & Integration Suite** | **138 passed, 8 deselected**, **82.65%** branch coverage | `pyproject.toml`, `tests/` |
| **Compose End-to-End & Fault Recovery** | Duplicate publish deduped (`89` bars), secret payload redacted in quarantine, DB outage & worker restart recovered with `0` duplicate predictions, dbt **11 models + 116 tests passed** | `reports/verification/repair_e2e.json` |
| **5-Minute Sustained Read API Load** | **3,000 requests** at **10.0 req/s** (concurrency 4), **0 errors**, latency **p50 5.34 ms / p95 9.22 ms / p99 10.30 ms**, scheduled-arrival **p95 9.30 ms** | `reports/benchmarks/read_api_5min_20261116.json` |
| **1× Four-Ticker Replay Latency** | **20/20 predictions**, DB finalization to prediction read **p50 0.265 s / p95 0.589 s** (budget ≤ 15 s) | `reports/benchmarks/replay_latency_4ticker_20261116.json` |
| **Warm / Fresh State Bootstrap** | **57.763 s** runtime to ready predictions (**78.551 s** total with bundle prep & dbt verification; budget ≤ 120 s) | `reports/verification/demo_fresh_state.json` |
| **Cold Download & `--no-cache` Build** | **170.376 s** cold `uv sync --no-cache` + **785.013 s** cold `docker compose build --no-cache` (**955.389 s** total), followed by **68.772 s** runtime to ready predictions | `reports/verification/cold_build_and_download_20261116.json` |
| **Image & Bundle Rollback** | Atomic switch between two distinct API/worker image digests (`5c08e8d7...` ↔ `1093d431...`) and retained releases in `model/artefacts/releases/` with `358` bars / `60` predictions preserved | `reports/verification/image_rollback_20261116.json` |
| **Read-Only TLS Proxy Isolation** | **7 passed** HTTPS tests via Caddy internal CA (`GET` 200, `POST`/`DELETE` 403, `/docs` 404, zero host ports on DB/Kafka/Grafana/API) | `reports/verification/tls_local_20261116.json` |

---

## 4. Known Limitations

1. **Historical availability assumption:** Intraday vendor CSVs (`IBM` 1-minute bars) record bar-open timestamps in Eastern Time without exchange feed arrival logs. The importer converts `America/New_York` to UTC across DST transitions and explicitly sets `available_at = bar_end + 5s` (`availability_assumed: true`).
2. **Data redistribution boundary:** Kibot's license permits internal evaluation but prohibits redistributing raw or derived data. Raw files, manifests, and trained bundles from the 63-session IBM run remain in gitignored `data/private/` and `reports/private/`; the public demo serves only the synthetic fixture (`fixture_smoke_v1`).
3. **Single-worker vertical ceiling:** One `prediction-worker` holds a per-run lease in `marts.prediction_worker_state` and processes batches sequentially in PostgreSQL. Scaling to hundreds of tickers would require partitioning `marts.final_bars` and sharding worker leases by ticker group.
4. **No tradable edge:** Price-only 1-minute technical indicators on a 5-minute horizon have near-zero signal-to-noise ratio (`Brier ~0.2500`). The system does not model bid-ask spread, queue position, or execution slippage.

---

## 5. Next Steps

1. **Multi-ticker licensed historical evaluation:** Acquire a redistributable or account-backed multi-equity 1-minute dataset (such as Alpaca Basic IEX historical bars across `AAPL`, `MSFT`, `NVDA`, and `TSLA`) to evaluate cross-sectional generalization.
2. **Offline foundation model challenger:** Benchmark a pretrained time-series foundation model (such as Chronos-2) offline against the `prior_baseline` and `xgboost` on identical 315-second purged cutoffs before considering serving changes.
3. **Incremental SQL materialization:** Replace the full-run window scan in `CANONICAL_FINAL_BAR_FEATURES_SQL` with a bounded 25-bar lookback per `(run_id, ticker, segment_id)` once single-run volume exceeds 100,000 bars.
