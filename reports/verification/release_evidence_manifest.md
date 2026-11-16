# MarketPulseAI — Release Evidence Manifest & Verified Resume Bullets

**Generated:** 16 November 2026  
**Machine-Readable Manifest:** [`reports/verification/release_evidence_manifest.json`](release_evidence_manifest.json)  
**Contract:** `mp_1m_5m_price_v1` (`1.0.0`)

---

## 1. Cryptographic & Execution Bindings

| Component | Identifier / Digest | Verification Artifact |
|---|---|---|
| **GitHub Actions CI Run** | Run [`43906352303`](https://github.com/Reiii0-0/MarketPulseAI/actions/runs/43906352303) (`rc/tahap7-ci-verify`, commit `45ff309e2bc97d2e2f9ef3c493da050ada266c9a`, PR #1) — **3/3 jobs green** (`quality-and-tests` 47 s, `browser-e2e-ui` 43 s, `compose-integration-recovery` 579 s) | [`github_ci_remote_20261116.json`](github_ci_remote_20261116.json) |
| **API & Worker Release Image** | `marketpulseai-api:local-finalization-20261116` (`sha256:5c08e8d79aa03f18f30f6703760f7e94b66324e813256f024b43c58c7b44f2bc`) | [`image_rollback_20261116.json`](image_rollback_20261116.json) |
| **API & Worker Rollback Image** | `marketpulseai-api:before-local-finalization-20261116` (`sha256:1093d431396529b1be4ac2893befca4106f47fca8e37c3c0df86bce442e0745e`) | [`image_rollback_20261116.json`](image_rollback_20261116.json) |
| **Public Smoke Fixture (`fixture_smoke_v1`)** | `data/fixtures/smoke_1m_v1.csv` (`89` rows, SHA-256 `8828a7756f7a69de72ac67dacc2715323088c1c3b50f1638be3ae1d6fab8e4be`) | [`repair_e2e.json`](repair_e2e.json) |
| **Served Demo Bundle & Runs** | Bundle `mp_1m_5m_price_v1_fixture_smoke_v1_8d766b1f` · Runs `replay-compose-1794841520` & `replay-compose-1794841520-second` (`89` bars/run, `178` snapshots, `10` predictions/run, `0` pending) | [`repair_e2e.json`](repair_e2e.json) |
| **Private Real-Data Evaluation (`kibot_ibm_20261116`)** | `24,531` 1-minute `IBM` bars across `63` sessions (`3` expanding folds, `17,865` pre-holdout samples, `4,468` holdout samples, `315 s` purge) | `reports/private/kibot_ibm_20261116/reproduction_check.json` |
| **Walkthrough Video (101.16 s)** | [`reports/portfolio/marketpulseai_demo.mp4`](../portfolio/marketpulseai_demo.mp4) (`1280x800`, H.264, `25` fps, `1,786,672` bytes) | [`release_evidence_manifest.json`](release_evidence_manifest.json) |
| **Portfolio Screenshots (3x)** | [`01_product_overview_and_prediction_detail.png`](../portfolio/01_product_overview_and_prediction_detail.png) (`1440x2215`), [`02_model_evidence_and_walk_forward.png`](../portfolio/02_model_evidence_and_walk_forward.png) (`1440x961`), [`03_failure_recovery_and_observability.png`](../portfolio/03_failure_recovery_and_observability.png) (`1440x960`) | [`release_evidence_manifest.json`](release_evidence_manifest.json) |

---

## 2. Section 6 Release Gates Checklist (All Verified)

- [x] **Clean bootstrap (`make demo`):** Completes without provider API keys; warm/fresh state reaches ready predictions in **57.763 s** (`78.551 s` total including bundle prep & dbt; [`demo_fresh_state.json`](demo_fresh_state.json)). Cold `--no-cache` dependency sync (**170.376 s**) and `--no-cache` Docker build (**785.013 s**) measured separately ([`cold_build_and_download_20261116.json`](cold_build_and_download_20261116.json)).
- [x] **Deterministic replay & run isolation:** Two consecutive Compose replay runs produce identical per-run business counts (**89 bars, 89 snapshots, 10 predictions, 0 pending**) without duplicating global raw events ([`repair_e2e.json`](repair_e2e.json)).
- [x] **Fault recovery & redaction:** Duplicate Kafka messages are ignored (`ON CONFLICT DO NOTHING`), malformed payloads have secret tokens redacted before quarantine (`raw.ingestion_quarantine`), and database/worker restarts resume from checkpoints with zero duplicate predictions ([`repair_e2e.json`](repair_e2e.json)).
- [x] **Zero future leakage & SQL/Python parity:** `14` canonical price features computed in PostgreSQL over `marts.final_bars` match the Python reference calculator with **0 hash mismatches** across `24,531` bars (`394.012 ms` SQL execution) and pass **11 dbt models + 116 data quality tests**.
- [x] **Purged walk-forward evaluation on real data:** Evaluated across **63 trading sessions** (`24,531` 1-minute `IBM` bars, `3` folds, `315 s` label maturity purge, `4,468` untouched holdout samples). `xgboost` won validation selection; `prior_baseline` achieved the lowest holdout Brier score (`0.249998` vs. `0.250077`) and is reported transparently without holdout retuning.
- [x] **Sustained 5-minute read API latency:** **3,000 requests** at **10.0 req/s** (concurrency 4) completed with **0 errors**, **p50 5.34 ms / p95 9.22 ms / p99 10.30 ms**, and scheduled-arrival **p95 9.30 ms** against the ≤ 300 ms budget ([`read_api_5min_20261116.json`](../benchmarks/read_api_5min_20261116.json)).
- [x] **1× four-ticker replay latency:** **20/20 predictions** across `AAPL`, `MSFT`, `NVDA`, and `TSLA` achieved **p50 0.265 s / p95 0.589 s** from DB bar finalization to prediction read against the ≤ 15 s budget ([`replay_latency_4ticker_20261116.json`](../benchmarks/replay_latency_4ticker_20261116.json)).
- [x] **TLS isolation & rollback:** Read-only Caddy TLS overlay passed **7/7 HTTPS tests** ([`tls_local_20261116.json`](tls_local_20261116.json)), and live API + worker image rollback preserved all `358` bars / `60` predictions ([`image_rollback_20261116.json`](image_rollback_20261116.json)).

---

## 3. Verified CV / Resume Bullet Points (Strictly Grounded in Measured Figures)

Use the bullets below directly on a Data Engineer or Machine Learning Engineer resume. Every number maps to a committed test or JSON artifact in this repository:

- **Streaming Data Pipeline & Fault Recovery:** Built an availability-ordered market replay and ingestion pipeline in Python, Apache Kafka (KRaft), PostgreSQL 15, and dbt (`11` models, `116` data tests) with business-key deduplication, secret-redacted poison-message quarantine, and transactional offset barriers; verified zero lost or duplicated events across database outages and worker restarts in Docker Compose CI.
- **Zero-Skew Feature Engineering:** Unified training and serving feature logic under a single canonical PostgreSQL SQL definition (`14` 1-minute OHLCV indicators) frozen into write-once snapshots with SHA-256 hashes, achieving `0` hash mismatches across `24,531` historical bars (`394 ms` SQL execution) and `0.589 s` p95 bar-finalization-to-prediction latency across 4 concurrent tickers at 1× replay speed.
- **Time-Aware ML Evaluation & Serving:** Implemented a purged expanding walk-forward evaluator (`315 s` horizon + grace purge) comparing calibrated XGBoost, logistic regression, and prior baselines across `63` intraday trading sessions (`24,531` bars, `4,468` untouched holdout samples) with atomic symlinked model bundles, SHA-256 artifact verification, and explicit `NO_SIGNAL` abstention bands.
- **Production Read API & Observability:** Delivered a read-only FastAPI service, zero-dependency single-page inspection UI, Caddy TLS proxy overlay, and provisioned Grafana 10.4 alerting stack sustaining `10.0 req/s` over 5 minutes (`3,000` requests, concurrency 4) at **p50 5.34 ms / p95 9.22 ms** latency with `0` errors and `57.8 s` warm bootstrap time.
