# Real Dataset Candidate Audit & Access Status

Status (16 November 2026): **private real-data evaluation available; public redistribution unresolved**. Kibot's account-free unadjusted samples were downloaded and audited; IBM was selected by session coverage, while sparse OIH remains a diagnostic sample. The acquisition, normalized CSV, manifest, evaluation and bundle stay in ignored `data/private/` and `reports/private/`. The engineering analysis and executed results live in [CASE_STUDY.md](../../CASE_STUDY.md) and [release_evidence_manifest.md](../../reports/verification/release_evidence_manifest.md).

Kibot source: [free samples](https://www.kibot.com/free-historical-intraday-data.html), [bar-open timestamp / missing-minute format](https://www.kibot.com/file-format/data-format-reference.html), [internal-use license](https://www.kibot.com/license.html). Data conversion uses America/New_York → UTC with DST, preserves unadjusted OHLCV, leaves gaps missing and explicitly assumes availability at bar end + 5 seconds. This is not a full point-in-time market-data guarantee. The public demo remains synthetic.

The candidate table below records the earlier provider-access audit. It is not a current claim that historical evaluation is blocked.

## Target Contract Requirements
- Tickers: `AAPL`, `MSFT`, `NVDA`, `TSLA` (or licensed subset chosen prior to Stage 4).
- Granularity: 1-minute OHLCV bars (`bar_interval_seconds = 60`), UTC timestamps, regular session filtering (`America/New_York` 09:30–16:00).
- Horizon: ~60 intraday sessions (~23,400 bars per ticker) to allow walk-forward folds plus calibration and holdout cutoffs.
- Features: Price-only baseline (`config/forecast_contract.json`); sentiment excluded unless native `available_at` and audited source are verified.

## Candidate Sources Evaluated

| Candidate | Coverage & Format | Redistribution / Licensing Notes | Cost / Access Status |
|---|---|---|---|
| **Alpaca Market Data API (IEX / SIP)** | 1m historical OHLCV bars, multi-year intraday depth | API output restricted by Alpaca Market Data Terms; aggregate evaluation metrics and small non-redistributable internal runs OK, raw market data redistribution disallowed. | Free tier available without upgrading; requires `ALPACA_API_KEY` / secret (not configured in `.env`). |
| **Massive / Polygon.io Stocks** | 1m historical OHLCV bars (up to 2 years on free tier) | Commercial data terms prohibit redistributing raw market bars in public Git repos; metrics/screenshots/synthetic fixtures only in public repo. | Free tier rate-limited (5 req/min); no `POLYGON_API_KEY` configured. |
| **Finnhub Stock API (Free tier)** | `/api/v1/quote` verified **HTTP 200** (snapshot quote + `t` timestamp, no trade volume). `/api/v1/stock/candle` (`1m`) verified **HTTP 403 Forbidden** on free tier. | No raw data redistribution. | Configured in `.env` (free tier only — historical 1m candles unavailable without paid plan, **do not upgrade**). |
| **Marketaux News API (Free tier)** | `/v1/news/all` verified **HTTP 200** when sending explicit `User-Agent` header (without `User-Agent`, Cloudflare returns `403 error code 1010`). Provides `published_at`, `uuid`, and `entities[].sentiment_score` (limit 3/req). | No raw news redistribution. | Configured in `.env` (free tier only — usable for audited sentiment timestamps, not OHLCV bars). |
| **Yahoo Finance (`yfinance`)** | 1m bars limited to ~7–30 recent days max | Unofficial scraping wrapper; violates strict >30-day (60-session) requirement and ToS for commercial/redistribution clarity. | Rejected as primary evaluation source. |

## Earlier acquisition requirements and remaining publication limits
1. Keep Finnhub and Marketaux strictly on **FREE tier** (Finnhub `/quote` for live quote feed; Marketaux `/v1/news/all` for `published_at` sentiment events).
2. The local evaluation uses the audited Kibot IBM sample. A real-data public demo needs separate redistribution rights; another provider still requires account access and a license review. No paid upgrade was made.
3. Generate the private manifest with exact SHA-256 checksum, ticker list, session count, and `availability_assumed: true` (`available_at = bar_end + 5s`); the private evaluation records the `UP`/`NOT_UP` distribution.
4. Keep Kibot raw/normalized data, manifests, reports and model bundles private. Public release uses synthetic fixtures (`fixture_smoke_v1`); publishing real-data derivatives requires a separate rights check.
