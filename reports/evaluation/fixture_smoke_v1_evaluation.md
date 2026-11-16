# Evaluation Report — fixture_smoke_v1

> **Provenance:** `synthetic_fixture` | **Real Market Data:** `False`

> **WARNING:** SYNTHETIC OR NON-PRODUCTION FIXTURE ONLY — DO NOT REPORT AS REAL MARKET PERFORMANCE.

- **Contract:** `mp_1m_5m_price_v1` v`1.0.0` (`price_only`)
- **Selection Metric:** `brier_score`
- **Champion Model (selected on validation):** `xgboost`
- **Challenger Models:** `prior_baseline, logistic_regression`
- **fit_information_end:** `2025-01-15T15:06:15+00:00`
- **Holdout Window:** `2025-01-15T15:07:00+00:00` .. `2025-01-15T15:10:00+00:00` (samples=4)

## Holdout Results (Single Untouched Evaluation)

| Model | Brier Score | Log Loss | Balanced Acc | ECE | Coverage |
|---|---|---|---|---|---|
| `prior_baseline` | 0.229339 | 0.651716 | 0.5000 | 0.2045 | 1.0000 |
| `logistic_regression` | 0.238559 | 0.670245 | 0.5000 | 0.2242 | 1.0000 |
| `xgboost` | 0.241154 | 0.675667 | 0.5000 | 0.1985 | 1.0000 |

- **Random-Walk Comparator (price persistence, separate from binary probabilities):** hit_rate=0.5000, balanced_accuracy=0.3333
