from __future__ import annotations

import json
import logging
import random
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, cast

from confluent_kafka import Producer

from producer.config import ProducerConfig
from producer.schema_validator import MarketTickMessage, ProvenanceType, SentimentEventMessage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

_SECRET_QUERY_RE = re.compile(r"(?i)(api_token|token|apikey)=([^&\s]+)")


def _has_real_api_key(key: str | None) -> bool:
    return bool(key and key.strip() and "your_" not in key.lower())


def _sanitize_error_text(exc: BaseException) -> str:
    return _SECRET_QUERY_RE.sub(r"\1=[REDACTED]", str(exc))


class LiveProducer:
    """Kafka producer separating real external provider ingestion from synthetic mock streams.

    In real_live mode, failed or tier-restricted provider calls record an explicit unavailable
    status rather than fabricating mock prices labeled as real market data.
    """

    def __init__(
        self,
        config: ProducerConfig,
        *,
        sleep_fn: Callable[[float], None] = time.sleep,
    ) -> None:
        self.config = config
        self.sleep_fn = sleep_fn
        self.producer = Producer(
            {
                "bootstrap.servers": config.kafka_bootstrap_servers,
                "acks": config.kafka_producer_acks,
                "retries": config.kafka_producer_retries,
            }
        )
        self.tickers = ["AAPL", "TSLA", "MSFT", "NVDA"]
        self.sequence_counter = 0
        self.mock_prices = {ticker: 150.0 + random.uniform(-10, 10) for ticker in self.tickers}

        if config.live_mode == "synthetic":
            self.use_finnhub = False
            self.use_marketaux = False
            self.mode = "synthetic_mock"
        else:
            self.use_finnhub = _has_real_api_key(config.finnhub_api_key)
            self.use_marketaux = _has_real_api_key(config.marketaux_api_key)
            if config.live_mode == "real" or self.use_finnhub or self.use_marketaux:
                self.mode = "real_live"
            else:
                self.mode = "synthetic_mock"

        self.provenance: ProvenanceType = (
            "real_live" if self.mode == "real_live" else "synthetic_mock"
        )
        self.unavailable_tickers: dict[str, str] = {}
        self.last_http_error: str | None = None

    def _http_get(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Helper for urllib GET requests with error handling and credential redaction."""
        self.last_http_error = None
        query_url = f"{url}?{urllib.parse.urlencode(params)}" if params else url
        req_headers = {
            "User-Agent": "MarketPulseAI/0.1 (+https://github.com/MarketPulseAI)",
            "Accept": "application/json",
            **(headers or {}),
        }
        req = urllib.request.Request(query_url, headers=req_headers)
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return cast(dict[str, Any], json.loads(resp.read().decode("utf-8")))
        except urllib.error.HTTPError as exc:
            self.last_http_error = f"http_{exc.code}"
            logger.warning("HTTP %s from provider %s: %s", exc.code, url, _sanitize_error_text(exc))
            return {}
        except Exception as exc:
            self.last_http_error = type(exc).__name__
            logger.error("Failed API request to %s: %s", url, _sanitize_error_text(exc))
            return {}

    def backfill_history(self) -> None:
        """Backfill recent history in synthetic mode; skip gracefully when free tier restricts candles."""
        if self.mode == "synthetic_mock":
            now = datetime.now(UTC)
            logger.info("Generating synthetic mock backfill (provenance=synthetic_mock)...")
            for ticker in self.tickers:
                price = self.mock_prices[ticker]
                for step in range(60, 0, -1):
                    ts = now.replace(second=0, microsecond=0)
                    ts = datetime.fromtimestamp(ts.timestamp() - step * 60, tz=UTC)
                    price *= 1.0 + random.uniform(-0.001, 0.001)
                    self.sequence_counter += 1
                    msg = MarketTickMessage(
                        ticker=ticker,
                        event_timestamp=ts,
                        event_at=ts,
                        available_at=ts,
                        ingested_at=now,
                        price=round(price, 2),
                        volume=random.randint(100, 1000),
                        replay_sequence=self.sequence_counter,
                        dataset_id="synthetic_live_stream",
                        source_event_id=f"synthetic_backfill:{ticker}:{int(ts.timestamp())}:{self.sequence_counter}",
                        provenance="synthetic_mock",
                    )
                    self.producer.produce(
                        topic=self.config.kafka_topic_market_ticks,
                        key=ticker,
                        value=msg.model_dump_json(),
                    )
                self.mock_prices[ticker] = price
            self.producer.flush()
            return

        if not self.use_finnhub:
            logger.info("Finnhub key absent in real_live mode; skipping candle backfill.")
            return

        now_ts = int(time.time())
        from_ts = now_ts - 3600
        for ticker in self.tickers:
            data = self._http_get(
                "https://finnhub.io/api/v1/stock/candle",
                {"symbol": ticker, "resolution": "1", "from": from_ts, "to": now_ts},
                {"X-Finnhub-Token": self.config.finnhub_api_key or ""},
            )
            if data.get("s") != "ok" or not data.get("c"):
                reason = self.last_http_error or str(data.get("s") or "unavailable")
                logger.info(
                    "Candle backfill unavailable for %s (provider=finnhub, reason=%s); continuing live quote polling",
                    ticker,
                    reason,
                )
                break

            now = datetime.now(UTC)
            for ts_sec, close_val, vol_val in zip(data["t"], data["c"], data["v"], strict=False):
                event_dt = datetime.fromtimestamp(int(ts_sec), tz=UTC)
                self.sequence_counter += 1
                msg = MarketTickMessage(
                    ticker=ticker,
                    event_timestamp=event_dt,
                    event_at=event_dt,
                    available_at=event_dt,
                    ingested_at=now,
                    price=float(close_val),
                    volume=max(int(vol_val), 0),
                    replay_sequence=self.sequence_counter,
                    dataset_id="finnhub_candle_backfill",
                    source_event_id=f"finnhub_candle:{ticker}:{int(ts_sec)}",
                    provenance="real_live",
                )
                self.producer.produce(
                    topic=self.config.kafka_topic_market_ticks,
                    key=ticker,
                    value=msg.model_dump_json(),
                )
        self.producer.flush()

    def get_market_ticks(self) -> None:
        """Poll real Finnhub quotes or emit explicitly labeled synthetic mock ticks."""
        now = datetime.now(UTC)
        for ticker in self.tickers:
            if self.mode == "real_live":
                if not self.use_finnhub:
                    self.unavailable_tickers[ticker] = "unavailable:missing_api_key"
                    continue
                data = self._http_get(
                    "https://finnhub.io/api/v1/quote",
                    {"symbol": ticker},
                    {"X-Finnhub-Token": self.config.finnhub_api_key or ""},
                )
                raw_price = data.get("c")
                if not isinstance(raw_price, int | float) or float(raw_price) <= 0.0:
                    reason = self.last_http_error or "unavailable_quote"
                    self.unavailable_tickers[ticker] = f"unavailable:{reason}"
                    logger.warning(
                        "Real market quote unavailable for %s (reason=%s); skipping synthetic fallback",
                        ticker,
                        reason,
                    )
                    continue
                self.unavailable_tickers.pop(ticker, None)
                price = float(raw_price)
                quote_ts = (
                    datetime.fromtimestamp(int(data["t"]), tz=UTC)
                    if isinstance(data.get("t"), int | float) and int(data["t"]) > 0
                    else now
                )
                self.sequence_counter += 1
                msg = MarketTickMessage(
                    ticker=ticker,
                    event_timestamp=quote_ts,
                    event_at=quote_ts,
                    available_at=now,
                    ingested_at=now,
                    price=price,
                    volume=0,
                    replay_sequence=self.sequence_counter,
                    dataset_id="finnhub_live_quote",
                    source_event_id=f"finnhub_quote:{ticker}:{int(quote_ts.timestamp())}:{self.sequence_counter}",
                    provenance="real_live",
                )
            else:
                price = self.mock_prices[ticker] * (1.0 + random.uniform(-0.0008, 0.0008))
                self.mock_prices[ticker] = price
                self.sequence_counter += 1
                msg = MarketTickMessage(
                    ticker=ticker,
                    event_timestamp=now,
                    event_at=now,
                    available_at=now,
                    ingested_at=now,
                    price=round(price, 2),
                    volume=random.randint(50, 500),
                    replay_sequence=self.sequence_counter,
                    dataset_id="synthetic_live_stream",
                    source_event_id=f"synthetic_tick:{ticker}:{int(now.timestamp() * 1000)}:{self.sequence_counter}",
                    provenance="synthetic_mock",
                )

            self.producer.produce(
                topic=self.config.kafka_topic_market_ticks,
                key=ticker,
                value=msg.model_dump_json(),
            )
            logger.info("Published Tick [%s] -> %s: %.2f", msg.provenance, ticker, msg.price)
        self.producer.poll(0)

    def get_sentiment(self) -> None:
        """Fetch real Marketaux sentiment or emit explicitly labeled synthetic sentiment."""
        now = datetime.now(UTC)
        if self.mode == "real_live":
            if not self.use_marketaux:
                logger.info("Marketaux key absent in real_live mode; sentiment unavailable.")
                return
            logger.info("Fetching news sentiment from Marketaux...")
            data = self._http_get(
                "https://api.marketaux.com/v1/news/all",
                {
                    "symbols": ",".join(self.tickers),
                    "filter_entities": "true",
                    "language": "en",
                    "api_token": self.config.marketaux_api_key or "",
                    "limit": 5,
                },
            )
            articles = data.get("data")
            if not isinstance(articles, list) or not articles:
                reason = self.last_http_error or "empty_response"
                logger.warning("Marketaux sentiment unavailable (reason=%s)", reason)
                return
            for art in articles:
                title = str(art.get("title") or "Market News")
                published_raw = art.get("published_at")
                try:
                    event_dt = (
                        datetime.fromisoformat(
                            str(published_raw).replace("Z", "+00:00")
                        ).astimezone(UTC)
                        if published_raw
                        else now
                    )
                except ValueError:
                    event_dt = now
                for entity in art.get("entities", []):
                    sym = entity.get("symbol")
                    if sym in self.tickers:
                        score = float(entity.get("sentiment_score") or 0.0)
                        self._publish_sentiment(
                            sym,
                            score,
                            title,
                            event_dt,
                            available_at=now,
                            dataset_id="marketaux_live_news",
                            source_event_id=f"marketaux:{art.get('uuid', self.sequence_counter)}:{sym}",
                            source="MARKETAUX",
                            provenance="real_live",
                        )
            return

        ticker = random.choice(self.tickers)
        score = round(random.uniform(-0.8, 0.8), 2)
        self._publish_sentiment(
            ticker,
            score,
            f"Synthetic market update for {ticker}",
            now,
            available_at=now,
            dataset_id="synthetic_live_stream",
            source_event_id=f"synthetic_sentiment:{ticker}:{int(now.timestamp() * 1000)}:{self.sequence_counter}",
            source="SYNTHETIC_DEMO",
            provenance="synthetic_mock",
        )

    def _publish_sentiment(
        self,
        ticker: str,
        score: float,
        headline: str,
        ts: datetime,
        *,
        available_at: datetime | None = None,
        dataset_id: str | None = None,
        source_event_id: str | None = None,
        source: str = "LIVE_FEED",
        provenance: ProvenanceType | None = None,
    ) -> None:
        self.sequence_counter += 1
        label = "NEUTRAL"
        if score > 0.1:
            label = "POSITIVE"
        elif score < -0.1:
            label = "NEGATIVE"
        avail_ts = available_at or ts
        resolved_prov = provenance or self.provenance
        resolved_dataset = dataset_id or (
            "marketaux_live_news" if resolved_prov == "real_live" else "synthetic_live_stream"
        )
        resolved_event_id = (
            source_event_id
            or f"{resolved_dataset}:{ticker}:{int(ts.timestamp())}:{self.sequence_counter}"
        )
        msg = SentimentEventMessage(
            ticker=ticker,
            event_timestamp=ts,
            event_at=ts,
            available_at=avail_ts,
            ingested_at=datetime.now(UTC),
            source=source,
            headline=headline[:1000],
            sentiment_score=score,
            sentiment_label=label,
            confidence=0.95,
            replay_sequence=self.sequence_counter,
            dataset_id=resolved_dataset,
            source_event_id=resolved_event_id,
            provenance=resolved_prov,
        )
        self.producer.produce(
            topic=self.config.kafka_topic_sentiment_events,
            key=ticker,
            value=msg.model_dump_json(),
        )
        logger.info("Published Sentiment [%s] -> %s: %s", resolved_prov, ticker, score)

    def run(self) -> None:
        """Main execution loop with managed timing."""
        logger.info("Starting Live Producer in mode=%s provenance=%s", self.mode, self.provenance)
        self.backfill_history()

        sentiment_interval = 3600
        last_sentiment_fetch = 0.0

        try:
            while True:
                self.get_market_ticks()
                now = time.time()
                if now - last_sentiment_fetch > sentiment_interval:
                    self.get_sentiment()
                    last_sentiment_fetch = now
                self.sleep_fn(2)
        except KeyboardInterrupt:
            logger.info("LiveProducer stopped.")
        finally:
            self.producer.flush()


if __name__ == "__main__":
    config = ProducerConfig()
    LiveProducer(config).run()
