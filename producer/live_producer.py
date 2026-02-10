from __future__ import annotations

import json
import logging
import random
import time
import urllib.parse
import urllib.request
from datetime import UTC, datetime, timedelta
from typing import Any

from confluent_kafka import Producer
from pydantic import ValidationError

from producer.config import ProducerConfig
from producer.schema_validator import MarketTickMessage, SentimentEventMessage

# Configuration for logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)


class LiveProducer:
    """
    Kafka producer with automated 6-hour backfilling.
    Uses standard urllib for high reliability and zero extra dependencies.
    """

    def __init__(self, config: ProducerConfig) -> None:
        self.config = config
        self.producer = Producer({
            "bootstrap.servers": config.kafka_bootstrap_servers,
            "acks": config.kafka_producer_acks,
            "retries": config.kafka_producer_retries,
        })
        self.tickers = ["AAPL", "TSLA", "MSFT", "NVDA"]
        self.sequence_counter = 0
        self.mock_prices = {ticker: 150.0 + random.uniform(-10, 10) for ticker in self.tickers}
        
        self.use_finnhub = self.config.finnhub_api_key and "your_" not in self.config.finnhub_api_key
        self.use_marketaux = self.config.marketaux_api_key and "your_" not in self.config.marketaux_api_key

    def _http_get(self, url: str, params: dict[str, str], headers: dict[str, str] | None = None) -> dict[str, Any]:
        query_string = urllib.parse.urlencode(params)
        full_url = f"{url}?{query_string}"
        req = urllib.request.Request(full_url, headers=headers or {})
        try:
            with urllib.request.urlopen(req, timeout=15) as response:
                return json.loads(response.read().decode("utf-8"))
        except Exception as e:
            logger.error("HTTP GET failed: %s", str(e))
            return {}

    def backfill_history(self) -> None:
        """Fetch last 6 hours of 1-minute data to populate the dashboard instantly."""
        if not self.use_finnhub:
            logger.info("Mock Mode: Skipping real history backfill.")
            return

        logger.info("Filling past 6 hours of 1-minute candles from Finnhub...")
        end_ts = int(time.time())
        start_ts = end_ts - (6 * 3600)
        
        for ticker in self.tickers:
            try:
                data = self._http_get(
                    "https://finnhub.io/api/v1/stock/candle",
                    {
                        "symbol": ticker,
                        "resolution": "1",
                        "from": str(start_ts),
                        "to": str(end_ts),
                        "token": self.config.finnhub_api_key or ""
                    }
                )
                
                if data.get("s") == "ok":
                    closes = data.get("c", [])
                    timestamps = data.get("t", [])
                    volumes = data.get("v", [])
                    
                    for i in range(len(closes)):
                        self.sequence_counter += 1
                        dt = datetime.fromtimestamp(timestamps[i], tz=UTC).isoformat()
                        
                        msg = MarketTickMessage(
                            ticker=ticker,
                            event_timestamp=dt,
                            price=closes[i],
                            bid=round(closes[i] * 0.9998, 2),
                            ask=round(closes[i] * 1.0002, 2),
                            volume=int(volumes[i]) if i < len(volumes) else 100,
                            trade_condition="BACKFILL",
                            replay_sequence=self.sequence_counter
                        )
                        self.producer.produce(
                            topic=self.config.kafka_topic_market_ticks,
                            key=ticker,
                            value=msg.model_dump_json()
                        )
                    logger.info("Backfilled %d points for %s", len(closes), ticker)
                
                time.sleep(1.0)
                self.producer.poll(0)
            except Exception as e:
                logger.error("Backfill failed for %s: %s", ticker, str(e))

    def get_market_ticks(self) -> None:
        for ticker in self.tickers:
            try:
                self.sequence_counter += 1
                current_dt = datetime.now(UTC).isoformat()
                
                if self.use_finnhub:
                    data = self._http_get(
                        "https://finnhub.io/api/v1/quote",
                        {"symbol": ticker},
                        {"X-Finnhub-Token": self.config.finnhub_api_key or ""}
                    )
                    price = float(data.get("c", self.mock_prices[ticker]))
                else:
                    self.mock_prices[ticker] += random.uniform(-0.5, 0.5)
                    price = round(self.mock_prices[ticker], 2)

                msg = MarketTickMessage(
                    ticker=ticker,
                    event_timestamp=current_dt,
                    price=price,
                    bid=round(price * 0.9998, 2),
                    ask=round(price * 1.0002, 2),
                    volume=random.randint(100, 5000),
                    trade_condition="REGULAR",
                    replay_sequence=self.sequence_counter
                )
                self.producer.produce(
                    topic=self.config.kafka_topic_market_ticks,
                    key=ticker,
                    value=msg.model_dump_json()
                )
                logger.info("Published Tick -> %s: $%s", ticker, price)
            except Exception as e:
                logger.error("Tick error for %s: %s", ticker, str(e))
            
            time.sleep(1.1)
            self.producer.poll(0)

    def get_sentiment(self) -> None:
        try:
            current_dt = datetime.now(UTC).isoformat()
            if self.use_marketaux:
                data = self._http_get(
                    "https://api.marketaux.com/v1/news/all",
                    {
                        "symbols": ",".join(self.tickers),
                        "filter_entities": "true",
                        "api_token": self.config.marketaux_api_key or ""
                    }
                )
                articles = data.get("data", [])
                for art in articles:
                    for entity in art.get("entities", []):
                        ticker = entity.get("symbol")
                        if ticker in self.tickers:
                            self.sequence_counter += 1
                            score = float(entity.get("sentiment_score", 0.0))
                            self._publish_sentiment(ticker, score, art.get("title", ""), current_dt)
            else:
                ticker = random.choice(self.tickers)
                self.sequence_counter += 1
                score = round(random.uniform(-1.0, 1.0), 4)
                self._publish_sentiment(ticker, score, f"Mock news: {ticker} outlook stable", current_dt)
        except Exception as e:
            logger.error("Sentiment error: %s", str(e))
        self.producer.poll(0)

    def _publish_sentiment(self, ticker: str, score: float, headline: str, ts: str) -> None:
        label = "NEUTRAL"
        if score > 0.1: label = "POSITIVE"
        elif score < -0.1: label = "NEGATIVE"
        msg = SentimentEventMessage(
            ticker=ticker, event_timestamp=ts, source="LIVE_FEED",
            headline=headline[:1000], sentiment_score=score, sentiment_label=label,
            confidence=0.95, replay_sequence=self.sequence_counter
        )
        self.producer.produce(topic=self.config.kafka_topic_sentiment_events, key=ticker, value=msg.model_dump_json())
        logger.info("Published Sentiment -> %s: %s", ticker, score)

    def run(self) -> None:
        """Main execution loop with managed timing."""
        logger.info("🚀 Starting Live Producer with Backfilling (1-hour sentiment interval)...")
        self.backfill_history()
        
        # News fetch interval: 60 minutes
        sentiment_interval = 3600 
        last_sentiment_fetch = 0.0
        
        try:
            while True:
                # 1. Market Prices
                self.get_market_ticks()

                # 2. News Sentiment (Hourly)
                now = time.time()
                if now - last_sentiment_fetch > sentiment_interval:
                    self.get_sentiment()
                    last_sentiment_fetch = now

                time.sleep(2)
        except KeyboardInterrupt:
            logger.info("🛑 Stopped.")
        finally:
            self.producer.flush()


if __name__ == "__main__":
    config = ProducerConfig()
    LiveProducer(config).run()
