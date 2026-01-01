"""Binance WebSocket producer."""

from typing import Dict, Any, Optional
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from ingestion.base_producer import BaseProducer
from ingestion.config import get_exchange_config, KAFKA_TOPIC_TRADES, KAFKA_TOPIC_ORDERBOOK, KAFKA_TOPIC_TICKER
from utils.logging_config import get_logger

logger = get_logger(__name__)


class BinanceProducer(BaseProducer):
    """Binance WebSocket producer for market data."""

    def __init__(self, kafka_bootstrap_servers: str, trading_pairs: list = None):
        """
        Initialize Binance producer.

        Args:
            kafka_bootstrap_servers: Kafka bootstrap servers
            trading_pairs: List of trading pairs (e.g., ['btcusdt', 'ethusdt'])
        """
        super().__init__(
            exchange_name="binance",
            kafka_bootstrap_servers=kafka_bootstrap_servers
        )

        self.config = get_exchange_config("binance")
        self.trading_pairs = trading_pairs or self.config.get('trading_pairs', [])

        logger.info(f"Binance producer initialized for pairs: {self.trading_pairs}")

    def get_websocket_url(self) -> str:
        """
        Get WebSocket URL for Binance combined streams.

        Returns:
            WebSocket URL with all subscribed streams
        """
        base_url = self.config['websocket']['base_url']

        # Build streams for all trading pairs
        streams = []
        for pair in self.trading_pairs:
            # Trade stream
            streams.append(f"{pair.lower()}@trade")
            # Depth stream (order book)
            streams.append(f"{pair.lower()}@depth20@100ms")
            # Ticker stream
            streams.append(f"{pair.lower()}@ticker")

        # Combine all streams
        combined_streams = '/'.join(streams)
        ws_url = f"{base_url}/stream?streams={combined_streams}"

        return ws_url

    def get_subscribe_message(self) -> Optional[Dict[str, Any]]:
        """
        Binance doesn't require subscription message for combined streams.

        Returns:
            None
        """
        return None

    def parse_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Parse Binance WebSocket message.

        Args:
            message: Raw WebSocket message from Binance

        Returns:
            Parsed message dictionary or None
        """
        try:
            # Binance combined stream format: {"stream": "...", "data": {...}}
            if 'stream' not in message or 'data' not in message:
                return None

            stream = message['stream']
            data = message['data']

            # Parse trade messages
            if '@trade' in stream:
                return self._parse_trade(data, stream)

            # Parse depth/orderbook messages
            elif '@depth' in stream:
                return self._parse_orderbook(data, stream)

            # Parse ticker messages
            elif '@ticker' in stream:
                return self._parse_ticker(data, stream)

            return None

        except Exception as e:
            logger.error(f"Error parsing Binance message: {e}")
            return None

    def _parse_trade(self, data: Dict[str, Any], stream: str) -> Dict[str, Any]:
        """
        Parse trade message.

        Args:
            data: Trade data
            stream: Stream name

        Returns:
            Parsed trade data
        """
        symbol = data.get('s', '').upper()  # BTCUSDT -> BTC/USDT format later

        return {
            'type': 'trade',
            'symbol': symbol,
            'price': float(data.get('p', 0)),
            'volume': float(data.get('q', 0)),
            'timestamp': data.get('T', 0),  # Trade time
            'side': 'buy' if data.get('m', False) is False else 'sell',  # m=true means buyer is maker
            'trade_id': data.get('t', 0)
        }

    def _parse_orderbook(self, data: Dict[str, Any], stream: str) -> Dict[str, Any]:
        """
        Parse order book message.

        Args:
            data: Order book data
            stream: Stream name

        Returns:
            Parsed order book data
        """
        # Extract symbol from stream name (e.g., "btcusdt@depth20@100ms")
        symbol = stream.split('@')[0].upper()

        # Convert bids and asks to proper format
        bids = [[float(price), float(qty)] for price, qty in data.get('bids', [])]
        asks = [[float(price), float(qty)] for price, qty in data.get('asks', [])]

        return {
            'type': 'orderbook',
            'symbol': symbol,
            'bids': bids[:20],  # Top 20 bids
            'asks': asks[:20],  # Top 20 asks
            'timestamp': data.get('E', 0),  # Event time
            'last_update_id': data.get('lastUpdateId', 0)
        }

    def _parse_ticker(self, data: Dict[str, Any], stream: str) -> Dict[str, Any]:
        """
        Parse ticker message.

        Args:
            data: Ticker data
            stream: Stream name

        Returns:
            Parsed ticker data
        """
        symbol = data.get('s', '').upper()

        return {
            'type': 'ticker',
            'symbol': symbol,
            'last_price': float(data.get('c', 0)),  # Close price
            'open_price': float(data.get('o', 0)),
            'high_price': float(data.get('h', 0)),
            'low_price': float(data.get('l', 0)),
            'volume': float(data.get('v', 0)),  # Base asset volume
            'quote_volume': float(data.get('q', 0)),  # Quote asset volume
            'price_change': float(data.get('p', 0)),
            'price_change_percent': float(data.get('P', 0)),
            'timestamp': data.get('E', 0),  # Event time
            'num_trades': data.get('n', 0)
        }

    def get_kafka_topic(self, message_type: str) -> str:
        """
        Get Kafka topic based on message type.

        Args:
            message_type: Type of message (trade, orderbook, ticker)

        Returns:
            Kafka topic name
        """
        topic_map = {
            'trade': KAFKA_TOPIC_TRADES,
            'orderbook': KAFKA_TOPIC_ORDERBOOK,
            'ticker': KAFKA_TOPIC_TICKER
        }

        return topic_map.get(message_type, KAFKA_TOPIC_TRADES)


def main():
    """Main entry point for Binance producer."""
    import os
    from dotenv import load_dotenv

    load_dotenv()

    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    trading_pairs = ['btcusdt', 'ethusdt', 'bnbusdt', 'solusdt', 'xrpusdt']

    producer = BinanceProducer(
        kafka_bootstrap_servers=kafka_servers,
        trading_pairs=trading_pairs
    )

    try:
        logger.info("Starting Binance producer...")
        producer.start()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    finally:
        producer.stop()


if __name__ == "__main__":
    from utils.logging_config import setup_logging
    import os

    log_level = os.getenv("LOG_LEVEL", "INFO")
    setup_logging(log_level=log_level)

    main()
