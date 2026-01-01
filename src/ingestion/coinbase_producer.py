"""Coinbase Pro WebSocket producer."""

from typing import Dict, Any, Optional
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from ingestion.base_producer import BaseProducer
from ingestion.config import get_exchange_config, KAFKA_TOPIC_TRADES, KAFKA_TOPIC_ORDERBOOK, KAFKA_TOPIC_TICKER
from utils.logging_config import get_logger

logger = get_logger(__name__)


class CoinbaseProducer(BaseProducer):
    """Coinbase Pro WebSocket producer for market data."""

    def __init__(self, kafka_bootstrap_servers: str, trading_pairs: list = None):
        """
        Initialize Coinbase producer.

        Args:
            kafka_bootstrap_servers: Kafka bootstrap servers
            trading_pairs: List of trading pairs (e.g., ['BTC-USD', 'ETH-USD'])
        """
        super().__init__(
            exchange_name="coinbase",
            kafka_bootstrap_servers=kafka_bootstrap_servers
        )

        self.config = get_exchange_config("coinbase")
        self.trading_pairs = trading_pairs or self.config.get('trading_pairs', [])
        self.channels = self.config['websocket']['channels']

        logger.info(f"Coinbase producer initialized for pairs: {self.trading_pairs}")

    def get_websocket_url(self) -> str:
        """
        Get WebSocket URL for Coinbase Pro.

        Returns:
            WebSocket URL
        """
        return self.config['websocket']['base_url']

    def get_subscribe_message(self) -> Optional[Dict[str, Any]]:
        """
        Get Coinbase Pro subscription message.

        Returns:
            Subscription message dictionary
        """
        return {
            "type": "subscribe",
            "product_ids": self.trading_pairs,
            "channels": self.channels
        }

    def parse_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Parse Coinbase Pro WebSocket message.

        Args:
            message: Raw WebSocket message from Coinbase

        Returns:
            Parsed message dictionary or None
        """
        try:
            msg_type = message.get('type', '')

            # Ignore subscription confirmation messages
            if msg_type in ['subscriptions', 'heartbeat']:
                return None

            # Parse match messages (trades)
            if msg_type in ['match', 'last_match']:
                return self._parse_trade(message)

            # Parse level2 snapshot/update (orderbook)
            elif msg_type in ['snapshot', 'l2update']:
                return self._parse_orderbook(message)

            # Parse ticker messages
            elif msg_type == 'ticker':
                return self._parse_ticker(message)

            return None

        except Exception as e:
            logger.error(f"Error parsing Coinbase message: {e}")
            return None

    def _parse_trade(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse trade message.

        Args:
            data: Trade data

        Returns:
            Parsed trade data
        """
        return {
            'type': 'trade',
            'symbol': data.get('product_id', ''),
            'price': float(data.get('price', 0)),
            'volume': float(data.get('size', 0)),
            'timestamp': data.get('time', ''),
            'side': data.get('side', ''),  # buy or sell
            'trade_id': data.get('trade_id', 0)
        }

    def _parse_orderbook(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse order book message.

        Args:
            data: Order book data

        Returns:
            Parsed order book data
        """
        msg_type = data.get('type', '')

        if msg_type == 'snapshot':
            # Full orderbook snapshot
            bids = [[float(price), float(size)] for price, size in data.get('bids', [])]
            asks = [[float(price), float(size)] for price, size in data.get('asks', [])]

            return {
                'type': 'orderbook',
                'symbol': data.get('product_id', ''),
                'bids': bids[:20],
                'asks': asks[:20],
                'timestamp': data.get('time', ''),
                'snapshot': True
            }

        elif msg_type == 'l2update':
            # Orderbook update
            changes = data.get('changes', [])

            # Separate bids and asks
            bids = [[float(price), float(size)] for side, price, size in changes if side == 'buy']
            asks = [[float(price), float(size)] for side, price, size in changes if side == 'sell']

            return {
                'type': 'orderbook',
                'symbol': data.get('product_id', ''),
                'bids': bids,
                'asks': asks,
                'timestamp': data.get('time', ''),
                'snapshot': False
            }

        return None

    def _parse_ticker(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse ticker message.

        Args:
            data: Ticker data

        Returns:
            Parsed ticker data
        """
        return {
            'type': 'ticker',
            'symbol': data.get('product_id', ''),
            'last_price': float(data.get('price', 0)),
            'open_price': float(data.get('open_24h', 0)),
            'high_price': float(data.get('high_24h', 0)),
            'low_price': float(data.get('low_24h', 0)),
            'volume': float(data.get('volume_24h', 0)),
            'best_bid': float(data.get('best_bid', 0)),
            'best_ask': float(data.get('best_ask', 0)),
            'timestamp': data.get('time', ''),
            'sequence': data.get('sequence', 0)
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
    """Main entry point for Coinbase producer."""
    import os
    from dotenv import load_dotenv

    load_dotenv()

    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    trading_pairs = ['BTC-USD', 'ETH-USD', 'SOL-USD', 'XRP-USD']

    producer = CoinbaseProducer(
        kafka_bootstrap_servers=kafka_servers,
        trading_pairs=trading_pairs
    )

    try:
        logger.info("Starting Coinbase producer...")
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
