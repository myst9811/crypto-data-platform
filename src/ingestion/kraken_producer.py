"""Kraken WebSocket producer."""

from typing import Dict, Any, Optional, List
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from ingestion.base_producer import BaseProducer
from ingestion.config import get_exchange_config, KAFKA_TOPIC_TRADES, KAFKA_TOPIC_ORDERBOOK, KAFKA_TOPIC_TICKER
from utils.logging_config import get_logger

logger = get_logger(__name__)


class KrakenProducer(BaseProducer):
    """Kraken WebSocket producer for market data."""

    def __init__(self, kafka_bootstrap_servers: str, trading_pairs: list = None):
        """
        Initialize Kraken producer.

        Args:
            kafka_bootstrap_servers: Kafka bootstrap servers
            trading_pairs: List of trading pairs (e.g., ['XBT/USD', 'ETH/USD'])
        """
        super().__init__(
            exchange_name="kraken",
            kafka_bootstrap_servers=kafka_bootstrap_servers
        )

        self.config = get_exchange_config("kraken")
        self.trading_pairs = trading_pairs or self.config.get('trading_pairs', [])
        self.channels = self.config['websocket']['channels']

        # Map to store channel IDs to symbols
        self.channel_map = {}

        logger.info(f"Kraken producer initialized for pairs: {self.trading_pairs}")

    def get_websocket_url(self) -> str:
        """
        Get WebSocket URL for Kraken.

        Returns:
            WebSocket URL
        """
        return self.config['websocket']['base_url']

    def get_subscribe_message(self) -> Optional[Dict[str, Any]]:
        """
        Get Kraken subscription message.

        Returns:
            Subscription message dictionary
        """
        return {
            "event": "subscribe",
            "pair": self.trading_pairs,
            "subscription": {
                "name": "trade"
            }
        }

    def parse_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Parse Kraken WebSocket message.

        Args:
            message: Raw WebSocket message from Kraken

        Returns:
            Parsed message dictionary or None
        """
        try:
            # Kraken sends both dict and list messages
            if isinstance(message, dict):
                # Handle subscription status messages
                event = message.get('event', '')

                if event == 'subscriptionStatus':
                    status = message.get('status', '')
                    if status == 'subscribed':
                        # Store channel mapping
                        channel_name = message.get('subscription', {}).get('name', '')
                        pair = message.get('pair', '')
                        channel_id = message.get('channelID', 0)
                        self.channel_map[channel_id] = (pair, channel_name)
                        logger.info(f"Kraken subscribed to {channel_name} for {pair}")
                    return None

                elif event in ['heartbeat', 'systemStatus']:
                    return None

            elif isinstance(message, list):
                # Data messages are arrays: [channelID, data, channelName, pair]
                if len(message) < 4:
                    return None

                channel_id = message[0]
                data = message[1]
                channel_name = message[2]
                pair = message[3]

                # Parse based on channel type
                if channel_name == 'trade':
                    return self._parse_trade(data, pair)

                elif channel_name in ['book-10', 'book-25', 'book-100', 'book']:
                    return self._parse_orderbook(data, pair)

                elif channel_name == 'ticker':
                    return self._parse_ticker(data, pair)

                elif channel_name == 'spread':
                    return self._parse_spread(data, pair)

            return None

        except Exception as e:
            logger.error(f"Error parsing Kraken message: {e}")
            return None

    def _parse_trade(self, data: List, pair: str) -> Optional[Dict[str, Any]]:
        """
        Parse trade message.

        Args:
            data: Trade data array
            pair: Trading pair

        Returns:
            Parsed trade data
        """
        # Kraken sends multiple trades in one message
        # Each trade: [price, volume, time, side, orderType, misc]
        if not data or not isinstance(data, list):
            return None

        # Take the first trade from the array
        trade = data[0] if isinstance(data[0], list) else None
        if not trade:
            return None

        return {
            'type': 'trade',
            'symbol': pair,
            'price': float(trade[0]),
            'volume': float(trade[1]),
            'timestamp': float(trade[2]),
            'side': trade[3],  # 'b' for buy, 's' for sell
            'order_type': trade[4] if len(trade) > 4 else '',
            'num_trades': len(data)
        }

    def _parse_orderbook(self, data: Dict[str, Any], pair: str) -> Optional[Dict[str, Any]]:
        """
        Parse order book message.

        Args:
            data: Order book data
            pair: Trading pair

        Returns:
            Parsed order book data
        """
        # Kraken orderbook format: {"as": [[price, volume, timestamp], ...], "bs": [[price, volume, timestamp], ...]}
        # or updates: {"a": [...], "b": [...]}

        bids = []
        asks = []

        # Full snapshot
        if 'bs' in data:
            bids = [[float(price), float(volume)] for price, volume, _ in data.get('bs', [])]
        if 'as' in data:
            asks = [[float(price), float(volume)] for price, volume, _ in data.get('as', [])]

        # Updates
        if 'b' in data:
            bids = [[float(price), float(volume)] for price, volume, _ in data.get('b', [])]
        if 'a' in data:
            asks = [[float(price), float(volume)] for price, volume, _ in data.get('a', [])]

        if not bids and not asks:
            return None

        return {
            'type': 'orderbook',
            'symbol': pair,
            'bids': bids[:20],
            'asks': asks[:20],
            'timestamp': None,  # Kraken doesn't provide timestamp in orderbook updates
            'snapshot': 'bs' in data or 'as' in data
        }

    def _parse_ticker(self, data: Dict[str, Any], pair: str) -> Dict[str, Any]:
        """
        Parse ticker message.

        Args:
            data: Ticker data
            pair: Trading pair

        Returns:
            Parsed ticker data
        """
        # Kraken ticker format: {"a": [price, wholeLotVolume, lotVolume], "b": [...], "c": [price, lotVolume], ...}
        return {
            'type': 'ticker',
            'symbol': pair,
            'last_price': float(data.get('c', [0])[0]),  # Last trade closed
            'open_price': float(data.get('o', [0])[0]),  # Today's opening price
            'high_price': float(data.get('h', [0])[0]),  # Today's high
            'low_price': float(data.get('l', [0])[0]),  # Today's low
            'volume': float(data.get('v', [0])[0]),  # Volume today
            'vwap': float(data.get('p', [0])[0]),  # Volume weighted average price today
            'num_trades': int(data.get('t', [0])[0]),  # Number of trades today
            'best_bid': float(data.get('b', [0])[0]),
            'best_ask': float(data.get('a', [0])[0]),
            'timestamp': None  # Kraken doesn't provide timestamp in ticker
        }

    def _parse_spread(self, data: List, pair: str) -> Dict[str, Any]:
        """
        Parse spread message.

        Args:
            data: Spread data
            pair: Trading pair

        Returns:
            Parsed spread data
        """
        # Spread format: [bid, ask, timestamp, bidVolume, askVolume]
        return {
            'type': 'spread',
            'symbol': pair,
            'bid': float(data[0]),
            'ask': float(data[1]),
            'timestamp': float(data[2]),
            'bid_volume': float(data[3]),
            'ask_volume': float(data[4])
        }

    def get_kafka_topic(self, message_type: str) -> str:
        """
        Get Kafka topic based on message type.

        Args:
            message_type: Type of message (trade, orderbook, ticker, spread)

        Returns:
            Kafka topic name
        """
        topic_map = {
            'trade': KAFKA_TOPIC_TRADES,
            'orderbook': KAFKA_TOPIC_ORDERBOOK,
            'ticker': KAFKA_TOPIC_TICKER,
            'spread': KAFKA_TOPIC_TICKER  # Spread goes to ticker topic
        }

        return topic_map.get(message_type, KAFKA_TOPIC_TRADES)


def main():
    """Main entry point for Kraken producer."""
    import os
    from dotenv import load_dotenv

    load_dotenv()

    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    trading_pairs = ['XBT/USD', 'ETH/USD', 'SOL/USD', 'XRP/USD']

    producer = KrakenProducer(
        kafka_bootstrap_servers=kafka_servers,
        trading_pairs=trading_pairs
    )

    try:
        logger.info("Starting Kraken producer...")
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
