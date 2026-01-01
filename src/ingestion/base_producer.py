"""Base WebSocket producer for crypto exchanges."""

import json
import time
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import websocket
from datetime import datetime

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from utils.kafka_utils import KafkaProducerWrapper
from utils.logging_config import get_logger

logger = get_logger(__name__)


class BaseProducer(ABC):
    """Abstract base class for exchange WebSocket producers."""

    def __init__(
        self,
        exchange_name: str,
        kafka_bootstrap_servers: str,
        max_retries: int = 5,
        backoff_multiplier: int = 2,
        initial_delay: int = 1
    ):
        """
        Initialize base producer.

        Args:
            exchange_name: Name of the exchange
            kafka_bootstrap_servers: Kafka bootstrap servers
            max_retries: Maximum number of reconnection retries
            backoff_multiplier: Backoff multiplier for exponential backoff
            initial_delay: Initial delay in seconds before reconnection
        """
        self.exchange_name = exchange_name
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.max_retries = max_retries
        self.backoff_multiplier = backoff_multiplier
        self.initial_delay = initial_delay

        self.ws: Optional[websocket.WebSocketApp] = None
        self.kafka_producer = KafkaProducerWrapper(
            bootstrap_servers=kafka_bootstrap_servers
        )

        self.retry_count = 0
        self.is_running = False

        logger.info(f"{self.exchange_name} producer initialized")

    @abstractmethod
    def get_websocket_url(self) -> str:
        """
        Get WebSocket URL for the exchange.

        Returns:
            WebSocket URL
        """
        pass

    @abstractmethod
    def get_subscribe_message(self) -> Optional[Dict[str, Any]]:
        """
        Get subscription message to send after connection.

        Returns:
            Subscription message dictionary or None
        """
        pass

    @abstractmethod
    def parse_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Parse incoming WebSocket message.

        Args:
            message: Raw WebSocket message

        Returns:
            Parsed message dictionary or None if message should be ignored
        """
        pass

    @abstractmethod
    def get_kafka_topic(self, message_type: str) -> str:
        """
        Get Kafka topic based on message type.

        Args:
            message_type: Type of message (trade, orderbook, ticker)

        Returns:
            Kafka topic name
        """
        pass

    def on_open(self, ws):
        """Callback when WebSocket connection is opened."""
        logger.info(f"{self.exchange_name} WebSocket connected")
        self.retry_count = 0

        # Send subscription message if needed
        subscribe_msg = self.get_subscribe_message()
        if subscribe_msg:
            ws.send(json.dumps(subscribe_msg))
            logger.info(f"{self.exchange_name} subscription sent: {subscribe_msg}")

    def on_message(self, ws, message):
        """
        Callback when WebSocket message is received.

        Args:
            ws: WebSocket instance
            message: Raw message string
        """
        try:
            # Parse JSON message
            raw_data = json.loads(message)

            # Exchange-specific parsing
            parsed_data = self.parse_message(raw_data)

            if parsed_data:
                # Add metadata
                enriched_data = {
                    **parsed_data,
                    'exchange': self.exchange_name,
                    'ingestion_timestamp': datetime.utcnow().isoformat(),
                    'raw_message': raw_data
                }

                # Determine Kafka topic
                message_type = parsed_data.get('type', 'unknown')
                topic = self.get_kafka_topic(message_type)

                # Send to Kafka
                self.kafka_producer.send(
                    topic=topic,
                    value=enriched_data,
                    key=parsed_data.get('symbol', None)
                )

                logger.debug(
                    f"{self.exchange_name} sent message to {topic}: "
                    f"type={message_type}, symbol={parsed_data.get('symbol')}"
                )

        except json.JSONDecodeError as e:
            logger.error(f"{self.exchange_name} failed to decode JSON: {e}")
        except Exception as e:
            logger.error(f"{self.exchange_name} error processing message: {e}")

    def on_error(self, ws, error):
        """
        Callback when WebSocket error occurs.

        Args:
            ws: WebSocket instance
            error: Error object
        """
        logger.error(f"{self.exchange_name} WebSocket error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        """
        Callback when WebSocket connection is closed.

        Args:
            ws: WebSocket instance
            close_status_code: Close status code
            close_msg: Close message
        """
        logger.warning(
            f"{self.exchange_name} WebSocket closed: "
            f"code={close_status_code}, msg={close_msg}"
        )

        # Attempt reconnection with exponential backoff
        if self.is_running and self.retry_count < self.max_retries:
            delay = self.initial_delay * (self.backoff_multiplier ** self.retry_count)
            self.retry_count += 1

            logger.info(
                f"{self.exchange_name} reconnecting in {delay}s "
                f"(attempt {self.retry_count}/{self.max_retries})"
            )

            time.sleep(delay)
            self.start()
        elif self.retry_count >= self.max_retries:
            logger.error(
                f"{self.exchange_name} max retries ({self.max_retries}) reached. "
                "Stopping producer."
            )
            self.is_running = False

    def start(self):
        """Start the WebSocket connection."""
        try:
            self.is_running = True
            ws_url = self.get_websocket_url()

            logger.info(f"{self.exchange_name} connecting to {ws_url}")

            self.ws = websocket.WebSocketApp(
                ws_url,
                on_open=self.on_open,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close
            )

            self.ws.run_forever()

        except Exception as e:
            logger.error(f"{self.exchange_name} failed to start: {e}")
            raise

    def stop(self):
        """Stop the WebSocket connection."""
        logger.info(f"{self.exchange_name} stopping producer")
        self.is_running = False

        if self.ws:
            self.ws.close()

        self.kafka_producer.flush()
        self.kafka_producer.close()

        logger.info(f"{self.exchange_name} producer stopped")
