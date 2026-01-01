"""Kafka utility functions and wrappers."""

import json
from typing import Any, Dict, Optional, Callable
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import logging

logger = logging.getLogger(__name__)


class KafkaProducerWrapper:
    """Wrapper for Kafka producer with built-in error handling and serialization."""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        compression_type: str = "snappy",
        **kwargs
    ):
        """
        Initialize Kafka producer.

        Args:
            bootstrap_servers: Kafka bootstrap servers
            compression_type: Compression type (gzip, snappy, lz4, zstd)
            **kwargs: Additional KafkaProducer arguments
        """
        self.bootstrap_servers = bootstrap_servers

        producer_config = {
            'bootstrap_servers': bootstrap_servers.split(','),
            'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
            'key_serializer': lambda k: k.encode('utf-8') if k else None,
            'compression_type': compression_type,
            'acks': 1,
            'retries': 3,
            'max_in_flight_requests_per_connection': 5,
            **kwargs
        }

        self.producer = KafkaProducer(**producer_config)
        logger.info(f"Kafka producer initialized with servers: {bootstrap_servers}")

    def send(
        self,
        topic: str,
        value: Dict[str, Any],
        key: Optional[str] = None,
        callback: Optional[Callable] = None
    ) -> None:
        """
        Send message to Kafka topic.

        Args:
            topic: Kafka topic name
            value: Message value (will be JSON serialized)
            key: Optional message key
            callback: Optional callback for delivery confirmation
        """
        try:
            future = self.producer.send(topic, value=value, key=key)

            if callback:
                future.add_callback(callback)
            else:
                future.add_callback(self._on_send_success)

            future.add_errback(self._on_send_error)

        except Exception as e:
            logger.error(f"Failed to send message to topic {topic}: {e}")
            raise

    def _on_send_success(self, record_metadata):
        """Callback for successful message delivery."""
        logger.debug(
            f"Message delivered to {record_metadata.topic} "
            f"partition {record_metadata.partition} "
            f"offset {record_metadata.offset}"
        )

    def _on_send_error(self, exc):
        """Callback for failed message delivery."""
        logger.error(f"Failed to deliver message: {exc}")

    def flush(self):
        """Flush pending messages."""
        self.producer.flush()
        logger.debug("Kafka producer flushed")

    def close(self):
        """Close the producer connection."""
        self.producer.close()
        logger.info("Kafka producer closed")


class KafkaConsumerWrapper:
    """Wrapper for Kafka consumer with built-in error handling and deserialization."""

    def __init__(
        self,
        topics: list,
        group_id: str,
        bootstrap_servers: str = "localhost:9092",
        auto_offset_reset: str = "latest",
        enable_auto_commit: bool = True,
        **kwargs
    ):
        """
        Initialize Kafka consumer.

        Args:
            topics: List of topics to subscribe to
            group_id: Consumer group ID
            bootstrap_servers: Kafka bootstrap servers
            auto_offset_reset: Where to start reading (earliest or latest)
            enable_auto_commit: Whether to auto-commit offsets
            **kwargs: Additional KafkaConsumer arguments
        """
        consumer_config = {
            'bootstrap_servers': bootstrap_servers.split(','),
            'group_id': group_id,
            'auto_offset_reset': auto_offset_reset,
            'enable_auto_commit': enable_auto_commit,
            'value_deserializer': lambda m: json.loads(m.decode('utf-8')),
            'key_deserializer': lambda k: k.decode('utf-8') if k else None,
            **kwargs
        }

        self.consumer = KafkaConsumer(*topics, **consumer_config)
        logger.info(
            f"Kafka consumer initialized for topics: {topics}, "
            f"group: {group_id}, servers: {bootstrap_servers}"
        )

    def consume(self, timeout_ms: int = 1000):
        """
        Consume messages from subscribed topics.

        Args:
            timeout_ms: Maximum time to wait for messages

        Yields:
            Consumed messages
        """
        try:
            for message in self.consumer:
                yield {
                    'topic': message.topic,
                    'partition': message.partition,
                    'offset': message.offset,
                    'key': message.key,
                    'value': message.value,
                    'timestamp': message.timestamp
                }
        except Exception as e:
            logger.error(f"Error consuming messages: {e}")
            raise

    def close(self):
        """Close the consumer connection."""
        self.consumer.close()
        logger.info("Kafka consumer closed")
