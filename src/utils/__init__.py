"""Utility modules for the crypto data platform."""

from .logging_config import setup_logging, get_logger
from .kafka_utils import KafkaProducerWrapper, KafkaConsumerWrapper

# Conditional import for Delta Lake (requires PySpark)
try:
    from .delta_utils import DeltaLakeManager
    DELTA_AVAILABLE = True
except ImportError:
    DeltaLakeManager = None
    DELTA_AVAILABLE = False

__all__ = [
    'setup_logging',
    'get_logger',
    'KafkaProducerWrapper',
    'KafkaConsumerWrapper',
    'DeltaLakeManager',
    'DELTA_AVAILABLE',
]
