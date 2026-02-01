"""TTL-based caching layer for serving module."""

import threading
from typing import Any, Optional, Callable, TypeVar
from functools import wraps
import hashlib
import json
from cachetools import TTLCache
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")


class DataCache:
    """Thread-safe TTL cache for data access layer."""

    def __init__(self, ttl: int = 10, max_size: int = 1000):
        """
        Initialize cache.

        Args:
            ttl: Time-to-live in seconds (default 10s for near-real-time)
            max_size: Maximum number of entries
        """
        self._cache: TTLCache = TTLCache(maxsize=max_size, ttl=ttl)
        self._lock = threading.RLock()
        self._ttl = ttl
        logger.info(f"DataCache initialized with TTL={ttl}s, max_size={max_size}")

    def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache.

        Args:
            key: Cache key

        Returns:
            Cached value or None if not found/expired
        """
        with self._lock:
            value = self._cache.get(key)
            if value is not None:
                logger.debug(f"Cache hit for key: {key}")
            return value

    def set(self, key: str, value: Any) -> None:
        """
        Set value in cache.

        Args:
            key: Cache key
            value: Value to cache
        """
        with self._lock:
            self._cache[key] = value
            logger.debug(f"Cached value for key: {key}")

    def delete(self, key: str) -> bool:
        """
        Delete a specific key from cache.

        Args:
            key: Cache key to delete

        Returns:
            True if key was deleted, False if not found
        """
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                logger.debug(f"Deleted cache key: {key}")
                return True
            return False

    def clear(self) -> None:
        """Clear all cached entries."""
        with self._lock:
            self._cache.clear()
            logger.info("Cache cleared")

    def size(self) -> int:
        """Get current cache size."""
        with self._lock:
            return len(self._cache)

    @staticmethod
    def make_key(*args, **kwargs) -> str:
        """
        Generate a cache key from arguments.

        Args:
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            Hash-based cache key
        """
        key_data = json.dumps({"args": args, "kwargs": kwargs}, sort_keys=True, default=str)
        return hashlib.md5(key_data.encode()).hexdigest()


def cached(cache: DataCache, key_prefix: str = ""):
    """
    Decorator for caching function results.

    Args:
        cache: DataCache instance
        key_prefix: Prefix for cache keys

    Returns:
        Decorated function with caching
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            # Generate cache key
            cache_key = f"{key_prefix}:{DataCache.make_key(*args[1:], **kwargs)}"

            # Try to get from cache
            cached_value = cache.get(cache_key)
            if cached_value is not None:
                return cached_value

            # Call function and cache result
            result = func(*args, **kwargs)
            cache.set(cache_key, result)
            return result

        return wrapper

    return decorator


# Global cache instance
_global_cache: Optional[DataCache] = None


def get_cache(ttl: int = 10, max_size: int = 1000) -> DataCache:
    """
    Get or create global cache instance.

    Args:
        ttl: Time-to-live in seconds
        max_size: Maximum cache size

    Returns:
        DataCache instance
    """
    global _global_cache
    if _global_cache is None:
        _global_cache = DataCache(ttl=ttl, max_size=max_size)
    return _global_cache
