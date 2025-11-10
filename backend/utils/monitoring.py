"""
Monitoring and metrics collection utilities.
"""
import time
import os
from typing import Dict, Any, Optional
import logging
from datetime import datetime, timedelta
import prometheus_client as prom
from prometheus_client import Counter, Histogram, Gauge
from functools import wraps
from contextlib import contextmanager

def setup_logging(log_level: str = "INFO", log_file: str = None) -> None:
    """
    Set up logging configuration for the application.
    
    Args:
        log_level: The logging level to use (default: INFO)
        log_file: Optional path to a log file. If provided, logs will be written to this file
                 in addition to console output.
    """
    # Convert string log level to logging constant
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    
    # Basic configuration
    config = {
        'level': numeric_level,
        'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        'datefmt': '%Y-%m-%d %H:%M:%S'
    }
    
    # If log file is specified, ensure directory exists and add file handler
    if log_file:
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
        config['filename'] = log_file
        config['filemode'] = 'a'  # Append mode
    
    # Apply configuration
    logging.basicConfig(**config)

# Configure logging
logger = logging.getLogger(__name__)

# Define metrics
REQUESTS_TOTAL = Counter(
    'email_generator_requests_total',
    'Total number of email generation requests',
    ['status', 'template_type']
)

GENERATION_TIME = Histogram(
    'email_generation_duration_seconds',
    'Time spent generating emails',
    ['template_type']
)

CACHE_HITS = Counter(
    'cache_hits_total',
    'Total number of cache hits',
    ['cache_type']
)

CACHE_MISSES = Counter(
    'cache_misses_total',
    'Total number of cache misses',
    ['cache_type']
)

ACTIVE_GENERATIONS = Gauge(
    'email_generations_active',
    'Number of email generations in progress'
)

TEMPLATE_REFRESH_TIME = Histogram(
    'template_refresh_duration_seconds',
    'Time spent refreshing templates'
)

DRIVE_API_CALLS = Counter(
    'drive_api_calls_total',
    'Total number of Google Drive API calls',
    ['operation']
)

ERROR_COUNT = Counter(
    'errors_total',
    'Total number of errors',
    ['error_type']
)

class MetricsCollector:
    """Collector for application metrics."""
    
    @staticmethod
    def track_request(template_type: str, status: str = "success"):
        """Track an email generation request."""
        REQUESTS_TOTAL.labels(status=status, template_type=template_type).inc()

    @staticmethod
    def track_cache(cache_type: str, hit: bool):
        """Track a cache hit or miss."""
        if hit:
            CACHE_HITS.labels(cache_type=cache_type).inc()
        else:
            CACHE_MISSES.labels(cache_type=cache_type).inc()

    @staticmethod
    def track_error(error_type: str):
        """Track an error occurrence."""
        ERROR_COUNT.labels(error_type=error_type).inc()

    @staticmethod
    def track_drive_call(operation: str):
        """Track a Google Drive API call."""
        DRIVE_API_CALLS.labels(operation=operation).inc()

    @staticmethod
    @contextmanager
    def generation_timer(template_type: str):
        """Time an email generation operation."""
        start_time = time.time()
        ACTIVE_GENERATIONS.inc()
        try:
            yield
        finally:
            GENERATION_TIME.labels(template_type=template_type).observe(
                time.time() - start_time
            )
            ACTIVE_GENERATIONS.dec()

    @staticmethod
    @contextmanager
    def template_refresh_timer():
        """Time a template refresh operation."""
        start_time = time.time()
        try:
            yield
        finally:
            TEMPLATE_REFRESH_TIME.observe(time.time() - start_time)

def track_performance(name: str):
    """
    Decorator to track function performance.
    
    Args:
        name: Name of the operation to track
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                duration = time.time() - start_time
                logger.info(
                    f"Operation {name} completed in {duration:.2f}s",
                    extra={
                        "operation": name,
                        "duration": duration,
                        "status": "success"
                    }
                )
                return result
            except Exception as e:
                duration = time.time() - start_time
                logger.error(
                    f"Operation {name} failed after {duration:.2f}s: {str(e)}",
                    extra={
                        "operation": name,
                        "duration": duration,
                        "status": "error",
                        "error": str(e)
                    },
                    exc_info=True
                )
                raise
        return wrapper
    return decorator

class PerformanceMonitor:
    """Monitor for tracking application performance metrics."""
    
    def __init__(self):
        self.start_time = datetime.now()
        self._metrics: Dict[str, Dict[str, Any]] = {
            "requests": {
                "total": 0,
                "success": 0,
                "error": 0
            },
            "cache": {
                "hits": 0,
                "misses": 0
            },
            "api_calls": {
                "drive": 0,
                "openai": 0
            },
            "errors": {}
        }

    def get_uptime(self) -> timedelta:
        """Get application uptime."""
        return datetime.now() - self.start_time

    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics."""
        return {
            "uptime": str(self.get_uptime()),
            "metrics": self._metrics
        }

    def record_request(self, success: bool = True):
        """Record an API request."""
        self._metrics["requests"]["total"] += 1
        if success:
            self._metrics["requests"]["success"] += 1
        else:
            self._metrics["requests"]["error"] += 1

    def record_cache_event(self, hit: bool):
        """Record a cache hit or miss."""
        if hit:
            self._metrics["cache"]["hits"] += 1
        else:
            self._metrics["cache"]["misses"] += 1

    def record_api_call(self, api: str):
        """Record an external API call."""
        self._metrics["api_calls"][api] = self._metrics["api_calls"].get(api, 0) + 1

    def record_error(self, error_type: str):
        """Record an error occurrence."""
        self._metrics["errors"][error_type] = self._metrics["errors"].get(error_type, 0) + 1

# Create global instances
metrics_collector = MetricsCollector()
performance_monitor = PerformanceMonitor()