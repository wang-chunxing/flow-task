"""Prometheus metrics configuration."""
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram


# 创建一个新的注册表
REGISTRY = CollectorRegistry(auto_describe=True)

# HTTP metrics
REQUEST_COUNT = Counter(
    'http_request_count',
    'Application Request Count',
    ['method', 'endpoint', 'http_status'],
    registry=REGISTRY
)

REQUEST_LATENCY = Histogram(
    'http_request_latency_seconds',
    'Application Request Latency',
    ['method', 'endpoint'],
    registry=REGISTRY
)

# Database metrics
DB_SESSIONS = Counter(
    'db_session_total',
    'Total number of database sessions created',
    ['session_type'],
    registry=REGISTRY
)

DB_SESSION_DURATION = Histogram(
    'db_session_duration_seconds',
    'Duration of database sessions',
    ['session_type', 'operation'],
    registry=REGISTRY
)

DB_ACTIVE_SESSIONS = Gauge(
    'db_active_sessions',
    'Number of active database sessions',
    ['session_type'],
    registry=REGISTRY
)

DB_ERRORS = Counter(
    'db_errors_total',
    'Total number of database errors',
    ['operation', 'error_type'],
    registry=REGISTRY
)
