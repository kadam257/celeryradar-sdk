"""Broker introspection — resolves the broker URL, opens a Redis connection,
and samples queue depths via LLEN. Redis-only for v1 (RabbitMQ deferred).
"""
import logging
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

_warned_unsupported = False


def resolve_broker_url(app, override=None):
    """Return a redis broker URL or None if unsupported. Logs once if unsupported."""
    url = override or getattr(app.conf, 'broker_url', None)
    if not url:
        return None
    scheme = urlparse(url).scheme
    if scheme in ('redis', 'rediss'):
        return url
    global _warned_unsupported
    if not _warned_unsupported:
        logger.warning(
            'celeryradar: queue depth monitoring supports redis brokers only (got %r); skipping',
            scheme,
        )
        _warned_unsupported = True
    return None


def connect_redis(broker_url):
    """Lazy-import redis so the SDK doesn't hard-require it for non-Redis users."""
    import redis
    return redis.Redis.from_url(broker_url)


def discover_queue_names(app):
    """Queue names declared in this Celery app."""
    try:
        return list(app.amqp.queues.keys())
    except Exception:
        return []


def sample_depths(redis_conn, queue_names):
    """Pipeline LLEN for each queue. Returns {queue_name: depth}.
    Queues that error individually are skipped, not raised."""
    if not queue_names:
        return {}
    pipe = redis_conn.pipeline()
    for name in queue_names:
        pipe.llen(name)
    try:
        results = pipe.execute()
    except Exception as e:
        logger.debug('celeryradar: pipelined LLEN failed: %s', e)
        return {}
    return {name: int(depth) for name, depth in zip(queue_names, results)}
