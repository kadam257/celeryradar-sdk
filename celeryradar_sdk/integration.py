import json
import logging
import os
import time
import socket

from celery.signals import (
    beat_init, heartbeat_sent, task_failure, task_postrun, task_prerun,
    task_retry, worker_process_init,
)

from . import beat, queues as queues_mod
from .client import Client
from .poller import Poller

logger = logging.getLogger(__name__)

_client = None
_task_state = {}  # task_id -> {'start': monotonic_float, 'args': list, 'kwargs': dict}
_last_heartbeat_sent = 0.0
HEARTBEAT_INTERVAL = 30.0

# Drop oversized arg/kwarg payloads with a marker rather than ballooning events.
ARGS_PAYLOAD_MAX_BYTES = 4096

_capture_args = True
_capture_exceptions = True
_poller = None
_poller_pid = None  # owning pid — fork-safety: child rebuilds Poller + Redis conn
_poller_config = None
_poller_start_warned = False
_worker_name_override = None
_app_name = None


def _worker_name():
    """Worker identity for ingest. Resolution: CELERYRADAR_WORKER_NAME env var >
    connect(worker_name=...) > socket.gethostname(). The env var wins so
    containerized deployments can set a stable name without code changes —
    socket.gethostname() in a k8s pod returns the ephemeral pod name and
    accumulates ghost workers in the dashboard on every restart."""
    return os.environ.get('CELERYRADAR_WORKER_NAME') or _worker_name_override or socket.gethostname()


def connect(api_key, app_name, endpoint='https://api.celeryradar.com', broker_url=None,
            capture_args=True, capture_exceptions=True, worker_name=None):
    """Wire CeleryRadar's signal handlers and start the queue depth poller.

    app_name identifies this Celery application within your account. Required.
    If you run multiple Celery apps under one API key — even on separate Redis
    brokers — give each a distinct app_name. Used to scope the broker-side
    leader-election lock and to disambiguate queue-name collisions in the
    dashboard (two apps that both have a queue called `celery` are kept
    separate by app_name).

    Idempotent — a second call is a no-op with a warning. Reconnecting with a
    different api_key or endpoint isn't supported; restart the process instead.

    Queue depth monitoring supports standard Redis-list brokers via redis:// or
    rediss:// URLs only. Redis Sentinel, Redis Cluster, and Redis Streams
    transports are not yet supported and will silently produce missing or
    incorrect depth samples — task events, heartbeats, and beat monitoring
    are unaffected. Pass broker_url= to override app.conf.broker_url (e.g. for
    a read-replica with scoped credentials).

    Pass capture_args=False to disable sending task args/kwargs with events
    (default True). Useful when args may contain PII or secrets — server still
    receives task name, state, runtime, and exception info.

    Pass capture_exceptions=False to disable sending exception text and
    tracebacks on failure/retry events (default True). Useful when exception
    messages may include user input (e.g. "ValueError: invalid email
    jane@acme.com"), DB errors with interpolated SQL params, or stack frames
    that leak codebase shape. The dashboard still records that the task
    failed — it just won't show the exception or traceback details.

    Pass worker_name= to override the hostname reported to ingest. Useful in
    Kubernetes / Docker where socket.gethostname() returns an ephemeral pod or
    container ID and rolling restarts otherwise produce a new "worker" row in
    the dashboard on every deploy. The CELERYRADAR_WORKER_NAME env var takes
    precedence over this argument.
    """
    global _client, _poller_config, _capture_args, _capture_exceptions, _worker_name_override, _app_name
    if not isinstance(app_name, str) or not app_name.strip():
        raise ValueError('app_name must be a non-empty string')
    if _client is not None:
        logger.warning('celeryradar_sdk.connect() called more than once; ignoring repeat call')
        return
    _client = Client(api_key, endpoint)
    _poller_config = (broker_url,)
    _capture_args = capture_args
    _capture_exceptions = capture_exceptions
    _worker_name_override = worker_name
    _app_name = app_name.strip()
    task_prerun.connect(_on_prerun)
    task_postrun.connect(_on_postrun)
    task_failure.connect(_on_failure)
    task_retry.connect(_on_retry)
    heartbeat_sent.connect(_on_heartbeat)
    beat.install(_client)

    # Spawn in each post-fork worker child and in the beat process.
    # weak=False because the closure isn't otherwise referenced.
    worker_process_init.connect(_start_poller, weak=False)
    beat_init.connect(_start_poller, weak=False)

    # Non-Celery callers (Django webserver, ad-hoc scripts) don't fork — spawn
    # immediately. Idempotent via Poller.start.
    _start_poller()


def _start_poller(*args, **kwargs):
    global _poller, _poller_pid, _poller_start_warned
    pid = os.getpid()
    # PID compare detects fork: the child inherits the parent's _poller
    # reference but the parent's thread didn't survive fork and the parent's
    # Redis socket is duped across processes. Both must be rebuilt.
    if _poller is not None and _poller_pid == pid:
        return
    if _client is None or _poller_config is None:
        return
    try:
        from celery import current_app
        broker_url = queues_mod.resolve_broker_url(current_app, override=_poller_config[0])
        if not broker_url:
            return
        redis_conn = queues_mod.connect_redis(broker_url)
        _poller = Poller(_client, current_app, redis_conn, _app_name)
        _poller_pid = pid
        _poller.start()
    except Exception as e:
        if not _poller_start_warned:
            _poller_start_warned = True
            logger.warning('celeryradar: queue depth poller failed to start: %s', e)


def _on_prerun(task_id, task, args=None, kwargs=None, **_):
    captured_args, captured_kwargs = _serialize_args(args, kwargs)
    _task_state[str(task_id)] = {
        'start': time.monotonic(),
        'args': captured_args,
        'kwargs': captured_kwargs,
    }
    _client.send({
        'type': 'task-started',
        'task_id': str(task_id),
        'task_name': task.name,
        'worker': _worker_name(),
        'queue': _get_queue(task),
        'args': captured_args,
        'kwargs': captured_kwargs,
        'retries': _retries(task),
        'timestamp': time.time(),
    })


def _on_postrun(task_id, task, state, **kwargs):
    if state != 'SUCCESS':
        return
    stash = _task_state.pop(str(task_id), None)
    start = stash['start'] if stash else None
    runtime = round(time.monotonic() - start, 4) if start is not None else None
    _client.send({
        'type': 'task-succeeded',
        'task_id': str(task_id),
        'task_name': task.name,
        'worker': _worker_name(),
        'runtime': runtime,
        'args': stash['args'] if stash else [],
        'kwargs': stash['kwargs'] if stash else {},
        'retries': _retries(task),
        'timestamp': time.time(),
    })


def _on_failure(task_id, exception, traceback, einfo, *args, **kwargs):
    sender = kwargs.get('sender')
    stash = _task_state.pop(str(task_id), None)
    _client.send({
        'type': 'task-failed',
        'task_id': str(task_id),
        'task_name': sender.name if sender else '',
        'worker': _worker_name(),
        'exception': repr(exception) if _capture_exceptions else '',
        'traceback': str(einfo) if _capture_exceptions else '',
        'args': stash['args'] if stash else [],
        'kwargs': stash['kwargs'] if stash else {},
        'retries': _retries(sender),
        'timestamp': time.time(),
    })


def _on_retry(request, reason, einfo, *args, **kwargs):
    # Read without popping: Celery reuses task_id across attempts and the next
    # prerun overwrites this entry. Popping here would lose args before the
    # eventual success/failure event.
    stash = _task_state.get(str(request.id))
    _client.send({
        'type': 'task-retried',
        'task_id': str(request.id),
        'task_name': request.task,
        'worker': _worker_name(),
        'exception': str(reason) if _capture_exceptions else '',
        'traceback': str(einfo) if _capture_exceptions else '',
        'retries': request.retries,
        'args': stash['args'] if stash else [],
        'kwargs': stash['kwargs'] if stash else {},
        'timestamp': time.time(),
    })


def _serialize_args(args, kwargs):
    """JSON-safe (args, kwargs), capped at ARGS_PAYLOAD_MAX_BYTES. Non-serializable
    values coerced via repr; oversized payloads replaced with a truncation marker.
    Returns ([], {}) when capture_args is disabled."""
    if not _capture_args:
        return [], {}
    try:
        coerced_args = json.loads(json.dumps(list(args or ()), default=repr))
        coerced_kwargs = json.loads(json.dumps(dict(kwargs or {}), default=repr))
    except (TypeError, ValueError):
        return ['__truncated__', 'unserializable'], {}
    size = len(json.dumps([coerced_args, coerced_kwargs]).encode('utf-8'))
    if size > ARGS_PAYLOAD_MAX_BYTES:
        return ['__truncated__', f'{size} bytes'], {}
    return coerced_args, coerced_kwargs


def _on_heartbeat(sender=None, **kwargs):
    global _last_heartbeat_sent
    now = time.time()
    if now - _last_heartbeat_sent < HEARTBEAT_INTERVAL:
        return
    _last_heartbeat_sent = now
    # retry=True: a missed heartbeat during an ingest outage produces a false
    # offline signal even though the worker was fine. Sparse (1/30s); safe to retry.
    _client.send({
        'type': 'worker-heartbeat',
        'hostname': _worker_name(),
        'queues': _discover_queues(),
        'timestamp': now,
    }, retry=True)


def _discover_queues():
    try:
        from celery import current_app
        return list(current_app.amqp.queues.keys())
    except Exception:
        return []


def _get_queue(task):
    try:
        return task.request.delivery_info.get('routing_key', '')
    except Exception:
        return ''


def _retries(task):
    # task.request.retries counts retries already done: 0 on attempt 1, 1 on
    # attempt 2, etc. Tagging every lifecycle event lets the dashboard group
    # them under the same attempt as the task_retry signal that triggered it.
    try:
        return task.request.retries
    except Exception:
        return 0
