"""Async ingest client.

send() is non-blocking — events are pushed to a bounded queue and drained by
a background thread over a keep-alive HTTPS connection. A slow or unavailable
ingest endpoint must never delay the host process.

Failure handling: on any error the event is dropped and the drain thread sleeps
with exponential backoff. We don't retry individual events by default because
that pyramids losses during real outages — the queue fills and new events drop
too. Use retry=True on send() for sparse high-importance events (heartbeats,
beat fires) where loss produces a false alert.

Fork-safe via PID compare in _ensure_thread: prefork children rebuild both the
drain thread and the HTTPS connection (the parent's TCP socket would otherwise
be duped across processes).
"""
import http.client
import json
import logging
import os
import queue
import threading
import time
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

DEFAULT_QUEUE_SIZE = 1000
DEFAULT_RETRY_QUEUE_SIZE = 100
DEFAULT_TIMEOUT = 5.0
MAX_BACKOFF_SECONDS = 30
DROP_LOG_THROTTLE_SECONDS = 60.0


class _HTTPPool:
    """Single keep-alive connection. Recreates on any error so callers can retry."""

    def __init__(self, full_url, timeout=DEFAULT_TIMEOUT):
        parsed = urlparse(full_url)
        self._host = parsed.hostname
        self._port = parsed.port or (443 if parsed.scheme == 'https' else 80)
        self._is_https = parsed.scheme == 'https'
        self._path = parsed.path or '/'
        self._timeout = timeout
        self._conn = None

    def post(self, body, headers):
        if self._conn is None:
            cls = http.client.HTTPSConnection if self._is_https else http.client.HTTPConnection
            self._conn = cls(self._host, self._port, timeout=self._timeout)
        try:
            self._conn.request('POST', self._path, body=body, headers=headers)
            resp = self._conn.getresponse()
            # Must drain the body before the connection is reusable for the next request.
            resp.read()
            return resp.status
        except Exception:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
            raise


class Client:
    """Non-blocking ingest client. The drain thread starts lazily on first send()
    so importing the SDK without using it costs nothing.

    `transport` is an injection point for tests; production uses _HTTPPool.
    """

    def __init__(self, api_key, endpoint='https://api.celeryradar.com',
                 max_queue_size=DEFAULT_QUEUE_SIZE,
                 retry_queue_size=DEFAULT_RETRY_QUEUE_SIZE, transport=None):
        self.api_key = api_key
        self.ingest_url = endpoint.rstrip('/') + '/ingest/'
        self._max_queue_size = max_queue_size
        self._retry_queue_size = retry_queue_size
        self._transport = transport
        # Queues carry (payload, retry_eligible) tuples. Retry queue is drained
        # when main is empty; drop-oldest on full so recent state survives.
        self._queue = queue.Queue(maxsize=max_queue_size)
        self._retry_queue = queue.Queue(maxsize=retry_queue_size)
        self._stop = threading.Event()
        self._thread = None
        self._thread_pid = None
        self._init_lock = threading.Lock()
        self._dropped_full = 0
        self._dropped_retry = 0
        self._last_drop_log = 0.0

    def send(self, payload, retry=False):
        """Enqueue one event. Never blocks; never raises.

        retry=True diverts failed events to a bounded retry queue drained when
        main is idle. Use only for sparse high-importance events (heartbeats,
        beat fires, schedule registrations) — high-frequency events fight for
        queue capacity during outages.
        """
        self._ensure_thread()
        try:
            self._queue.put_nowait((payload, retry))
        except queue.Full:
            self._dropped_full += 1
            self._maybe_log_drops()

    def stop(self):
        """Signal the drain thread to exit. Used by tests; production relies on
        daemon=True to die with the host process."""
        self._stop.set()

    def _ensure_thread(self):
        pid = os.getpid()
        if self._thread is not None and self._thread_pid == pid and self._thread.is_alive():
            return
        with self._init_lock:
            # Re-check under lock so concurrent first-sends don't double-spawn.
            if self._thread is not None and self._thread_pid == pid and self._thread.is_alive():
                return
            # Post-fork: the parent's queue contents and stop flag don't belong to
            # the child. Reset both. First-start path (no prior thread) preserves
            # what __init__ set so tests can pre-patch _stop.wait.
            if self._thread is not None and self._thread_pid != pid:
                self._queue = queue.Queue(maxsize=self._max_queue_size)
                self._retry_queue = queue.Queue(maxsize=self._retry_queue_size)
                self._stop = threading.Event()
            self._thread = threading.Thread(
                target=self._drain, name='celeryradar-sender', daemon=True,
            )
            self._thread_pid = pid
            self._thread.start()

    def _maybe_log_drops(self):
        now = time.time()
        if now - self._last_drop_log < DROP_LOG_THROTTLE_SECONDS:
            return
        logger.warning(
            'celeryradar_sdk: ingest queue full; dropped %d events (cumulative)',
            self._dropped_full,
        )
        self._last_drop_log = now

    def _drain(self):
        # Build the pool inside the drain thread so post-fork children get a
        # fresh connection (the parent's TCP socket would otherwise be duped).
        pool = None if self._transport is not None else _HTTPPool(self.ingest_url)
        consecutive_failures = 0
        while not self._stop.is_set():
            payload, retry, source_queue = self._next_item()
            if payload is None:
                continue
            try:
                status = self._post(pool, payload)
                if status >= 500:
                    raise RuntimeError(f'ingest returned {status}')
                if status >= 400:
                    # 4xx is a config error (auth, schema). Drop without backoff —
                    # this event is never going to succeed, but the next one might.
                    logger.warning(
                        'celeryradar_sdk: ingest returned %s — event dropped', status,
                    )
                consecutive_failures = 0
            except Exception as e:
                consecutive_failures += 1
                if retry:
                    self._enqueue_retry(payload)
                backoff = min(MAX_BACKOFF_SECONDS, 2 ** min(consecutive_failures, 5))
                logger.debug(
                    'celeryradar_sdk send failed (%s); backing off %ds', e, backoff,
                )
                self._stop.wait(backoff)
            finally:
                source_queue.task_done()

    def _next_item(self):
        """Main queue first, retry queue when main is empty."""
        try:
            payload, retry = self._queue.get(timeout=0.5)
            return payload, retry, self._queue
        except queue.Empty:
            pass
        try:
            payload, retry = self._retry_queue.get_nowait()
            return payload, retry, self._retry_queue
        except queue.Empty:
            return None, None, None

    def _enqueue_retry(self, payload):
        """On full, drop the oldest so the most recent state survives —
        absence alerts care about recent heartbeats, not old ones."""
        try:
            self._retry_queue.put_nowait((payload, True))
            return
        except queue.Full:
            pass
        # Drop-oldest is safe here: only the drain thread reads the retry queue,
        # and we're on the drain thread.
        try:
            self._retry_queue.get_nowait()
            self._retry_queue.task_done()
            self._dropped_retry += 1
        except queue.Empty:
            pass
        try:
            self._retry_queue.put_nowait((payload, True))
        except queue.Full:
            self._dropped_retry += 1

    def _post(self, pool, payload):
        body = json.dumps(payload).encode()
        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json',
        }
        if self._transport is not None:
            return self._transport(body, headers)
        return pool.post(body, headers)
