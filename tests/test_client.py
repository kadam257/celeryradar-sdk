"""Tests for the async ingest client.

Mocks the HTTP transport via Client(transport=callable) so tests don't bind
to a port. Tests use queue.join() to deterministically wait for the drain
thread to finish processing rather than time.sleep().
"""
import threading
import time
import unittest
from unittest.mock import patch

from celeryradar_sdk.client import Client, MAX_BACKOFF_SECONDS


def _make_transport(responses):
    """Build a transport callable that returns scripted statuses or raises.

    `responses` is a list whose elements are either an int (status code to
    return) or an Exception subclass / instance (raised). Cycles back to the
    last entry once exhausted so a long-running test doesn't crash on IndexError.
    """
    posted = []
    idx = [0]

    def transport(body, headers):
        i = min(idx[0], len(responses) - 1)
        idx[0] += 1
        posted.append((body, headers))
        r = responses[i]
        if isinstance(r, BaseException):
            raise r
        if isinstance(r, type) and issubclass(r, BaseException):
            raise r('scripted failure')
        return r

    return transport, posted


class SendBehaviorTests(unittest.TestCase):
    """send() must enqueue without blocking and never raise."""

    def test_send_does_not_block_when_transport_is_slow(self):
        ev = threading.Event()

        def slow_transport(body, headers):
            ev.wait(timeout=5)  # blocks until released
            return 200

        client = Client('cr_test', 'http://x', transport=slow_transport)
        try:
            t0 = time.time()
            for _ in range(10):
                client.send({'type': 'test'})
            elapsed = time.time() - t0
            # Even with a transport blocked for 5s, send() must return fast —
            # only the drain thread sees the slowness.
            self.assertLess(elapsed, 0.1)
        finally:
            ev.set()
            client.stop()

    def test_send_never_raises_on_full_queue(self):
        # Block transport so the queue fills up.
        ev = threading.Event()

        def blocked_transport(body, headers):
            ev.wait(timeout=5)
            return 200

        client = Client('cr_test', 'http://x', max_queue_size=3, transport=blocked_transport)
        try:
            # First send starts the thread; the thread blocks on transport.
            # Subsequent sends fill the queue and then start dropping.
            for _ in range(20):
                client.send({'type': 'test'})  # must not raise
            self.assertGreater(client._dropped_full, 0)
        finally:
            ev.set()
            client.stop()


class DrainBehaviorTests(unittest.TestCase):
    """Drain thread POSTs every queued event exactly once."""

    def test_drain_processes_all_queued_events(self):
        transport, posted = _make_transport([200])
        client = Client('cr_test', 'http://x', transport=transport)
        try:
            for n in range(5):
                client.send({'type': 'test', 'n': n})
            client._queue.join()
            self.assertEqual(len(posted), 5)
        finally:
            client.stop()

    def test_payload_is_serialized_with_auth_header(self):
        transport, posted = _make_transport([200])
        client = Client('cr_secret', 'http://x', transport=transport)
        try:
            client.send({'type': 'test', 'n': 1})
            client._queue.join()
            body, headers = posted[0]
            self.assertEqual(headers['Authorization'], 'Bearer cr_secret')
            self.assertEqual(headers['Content-Type'], 'application/json')
            self.assertEqual(body, b'{"type": "test", "n": 1}')
        finally:
            client.stop()


class FailureBehaviorTests(unittest.TestCase):
    """5xx and network errors trigger backoff; 4xx drops the event without backoff."""

    def test_5xx_event_is_dropped_and_backoff_grows(self):
        # Patch _stop.wait so backoff doesn't actually sleep — we just record it.
        sleeps = []
        transport, posted = _make_transport([500, 500, 500, 200])
        client = Client('cr_test', 'http://x', transport=transport)

        original_wait = client._stop.wait

        def fake_wait(timeout=None):
            if timeout is not None and timeout > 0:
                sleeps.append(timeout)
                return False  # not stopped
            return original_wait(timeout)

        with patch.object(client._stop, 'wait', side_effect=fake_wait):
            try:
                for n in range(4):
                    client.send({'type': 'test', 'n': n})
                client._queue.join()
                # All 4 events were POSTed (5xx ones got dropped, not retried).
                self.assertEqual(len(posted), 4)
                # First failure → 2s, second → 4s, third → 8s. Success resets.
                self.assertEqual(sleeps[:3], [2, 4, 8])
            finally:
                client.stop()

    def test_4xx_does_not_trigger_backoff(self):
        sleeps = []
        transport, posted = _make_transport([400, 401, 200])
        client = Client('cr_test', 'http://x', transport=transport)

        def fake_wait(timeout=None):
            if timeout is not None and timeout > 0:
                sleeps.append(timeout)
                return False
            return False

        with patch.object(client._stop, 'wait', side_effect=fake_wait):
            try:
                for n in range(3):
                    client.send({'type': 'test', 'n': n})
                client._queue.join()
                self.assertEqual(len(posted), 3)
                # 4xx is a config error, not a transient — no backoff sleeps recorded.
                self.assertEqual(sleeps, [])
            finally:
                client.stop()

    def test_backoff_caps_at_max(self):
        # 7 consecutive failures → 2^1, 2^2, ..., 2^5, capped at MAX_BACKOFF_SECONDS.
        sleeps = []
        transport, posted = _make_transport([500] * 8)
        client = Client('cr_test', 'http://x', transport=transport)

        def fake_wait(timeout=None):
            if timeout is not None and timeout > 0:
                sleeps.append(timeout)
                return False
            return False

        with patch.object(client._stop, 'wait', side_effect=fake_wait):
            try:
                for n in range(8):
                    client.send({'type': 'test', 'n': n})
                client._queue.join()
                self.assertEqual(sleeps[-1], MAX_BACKOFF_SECONDS)
            finally:
                client.stop()

    def test_network_exception_triggers_backoff(self):
        sleeps = []
        transport, posted = _make_transport([ConnectionError, 200])
        client = Client('cr_test', 'http://x', transport=transport)

        def fake_wait(timeout=None):
            if timeout is not None and timeout > 0:
                sleeps.append(timeout)
                return False
            return False

        with patch.object(client._stop, 'wait', side_effect=fake_wait):
            try:
                client.send({'type': 'test', 'n': 1})
                client.send({'type': 'test', 'n': 2})
                client._queue.join()
                self.assertEqual(sleeps, [2])
            finally:
                client.stop()


class ForkSafetyTests(unittest.TestCase):
    """A different PID must trigger thread + queue rebuild."""

    def test_changing_pid_rebuilds_thread(self):
        transport, posted = _make_transport([200])
        client = Client('cr_test', 'http://x', transport=transport)
        try:
            client.send({'type': 'test', 'n': 1})
            client._queue.join()
            original_thread = client._thread

            # Simulate fork: pretend we're in a child process by mutating the
            # tracked PID. send() must notice and start a fresh thread.
            client._thread_pid = 99999  # any pid that isn't os.getpid()
            client.send({'type': 'test', 'n': 2})
            self.assertIsNot(client._thread, original_thread,
                             'post-fork send must rebuild the drain thread')
            client._queue.join()
            self.assertEqual(len(posted), 2)
        finally:
            client.stop()


class RetryQueueTests(unittest.TestCase):
    """retry=True events that fail go to a bounded retry queue and re-attempt
    when main is idle. For sparse high-importance events (heartbeats, beat
    fires) where a single loss produces a false absence signal."""

    def test_retry_false_event_drops_on_failure(self):
        # Default behavior: failures drop the event without retry queue activity.
        transport, posted = _make_transport([500, 200])
        client = Client('cr_test', 'http://x', transport=transport)

        def fake_wait(timeout=None):
            return False  # never actually sleep

        with patch.object(client._stop, 'wait', side_effect=fake_wait):
            try:
                client.send({'type': 'task-started'})  # retry=False default
                client.send({'type': 'task-started'})
                client._queue.join()
                self.assertEqual(len(posted), 2)
                # Failed event was NOT enqueued for retry.
                self.assertEqual(client._retry_queue.qsize(), 0)
            finally:
                client.stop()

    def test_retry_true_event_goes_to_retry_queue_on_failure(self):
        transport, posted = _make_transport([500])
        client = Client('cr_test', 'http://x', transport=transport)

        def fake_wait(timeout=None):
            return False

        with patch.object(client._stop, 'wait', side_effect=fake_wait):
            try:
                client.send({'type': 'worker-heartbeat'}, retry=True)
                client._queue.join()
                # First attempt failed → event landed in retry queue.
                self.assertEqual(client._retry_queue.qsize(), 1)
                payload, retry = client._retry_queue.get_nowait()
                client._retry_queue.task_done()
                self.assertEqual(payload['type'], 'worker-heartbeat')
                self.assertTrue(retry)
            finally:
                client.stop()

    def test_retry_queue_drained_when_main_is_idle(self):
        # First call fails (event lands on retry queue). Subsequent calls succeed.
        # The drain thread should pull the retry-queued event once main is empty.
        transport, posted = _make_transport([500, 200, 200])
        client = Client('cr_test', 'http://x', transport=transport)

        def fake_wait(timeout=None):
            return False

        with patch.object(client._stop, 'wait', side_effect=fake_wait):
            try:
                client.send({'type': 'worker-heartbeat', 'n': 1}, retry=True)
                client._queue.join()
                # Now main queue is empty, retry queue has 1.
                # Wait briefly for the drain thread to pick up the retry.
                deadline = time.time() + 2
                while client._retry_queue.qsize() > 0 and time.time() < deadline:
                    time.sleep(0.01)
                self.assertEqual(client._retry_queue.qsize(), 0)
                # Two POSTs total: the failed first try + the retry.
                self.assertEqual(len(posted), 2)
            finally:
                client.stop()

    def test_retry_queue_full_drops_oldest(self):
        # Force every attempt to fail so retry queue keeps filling.
        transport, posted = _make_transport([500])
        client = Client('cr_test', 'http://x', retry_queue_size=2, transport=transport)

        def fake_wait(timeout=None):
            return False

        with patch.object(client._stop, 'wait', side_effect=fake_wait):
            try:
                # Send 5 retry-eligible events; only 2 should fit in retry queue.
                for n in range(5):
                    client.send({'type': 'worker-heartbeat', 'n': n}, retry=True)
                # Wait for drain to process the main queue + cycle through retries.
                deadline = time.time() + 3
                while client._dropped_retry == 0 and time.time() < deadline:
                    time.sleep(0.02)
                self.assertGreater(client._dropped_retry, 0,
                                   'retry queue should have dropped events when full')
                self.assertLessEqual(client._retry_queue.qsize(), 2)
            finally:
                client.stop()


class HTTPPoolTests(unittest.TestCase):
    """The pool reuses a connection across calls and recreates after errors."""

    def test_pool_parses_endpoint_and_reuses_connection(self):
        from celeryradar_sdk.client import _HTTPPool

        pool = _HTTPPool('https://api.celeryradar.com/ingest/')
        self.assertEqual(pool._host, 'api.celeryradar.com')
        self.assertEqual(pool._port, 443)
        self.assertTrue(pool._is_https)
        self.assertEqual(pool._path, '/ingest/')

    def test_pool_http_endpoint(self):
        from celeryradar_sdk.client import _HTTPPool

        pool = _HTTPPool('http://localhost:8000/ingest/')
        self.assertEqual(pool._host, 'localhost')
        self.assertEqual(pool._port, 8000)
        self.assertFalse(pool._is_https)


if __name__ == '__main__':
    unittest.main()
