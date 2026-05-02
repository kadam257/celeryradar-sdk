import time
import unittest
from unittest.mock import MagicMock

import fakeredis

from celeryradar_sdk.poller import LOCK_KEY_PREFIX, Poller, lock_key_for


class FakeApp:
    def __init__(self, queue_names=()):
        self.amqp = MagicMock()
        self.amqp.queues.keys.return_value = list(queue_names)


class FakeClient:
    def __init__(self, api_key='test-api-key'):
        self.api_key = api_key
        self.send = MagicMock()


class PollerLockTests(unittest.TestCase):
    def setUp(self):
        self.redis = fakeredis.FakeRedis()
        self.client = FakeClient()
        self.app = FakeApp(queue_names=['celery'])
        self.app_name = 'orders'

    def _make(self):
        # Threaded loop disabled — we drive _tick directly to keep tests deterministic.
        return Poller(self.client, self.app, self.redis, self.app_name)

    def test_first_tick_acquires_lock_and_sends(self):
        self.redis.rpush('celery', 'a', 'b')
        p = self._make()
        p._tick()
        self.assertEqual(self.redis.get(p.lock_key), p._id.encode())
        self.client.send.assert_called_once()
        payload = self.client.send.call_args[0][0]
        self.assertEqual(payload['type'], 'queue-depth')
        self.assertEqual(payload['app_name'], 'orders')
        self.assertEqual(payload['samples'], [{'queue_name': 'celery', 'depth': 2}])

    def test_second_contender_does_not_acquire(self):
        leader = self._make()
        leader._tick()  # leader holds lock
        follower = Poller(self.client, self.app, self.redis, self.app_name)

        self.client.send.reset_mock()
        follower._tick()

        # Lock value still == leader's id, follower didn't send.
        self.assertEqual(self.redis.get(leader.lock_key), leader._id.encode())
        self.client.send.assert_not_called()

    def test_owner_refreshes_ttl_on_subsequent_tick(self):
        p = self._make()
        p._tick()
        # Simulate 30s of clock progress on the lock TTL.
        self.redis.pexpire(p.lock_key, 30_000)
        ttl_before = self.redis.pttl(p.lock_key)
        p._tick()
        ttl_after = self.redis.pttl(p.lock_key)
        # Lock TTL got bumped back up to ~lock_ttl seconds.
        self.assertGreater(ttl_after, ttl_before)
        self.assertGreater(ttl_after, 50_000)

    def test_refresh_does_not_steal_lock_from_other_owner(self):
        # Process A holds the lock; process B's tick must not be able to refresh it.
        a = self._make()
        a._tick()
        b = Poller(self.client, self.app, self.redis, self.app_name)

        # Verify B's refresh attempt: SET NX fails (lock held), then refresh script
        # checks value against B's id and returns 0.
        acquired = b._acquire_or_refresh()
        self.assertFalse(acquired)
        # Lock value still belongs to A.
        self.assertEqual(self.redis.get(a.lock_key), a._id.encode())

    def test_no_queues_means_no_send(self):
        p = Poller(self.client, FakeApp(queue_names=[]), self.redis, self.app_name)
        p._tick()
        self.client.send.assert_not_called()
        # Lock still acquired even when there's nothing to report.
        self.assertEqual(self.redis.get(p.lock_key), p._id.encode())

    def test_tick_swallows_send_errors(self):
        self.client.send.side_effect = RuntimeError('ingest unreachable')
        self.redis.rpush('celery', 'a')
        p = self._make()
        # _run wraps _tick in try/except — direct _tick call here propagates the error,
        # so we test the loop wrapper instead by simulating one iteration.
        p._stop.set()  # ensure loop exits after one pass
        p._run()  # must not raise

    def test_timestamp_is_bucketed_to_poll_interval(self):
        # Samples in the same poll_interval window must share a timestamp so
        # replay is idempotent.
        self.redis.rpush('celery', 'a')
        p1 = Poller(self.client, self.app, self.redis, self.app_name, poll_interval=30)
        p1._sample_and_send()
        ts1 = self.client.send.call_args[0][0]['timestamp']
        self.assertEqual(ts1 % 30, 0, 'timestamp should be a multiple of poll_interval')
        self.assertLessEqual(ts1, time.time())
        self.assertGreater(ts1, time.time() - 30)


class LockKeyScopingTests(unittest.TestCase):
    def test_lock_key_is_hashed(self):
        # Raw api_key must not appear in the lock key — broker keyspace is
        # not a credential store.
        key = lock_key_for('sk_live_abc123secret', 'orders')
        self.assertTrue(key.startswith(LOCK_KEY_PREFIX + '::'))
        self.assertNotIn('sk_live_abc123secret', key)
        self.assertNotIn('orders', key)

    def test_different_app_names_produce_different_keys(self):
        a = lock_key_for('same-key', 'orders')
        b = lock_key_for('same-key', 'workers')
        self.assertNotEqual(a, b)

    def test_different_api_keys_produce_different_keys(self):
        a = lock_key_for('key-a', 'same-app')
        b = lock_key_for('key-b', 'same-app')
        self.assertNotEqual(a, b)

    def test_two_apps_one_redis_elect_independent_leaders(self):
        # The whole point of this change: app_a and app_b on the same Redis
        # broker each acquire their own lock and both ship samples.
        redis = fakeredis.FakeRedis()
        client = FakeClient()
        app = FakeApp(queue_names=['celery'])
        redis.rpush('celery', 'a', 'b', 'c')

        poller_a = Poller(client, app, redis, 'app_a')
        poller_b = Poller(client, app, redis, 'app_b')

        poller_a._tick()
        poller_b._tick()

        # Both shipped queue-depth events.
        self.assertEqual(client.send.call_count, 2)
        app_names_sent = {c[0][0]['app_name'] for c in client.send.call_args_list}
        self.assertEqual(app_names_sent, {'app_a', 'app_b'})


class PollerLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.redis = fakeredis.FakeRedis()
        self.client = FakeClient()
        self.app = FakeApp(queue_names=['celery'])

    def test_start_is_idempotent(self):
        p = Poller(self.client, self.app, self.redis, 'orders', poll_interval=60)
        p.start()
        first = p._thread
        p.start()
        self.assertIs(p._thread, first, 'second start() must not replace the thread')
        p.stop()
        first.join(timeout=2)

    def test_stop_exits_loop(self):
        p = Poller(self.client, self.app, self.redis, 'orders', poll_interval=60)
        p.start()
        # Give the thread a moment to enter its first wait().
        time.sleep(0.05)
        p.stop()
        p._thread.join(timeout=2)
        self.assertFalse(p._thread.is_alive(), 'thread must exit when stop() is called')


if __name__ == '__main__':
    unittest.main()
