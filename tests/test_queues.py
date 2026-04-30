import unittest
from unittest.mock import MagicMock

import fakeredis

from celeryradar_sdk import queues as queues_mod


class FakeApp:
    def __init__(self, broker_url=None, queue_names=()):
        self.conf = MagicMock(broker_url=broker_url)
        self.amqp = MagicMock()
        self.amqp.queues.keys.return_value = list(queue_names)


class ResolveBrokerUrlTests(unittest.TestCase):
    def setUp(self):
        # Reset module-level warning state so each test sees a fresh log.
        queues_mod._warned_unsupported = False

    def test_redis_url_passes(self):
        app = FakeApp(broker_url='redis://localhost:6379/0')
        self.assertEqual(queues_mod.resolve_broker_url(app), 'redis://localhost:6379/0')

    def test_rediss_url_passes(self):
        app = FakeApp(broker_url='rediss://example.com:6380/0')
        self.assertEqual(queues_mod.resolve_broker_url(app), 'rediss://example.com:6380/0')

    def test_amqp_url_returns_none_with_warning(self):
        app = FakeApp(broker_url='amqp://guest@localhost:5672/')
        with self.assertLogs('celeryradar_sdk.queues', level='WARNING') as cm:
            result = queues_mod.resolve_broker_url(app)
        self.assertIsNone(result)
        self.assertTrue(any('redis brokers only' in m for m in cm.output))

    def test_warning_only_logged_once(self):
        app = FakeApp(broker_url='amqp://guest@localhost:5672/')
        with self.assertLogs('celeryradar_sdk.queues', level='WARNING') as cm:
            queues_mod.resolve_broker_url(app)
            queues_mod.resolve_broker_url(app)
        self.assertEqual(len([m for m in cm.output if 'redis brokers only' in m]), 1)

    def test_override_takes_precedence(self):
        app = FakeApp(broker_url='amqp://...')
        result = queues_mod.resolve_broker_url(app, override='redis://override:6379/0')
        self.assertEqual(result, 'redis://override:6379/0')

    def test_no_broker_returns_none(self):
        app = FakeApp(broker_url=None)
        self.assertIsNone(queues_mod.resolve_broker_url(app))


class SampleDepthsTests(unittest.TestCase):
    def setUp(self):
        self.r = fakeredis.FakeRedis()

    def test_returns_lengths_per_queue(self):
        self.r.rpush('celery', 'a', 'b', 'c')
        self.r.rpush('high', 'x')
        depths = queues_mod.sample_depths(self.r, ['celery', 'high', 'empty'])
        self.assertEqual(depths, {'celery': 3, 'high': 1, 'empty': 0})

    def test_empty_queue_list_returns_empty_dict(self):
        self.assertEqual(queues_mod.sample_depths(self.r, []), {})


class DiscoverQueueNamesTests(unittest.TestCase):
    def test_returns_app_queue_keys(self):
        app = FakeApp(queue_names=['celery', 'high'])
        self.assertEqual(set(queues_mod.discover_queue_names(app)), {'celery', 'high'})

    def test_swallows_introspection_errors(self):
        app = MagicMock()
        app.amqp.queues.keys.side_effect = RuntimeError('boom')
        self.assertEqual(queues_mod.discover_queue_names(app), [])


if __name__ == '__main__':
    unittest.main()
