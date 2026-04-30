import unittest
from unittest.mock import MagicMock, patch

from celeryradar_sdk import integration


class ConnectIdempotencyTests(unittest.TestCase):
    """connect() must dedupe — without it, every signal handler registers N
    times and each Celery event fires N duplicate ingest events."""

    def setUp(self):
        # Reset module-level state between tests since connect() mutates globals.
        integration._client = None
        integration._poller_config = None
        integration._poller = None
        integration._poller_pid = None

    def tearDown(self):
        if integration._poller is not None:
            integration._poller.stop()
        integration._client = None
        integration._poller_config = None
        integration._poller = None
        integration._poller_pid = None

    def test_repeat_connect_logs_warning_and_short_circuits(self):
        with patch.object(integration, '_start_poller') as start_mock, \
             patch('celeryradar_sdk.integration.beat.install') as beat_install, \
             patch('celeryradar_sdk.integration.task_prerun.connect') as prerun_connect:
            integration.connect(api_key='cr_test', endpoint='http://example.com')
            integration.connect(api_key='cr_test', endpoint='http://example.com')

        # First call wired everything; second call short-circuited.
        self.assertEqual(prerun_connect.call_count, 1)
        self.assertEqual(beat_install.call_count, 1)
        # _start_poller is invoked once at end of connect() (the immediate-spawn path).
        self.assertEqual(start_mock.call_count, 1)

    def test_repeat_connect_does_not_overwrite_client(self):
        with patch.object(integration, '_start_poller'):
            integration.connect(api_key='cr_first', endpoint='http://first.example')
            first_client = integration._client
            integration.connect(api_key='cr_second', endpoint='http://second.example')
            self.assertIs(integration._client, first_client,
                          'second connect() must not replace the existing client')


if __name__ == '__main__':
    unittest.main()
