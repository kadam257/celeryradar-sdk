import socket
import unittest
from unittest.mock import patch

from celeryradar_sdk import integration


class WorkerNameResolutionTests(unittest.TestCase):
    """_worker_name() resolves: env var > connect kwarg > socket.gethostname().
    The env var exists so containerized deploys can set a stable name without
    code changes — pod hostnames rotate on every restart."""

    def setUp(self):
        integration._worker_name_override = None

    def tearDown(self):
        integration._worker_name_override = None

    def test_falls_back_to_gethostname(self):
        with patch.dict('os.environ', {}, clear=False):
            integration.os.environ.pop('CELERYRADAR_WORKER_NAME', None)
            self.assertEqual(integration._worker_name(), socket.gethostname())

    def test_kwarg_override_wins_over_gethostname(self):
        integration._worker_name_override = 'my-deployment'
        with patch.dict('os.environ', {}, clear=False):
            integration.os.environ.pop('CELERYRADAR_WORKER_NAME', None)
            self.assertEqual(integration._worker_name(), 'my-deployment')

    def test_env_var_wins_over_kwarg(self):
        integration._worker_name_override = 'kwarg-name'
        with patch.dict('os.environ', {'CELERYRADAR_WORKER_NAME': 'env-name'}):
            self.assertEqual(integration._worker_name(), 'env-name')

    def test_env_var_wins_over_gethostname(self):
        with patch.dict('os.environ', {'CELERYRADAR_WORKER_NAME': 'env-name'}):
            self.assertEqual(integration._worker_name(), 'env-name')

    def test_empty_env_var_falls_through(self):
        # Empty string is falsy — should fall through to the next source rather
        # than send an empty 'worker' field to ingest.
        integration._worker_name_override = 'kwarg-name'
        with patch.dict('os.environ', {'CELERYRADAR_WORKER_NAME': ''}):
            self.assertEqual(integration._worker_name(), 'kwarg-name')


class ConnectAcceptsWorkerNameTests(unittest.TestCase):
    """connect(worker_name=...) stores the override for later resolution."""

    def setUp(self):
        integration._client = None
        integration._poller_config = None
        integration._poller = None
        integration._poller_pid = None
        integration._worker_name_override = None

    def tearDown(self):
        if integration._poller is not None:
            integration._poller.stop()
        integration._client = None
        integration._poller_config = None
        integration._poller = None
        integration._poller_pid = None
        integration._worker_name_override = None

    def test_connect_stores_worker_name(self):
        with patch.object(integration, '_start_poller'), \
             patch('celeryradar_sdk.integration.beat.install'), \
             patch('celeryradar_sdk.integration.task_prerun.connect'):
            integration.connect(api_key='cr_test', endpoint='http://example.com',
                                worker_name='custom-worker')
        self.assertEqual(integration._worker_name_override, 'custom-worker')

    def test_connect_default_worker_name_is_none(self):
        with patch.object(integration, '_start_poller'), \
             patch('celeryradar_sdk.integration.beat.install'), \
             patch('celeryradar_sdk.integration.task_prerun.connect'):
            integration.connect(api_key='cr_test', endpoint='http://example.com')
        self.assertIsNone(integration._worker_name_override)


if __name__ == '__main__':
    unittest.main()
