import unittest
from unittest.mock import MagicMock, patch

from celeryradar_sdk import integration


class CaptureExceptionsToggleTests(unittest.TestCase):
    """capture_exceptions=False scrubs exception + traceback from failure/retry
    events so customers with PII in error messages or sensitive codebase
    structure can opt out. Wire format is preserved (empty strings, not missing
    keys) so the backend never sees a malformed payload."""

    def setUp(self):
        integration._client = MagicMock()
        integration._task_state.clear()
        integration._capture_exceptions = False

    def tearDown(self):
        integration._client = None
        integration._task_state.clear()
        integration._capture_exceptions = True

    def test_failure_scrubs_when_disabled(self):
        sender = MagicMock(); sender.name = 'myapp.t'
        sender.request.retries = 0
        integration._on_failure(
            task_id='tid', exception=ValueError('bad email jane@acme.com'),
            traceback=None, einfo='Traceback (most recent call last)\n  ...',
            sender=sender,
        )
        sent = integration._client.send.call_args[0][0]
        self.assertEqual(sent['exception'], '')
        self.assertEqual(sent['traceback'], '')
        # Failure is still recorded — only exception details are scrubbed.
        self.assertEqual(sent['type'], 'task-failed')
        self.assertEqual(sent['task_name'], 'myapp.t')

    def test_retry_scrubs_when_disabled(self):
        request = MagicMock(id='tid', task='myapp.t', retries=1)
        integration._on_retry(
            request=request, reason=Exception('429 Too Many Requests'),
            einfo='retry traceback ...',
        )
        sent = integration._client.send.call_args[0][0]
        self.assertEqual(sent['exception'], '')
        self.assertEqual(sent['traceback'], '')
        self.assertEqual(sent['type'], 'task-retried')
        self.assertEqual(sent['retries'], 1)


class ConnectCaptureExceptionsKwargTests(unittest.TestCase):
    """connect(capture_exceptions=False) must wire the kwarg to the module
    global. Without this, the kwarg would silently no-op."""

    def setUp(self):
        integration._client = None
        integration._poller_config = None
        integration._poller = None
        integration._poller_pid = None
        integration._capture_exceptions = True

    def tearDown(self):
        if integration._poller is not None:
            integration._poller.stop()
        integration._client = None
        integration._poller_config = None
        integration._poller = None
        integration._poller_pid = None
        integration._capture_exceptions = True

    def test_false_kwarg_disables_capture(self):
        with patch.object(integration, '_start_poller'), \
             patch('celeryradar_sdk.integration.beat.install'):
            integration.connect(
                api_key='cr_test', app_name='myapp', endpoint='http://example.com',
                capture_exceptions=False,
            )
        self.assertFalse(integration._capture_exceptions)


if __name__ == '__main__':
    unittest.main()
