import json
import unittest
from unittest.mock import MagicMock

from celeryradar_sdk import integration


class SerializeArgsTests(unittest.TestCase):
    """_serialize_args coerces customer args/kwargs to JSON-safe payloads.
    Non-serializable values fall through repr; oversized payloads are dropped
    with a marker so events don't balloon."""

    def setUp(self):
        integration._capture_args = True

    def test_plain_jsonable_values_pass_through(self):
        args, kwargs = integration._serialize_args([1, 'two', 3.0], {'k': 'v'})
        self.assertEqual(args, [1, 'two', 3.0])
        self.assertEqual(kwargs, {'k': 'v'})

    def test_non_serializable_coerced_via_repr(self):
        class Custom:
            def __repr__(self):
                return '<Custom obj>'
        args, kwargs = integration._serialize_args([Custom()], {'x': Custom()})
        self.assertEqual(args, ['<Custom obj>'])
        self.assertEqual(kwargs, {'x': '<Custom obj>'})

    def test_oversized_payload_replaced_with_marker(self):
        big = 'x' * (integration.ARGS_PAYLOAD_MAX_BYTES + 100)
        args, kwargs = integration._serialize_args([big], {})
        self.assertEqual(len(args), 2)
        self.assertEqual(args[0], '__truncated__')
        self.assertIn('bytes', args[1])
        self.assertEqual(kwargs, {})

    def test_capture_args_disabled_returns_empty(self):
        integration._capture_args = False
        try:
            args, kwargs = integration._serialize_args([1, 2], {'k': 'v'})
            self.assertEqual(args, [])
            self.assertEqual(kwargs, {})
        finally:
            integration._capture_args = True

    def test_none_inputs_default_to_empty(self):
        args, kwargs = integration._serialize_args(None, None)
        self.assertEqual(args, [])
        self.assertEqual(kwargs, {})


class PrerunCapturesArgsTests(unittest.TestCase):
    """_on_prerun must stash args+kwargs so terminal handlers can re-emit them."""

    def setUp(self):
        integration._client = MagicMock()
        integration._task_state.clear()
        integration._capture_args = True

    def tearDown(self):
        integration._client = None
        integration._task_state.clear()

    def test_prerun_stashes_args_and_emits_started(self):
        task = MagicMock(name='myapp.do_thing')
        task.name = 'myapp.do_thing'
        task.request.delivery_info = {'routing_key': 'default'}
        task.request.retries = 0

        integration._on_prerun(task_id='tid-1', task=task,
                               args=[1, 2], kwargs={'k': 'v'})

        self.assertIn('tid-1', integration._task_state)
        stash = integration._task_state['tid-1']
        self.assertEqual(stash['args'], [1, 2])
        self.assertEqual(stash['kwargs'], {'k': 'v'})

        sent = integration._client.send.call_args[0][0]
        self.assertEqual(sent['type'], 'task-started')
        self.assertEqual(sent['args'], [1, 2])
        self.assertEqual(sent['kwargs'], {'k': 'v'})
        self.assertEqual(sent['retries'], 0)

    def test_prerun_emits_retries_on_subsequent_attempt(self):
        # Started events must carry the current retries count so downstream
        # consumers can group them under the right attempt — defaulting to 0
        # would lump every attempt together.
        task = MagicMock(); task.name = 'myapp.t'
        task.request.delivery_info = {'routing_key': 'default'}
        task.request.retries = 2  # 3rd attempt
        integration._on_prerun(task_id='tid-1b', task=task, args=[], kwargs={})
        sent = integration._client.send.call_args[0][0]
        self.assertEqual(sent['retries'], 2)

    def test_postrun_pulls_stashed_args(self):
        task = MagicMock(); task.name = 'myapp.t'
        task.request.retries = 0
        integration._task_state['tid-2'] = {
            'start': 0.0, 'args': ['a'], 'kwargs': {'b': 1},
        }
        integration._on_postrun(task_id='tid-2', task=task, state='SUCCESS')
        sent = integration._client.send.call_args[0][0]
        self.assertEqual(sent['args'], ['a'])
        self.assertEqual(sent['kwargs'], {'b': 1})
        self.assertEqual(sent['retries'], 0)
        self.assertNotIn('tid-2', integration._task_state)

    def test_postrun_emits_retries_on_eventual_success(self):
        task = MagicMock(); task.name = 'myapp.t'
        task.request.retries = 3  # task succeeded on attempt 4
        integration._task_state['tid-2b'] = {
            'start': 0.0, 'args': [], 'kwargs': {},
        }
        integration._on_postrun(task_id='tid-2b', task=task, state='SUCCESS')
        sent = integration._client.send.call_args[0][0]
        self.assertEqual(sent['retries'], 3)

    def test_failure_emits_retries_from_sender(self):
        sender = MagicMock(); sender.name = 'myapp.t'
        sender.request.retries = 3  # gave up after 3 retries
        integration._on_failure(
            task_id='tid-f', exception=ValueError('x'),
            traceback=None, einfo='ei', sender=sender,
        )
        sent = integration._client.send.call_args[0][0]
        self.assertEqual(sent['retries'], 3)

    def test_retry_does_not_pop_stash(self):
        # Celery reuses task_id across retries; prerun fires again on next attempt
        # and overwrites. Popping here would lose args before the eventual
        # success/failure event.
        integration._task_state['tid-3'] = {
            'start': 0.0, 'args': ['a'], 'kwargs': {},
        }
        request = MagicMock(id='tid-3', task='myapp.t', retries=1)
        integration._on_retry(request=request, reason=Exception('x'), einfo='ei')
        self.assertIn('tid-3', integration._task_state)


if __name__ == '__main__':
    unittest.main()
