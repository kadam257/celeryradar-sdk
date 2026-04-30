import unittest

from celery.schedules import crontab, schedule

from celeryradar_sdk.beat import _normalize_entry


class FakeEntry:
    """Minimal duck-typed beat ScheduleEntry stand-in."""
    def __init__(self, task, sched):
        self.task = task
        self.schedule = sched


class NormalizeEntryTests(unittest.TestCase):

    def test_crontab_simple(self):
        entry = FakeEntry('myapp.daily', crontab(minute='0', hour='9'))
        result = _normalize_entry('daily-9am', entry)
        self.assertEqual(result, {
            'entry_name': 'daily-9am',
            'task_name': 'myapp.daily',
            'schedule_type': 'cron',
            'schedule_expr': '0 9 * * *',
        })

    def test_crontab_every_minute(self):
        entry = FakeEntry('myapp.tick', crontab(minute='*'))
        result = _normalize_entry('tick', entry)
        self.assertEqual(result['schedule_expr'], '* * * * *')

    def test_crontab_with_range(self):
        entry = FakeEntry('myapp.business', crontab(minute='0', hour='9-17', day_of_week='1-5'))
        result = _normalize_entry('business-hours', entry)
        self.assertEqual(result['schedule_expr'], '0 9-17 * * 1-5')

    def test_crontab_with_step(self):
        entry = FakeEntry('myapp.sync', crontab(minute='*/5'))
        result = _normalize_entry('every-5', entry)
        self.assertEqual(result['schedule_expr'], '*/5 * * * *')

    def test_interval_seconds(self):
        entry = FakeEntry('myapp.poll', schedule(run_every=30.0))
        result = _normalize_entry('poll', entry)
        self.assertEqual(result, {
            'entry_name': 'poll',
            'task_name': 'myapp.poll',
            'schedule_type': 'interval',
            'schedule_expr': '30.0',
        })

    def test_interval_minutes_normalized_to_seconds(self):
        from datetime import timedelta
        entry = FakeEntry('myapp.minutely', schedule(run_every=timedelta(minutes=5)))
        result = _normalize_entry('every-5min', entry)
        self.assertEqual(result['schedule_type'], 'interval')
        self.assertEqual(result['schedule_expr'], '300.0')

    def test_unknown_schedule_type_returns_none(self):
        class WeirdSchedule:
            pass
        entry = FakeEntry('myapp.weird', WeirdSchedule())
        self.assertIsNone(_normalize_entry('weird', entry))


if __name__ == '__main__':
    unittest.main()
