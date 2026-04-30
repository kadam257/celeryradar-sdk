import logging
import time

from celery.schedules import crontab, schedule
from celery.signals import beat_init, before_task_publish

logger = logging.getLogger(__name__)

_in_beat = False
_registered_tasks = set()  # task_names with at least one active schedule
_last_payloads = {}  # entry_name -> payload tuple, for dedupe
_last_sync_time = 0.0
SYNC_INTERVAL_SECONDS = 30.0


def install(client):
    """Wire beat_init + before_task_publish to forward schedules and fires."""
    def _beat_init_handler(sender=None, **_):
        _on_beat_init(client, sender)

    def _publish_handler(sender=None, **_):
        _on_publish(client, sender)

    # weak=False because the closures over `client` aren't otherwise referenced
    # and Celery's signal framework holds handlers weakly by default.
    beat_init.connect(_beat_init_handler, weak=False)
    before_task_publish.connect(_publish_handler, weak=False)


def _on_beat_init(client, service):
    global _in_beat
    _in_beat = True
    if service is None:
        return

    # `sender` is celery.beat.Service; the Scheduler is on `.scheduler`. Fall
    # back to `service` directly in case a future Celery version sends the
    # scheduler as the sender.
    scheduler = getattr(service, 'scheduler', None) or service
    if not hasattr(scheduler, 'schedule'):
        logger.debug('celeryradar: beat_init sender has no scheduler.schedule, skipping')
        return

    _sync_schedules(client, scheduler)

    # Wrap scheduler.tick to periodically re-read schedule state. tick() is the
    # universal hook — sync() is a no-op for some schedulers (RedBeat reads
    # Redis every tick). Throttled to SYNC_INTERVAL_SECONDS.
    original_tick = scheduler.tick
    def wrapped_tick(*args, **kwargs):
        try:
            return original_tick(*args, **kwargs)
        finally:
            global _last_sync_time
            now = time.time()
            if now - _last_sync_time >= SYNC_INTERVAL_SECONDS:
                _last_sync_time = now
                try:
                    _sync_schedules(client, scheduler)
                except Exception as e:
                    logger.debug('celeryradar re-sync failed: %s', e)
    scheduler.tick = wrapped_tick


def _sync_schedules(client, scheduler):
    tz = str(getattr(scheduler.app.conf, 'timezone', None) or 'UTC')
    current_entries = set()
    current_tasks = set()
    current_payloads = {}

    for name, entry in _enumerate_entries(scheduler).items():
        try:
            normalized = _normalize_entry(name, entry)
        except Exception as e:
            # Custom schedulers or future Celery versions may surface entries
            # whose shape we don't expect. Skip the bad entry rather than
            # killing the whole sync.
            logger.debug('celeryradar: failed to normalize schedule %r: %s', name, e)
            continue
        if normalized is None:
            continue
        entry_name = normalized['entry_name']
        current_entries.add(entry_name)
        current_tasks.add(normalized['task_name'])
        key = (normalized['task_name'], normalized['schedule_type'], normalized['schedule_expr'], tz)
        current_payloads[entry_name] = key

        if _last_payloads.get(entry_name) != key:
            # retry=True: a dropped registration leaves the schedule unknown
            # until the next sync — fires arriving before then are unmatched.
            client.send({**normalized, 'type': 'schedule-register', 'timezone': tz},
                        retry=True)

    # retry=True: a missed snapshot leaves a removed schedule looking active
    # until the next sync, which can produce a false absence signal.
    client.send({'type': 'schedule-snapshot', 'entry_names': sorted(current_entries)},
                retry=True)

    _registered_tasks.clear()
    _registered_tasks.update(current_tasks)
    _last_payloads.clear()
    _last_payloads.update(current_payloads)


def _on_publish(client, task_name):
    if not _in_beat or task_name not in _registered_tasks:
        return
    # retry=True: a missed beat-fired during an outage looks like a missed
    # schedule and produces a false alert. Sparse (one per cadence); safe to retry.
    client.send({
        'type': 'beat-fired',
        'task_name': task_name,
        'timestamp': time.time(),
    }, retry=True)


def _enumerate_entries(scheduler):
    """{name: entry} for ALL registered schedules. RedBeat's `.schedule` only
    returns due-or-near-due entries, so iterate its Redis sorted set directly."""
    if scheduler.__class__.__name__ == 'RedBeatScheduler':
        try:
            from redbeat.schedulers import get_redis
            redis = get_redis(scheduler.app)
            keys = redis.zrange(scheduler.app.redbeat_conf.schedule_key, 0, -1)
            entries = {}
            for key in keys:
                if isinstance(key, bytes):
                    key = key.decode()
                try:
                    entry = scheduler.Entry.from_key(key, app=scheduler.app)
                    entries[entry.name] = entry
                except Exception:
                    continue
            return entries
        except Exception as e:
            logger.debug('celeryradar: RedBeat enumeration failed, falling back: %s', e)
    return scheduler.schedule


def _normalize_entry(name, entry):
    s = entry.schedule
    if isinstance(s, crontab):
        expr = ' '.join([
            s._orig_minute, s._orig_hour, s._orig_day_of_month,
            s._orig_month_of_year, s._orig_day_of_week,
        ])
        return {'entry_name': name, 'task_name': entry.task,
                'schedule_type': 'cron', 'schedule_expr': expr}
    if isinstance(s, schedule):
        return {'entry_name': name, 'task_name': entry.task,
                'schedule_type': 'interval',
                'schedule_expr': str(s.run_every.total_seconds())}
    logger.warning('celeryradar: skipping schedule %r — unsupported type %s', name, type(s).__name__)
    return None
