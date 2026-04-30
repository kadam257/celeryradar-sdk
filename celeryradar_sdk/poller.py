"""Leader-elected queue depth poller.

Every process running connect() contends for a single Redis lock. The winner
samples LLEN for each declared queue and ships a batched queue-depth event;
losers sleep and retry. Lock has a 60s TTL refreshed every POLL_INTERVAL while
held — if the leader crashes, the TTL expires and the next contender takes over.
"""
import logging
import threading
import time
import uuid

from . import queues as queues_mod

logger = logging.getLogger(__name__)

LOCK_KEY = 'celeryradar::queue-poll-lock'
LOCK_TTL_SECONDS = 60
POLL_INTERVAL_SECONDS = 30

# Lua: extend TTL only if we still own the lock. Without the value-check we'd
# extend a lock that expired and was reclaimed by another process.
_REFRESH_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("pexpire", KEYS[1], ARGV[2])
else
    return 0
end
"""


class Poller:
    def __init__(self, client, app, redis_conn,
                 lock_ttl=LOCK_TTL_SECONDS, poll_interval=POLL_INTERVAL_SECONDS):
        self.client = client
        self.app = app
        self.redis = redis_conn
        self.lock_ttl = lock_ttl
        self.poll_interval = poll_interval
        self._id = uuid.uuid4().hex
        self._stop = threading.Event()
        self._thread = None

    def start(self):
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, name='celeryradar-poller', daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()

    def _run(self):
        while not self._stop.is_set():
            try:
                self._tick()
            except Exception as e:
                # Never raise out of the loop — SDK must not affect host process.
                logger.debug('celeryradar poller tick failed: %s', e)
            self._stop.wait(self.poll_interval)

    def _tick(self):
        if self._acquire_or_refresh():
            self._sample_and_send()

    def _acquire_or_refresh(self):
        # SET NX EX is atomic — exactly one contender wins.
        acquired = self.redis.set(LOCK_KEY, self._id, nx=True, ex=self.lock_ttl)
        if acquired:
            return True
        # We may already hold it from a prior tick — refresh TTL only if so.
        refreshed = self.redis.eval(
            _REFRESH_SCRIPT, 1, LOCK_KEY, self._id, self.lock_ttl * 1000,
        )
        return bool(refreshed)

    def _sample_and_send(self):
        queue_names = queues_mod.discover_queue_names(self.app)
        depths = queues_mod.sample_depths(self.redis, queue_names)
        if not depths:
            return
        # Bucket timestamps into poll-interval slots. Two pollers that overlap
        # (slow leader's lock expired mid-sample, contender took it) land on
        # the same bucket so replay is idempotent.
        bucket = int(time.time() // self.poll_interval) * self.poll_interval
        self.client.send({
            'type': 'queue-depth',
            'timestamp': bucket,
            'samples': [{'queue_name': n, 'depth': d} for n, d in depths.items()],
        })
