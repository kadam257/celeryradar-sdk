# celeryradar-sdk

[![PyPI version](https://img.shields.io/pypi/v/celeryradar-sdk.svg)](https://pypi.org/project/celeryradar-sdk/)
[![Python versions](https://img.shields.io/pypi/pyversions/celeryradar-sdk.svg)](https://pypi.org/project/celeryradar-sdk/)
[![License: MIT](https://img.shields.io/pypi/l/celeryradar-sdk.svg)](https://github.com/kadam257/celeryradar-sdk/blob/main/LICENSE)

Celery monitoring SDK for [CeleryRadar](https://celeryradar.com). Hooks Celery's
standard signals to ship task events, worker heartbeats, beat schedules, and
queue depth.

![CeleryRadar dashboard](https://raw.githubusercontent.com/kadam257/celeryradar-sdk/main/docs/screenshot.png)

## What gets monitored

- **Task events** — start, success, failure, retry, runtime, exception type
- **Worker heartbeats** — online/offline detection per worker hostname
- **Beat schedules** — fires, misses, and drift from the expected interval
- **Queue depth** — Redis broker depth per queue (cluster supported; RabbitMQ and SQS planned)

## Install

Requires Python 3.9+ and Celery 5.0+.

```bash
pip install celeryradar-sdk
```

## Use

Sign up at [celeryradar.com](https://celeryradar.com) to get your API key, then:

```python
import celeryradar_sdk

celeryradar_sdk.connect(api_key="cr_...")
```

That's it. The SDK is async and non-blocking — if the ingest endpoint is slow
or unreachable, your workers don't notice; events drop with a warning rather
than back up your task queue.

## Configuration

Common options:

```python
celeryradar_sdk.connect(
    api_key="cr_...",
    capture_args=False,         # don't send task args/kwargs (default True)
    worker_name="api-worker-1", # override hostname; useful in k8s/Docker
    broker_url="redis://...",   # override app.conf.broker_url for the depth poller
)
```

`CELERYRADAR_WORKER_NAME` is also read from the environment and takes precedence
over `worker_name=`. See the [configuration docs](https://celeryradar.com/docs/configuration/)
for the full reference.

## How it differs from Flower

Flower is a real-time inspector and admin tool — great for browsing the current
task queue and revoking tasks. CeleryRadar is the persistence layer above it:
history, alerts, dashboards, and trend analysis. They don't conflict.

## Documentation

- [Quick start](https://celeryradar.com/docs/) — get connected in five minutes
- [Configuration](https://celeryradar.com/docs/configuration/) — every kwarg and env var
- [How the SDK works](https://celeryradar.com/docs/how-it-works/) — async ingest, retry queue, fork safety
- [What gets monitored](https://celeryradar.com/docs/monitoring/) — signals and detection
- [Troubleshooting](https://celeryradar.com/docs/troubleshooting/) — common issues and fixes

## License

MIT
