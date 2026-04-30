# celeryradar-sdk

Celery monitoring SDK for [CeleryRadar](https://celeryradar.com) — task events,
worker heartbeats, beat schedules, and queue depths.

## Install

```bash
pip install celeryradar-sdk
```

## Use

```python
import celeryradar_sdk

celeryradar_sdk.connect(api_key="cr_...")
```

That's it. The SDK hooks Celery's standard signals; no agents, no sidecars,
no broker plugins.

## Docs

Full documentation: <https://celeryradar.com/docs/>

## License

MIT
