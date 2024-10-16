from . import Span
from datetime import datetime


def encode_trace(spans: list[Span]) -> None:
    """Encode a trace for logs and eventual storage in OpenSearch."""
    from orjson import dumps

    last_span = spans[-1]
    if "trace.parent_id" not in last_span:
        # We only send the Trace Group metadata if we're ending
        # the trace here.
        # If it was started somewhere else, it will be set there.
        tg = {
            "traceGroup": last_span["name"],
            "traceGroupFields": {
                "endTime": datetime.fromtimestamp(
                    last_span["time"] + (last_span["duration_ms"] / 1000)
                ).isoformat(timespec="microseconds"),
                "durationInNanos": int(last_span["duration_ms"] * 1_000_000),
            },
        }
    else:
        tg = {}

    for span in spans:
        span_dict = {
            "logger": "trace",
            "traceId": span["trace.trace_id"],
            "spanId": span["trace.span_id"],
            "name": span["name"],
            "startTime": datetime.fromtimestamp(span["time"]).isoformat(
                timespec="microseconds"
            ),
            "endTime": (
                datetime.fromtimestamp(
                    span["time"] + (span["duration_ms"] / 1000)
                ).isoformat(timespec="microseconds")
            ),
            "serviceName": span["service.name"],
            "durationInNanos": int(span["duration_ms"] * 1_000_000),
            "parentSpanId": span.get("trace.parent_id", ""),
        }

        metadata = span["metadata"]
        print(dumps(span_dict | metadata | tg).decode(), flush=True)
