from asyncio import sleep
from contextlib import contextmanager
from itertools import chain
from os import urandom
from typing import Callable, Final, Iterator, Literal, NoReturn, NotRequired, TypedDict

from aiohttp import ClientSession
from attrs import define
from orjson import dumps

from . import Metadata, SpanId, TraceId, TracerBase
from . import Span as USpan


def trace_id_factory() -> str:
    return urandom(16).hex()


def span_id_factory() -> str:
    return urandom(8).hex()


@define
class Tracer(TracerBase):
    """An OTel-specific tracer."""

    _trace_id_factory: Callable[[], TraceId] = trace_id_factory
    _span_id_factory: Callable[[], SpanId] = span_id_factory

    @contextmanager
    def trace(
        self,
        name: str,
        trace_metadata: Metadata = {},
        /,
        *,
        kind: Literal[
            "client", "server", "internal", "producer", "consumer"
        ] = "server",
        **kwargs: str | int,
    ) -> Iterator[dict[str, str | int]]:
        """Start a trace and a span, trace_chance permitting.

        Args:
            trace_metadata: Metadata that will be added to each child span.
            kwargs: Metadata that will be added to this span.

        Returns:
            A dictionary that can be used to add span metadata.
        """
        with self._trace(name, trace_metadata, kind=kind, **kwargs) as md:
            yield md

    @contextmanager
    def span(
        self,
        name: str,
        /,
        *,
        parent: tuple[TraceId, Metadata, SpanId, list[USpan]] | None = None,
        kind: Literal[
            "client", "server", "internal", "producer", "consumer"
        ] = "server",
        **kwargs: str | int,
    ) -> Iterator[Metadata]:
        with self._span(name, parent, kind=kind, **kwargs) as md:
            yield md


class StringValue(TypedDict):
    stringValue: str


class IntValue(TypedDict):
    intValue: int


class KVPair(TypedDict):
    key: str
    value: StringValue | IntValue


class Resource(TypedDict):
    attributes: list[KVPair]


class Span(TypedDict):
    traceId: str
    spanId: str
    parentSpanId: NotRequired[str]
    name: str
    startTimeUnixNano: str
    endTimeUnixNano: str
    kind: int
    attributes: list[KVPair]


class Scope(TypedDict):
    name: str
    version: str
    attributes: list[KVPair]


class InstrumentationScope(TypedDict):
    name: NotRequired[str]
    version: NotRequired[str]
    attributes: NotRequired[list[KVPair]]


class ScopeSpan(TypedDict):
    scope: InstrumentationScope
    spans: list[Span]


class ResourceSpan(TypedDict):
    resource: Resource
    scopeSpans: list[ScopeSpan]


class Payload(TypedDict):
    resourceSpans: list[ResourceSpan]


async def send_to_otel(
    tracer: Tracer, http_client: ClientSession, url: str
) -> NoReturn:
    """Continually send traces to an OTel receiver via HTTP, until cancelled.

    The OTel collector uses port 4318 by default, and the URL prefix of
    `/v1/traces`.
    """
    buffer: list[USpan] = []

    def add_to_buffer(spans: list[USpan]) -> None:
        if len(buffer) < 1000:
            buffer.extend(spans)

    tracer.receivers.append(add_to_buffer)

    while True:
        # We wait for a minimum number of spans so we don't get ourselves
        # into a infinite loop, since we generate a span each send.
        while len(buffer) > 1:
            buf = buffer
            buffer = []
            payload = dumps(_utrace_spans_to_otel(tracer, buf))
            with tracer.trace("utrace.send", kind="client", num_spans=len(buf)):
                resp = await http_client.post(
                    url, data=payload, headers={"content-type": "application/json"}
                )
                print(await resp.read())
                resp.raise_for_status()
        await sleep(5)


def _utrace_spans_to_otel(tracer: Tracer, spans: list[USpan]) -> Payload:
    """Convert utrace spans into OTel spans."""
    return {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [
                        {
                            "key": k,
                            "value": {"stringValue": v}
                            if isinstance(v, str)
                            else {"intValue": v},  # type: ignore
                        }
                        for k, v in tracer.metadata.items()
                    ]
                },
                "scopeSpans": [
                    {
                        "scope": {},
                        "spans": [_utrace_span_to_otel(span) for span in spans],
                    }
                ],
            }
        ]
    }


_KIND_TO_OTEL: Final = {
    "internal": 1,
    "server": 2,
    "client": 3,
    "producer": 4,
    "consumer": 5,
}


def _utrace_span_to_otel(span: USpan) -> Span:
    res: Span = {
        "traceId": span["trace.trace_id"],
        "spanId": span["trace.span_id"],
        "startTimeUnixNano": str(int(span["time"] * 1_000_000_000)),
        "endTimeUnixNano": str(
            int(span["time"] * 1_000_000_000 + span["duration_ms"] * 1_000_000)
        ),
        "kind": _KIND_TO_OTEL[str(span["metadata"]["kind"])],
        "name": span["name"],
        "attributes": [
            {
                "key": k,
                "value": {"stringValue": v} if isinstance(v, str) else {"intValue": v},  # type: ignore
            }
            for k, v in chain(span["trace_metadata"].items(), span["metadata"].items())
            if k != "kind"
        ],
    }
    if "trace.parent_id" in span:
        res["parentSpanId"] = span["trace.parent_id"]
    return res
