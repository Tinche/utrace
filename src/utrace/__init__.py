"""The beginnings of a simple tracing module."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from itertools import count
from random import random
from secrets import token_hex
from time import perf_counter, time
from typing import Any, Callable, Iterator, Mapping, NotRequired, TypedDict

from attrs import Factory, define, field
from rich import print as rich_print
from rich.columns import Columns
from rich.console import Group
from rich.text import Text
from rich.tree import Tree

__all__ = ["Span", "Tracer", "TracerBase", "TraceId", "SpanId", "Metadata"]


type Instant = float
type DurationMS = float  # Milliseconds
type Metadata = dict[str, str | int]

Span = TypedDict(
    "Span",
    {
        "name": str,
        "time": Instant,
        "duration_ms": DurationMS,
        "trace.trace_id": str,
        "trace.span_id": str,
        "trace.parent_id": NotRequired[str],
        "metadata": Metadata,
        "trace_metadata": Metadata,
        "tracer_metadata": Metadata,
    },
)

type TraceId = str
type SpanId = str


_trace_cnt = count().__next__
_span_cnt = count().__next__
_trace_prefix = token_hex(8)


@define
class TracerBase:
    """
    A base class for various tracers, so they can have different
    public APIS.
    """

    service_name: str
    metadata: Metadata = Factory(dict)
    receivers: list[Callable[[list[Span]], Any]] = []
    trace_chance: float | None = None
    _trace_id_factory: Callable[[], TraceId] = lambda: f"{_trace_prefix}:{_trace_cnt()}"
    _span_id_factory: Callable[[], SpanId] = lambda: f"{_trace_prefix}:{_span_cnt()}"
    _active_trace_and_span: ContextVar[
        tuple[TraceId, Metadata, SpanId, list[Span]] | None
    ] = field(factory=lambda: ContextVar("active", default=None), init=False)

    def __attrs_post_init__(self) -> None:
        self.metadata["service.name"] = self.service_name

    @contextmanager
    def _trace(
        self,
        name: str,
        trace_metadata: dict[str, str | int] = {},
        /,
        **kwargs: str | int,
    ) -> Iterator[dict[str, str | int]]:
        """Start a trace and a span, trace_chance permitting.

        Returns:
            A dictionary that can be used to add metadata.
        """
        if self.trace_chance is not None and random() > self.trace_chance:
            yield {}
            return
        trace_id = self._trace_id_factory()
        span_id = self._span_id_factory()
        start = time()
        duration_start = perf_counter()
        span_metadata = kwargs
        child_spans: list[Span] = []
        token = self._active_trace_and_span.set(
            (trace_id, trace_metadata, span_id, child_spans)
        )
        try:
            yield span_metadata
        except BaseException as exc:
            span_metadata["error"] = repr(exc)
            raise
        finally:
            self._active_trace_and_span.reset(token)
            duration = perf_counter() - duration_start
            child_spans.append(
                {
                    "name": name,
                    "time": start,
                    "duration_ms": duration * 1000,
                    "trace.trace_id": trace_id,
                    "trace.span_id": span_id,
                    "metadata": span_metadata,
                    "trace_metadata": trace_metadata,
                    "tracer_metadata": self.metadata,
                }
            )
            self._emit(child_spans)

    @contextmanager
    def _span(
        self,
        name: str,
        parent: tuple[TraceId, Metadata, SpanId, list[Span]] | None = None,
        **kwargs: str | int,
    ) -> Iterator[dict[str, str | int]]:
        """Start a new span, if there is a trace active."""
        parent = parent or self._active_trace_and_span.get()
        span_metadata: Metadata = kwargs
        if parent is None:
            yield span_metadata
            return

        trace_id, trace_metadata, parent_span_id, children = parent
        start = time()
        duration_start = perf_counter()
        span_id = self._span_id_factory()
        token = self._active_trace_and_span.set(
            (trace_id, trace_metadata, span_id, children)
        )
        try:
            yield span_metadata
        except BaseException as exc:
            span_metadata["error"] = repr(exc)
            raise
        finally:
            self._active_trace_and_span.reset(token)
            duration = perf_counter() - duration_start
            children.append(
                {
                    "name": name,
                    "time": start,
                    "duration_ms": duration * 1000,
                    "trace.trace_id": trace_id,
                    "trace.span_id": span_id,
                    "trace.parent_id": parent_span_id,
                    "metadata": span_metadata,
                    "trace_metadata": trace_metadata,
                    "tracer_metadata": self.metadata,
                }
            )

    def _emit(self, spans: list[Span]) -> None:
        """When a unit of work is finished, notify all receivers."""
        for receiver in self.receivers:
            receiver(spans)


@define
class Tracer(TracerBase):
    """A tracer for generating traces to be sent to a tracing service."""

    @contextmanager
    def trace(self, name: str, **kwargs: str | int) -> Iterator[dict[str, str | int]]:
        """Start a trace and a span, trace_chance permitting.

        Return a dictionary that can be used to add metadata.
        """
        with self._trace(name, {}, **kwargs) as md:
            yield md

    @contextmanager
    def span(
        self,
        name: str,
        parent: tuple[TraceId, Metadata, SpanId, list[Span]] | None = None,
        **kwargs: str | int,
    ) -> Iterator[Metadata]:
        """Start a new span, if there is a trace active."""
        with self._span(name, parent, **kwargs) as md:
            yield md

    @contextmanager
    def span_from_dict(
        self, name: str, parent_dict: Mapping[str, str], **kwargs: str
    ) -> Iterator[Metadata]:
        """Start a child span, if possible."""
        if "_trace" not in parent_dict:
            yield {}
            return
        trace_id, parent_id = parent_dict["_trace"].split(",")
        children: list[Span] = []
        try:
            with self.span(
                name, (trace_id, {}, parent_id, children), **kwargs
            ) as span_metadata:
                yield span_metadata
        finally:
            self._emit(children)

    def span_to_dict(self) -> dict[str, str]:
        parent = self._active_trace_and_span.get()
        return {} if parent is None else {"_trace": f"{parent[0]},{parent[1]}"}


def print_trace(spans: list[Span]) -> None:
    """Format a trace with Rich and print it out in the terminal."""

    start = min(s["time"] for s in spans)
    end = max(s["time"] + (s["duration_ms"] / 1000) for s in spans)

    trace_span_ids = {s["trace.span_id"] for s in spans}

    root_span = [
        s
        for s in spans
        if ("trace.parent_id" not in s or s["trace.parent_id"] not in trace_span_ids)
    ][0]
    tree = Tree(t := Text(root_span["name"], style="bold white"))
    t.set_length(30)
    data = _process_children(root_span, spans, start, end, tree)

    width = 120
    lines = []
    durations = []  # In seconds
    metadata_strings = []
    for _, start, stop, dur, metadata in data:
        prefix = int(start * width) * " "
        body = int((stop - start) * width) * "━"
        suffix = int((1.0 - stop) * width) * " "
        line = Text(prefix + body + suffix)
        line.set_length(width)
        lines.append(line)
        durations.append(dur)
        metadata_strings.append(
            " ".join(f"{k}=[magenta]{v}[/]" for k, v in metadata.items())
        )

    g = Columns(
        [
            tree,
            Group(f"[bold sea_green2]{lines[0]}[/]", *[line for line in lines[1:]]),
            Group(*[f"[dim]{dur * 1000:4.0f} [italic]ms[/][/]" for dur in durations]),
            Group(*metadata_strings),
        ]
    )
    rich_print(g)


def _process_children(
    parent: Span, spans: list[Span], start: float, end: float, tree: Tree
) -> list[tuple[str, Instant, Instant, float, dict[str, str | int]]]:
    total_duration = end - start
    span_duration = parent["duration_ms"] / 1000
    start_pct = (parent["time"] - start) / total_duration
    res = [
        (
            parent["name"],
            start_pct,
            start_pct + (span_duration / total_duration),
            span_duration,
            parent["metadata"],
        )
    ]
    children = [s for s in spans if s.get("trace.parent_id") == parent["trace.span_id"]]
    for child in children:
        child_tree = tree.add(child["name"])
        child_data = _process_children(child, spans, start, end, child_tree)
        res.extend(child_data)
    return res
