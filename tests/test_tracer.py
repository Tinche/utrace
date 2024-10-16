from utrace import Span, Tracer


def test_trace_and_span() -> None:
    tracer = Tracer()

    all_spans: list[Span] = []

    def receiver(spans: list[Span]) -> None:
        all_spans.extend(spans)

    tracer.receivers.append(receiver)

    with tracer.trace("trace", trace_metadata="test"):
        with tracer.span("span", span_metadata="span test"):
            pass

    assert len(all_spans) == 2
    assert all_spans[0]["name"] == "span"
    assert all_spans[0]["metadata"] == {"span_metadata": "span test"}
    assert all_spans[0]["trace.parent_id"] == all_spans[1]["trace.span_id"]
    assert all_spans[0]["trace.trace_id"] == all_spans[1]["trace.trace_id"]

    assert all_spans[1]["name"] == "trace"
    assert all_spans[1]["trace_metadata"] == "test"  # type: ignore