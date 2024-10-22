from utrace import Span
from utrace.otel import Tracer


async def test_spanning() -> None:
    """`Tracer.spanning` works."""

    all_spans: list[Span] = []

    tracer = Tracer("service", receivers=[all_spans.extend])

    class MyClass:
        @tracer.spanning()
        def test(self) -> int:
            return 1

        @tracer.spanning("test2span")
        def test2(self) -> int:
            return 2

        @tracer.spanning()
        async def test3(self) -> int:
            return 3

        @tracer.spanning("test4span")
        async def test4(self) -> int:
            return 4

    my_class = MyClass()

    with tracer.trace("trace"):
        assert my_class.test() == 1

    assert len(all_spans) == 2
    assert all_spans[0]["name"] == MyClass.test.__name__

    all_spans.clear()
    with tracer.trace("trace"):
        assert my_class.test2() == 2
    assert len(all_spans) == 2
    assert all_spans[0]["name"] == "test2span"

    all_spans.clear()
    with tracer.trace("trace"):
        assert await my_class.test3() == 3
    assert len(all_spans) == 2
    assert all_spans[0]["name"] == "test3"

    all_spans.clear()
    with tracer.trace("trace"):
        assert await my_class.test4() == 4
    assert len(all_spans) == 2
    assert all_spans[0]["name"] == "test4span"
