from typing import Any

import pytest

from pypelines.core import PIPELINE_END, Pipeline


def test_default_data():
    default = object()
    passed = object()

    class DefaultPipeline(Pipeline):
        def default_data(self):
            return default

    assert Pipeline(PIPELINE_END).run() is NotImplemented
    assert Pipeline(PIPELINE_END).run(passed) is passed
    assert DefaultPipeline(PIPELINE_END).run() is default
    assert DefaultPipeline(PIPELINE_END).run(passed) is passed


def test_wrap():
    passed = object()
    wrapped = Pipeline.wrap(passed)
    assert wrapped is passed

    assert Pipeline.wrap() is NotImplemented

    class WrapPipeline(Pipeline):
        @staticmethod
        def wrap(data: Any) -> Any:
            return [data]

    wrapped = WrapPipeline.wrap(passed)
    assert wrapped == [passed]


def test_create():
    def add_1(data):
        return data + 1

    def halve(data):
        return data / 2

    pipeline1 = Pipeline.create(add_1)
    assert pipeline1.transform is add_1
    assert pipeline1.rest is PIPELINE_END

    pipeline2 = Pipeline.create(halve, rest=pipeline1)
    assert pipeline2.transform is halve
    assert pipeline2.rest is pipeline1

    with pytest.raises(TypeError):
        _ = Pipeline.create(halve, rest=None)


def test_duck_typing_bind(
    add_one: Pipeline[float, float], double: Pipeline[float, float]
):
    assert (1 >> (add_one >> double >> (lambda x: str(x)))) == "4"

    with pytest.raises(TypeError):
        _ = add_one >> double >> object()  # type: ignore


def test_step():
    assert (
        Pipeline.create(PIPELINE_END).transform
        is Pipeline.step(PIPELINE_END).transform
        is PIPELINE_END
    )


def test_run(add_one: Pipeline[float, float]):
    with pytest.raises(TypeError):
        add_one.run()

    assert add_one(10) == add_one.run(10) == 11


def test_bind(add_one: Pipeline[float, float], double: Pipeline[float, float]):
    chained_with_bind = add_one.bind(double)
    chained_with_pipe = add_one >> double

    expected_steps = (add_one.transform, double.transform)

    def get_steps(pipeline):
        while pipeline.rest is not PIPELINE_END:
            yield pipeline.transform
            pipeline = pipeline.rest
        yield pipeline.transform

    assert isinstance(chained_with_bind, Pipeline)
    assert isinstance(chained_with_pipe, Pipeline)

    assert (
        tuple(get_steps(chained_with_bind))
        == tuple(get_steps(chained_with_pipe))
        == expected_steps
    )

    assert chained_with_bind(1) == chained_with_pipe(1) == 4


def test_wrap_bind(add_one: Pipeline[float, float], double: Pipeline[float, float]):
    chained = 1 >> add_one >> double

    assert chained == 4


def test_tail(
    add_one: Pipeline[float, float],
    double: Pipeline[float, float],
    negate: Pipeline[float, float],
):
    assert (add_one >> double >> negate).tail == negate


def test_repr(add_one: Pipeline[float, float], double: Pipeline[float, float]):
    @Pipeline.step
    def unannotated(x):
        return x

    assert repr(add_one >> double) == "Pipeline[float -> float]"
    assert repr(add_one >> unannotated) == "Pipeline[float -> Any]"
    assert repr(unannotated >> double) == "Pipeline[Any -> float]"
    assert "<function add_one" in repr(add_one)


def test_explain(add_one: Pipeline[float, float], double: Pipeline[float, float]):
    pipeline = add_one >> double
    assert pipeline.explain() == "add_one.<locals>._add_one >> double.<locals>._double"


def test_frozen(double: Pipeline[float, float]):
    with pytest.raises(TypeError):
        double.transform = None  # type: ignore

    with pytest.raises(TypeError):
        double.rest = None  # type: ignore


def test_iter(add_one: Pipeline[float, float], double: Pipeline[float, float]):
    assert list(add_one >> double) == [add_one.transform, double.transform]


def test_debug(add_one: Pipeline[float, float], double: Pipeline[float, float]):
    pipeline = add_one >> double

    debugger = pipeline.debug()
    assert debugger.step(10) == 11
    assert debugger.step(11) == 22
    with pytest.raises(IndexError):
        debugger.step(22)

    debugger = pipeline.debug()
    assert debugger.step(10, n=2) == 22

    debugger = pipeline.debug()
    with pytest.raises(IndexError):
        debugger.step(22, n=3)


def test_from_json():
    @Pipeline.step
    def dummy(x):
        return x

    passed = object()

    assert dummy.from_json(passed) is passed


def test_base_validator():
    @Pipeline.step
    def dummy(x):
        return x

    assert dummy.base_validator.transform is PIPELINE_END


def test_undecorated():
    def double(x: float) -> float:
        return 2 * x

    def negate(x: float) -> float:
        return -x

    def to_string(x):
        return str(x)

    assert 2 >> (Pipeline() >> double >> negate >> to_string) == "-4"
