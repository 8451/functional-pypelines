import tempfile

import pytest

from functional_pypelines.core import Pipeline


@pytest.fixture(scope="package")
def add_one() -> Pipeline[float, float]:
    @Pipeline.step
    def _add_one(x: float) -> float:
        return x + 1

    return _add_one


@pytest.fixture(scope="package")
def double() -> Pipeline[float, float]:
    @Pipeline.step
    def _double(x: float) -> float:
        return 2 * x

    return _double


@pytest.fixture(scope="package")
def negate() -> Pipeline[float, float]:
    @Pipeline.step
    def _negate(x: float) -> float:
        return -x

    return _negate


@pytest.fixture()
def temp_file():
    temp_file = tempfile.NamedTemporaryFile()

    yield temp_file

    temp_file.close()


@pytest.fixture()
def temp_file_name(temp_file):
    yield temp_file.name
