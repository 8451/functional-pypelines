import pytest

from functional_pypelines.core import Pipeline
from functional_pypelines.validator import (
    FAILURE,
    ValidationError,
    ValidatorData,
    ValidatorPipeline,
)


def test_validator():
    @ValidatorPipeline.step
    def check_for_x(pipeline, data):
        if "x" not in data:
            return FAILURE("No x in data")

    @ValidatorPipeline.step
    def check_for_y(pipeline, data):
        if "y" not in data:
            return FAILURE("No y in data")

    @Pipeline.step
    def add_x_and_y(data):
        return data["x"] + data["y"]

    validator = check_for_x >> check_for_y

    with pytest.raises(ValidationError):
        validator.validate(add_x_and_y, {})

    with pytest.raises(ValidationError):
        validator.validate(add_x_and_y, {"x": 2})

    with pytest.raises(ValidationError):
        validator.validate(add_x_and_y, {"y": 2})

    assert validator.validate_and_run(add_x_and_y, {"x": 2, "y": 2}) == 4


def test_no_return(add_one: Pipeline):
    @ValidatorPipeline.step
    def dummy(pipeline, data):
        pass

    assert dummy.run(ValidatorData(add_one)).result.valid
