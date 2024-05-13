from typing import Any

from pypelines import Pipeline
from pypelines.validator import FAILURE, SUCCESS, ValidatorPipeline


@Pipeline.step
def double(x: float) -> float:
    return 2 * x


@Pipeline.step
def negate(x: float) -> float:
    return -x


@Pipeline.step
def to_string(x: Any) -> str:
    return str(x)


def undecorated(x):
    return x


@ValidatorPipeline.step
def dummy_validator(pipeline: Pipeline, data: Any):
    return SUCCESS


@ValidatorPipeline.step
def always_fail_validator(pipeline: Pipeline, data: Any):
    return FAILURE("I always fail")
