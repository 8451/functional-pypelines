import sys
import time
from dataclasses import dataclass
from functools import reduce
from typing import Generic, Iterable, List, Optional, TypeVar, Union

from .._logging import log
from ..core import Pipeline
from ..types import JSONType
from ..validator import ValidationError, ValidatorData, ValidatorPipeline
from ._import import import_string

# Support Python < 3.8
if sys.version_info >= (3, 8):  # pragma: nocover
    from typing import TypedDict
else:  # pragma: nocover
    from typing_extensions import TypedDict


# -----------------------------------------------------------------------------
# API Pipeline class for running a pipeline from a json
# -----------------------------------------------------------------------------

A = TypeVar("A")
B = TypeVar("B")


class PayloadData(TypedDict):
    PIPELINE: List[str]
    DATA: JSONType
    VALIDATORS: Optional[List[str]]


@dataclass
class APIData(Generic[A, B]):
    payload: PayloadData
    pipeline: Optional[Pipeline[A, B]] = None
    data: Optional[A] = None
    validator: Optional[ValidatorPipeline] = None

    def update(self, **kwargs) -> "APIData[A, B]":
        kwargs = {k: kwargs.get(k, getattr(self, k)) for k in APIData.__annotations__}
        return APIData(**kwargs)


class APIPipeline(Pipeline[APIData, APIData]):
    @staticmethod
    def wrap(data: Union[PayloadData, APIData]) -> APIData:
        if isinstance(data, dict):
            data = APIData(payload=data)
        return data


# -----------------------------------------------------------------------------
# Helper functions
# -----------------------------------------------------------------------------
def _ensure_defined(a: Optional[A], msg: Optional[str] = None) -> A:
    if a is None:
        raise TypeError() if not msg else TypeError(msg)
    return a


# -----------------------------------------------------------------------------
# Converting a list of strings into a pipeline object
# -----------------------------------------------------------------------------


@Pipeline.step
def load_steps(paths: List[str]) -> Iterable[Pipeline]:
    return map(import_string, paths)


def _bind(left: Pipeline, right: Pipeline) -> Pipeline:
    if not isinstance(left, Pipeline):
        left = Pipeline(left)

    return left.bind(right)


@Pipeline.step
def combine(steps: Iterable[Pipeline]) -> Pipeline:
    return reduce(_bind, steps)


build_pipeline = load_steps >> combine


# -----------------------------------------------------------------------------
# Parsing the json dictionary
# -----------------------------------------------------------------------------


@APIPipeline.step
def parse_pipeline(api_data: APIData[A, B]) -> APIData[A, B]:
    pipeline = build_pipeline(api_data.payload["PIPELINE"])

    return api_data.update(pipeline=pipeline)


@APIPipeline.step
def parse_validators(api_data: APIData[A, B]) -> APIData[A, B]:
    pipeline = _ensure_defined(api_data.pipeline)
    validator = pipeline.base_validator

    extra_validator_paths = api_data.payload.get("VALIDATORS", None)
    if extra_validator_paths is not None:
        validator = validator >> build_pipeline(extra_validator_paths)

    return api_data.update(validator=validator)


@APIPipeline.step
def parse_data(api_data: APIData[A, B]) -> APIData[A, B]:
    pipeline = _ensure_defined(api_data.pipeline)

    data = api_data.payload.get("DATA", {})
    parsed = pipeline.from_json(data)

    return api_data.update(data=parsed)


parse = parse_pipeline >> parse_validators >> parse_data


# -----------------------------------------------------------------------------
# Validating the inputs
# -----------------------------------------------------------------------------


@APIPipeline.step
def validate(api_data: APIData[A, B]) -> APIData[A, B]:
    pipeline = _ensure_defined(api_data.pipeline)
    validator = _ensure_defined(api_data.validator)
    data: A = _ensure_defined(api_data.data)

    validated = validator.run(ValidatorData(pipeline, data))

    if not validated.result.valid:
        raise ValidationError(validated.result.reason)

    return api_data.update(pipeline=validated.pipeline, data=validated.data)


# -----------------------------------------------------------------------------
# Running the pipeline
# -----------------------------------------------------------------------------


@APIPipeline.step
def run_pipeline(api_data: APIData[A, B]) -> B:
    pipeline = _ensure_defined(api_data.pipeline)
    data: A = _ensure_defined(api_data.data)

    start = time.time()

    result = pipeline.run(data, report=True)

    log("")
    log(f"✓ Pipeline complete in {time.time() - start:.2f} seconds.", fg="green")

    return result


run = parse >> validate >> run_pipeline
dry_run = parse >> validate


run.__doc__ = """Run the pipeline defined in the config dictionary.

    The config must be a dictionary with the following structure:

    .. code-block::

        {
            "PIPELINE": [
                ...
            ],
            "DATA": ...,
            "VALIDATORS": [
                ...
            ]
        }

    The "PIPELINE" key must be a list of strings, where each string is a
    fully-qualified name of a pipeline class. The pipeline classes must be
    importable from the current working directory.

    The "DATA" key (OPTIONAL) must be a JSON object. This object will be
    passed to the pipeline as the data argument, and will be parsed using
    the Pipeline class's :meth:`pypelines.Pipeline.from_json` method.
    If the "DATA" key is not present, the pipeline will be passed the result
    of calling the Pipeline class's :meth:`pypelines.Pipeline.default_data`
    method.

    The "VALIDATORS" key (OPTIONAL) must be a list of strings, where each
    string  is a fully-qualified name of a pipeline class. The pipeline
    classes must be importable from the current working directory. The
    validator pipelines should be subclasses of
    :class:`pypelines.validator.ValidatorPipeline`,
    and will be run before the main pipeline. If the validator pipeline fails,
    the main pipeline will not be run. If the main pipeline's class defines
    a :meth:`pypelines.Pipeline.base_validator` attribute, it will be run
    before the validator pipelines defined here.

    Parameters
    ----------
    config : dict
        Dictionary defining the pipeline to run.

    Returns
    -------
    Any
        The result of running the pipeline.

    Raises
    ------
    ValidationError
        If the pipeline fails to validate.
    """

dry_run.__doc__ = """Validate that the defined pipeline will run without errors."""
