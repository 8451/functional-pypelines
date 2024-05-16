from dataclasses import dataclass
from functools import wraps
from typing import Any, Callable, Optional, Tuple, TypeVar

from .core import Pipeline

A = TypeVar("A")
B = TypeVar("B")


class ValidationError(Exception):
    """Raised when validation fails."""

    pass


@dataclass(frozen=True)
class ValidationResult:
    valid: bool = True
    reason: Optional[str] = None


class _Failure:
    """Indicates a failed validation.

    Parameters
    ----------
    reason : str
        The reason for the failure.

    Returns
    -------
    result : ValidationResult
        The failed validation result.
    """

    def __call__(self, reason: str):
        return ValidationResult(valid=False, reason=reason)


SUCCESS = ValidationResult()
FAILURE = _Failure()


@dataclass
class ValidatorData:
    pipeline: Pipeline
    data: Any = None
    result: ValidationResult = ValidationResult()

    def __post_init__(self):
        if self.data is None:
            self.data = self.pipeline.default_data()


class ValidatorPipeline(Pipeline[ValidatorData, ValidatorData]):
    """A pipeline for validating another pipeline.

    ValidatorPipelines are used to validate a pipeline before it is
    run. They are useful for validating user input before running a pipeline
    on it. For example, a validator pipeline could be used to validate that
    a user has provided a valid filepath before running a pipeline that
    processes the file.

    Validator Pipeline steps should take two arguments: a :class:`Pipeline`
    and its input data. The step should return a :class:`ValidationResult`,
    using either the :class:`SUCCESS` singleton or :func:`FAILURE` function.

    See Also
    --------
    :meth:`ValidatorPipeline.step`
    """

    @classmethod
    def step(
        cls,
        f: Callable[[Pipeline[A, Any], A], ValidationResult],  # type: ignore [override]
    ) -> "ValidatorPipeline":
        """Decorator for creating a validator step.

        Parameters
        ----------
        f : Callable[[Pipeline[A, Any], A], ValidationResult]
            The function to decorate. Must take a :class:`Pipeline` and its
            input data as arguments, and return a :class:`ValidationResult`.

        Returns
        -------
        ValidatorPipeline
            The decorated validator pipeline.


        Example Usage
        -------------
        >>> from typing import Any
        >>> import os
        >>> from functional_pypelines import Pipeline
        >>> from functional_pypelines.validator import (
        ...     ValidatorPipeline, SUCCESS, FAILURE, ValidationResult
        ... )
        >>>
        >>> @Pipeline.step
        ... def read_file(filepath: str) -> str:
        ...     with open(filepath, 'r') as f:
        ...         return f.read()
        >>>
        >>> @ValidatorPipeline.step
        ... def validate_filepath(
        ...     pipeline: Pipeline,
        ...     data: Any
        ... ) -> ValidationResult:
        ...    if not isinstance(data, str):
        ...        return FAILURE('Filepath must be a string.')
        ...    if not os.path.exists(data):
        ...        return FAILURE('File does not exist.')
        ...    if not os.path.isfile(data):
        ...        return FAILURE('Filepath must point to a file.')
        ...    return SUCCESS
        >>>
        """

        @wraps(f)
        def _step(data: ValidatorData) -> ValidatorData:
            if not data.result.valid:
                return data

            result = f(data.pipeline, data.data)
            if result is None:
                result = SUCCESS
            return ValidatorData(data.pipeline, data.data, result)

        return cls(_step)

    def validate(self, pipeline: Pipeline, *args, **kwargs) -> Tuple[Pipeline, A]:
        """Validate a pipeline and its input data.

        Parameters
        ----------
        pipeline : Pipeline
            The pipeline to validate.
        args : tuple
            The input data to validate.
        kwargs : dict
            The input data to validate.

        Returns
        -------
        Tuple[Pipeline, A]
            The validated pipeline and input data.

        Raises
        ------
        ValidationError
            If the pipeline/data is invalid.
        """
        data = ValidatorData(pipeline, pipeline.wrap(*args, **kwargs))
        validated = self.run(data)
        if not validated.result.valid:
            raise ValidationError(validated.result.reason)

        return validated.pipeline, validated.data

    def validate_and_run(
        self, pipeline: Pipeline[A, B], data: A, *, report: bool = False
    ) -> B:
        """Validate a pipeline and its input data, then run the pipeline.

        Parameters
        ----------
        pipeline : Pipeline
            The pipeline to validate.
        data : Any
            The input data to validate.
        report : bool, optional
            Whether to report the pipeline's progress, by default False

        Returns
        -------
        B
            The result of running the pipeline.

        Raises
        ------
        ValidationError
            If the pipeline/data is invalid.
        """
        pipeline, data = self.validate(pipeline, data)

        return pipeline.run(data, report=report)
