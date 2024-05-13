import inspect
import re
import warnings
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    Iterator,
    Optional,
    TypeVar,
    Union,
)

from ._immutables import Frozen
from ._logging import log
from .types import JSONType

A = TypeVar("A")
B = TypeVar("B", bound=Any)
C = TypeVar("C")


if TYPE_CHECKING:
    from .validator import ValidatorData  # pragma: no cover


PipelineStep = Callable[[Any], Any]
PipelineStepAB = Callable[[A], B]


class Identity:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)

        return cls._instance

    def __call__(self, data: A, *, report: bool = False) -> A:
        return data


PIPELINE_END = Identity()


class PipelineIterator(Iterator):
    """
    Helper class used for looping through the steps of a Pipeline
    """

    def __init__(self, pipeline: "Pipeline"):
        self.pipeline = pipeline

    def __next__(self):
        if self.finished:
            raise StopIteration

        transform = self.pipeline.transform
        self.pipeline = self.pipeline.rest

        return transform

    @property
    def finished(self) -> bool:
        return self.pipeline is PIPELINE_END


class PipelineDebugger(PipelineIterator):
    """Helper class used for debugging Pipelines.

    This class is used to step through the steps of a Pipeline for debugging
    purposes. It is an iterator that yields the steps of the Pipeline, and
    can be used to step through the Pipeline one step at a time.

    Example Usage
    -------------
    >>> from pypelines import Pipeline
    >>>
    >>> @Pipeline.step
    ... def add_one(x: float) -> float:
    ...     return x + 1
    ...
    >>> @Pipeline.step
    ... def double(x: float) -> float:
    ...     return x * 2
    ...
    >>> @Pipeline.step
    ... def square(x: float) -> float:
    ...     return x ** 2
    ...
    >>> my_pipeline = add_one >> double >> square
    >>> debugger = my_pipeline.debug()
    >>> debugger.step(2)  # 3
    >>> debugger.step(3)  # 6
    >>> debugger.step(6)  # 36
    >>> debugger.step(36)  # IndexError
    """

    def step(self, data: Any, *, n=1):
        for _ in range(n):
            if self.finished:
                raise IndexError("PipelineDebugger has finished every step.")
            transform = next(self)
            log(f"‣ Entering {transform}", fg="cyan")
            data = transform(data)

            rest_msg = (
                "ⓘ Pipeline Complete"
                if self.finished
                else f"ⓘ Next: {self.pipeline.transform}"
            )
            log(rest_msg, fg="cyan")

        return data


class Pipeline(Frozen, Generic[A, B]):
    """Composable function of a single variable.

    This class is used to build pipelines of functions that take a single
    argument and return a single value. It can either be used as a decorator,
    or in-line to build a pipeline.

    Example Usage
    -------------
    >>> from pypelines import Pipeline
    >>>
    >>> # As a decorator
    >>> @Pipeline.step
    ... def add_one(x: float) -> float:
    ...     return x + 1
    ...
    >>> @Pipeline.step
    ... def double(x: float) -> float:
    ...     return x * 2
    ...
    >>> @Pipeline.step
    ... def square(x: float) -> float:
    ...     return x ** 2
    ...
    >>> my_pipeline = add_one >> double >> square
    >>> my_pipeline  # Pipeline[float -> float]
    >>> my_pipeline(2)  # 36
    >>>
    >>> # In-line
    >>> def add_one(x: float) -> float:
    ...     return x + 1
    ...
    >>> def double(x: float) -> float:
    ...     return x * 2
    ...
    >>> def square(x: float) -> float:
    ...     return x ** 2
    ...
    >>> my_pipeline = Pipeline(add_one) >> double >> square
    >>> my_pipeline  # Pipeline[float -> float]
    >>> my_pipeline(2)  # 36
    """

    transform: Callable[[A], Any]
    rest: Union["Pipeline[Any, B]", Identity]

    def __init__(
        self,
        transform: Callable[[A], Any] = lambda x: x,
        *,
        rest: Union["Pipeline[Any, B]", Identity] = PIPELINE_END,
    ):
        if not isinstance(rest, Pipeline) and rest is not PIPELINE_END:
            raise TypeError(
                'The "rest" parameter of Pipeline init must be another '
                "Pipeline or omitted."
            )
        self.transform = transform
        self.rest = rest

        super().__init__()

    @classmethod
    def step(cls, f: Callable[[A], B]) -> "Pipeline[A, B]":
        """Decorator to turn a function into a Pipeline step.

        For the base :class:`Pipeline` class, this is does the same thing as
        the constructor, but it can be overridden to provide additional
        functionality.

        For example, if you wanted to pass around a dictionary of keyword
        arguments to maintain state, you could do something like this:

        >>> from typing import Callable, Dict, Any
        >>> from pypelines import Pipeline
        >>>
        >>> KwargType = Dict[str, Any]
        >>>
        >>> class KwargPipeline(Pipeline[KwargType, KwargType]):
        ...     @classmethod
        ...     def step(cls, f: Callable[..., KwargType]) -> 'KwargPipeline':
        ...         def wrapped(kwargs: KwargType) -> KwargType:
        ...             return {**kwargs, **f(**kwargs)}
        ...         return cls(wrapped)
        >>>
        >>> @KwargPipeline.step
        ... def add(x: int, y: int, **kwargs) -> KwargType:
        ...     return {'z': x + y}
        ...
        >>> add({'x': 1, 'y': 2})  # {'x': 1, 'y': 2, 'z': 3}


        Parameters
        ----------
        f : Callable[[A], B]
            The function to turn into a Pipeline step.

        Returns
        -------
        Pipeline[A, B]
            The Pipeline step.
        """
        if not callable(f):
            raise TypeError("Only callables can be converted to pipeline steps.")
        return cls(f)

    @classmethod
    def default_data(cls) -> A:
        """Returns the default data for this Pipeline.

        This method is called when the Pipeline is called without any
        arguments. By default, it raises a warning and returns
        NotImplemented. This method should be overridden in subclasses
        if you want to be able to call the Pipeline without any arguments.

        Returns
        -------
        A
            The default data.
        """
        warnings.warn(
            f"{cls} was called without any arguments, but does not "
            f"implement a default_data method. Returning NotImplemented."
        )
        return NotImplemented  # type: ignore

    @classmethod
    def wrap(cls, *args, **kwargs) -> A:
        """Wraps the arguments passed to the Pipeline.

        This method is called when the Pipeline is called with arguments.
        By default, it returns the first argument passed to the Pipeline.
        This method should be overridden in subclasses if you want to be
        able to call the Pipeline with multiple arguments, or keyword
        arguments.

        Parameters
        ----------
        *args
            The arguments passed to the Pipeline.
        **kwargs
            The keyword arguments passed to the Pipeline.

        Returns
        -------
        A
            The wrapped data.
        """
        if not len(args):
            return cls.default_data()

        return args[0]

    @classmethod
    def from_json(cls, data: JSONType) -> A:
        """Converts JSON data into the Pipeline's input type.

        This method is called when the Pipeline is called with JSON data.
        By default, it just calls :meth:`wrap` with the JSON data. This
        method should be overridden in subclasses if you want to be able
        to call the Pipeline with JSON data.

        Parameters
        ----------
        data : JSONType
            The JSON data to convert.

        Returns
        -------
        A
            The converted data.

        See Also
        --------
        CLI
        """
        return cls.wrap(data)

    @property
    def base_validator(self) -> "Pipeline[ValidatorData, ValidatorData]":
        """Returns the base validator for this Pipeline.

        Validators are Pipelines that perform static analysis of a Pipeline
        and input data to ensure that the Pipeline can be run with the given
        data. By default, this method returns a Pipeline that just returns
        whatever data is passed to it. This method should be overridden in
        subclasses if you want to be able to validate the Pipeline.

        When a Pipeline is invoked via the CLI, the CLI will automatically
        run the Pipeline through its base validator before running it. If
        the base validator fails, the CLI will print an error message and
        exit.

        Returns
        -------
        Pipeline[ValidatorData, ValidatorData]
            The base validator.

        See Also
        --------
        :class:`pypelines.validator.Validator`
        CLI
        """
        return Pipeline(Identity())

    @classmethod
    def create(
        cls,
        transform: Callable[[A], B],
        *,
        rest: Union["Pipeline[B, C]", Identity] = PIPELINE_END,
    ) -> "Pipeline[A, C]":
        """Creates a new Pipeline. Alias for the constructor."""
        return cls(transform, rest=rest)  # type: ignore

    def run(self, data: Optional[A] = None, *, report: bool = False) -> B:
        """Runs the Pipeline.

        Parameters
        ----------
        data : Optional[A], optional
            The data to pass to the Pipeline, by default :code:`None`.
            If :code:`None`, the Pipeline will be called with the result
            of :meth:`default_data`.
        report : bool, optional
            Whether or not to print the name of each step as it is entered,
            by default :code:`False`.

        Returns
        -------
        B
            The result of running the Pipeline.
        """
        data = data if data is not None else self.default_data()

        if report:
            log(f"‣ Entering {self.transform}", fg="cyan")

        return self.rest(self.transform(data), report=report)

    def bind(self: "Pipeline[A, B]", nxt: Callable[[B], C]) -> "Pipeline[A, C]":
        """Composes two Pipelines together.

        The :code:`>>` operator is an alias for this method.

        Parameters
        ----------
        nxt : Pipeline[B, C]
            The Pipeline to compose with.

        Returns
        -------
        Pipeline[A, C]
            The composed Pipeline.
        """
        if not isinstance(nxt, Pipeline):
            nxt = self.step(nxt)  # type: ignore [arg-type]

        rest: Pipeline[B, C]
        if isinstance(self.rest, Identity):
            rest = nxt
        else:
            rest = self.rest.bind(nxt)

        return self.create(self.transform, rest=rest)

    def debug(self) -> PipelineDebugger:
        """Returns a PipelineDebugger for this Pipeline.

        Returns
        -------
        PipelineDebugger
            The PipelineDebugger.

        See Also
        --------
        :class:`PipelineDebugger`
        """
        return PipelineDebugger(self)

    @property
    def tail(self) -> "Pipeline":
        if self.rest is PIPELINE_END:
            return self
        else:
            self.rest: Pipeline
            return self.rest.tail  # type: ignore

    @staticmethod
    def _annotation_to_string(annotation: Any):
        dunder_name = getattr(annotation, "__name__", None)
        under_name = getattr(annotation, "_name", None)

        name = str(dunder_name or under_name or annotation)

        return re.sub(r"^typing\.", "", name)

    def _from_annotation(self) -> str:
        s = list(inspect.signature(self.transform).parameters.values())[0]
        t = s.annotation
        if t is s.empty:
            return "Any"

        return self._annotation_to_string(t)

    def _to_annotation(self) -> str:
        s = inspect.signature(self.tail.transform)
        t = s.return_annotation
        if t is s.empty:
            return "Any"

        return self._annotation_to_string(t)

    def _explain(self):
        qualname = getattr(self.transform, "__qualname__", None)
        dundername = getattr(self.transform, "__name__", None)
        name = getattr(self.transform, "_name", None)

        return qualname or dundername or name or str(self.transform)

    def explain(self):
        if self is self.tail:
            return str(self._explain())
        else:
            return f"{self._explain()} >> {self.rest.explain()}"

    def __call__(self, *args, report: bool = False, **kwargs) -> B:
        """Runs the Pipeline, after wrapping the arguments via :meth:`wrap`.

        Parameters
        ----------
        *args
            The arguments to pass to :meth:`wrap`.
        report : bool, optional
            Whether or not to print the name of each step as it is entered,
            by default :code:`False`.
        **kwargs
            The keyword arguments to pass to :meth:`wrap`.

        Returns
        -------
        B
            The result of running the Pipeline.
        """
        data = self.wrap(*args, **kwargs)
        return self.run(data, report=report)

    def __iter__(self):
        """Returns an iterator over the steps of the Pipeline.

        Allow the steps of the Pipeline to be iterated over.

        Example Usage
        -------------

        >>> from pypelines import Pipeline
        >>>
        >>> @Pipeline.step
        ... def add_one(x: float) -> float:
        ...     return x + 1
        ...
        >>> @Pipeline.step
        ... def double(x: float) -> float:
        ...     return x * 2
        ...
        >>> @Pipeline.step
        ... def square(x: float) -> float:
        ...     return x ** 2
        ...
        >>> my_pipeline = add_one >> double >> square
        >>> for step in my_pipeline:
        ...     print(step)
        ...
        <function add_one at 0x7f8a6c3d0d30>
        <function double at 0x7f8a6c3d0e18>
        <function square at 0x7f8a6c3d0ea0>
        """
        return PipelineIterator(self)

    def __rshift__(self, right: Callable[[B], C]) -> "Pipeline[A, C]":
        """
        Abusing the bitshift operator >> to let us make pipelines by composing
        steps
        """
        return self.bind(right)

    def __rrshift__(self, data: Any) -> B:
        """
        Lets us pipe data in from the left
        """
        return self.run(self.wrap(data))

    def __repr__(self):
        if self.rest is PIPELINE_END:
            return f"Pipeline({repr(self.transform)})"
        else:
            return (
                f"Pipeline["
                f"{self._from_annotation()} -> {self._to_annotation()}"
                f"]"
            )

    def __setattr__(self, key, value):
        if key == "__doc__" and self.__doc__ is None:
            object.__setattr__(self, key, value)

        else:
            super().__setattr__(key, value)
