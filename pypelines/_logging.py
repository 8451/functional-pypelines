import logging
from abc import ABCMeta, abstractmethod
from typing import Optional

import click


class PypelinesLogger(metaclass=ABCMeta):  # pragma: nocover
    @abstractmethod
    def log(self, msg, **kwargs):
        return NotImplemented


class ClickLogger(PypelinesLogger):
    def log(self, msg, **kwargs):
        click.secho(msg, **kwargs)


class FileLogger(PypelinesLogger):
    _path: str
    _logger: Optional[logging.Logger]
    _handler: Optional[logging.Handler]

    def __init__(self, path: str):
        self._path = path
        self._logger = logging.getLogger("pypelines")
        self._handler = logging.FileHandler(path)

        self._logger.setLevel(logging.INFO)
        self._handler.setLevel(logging.INFO)
        self._handler.setFormatter(logging.Formatter("%(message)s"))

        self._logger.addHandler(self._handler)

    def log(self, msg, **kwargs):
        self._logger.info(msg)


class _LOGGER:
    logger: PypelinesLogger = ClickLogger()

    @classmethod
    def log(cls, msg, **kwargs):
        cls.logger.log(msg, **kwargs)

    @classmethod
    def set_logger(cls, to: Optional[str] = None):
        to = "stdout" if to is None else to

        if to.lower() == "stdout":
            cls.logger = ClickLogger()
        else:
            cls.logger = FileLogger(to)


def set_logger(to: Optional[str]):
    _LOGGER.set_logger(to)


def log(msg, **kwargs):
    _LOGGER.log(msg, **kwargs)
