import json

import pytest

import functional_pypelines.api.core as api
from functional_pypelines.core import Pipeline
from functional_pypelines.validator import ValidationError

good_config = json.load(open("tests/pkg/good_config.json"))
bad_config = json.load(open("tests/pkg/bad_config.json"))


def test_build_pipeline():
    pipeline = api.build_pipeline(good_config["PIPELINE"])

    assert pipeline(2) == "-4"

    pipeline = api.build_pipeline(
        ["pkg.pipeline.undecorated"] + good_config["PIPELINE"]
    )

    assert pipeline(2) == "-4"


def test_parse():
    parsed: api.APIData = api.parse(good_config)

    assert parsed.pipeline(2) == "-4"
    assert parsed.data == 2
    assert isinstance(parsed.validator, Pipeline)


def test_validate():
    good: api.APIData = api.parse(good_config)
    api.validate(good)

    bad: api.APIData = api.parse(bad_config)
    with pytest.raises(ValidationError):
        api.validate(bad)


def test_run():
    assert api.run(good_config) == "-4"
    assert api.dry_run(good_config) is not None

    with pytest.raises(ValidationError):
        api.run(bad_config)

    with pytest.raises(ValidationError):
        api.dry_run(bad_config)


def test_ensure_defined():
    msg = "test message"

    with pytest.raises(TypeError):
        api._ensure_defined(None)

    with pytest.raises(TypeError) as exc_info:
        api._ensure_defined(None, msg)

    assert msg in str(exc_info.value)
