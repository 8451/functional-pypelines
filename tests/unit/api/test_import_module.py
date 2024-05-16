import pytest
from pkg.pipeline import double

from functional_pypelines.api._import import import_string


def test_import_string():
    assert import_string("pkg.pipeline.double") == double

    with pytest.raises(ImportError):
        import_string("pkg.pipeline.not_exists")

    with pytest.raises(ImportError):
        import_string("")
