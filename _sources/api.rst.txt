Functional Pypelines API
========================

Pipeline Class
--------------
.. autoclass:: functional_pypelines.Pipeline
    :members:

.. autoclass:: functional_pypelines.core.PipelineDebugger
    :members:

Validators
----------
.. autoclass:: functional_pypelines.validator.ValidatorPipeline
    :members:

.. autoclass:: functional_pypelines.validator.SUCCESS
.. autofunction:: functional_pypelines.validator.FAILURE

JSON API
--------
.. autofunction:: functional_pypelines.run
.. autofunction:: functional_pypelines.api.core.dry_run

CLI
---
.. click:: functional_pypelines.api.cli:cli_run
   :prog: functional_pypelines
   :nested: full
