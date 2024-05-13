Pypelines API
=============

Pipeline Class
--------------
.. autoclass:: pypelines.Pipeline
    :members:

.. autoclass:: pypelines.core.PipelineDebugger
    :members:

Validators
----------
.. autoclass:: pypelines.validator.ValidatorPipeline
    :members:

.. autoclass:: pypelines.validator.SUCCESS
.. autofunction:: pypelines.validator.FAILURE

JSON API
--------
.. autofunction:: pypelines.run
.. autofunction:: pypelines.api.core.dry_run

CLI
---
.. click:: pypelines.api.cli:cli_run
   :prog: pypelines
   :nested: full
