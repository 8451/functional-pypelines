import json
from json.decoder import JSONDecodeError

import click
from click_default_group import DefaultGroup

from .._logging import log, set_logger
from ..validator import ValidationError
from . import core


@click.group(cls=DefaultGroup, default="run", default_if_no_args=False)
def cli_run():
    """Run a pipeline defined in a JSON file from the command line.

    This is a wrapper for pypelines.run. The config file must be a
    JSON file that can be loaded into a dictionary.
    """
    pass


@cli_run.command()
@click.option(
    "-c",
    "--config",
    required=True,
    type=str,
    envvar="PYPELINES_CONFIG",
    help="May also be passed with the environment variable PYPELINES_CONFIG",
)
@click.option(
    "-o",
    "--output",
    required=False,
    type=str,
    help="Filepath where output should be directed. " "Directed to stdout if omitted.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="During run, only validate that the pipeline is runnable.",
)
def run(config, output, dry_run):
    try:
        with open(config, "r") as f:
            config_contents = json.load(f)
    except JSONDecodeError as e:
        msg = (
            f"The config file {config} could not be read as JSON. "
            f"Check for missing commas or use a online JSON validator."
        )
        raise click.exceptions.ClickException(msg) from e

    set_logger(output)

    log("=" * 32, fg="yellow")
    log("Loading Pipeline...", fg="yellow")
    log("=" * 32, fg="yellow")
    log("")
    log(f"Using config file: {config}", fg="yellow")
    log("")
    log("=" * 32, fg="yellow")
    log("Running pipeline", fg="yellow")
    log("=" * 32, fg="yellow")
    log("")

    pipeline_runner = core.dry_run if dry_run else core.run

    try:
        pipeline_runner(config_contents)
    except ValidationError as e:
        msg = (
            f"The pipeline failed to validate for the following reason:\n"
            f"  {e.args[0]}"
        )
        raise click.exceptions.ClickException(msg)
