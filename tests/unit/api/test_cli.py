def test_cli_default_args(script_runner):
    """
    Test that the default arguments passed to the CLI return expected output.
    """
    ret = script_runner.run(["functional-pypelines"])

    assert ret.success
    assert ret.stdout.startswith("Usage: ")
    assert "functional-pypelines [OPTIONS] COMMAND [ARGS]" in ret.stdout


def test_cli_help(script_runner):
    """
    Test the help text of the CLI includes how to pass configs in
    """
    ret = script_runner.run(["functional-pypelines", "--help"])

    assert ret.success

    assert "Run a pipeline defined in a JSON file from the command line" in ret.stdout
    assert "run*" in ret.stdout


def test_cli_run_no_config(script_runner):
    """
    Make sure running without a config returns as expected
    """
    ret = script_runner.run(["functional-pypelines", "run"])

    assert not ret.success
    assert "Missing option '-c'" in ret.stderr


def test_cli_config_option(script_runner):
    """
    Test the CLI can open and read the config file passed via option
    """
    ret = script_runner.run(["functional-pypelines", "-c", "tests/pkg/good_config.json"])

    assert ret.success
    assert "good_config.json" in ret.stdout


def test_cli_config_option_validation_fails(script_runner):
    """
    Test the CLI can open and read the config file passed via option
    """
    ret = script_runner.run(["functional-pypelines", "-c", "tests/pkg/bad_config.json"])

    assert not ret.success
    assert "The pipeline failed to validate" in ret.stderr


def test_cli_config_option_bad_json(script_runner):
    """
    Test the CLI can open and read the config file passed via option
    """
    ret = script_runner.run(["functional-pypelines", "-c", "tests/pkg/invalid.json"])

    assert not ret.success
    assert "invalid.json could not be read as JSON" in ret.stderr


def test_cli_log_file(script_runner, temp_file_name):
    """
    Test the CLI can open and read the config file passed via option
    """
    ret = script_runner.run(
        ["functional-pypelines", "-c", "tests/pkg/good_config.json", "-o", temp_file_name]
    )

    assert ret.success
    assert "Loading Pipeline..." not in ret.stdout

    with open(temp_file_name) as file:
        assert "Loading Pipeline..." in file.read()
