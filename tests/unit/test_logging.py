from pypelines._logging import log, set_logger


def test_log_to_file(temp_file_name: str):
    set_logger(temp_file_name)
    log("test msg")

    with open(temp_file_name) as file:
        assert "test msg" in file.read()

    set_logger(None)
