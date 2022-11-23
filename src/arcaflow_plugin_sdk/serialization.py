import io
import json
from typing import Any

import yaml


def load_from_file(file_name: str) -> Any:
    """
    This function loads a YAML or JSON structure from a file.

    :param file_name: File name ending in JSON or YAML.
    :return: the decoded structure.
    """
    if file_name.endswith(".json"):
        try:
            with open(file_name) as f:
                return json.load(f)
        except BaseException as e:
            raise LoadFromFileException(
                "Failed to load JSON from {}: {}".format(file_name, e.__str__())
            ) from e
    elif file_name.endswith(".yaml") or file_name.endswith(".yml"):
        try:
            with open(file_name) as f:
                return yaml.safe_load(f)
        except BaseException as e:
            raise LoadFromFileException(
                "Failed to load YAML from {}: {}".format(file_name, e.__str__())
            ) from e
    else:
        raise LoadFromFileException("Unsupported file extension: {}".format(file_name))


def load_from_stdin(stdin: io.TextIOWrapper) -> Any:
    """
    This function reads from the standard input and returns a Python data structure.

    :param stdin: the standard input
    :return: the decoded structure.
    """
    stdin_data = stdin.buffer.read().decode("utf-8")
    if stdin_data.startswith("{"):
        try:
            return json.loads(stdin_data)
        except BaseException as e:
            raise LoadFromStdinException(
                "Failed to load JSON from stdin: {}".format(e.__str__())
            ) from e
    else:
        try:
            return yaml.safe_load(stdin_data)
        except BaseException as e:
            raise LoadFromStdinException(
                "Failed to load YAML from stdin: {}".format(e.__str__())
            ) from e


class LoadFromStdinException(Exception):
    msg: str

    def __str__(self) -> str:
        return self.msg


class LoadFromFileException(Exception):
    _msg: str

    def __init__(self, msg: str):
        if len(msg) == 0:
            msg = "Failed to load configuration file"
        self._msg = msg

    @property
    def msg(self) -> str:
        return self._msg

    def __str__(self) -> str:
        return self.msg
