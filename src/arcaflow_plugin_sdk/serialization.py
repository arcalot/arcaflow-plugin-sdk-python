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
            raise LoadFromFileException("Failed to load JSON from {}: {}".format(file_name, e.__str__())) from e
    elif file_name.endswith(".yaml") or file_name.endswith(".yml"):
        try:
            with open(file_name) as f:
                return yaml.safe_load(f)
        except BaseException as e:
            raise LoadFromFileException("Failed to load YAML from {}: {}".format(file_name, e.__str__())) from e
    else:
        raise LoadFromFileException("Unsupported file extension: {}".format(file_name))


class LoadFromFileException(Exception):
    msg: str

    def __str__(self) -> str:
        return self.msg