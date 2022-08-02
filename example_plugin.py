#!/usr/bin/env python3
# Copyright ARCALOT Authors
# SPDX-License-Identifier: Apache-2.0

import re
import sys
import typing
from dataclasses import dataclass
from arcaflow_plugin_sdk import plugin, validation


@dataclass
class FullName:
    """
    A full name holds the first and last name of an individual.
    """
    first_name: typing.Annotated[str, validation.min(1), validation.pattern(re.compile("[a-zA-Z]"))]
    last_name: typing.Annotated[str, validation.min(1), validation.pattern(re.compile("[a-zA-Z]"))]

    def __str__(self) -> str:
        """
        :return: the string representation of this name
        """
        return self.first_name + " " + self.last_name


@dataclass
class Nickname:
    """
    A nickname is a simplified form of the name that only holds the preferred name of an individual.
    """
    nick: typing.Annotated[str, validation.min(1), validation.pattern(re.compile("[a-zA-Z]"))]

    def __str__(self) -> str:
        """
        :return: the string representation of this name
        """
        return self.nick


@dataclass
class InputParams:
    """
    This is the data structure for the input parameters of the step defined below.
    """
    name: typing.Union[FullName, Nickname]


@dataclass
class SuccessOutput:
    """
    This is the output data structure for the success case.
    """
    message: str


@dataclass
class ErrorOutput:
    """
    This is the output data structure in the error  case.
    """
    error: str


# The following is a decorator (starting with @). We add this in front of our function to define the metadata for our
# step.
@plugin.step(
    id="hello-world",
    name="Hello world!",
    description="Says hello :)",
    outputs={"success": SuccessOutput, "error": ErrorOutput},
)
def hello_world(params: InputParams) -> typing.Tuple[str, typing.Union[SuccessOutput, ErrorOutput]]:
    """
    The function  is the implementation for the step. It needs the decorator above to make it into a  step. The type
    hints for the params are required.

    :param params:

    :return: the string identifying which output it is, as well the output structure
    """

    return "success", SuccessOutput(
        "Hello, {}!".format(params.name))


if __name__ == "__main__":
    sys.exit(plugin.run(plugin.build_schema(
        # List your step functions here:
        hello_world,
    )))
