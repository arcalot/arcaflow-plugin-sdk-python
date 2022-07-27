#!/usr/bin/env python3

import re
import sys
import typing
from dataclasses import dataclass
from typing import List
from arcaflow_plugin_sdk import plugin


@dataclass
class PodScenarioParams:
    """
    This is the data structure for the input parameters of the step defined below.
    """
    namespace_pattern: re.Pattern = re.compile(".*")
    pod_name_pattern: re.Pattern = re.compile(".*")


@dataclass
class Pod:
    """
    This is the data structure for a pod returned in the success case below.
    """
    namespace: str
    name: str


@dataclass
class PodScenarioResults:
    """
    This is the output data structure for the success case.
    """
    pods_killed: List[Pod]


@dataclass
class PodScenarioError:
    """
    This is the output data structure in the error  case.
    """
    error: str


# The following is a decorator (starting with @). We add this in front of our function to define the metadata for our
# step.
@plugin.step(
    id="pod",
    name="Pod scenario",
    description="Kill one or more pods matching the criteria",
    outputs={"success": PodScenarioResults, "error": PodScenarioError},
)
def pod_scenario(params: PodScenarioParams) -> typing.Tuple[str, typing.Union[PodScenarioResults, PodScenarioError]]:
    """
    The function  is the implementation for the step. It needs the decorator above to make it into a  step. The type
    hints for the params are required.

    :param params:

    :return: the string identifying which output it is, as well the output structure
    """

    # TODO add your implementation here

    return "error", PodScenarioError(
        "Cannot kill pod %s in namespace %s, function not implemented" % (
            params.pod_name_pattern.pattern,
            params.namespace_pattern.pattern
        ))


if __name__ == "__main__":
    sys.exit(plugin.run(plugin.build_schema(
        # List your step functions here:
        pod_scenario,
    )))
