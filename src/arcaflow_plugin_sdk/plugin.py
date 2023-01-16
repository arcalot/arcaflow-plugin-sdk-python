import inspect
import io
import json
import sys
import traceback
import typing
from optparse import OptionParser
from sys import argv, stderr, stdin, stdout
from typing import Callable, Dict, List, Type, TypeVar

import yaml

from arcaflow_plugin_sdk import jsonschema, schema, serialization
from arcaflow_plugin_sdk.schema import (
    BadArgumentException,
    InvalidInputException,
    InvalidOutputException,
)

_issue_url = "https://github.com/arcalot/arcaflow-plugin-sdk-python/issues"

InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")

_step_decorator_param = Callable[[InputT], OutputT]


def step(
    id: str,
    name: str,
    description: str,
    outputs: Dict[str, Type],
    icon: typing.Optional[str] = None,
) -> Callable[[_step_decorator_param], schema.StepType]:
    """
    ``@plugin.step`` is a decorator that takes a function with a single parameter and creates a schema for it that you can
    use with ``plugin.build_schema``.

    :param id: The identifier for the step.
    :param name: The human-readable name for the step.
    :param description: The human-readable description for the step.
    :param outputs: A dict linking response IDs to response object types.
    :param icon: SVG icon for this step.
    :return: A schema for the step.
    """

    def step_decorator(func: _step_decorator_param) -> schema.StepType:
        if id == "":
            raise BadArgumentException("Steps cannot have an empty ID")
        if name == "":
            raise BadArgumentException("Steps cannot have an empty name")
        sig = inspect.signature(func)
        if len(sig.parameters) != 1:
            raise BadArgumentException(
                "The '%s' (id: %s) step must have exactly one parameter" % (name, id)
            )
        input_param = list(sig.parameters.values())[0]
        if input_param.annotation is inspect.Parameter.empty:
            raise BadArgumentException(
                "The '%s' (id: %s) step parameter must have a type annotation"
                % (name, id)
            )
        if isinstance(input_param.annotation, str):
            raise BadArgumentException(
                "Stringized type annotation encountered in %s (id: %s). Please make sure you "
                "don't import annotations from __future__ to avoid this problem."
                % (name, id)
            )

        new_responses: Dict[str, schema.StepOutputType] = {}
        for response_id in list(outputs.keys()):
            scope = build_object_schema(outputs[response_id])
            new_responses[response_id] = schema.StepOutputType(
                scope,
            )

        return schema.StepType(
            id,
            display=schema.DisplayValue(
                name=name,
                description=description,
                icon=icon,
            ),
            input=build_object_schema(input_param.annotation),
            outputs=new_responses,
            handler=func,
        )

    return step_decorator


class _ExitException(Exception):
    def __init__(self, exit_code: int, msg: str):
        self.exit_code = exit_code
        self.msg = msg


class _CustomOptionParser(OptionParser):
    def error(self, msg):
        raise _ExitException(2, msg + "\n" + self.get_usage())


SchemaBuildException = schema.SchemaBuildException
build_object_schema = schema.build_object_schema


def run(
    s: schema.SchemaType,
    argv: List[str] = tuple(argv),
    stdin: io.TextIOWrapper = stdin,
    stdout: io.TextIOWrapper = stdout,
    stderr: io.TextIOWrapper = stderr,
) -> int:
    """
    Run takes a schema and runs it as a command line utility. It returns the exit code of the program. It is intended
    to be used as an entry point for your plugin.

    :param s: the schema to run
    :param argv: command line arguments
    :param stdin: standard input
    :param stdout: standard output
    :param stderr: standard error
    :return: exit code
    """
    try:
        parser = _CustomOptionParser()
        parser.add_option(
            "-f",
            "--file",
            dest="filename",
            help="Configuration file to read configuration from. Pass - to read from stdin.",
            metavar="FILE",
        )
        parser.add_option(
            "--schema",
            dest="schema",
            action="store_true",
            help="Print Arcaflow schema.",
        )
        parser.add_option(
            "--atp",
            dest="atp",
            action="store_true",
            help="Run the Arcaflow Transport Protocol endpoint.",
        )
        parser.add_option(
            "--json-schema",
            dest="json_schema",
            help="Print JSON schema for either the input or the output.",
            metavar="KIND",
        )
        parser.add_option(
            "-s",
            "--step",
            dest="step",
            help="Which step to run? One of: " + ", ".join(s.steps.keys()),
            metavar="STEPID",
        )
        parser.add_option(
            "-d",
            "--debug",
            action="store_true",
            dest="debug",
            help="Enable debug mode (print step output and stack traces).",
        )
        (options, remaining_args) = parser.parse_args(list(argv[1:]))
        if len(remaining_args) > 0:
            raise _ExitException(
                64,
                "Unable to parse arguments: ["
                + ", ".join(remaining_args)
                + "]\n"
                + parser.get_usage(),
            )
        action = None
        if options.filename is not None:
            action = "file"
        if options.json_schema is not None:
            if action is not None:
                raise _ExitException(
                    64, "--{} and --json-schema cannot be used together".format(action)
                )
            action = "json-schema"
        if options.schema is not None:
            if action is not None:
                raise _ExitException(
                    64, "--{} and --schema cannot be used together".format(action)
                )
            action = "schema"
        if options.atp is not None:
            if action is not None:
                raise _ExitException(
                    64, "--{} and --atp cannot be used together".format(action)
                )
            action = "atp"
        if action is None:
            raise _ExitException(
                64,
                "At least one of --file, --json-schema, or --schema must be specified",
            )

        if action == "file" or action == "json-schema":
            if len(s.steps) > 1 and options.step is None:
                raise _ExitException(
                    64,
                    "-s|--step is required\n"
                    + parser.get_usage()
                    + "\nSteps: "
                    + str(list(s.steps.keys())),
                )
            if options.step is not None:
                step_id = options.step
            else:
                step_id = list(s.steps.keys())[0]
        if action == "file":
            return _execute_file(step_id, s, options, stdin, stdout, stderr)
        elif action == "atp":
            from arcaflow_plugin_sdk import atp

            return atp.run_plugin(s, stdin.buffer, stdout.buffer, stdout.buffer)
        elif action == "json-schema":
            return _print_json_schema(step_id, s, options, stdout)
        elif action == "schema":
            return _print_schema(s, options, stdout)
    except serialization.LoadFromFileException as e:
        stderr.write(e.msg + "\n")
        return 64
    except _ExitException as e:
        stderr.write(e.msg + "\n")
        return e.exit_code


def build_schema(*args: schema.StepType) -> schema.SchemaType:
    """
    This function takes functions annotated with ``@plugin.step`` and creates a schema from them.

    :param args: the steps to be added to the schema
    :return: a callable schema

    **Example**

    Imports:

    >>> from arcaflow_plugin_sdk import plugin
    >>> from dataclasses import dataclass

    Create an input dataclass:

    >>> @dataclass
    ... class InputData:
    ...    name: str

    Create an output dataclass:

    >>> @dataclass
    ... class OutputData:
    ...    message: str

    Create the plugin:

    >>> @plugin.step(
    ...     id="hello-world",
    ...     name="Hello world!",
    ...     description="Says hello :)",
    ...     outputs={"success": OutputData},
    ... )
    ... def hello_world(params: InputData) -> typing.Tuple[str, typing.Union[OutputData]]:
    ...     return "success", OutputData("Hello, {}!".format(params.name))

    Create the schema from one or more step functions:

    >>> plugin_schema = plugin.build_schema(hello_world)

    You can now call the step schema directly with data validation:

    >>> plugin_schema("hello-world", {"name": "Arca Lot"})
    ('success', {'message': 'Hello, Arca Lot!'})
    """
    steps_by_id: Dict[str, schema.StepType] = {}
    for step in args:
        if step.id in steps_by_id:
            raise BadArgumentException("Duplicate step ID %s" % step.id)
        steps_by_id[step.id] = step
    return schema.SchemaType(steps_by_id)


def _execute_file(
    step_id: str,
    s: schema.SchemaType,
    options,
    stdin: io.TextIOWrapper,
    stdout: io.TextIOWrapper,
    stderr: io.TextIOWrapper,
) -> int:
    filename: str = options.filename
    if filename == "-":
        data = serialization.load_from_stdin(stdin)
    else:
        data = serialization.load_from_file(filename)
    original_stdout = sys.stdout
    original_stderr = sys.stderr
    out_buffer = io.StringIO()
    if options.debug:
        # Redirect stdout to stderr for debug logging
        sys.stdout = stderr
        sys.stderr = stderr
    else:
        sys.stdout = out_buffer
        sys.stderr = out_buffer
    try:
        output_id, output_data = s(step_id, data)
        output = {
            "output_id": output_id,
            "output_data": output_data,
            "debug_logs": out_buffer.getvalue(),
        }
        stdout.write(yaml.dump(output, sort_keys=False))
        return 0
    except InvalidInputException as e:
        stderr.write(
            "Invalid input encountered while executing step '{}' from file '{}':\n  {}\n\n".format(
                step_id, filename, e.__str__()
            )
        )
        if options.debug:
            traceback.print_exc(chain=True)
        else:
            stderr.write("Set --debug to print a stack trace.")
        return 65
    except InvalidOutputException as e:
        stderr.write(
            "Bug: invalid output encountered while executing step '{}' from file '{}':\n  {}\n\n".format(
                step_id, filename, e.__str__()
            )
        )
        if options.debug:
            traceback.print_exc(chain=True)
        else:
            stderr.write("Set --debug to print a stack trace.")
        return 70
    finally:
        sys.stdout = original_stdout
        sys.stderr = original_stderr


def _print_json_schema(step_id, s, options, stdout):
    if step_id not in s.steps:
        raise _ExitException(
            64,
            'Unknown step "{}". Steps: {}'.format(step_id, str(list(s.steps.keys()))),
        )
    if options.json_schema == "input":
        data = jsonschema.step_input(s.steps[step_id])
    elif options.json_schema == "output":
        data = jsonschema.step_outputs(s.steps[step_id])
    else:
        raise _ExitException(64, "--json-schema must be one of 'input' or 'output'")
    stdout.write(json.dumps(data, indent="  "))
    return 0


def _print_schema(s, options, stdout):
    stdout.write(yaml.dump(schema.SCHEMA_SCHEMA.serialize(s)))
    return 0


test_object_serialization = schema.test_object_serialization

if __name__ == "__main__":
    import doctest

    doctest.testmod()
