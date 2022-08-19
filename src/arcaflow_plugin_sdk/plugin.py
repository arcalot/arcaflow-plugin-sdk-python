import dataclasses
import inspect
import io
import json
import os
import pprint
import re
import sys
import traceback
import types
import typing
from types import GenericAlias

import yaml
from dataclasses import fields
from enum import Enum
from sys import argv, stdin, stdout, stderr
from optparse import OptionParser
from typing import List, Callable, TypeVar, Dict, Any, Type, get_origin, get_args

from arcaflow_plugin_sdk import schema, serialization, jsonschema, http
from arcaflow_plugin_sdk.schema import BadArgumentException, Field, InvalidInputException, InvalidOutputException, \
    ConstraintException, TypeID, ObjectT, TypeT

_issue_url = "https://github.com/arcalot/arcaflow-plugin-sdk-python/issues"

InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")

_step_decorator_param = Callable[[InputT], OutputT]


def step(
        id: str,
        name: str,
        description: str,
        outputs: Dict[str, Type]
) -> Callable[
    [_step_decorator_param],
    schema.StepSchema[InputT]
]:
    """
    @plugin.step is a decorator that takes a function with a single parameter and creates a schema for it that you can
    use with plugin.build_schema.

    :param id: The identifier for the step.
    :param name: The human-readable name for the step.
    :param description: The human-readable description for the step.
    :param outputs: A dict linking response IDs to response object types.
    :return: A schema for the step.
    """

    def step_decorator(func: _step_decorator_param) -> schema.StepSchema[InputT]:
        if id == "":
            raise BadArgumentException("Steps cannot have an empty ID")
        if name == "":
            raise BadArgumentException("Steps cannot have an empty name")
        sig = inspect.signature(func)
        if len(sig.parameters) != 1:
            raise BadArgumentException("The '%s' (id: %s) step must have exactly one parameter" % (name, id))
        input_param = list(sig.parameters.values())[0]
        if input_param.annotation is inspect.Parameter.empty:
            raise BadArgumentException("The '%s' (id: %s) step parameter must have a type annotation" % (name, id))
        if isinstance(input_param.annotation, str):
            raise BadArgumentException("Stringized type annotation encountered in %s (id: %s). Please make sure you "
                                       "don't import annotations from __future__ to avoid this problem." % (name, id))

        new_responses: Dict[str, schema.ObjectType] = {}
        for response_id in list(outputs.keys()):
            new_responses[response_id] = build_object_schema(outputs[response_id], True)

        return schema.StepSchema(
            id,
            name,
            description,
            input=build_object_schema(input_param.annotation, True),
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


class SchemaBuildException(Exception):
    def __init__(self, path: typing.Tuple[str], msg: str):
        self.path = path
        self.msg = msg

    def __str__(self) -> str:
        if len(self.path) == 0:
            return "Invalid schema definition: %s" % self.msg
        return "Invalid schema definition for %s: %s" % (" -> ".join(self.path), self.msg)


class _Resolver:
    @classmethod
    def resolve(cls, t: any) -> schema.AbstractType:
        path: typing.List[str] = []
        if hasattr(t, "__name__"):
            path.append(t.__name__)

        return cls._resolve_abstract_type(t, tuple(path))

    @classmethod
    def _resolve_abstract_type(cls, t: any, path: typing.Tuple[str]) -> schema.AbstractType:
        result = cls._resolve(t, path)
        if isinstance(result, schema.Field):
            res: schema.Field = result
            new_path = list(path)
            new_path.append(res.name)
            raise SchemaBuildException(
                tuple(new_path),
                "Unsupported attribute combination, you can only use typing.Optional, etc. in classes, but not in "
                "lists, dicts, etc." % res.name
            )
        res: schema.AbstractType = result
        return res

    @classmethod
    def _resolve_field(cls, t: any, path: typing.Tuple[str]) -> schema.Field:
        result = cls._resolve(t, path)
        if not isinstance(result, schema.Field):
            result = schema.Field(
                result
            )
        return result

    @classmethod
    def _resolve(cls, t: any, path: typing.Tuple[str]) -> typing.Union[schema.AbstractType, schema.Field]:
        if isinstance(t, type):
            return cls._resolve_type(t, path)
        elif isinstance(t, str):
            return cls._resolve_string(t, path)
        elif isinstance(t, bool):
            return cls._resolve_bool(t, path)
        elif isinstance(t, int):
            return cls._resolve_int(t, path)
        elif isinstance(t, float):
            return cls._resolve_float(t, path)
        elif isinstance(t, list):
            return cls._resolve_list(t, path)
        elif isinstance(t, dict):
            return cls._resolve_dict(t, path)
        elif get_origin(t) == list:
            return cls._resolve_list_annotation(t, path)
        elif get_origin(t) == dict:
            return cls._resolve_dict_annotation(t, path)
        elif get_origin(t) == typing.Union:
            return cls._resolve_union(t, path)
        elif get_origin(t) == typing.Annotated:
            return cls._resolve_annotated(t, path)
        else:
            raise SchemaBuildException(path, "Unable to resolve underlying type: %s" % type(t).__name__)

    @classmethod
    def _resolve_type(cls, t, path: typing.Tuple[str]):
        if issubclass(t, Enum):
            return _Resolver._resolve_enum(t, path)
        elif t == re.Pattern:
            return _Resolver._resolve_pattern(t, path)
        elif t == str:
            return _Resolver._resolve_string_type(t, path)
        elif t == bool:
            return _Resolver._resolve_bool_type(t, path)
        elif t == int:
            return _Resolver._resolve_int_type(t, path)
        elif t == float:
            return _Resolver._resolve_float_type(t, path)
        elif t == list:
            return _Resolver._resolve_list_type(t, path)
        elif get_origin(t) == dict:
            return _Resolver._resolve_dict_annotation(t, path)
        elif t == dict:
            return _Resolver._resolve_dict_type(t, path)
        return _Resolver._resolve_class(t, path)

    @classmethod
    def _resolve_enum(cls, t, path: typing.Tuple[str]) -> schema.AbstractType:
        try:
            return schema.EnumType(
                t
            )
        except Exception as e:
            raise SchemaBuildException(path, "Constraint exception while creating enum type") from e

    @classmethod
    def _resolve_dataclass_field(cls, t: dataclasses.Field, path: typing.Tuple[str]) -> typing.Tuple[str, schema.Field]:
        underlying_type = cls._resolve_field(t.type, path)
        if underlying_type.name == "":
            meta_name = t.metadata.get("name")
            if meta_name != "" and meta_name is not None:
                underlying_type.name = meta_name
            else:
                underlying_type.name = t.name
        meta_id = t.metadata.get("id")
        if meta_id is None:
            meta_id = t.name
        else:
            underlying_type.field_override = t.name
        meta_description = t.metadata.get("description")
        if meta_description != "" and meta_description is not None:
            underlying_type.description = meta_description
        if t.default != dataclasses.MISSING or t.default_factory != dataclasses.MISSING:
            underlying_type.required = False
        elif not underlying_type.required:
            raise SchemaBuildException(
                path,
                "Field is marked as optional, but does not have a default value set. "
                "Please set a default value for this field."
            )
        return meta_id, underlying_type

    @classmethod
    def _resolve_class(cls, t, path: typing.Tuple[str]) -> schema.AbstractType:
        final_fields: Dict[str, Field] = {}

        try:
            fields_list = fields(t)
        except TypeError as e:
            unsupported_types = {
                tuple: "tuples",
                complex: "complex numbers",
                bytes: "bytes",
                bytearray: "bytearrays",
                range: "banges",
                memoryview: "memoryviews",
                set: "sets",
                frozenset: "frozensets",
                GenericAlias: "generic aliases",
                types.ModuleType: "modules",
            }
            for unsupported_type, unsupported_type_name in unsupported_types.items():
                if isinstance(t, unsupported_type) or t == unsupported_type:
                    raise SchemaBuildException(
                        path,
                        "{} are not supported by the Arcaflow typing system and cannot be used in input or output data"
                        "types. Please use one of the supported types, or file an issue at {} with your use case to "
                        "get them included.".format(
                            unsupported_type_name,
                            _issue_url
                        )
                    )
            raise SchemaBuildException(
                path,
                "{} is not a dataclass or a supported type. Please use the @dataclasses.dataclass decorator on your "
                "class or use a supported native type. If this is a native Python type and you want to request support "
                "for it in the Arcaflow SDK, please open an issue at {} to get it included.".format(
                    t.__name__,
                    _issue_url
                ),
            ) from e

        for f in fields_list:
            new_path = list(path)
            new_path.append(f.name)
            name, final_field = cls._resolve_dataclass_field(f, tuple(new_path))
            final_fields[name] = final_field

        try:
            return schema.ObjectType(
                t,
                final_fields,
            )
        except Exception as e:
            raise SchemaBuildException(path, "Failed to create object type: {}".format(e.__str__())) from e

    @classmethod
    def _resolve_bool_type(cls, t, path: typing.Tuple[str]) -> schema.BoolType:
        try:
            return schema.BoolType()
        except Exception as e:
            raise SchemaBuildException(path, "Constraint exception while creating bool type") from e

    @classmethod
    def _resolve_bool(cls, t, path: typing.Tuple[str]) -> schema.BoolType:
        try:
            return schema.BoolType()
        except Exception as e:
            raise SchemaBuildException(path, "Constraint exception while creating bool type") from e

    @classmethod
    def _resolve_string_type(cls, t, path: typing.Tuple[str]) -> schema.StringType:
        try:
            return schema.StringType()
        except Exception as e:
            raise SchemaBuildException(path, "Constraint exception while creating string type") from e

    @classmethod
    def _resolve_string(cls, t, path: typing.Tuple[str]) -> schema.StringType:
        try:
            return schema.StringType()
        except Exception as e:
            raise SchemaBuildException(path, "Constraint exception while creating string type") from e

    @classmethod
    def _resolve_int(cls, t, path: typing.Tuple[str]) -> schema.IntType:
        try:
            return schema.IntType()
        except Exception as e:
            raise SchemaBuildException(path, "Constraint exception while creating int type") from e

    @classmethod
    def _resolve_int_type(cls, t, path: typing.Tuple[str]) -> schema.IntType:
        try:
            return schema.IntType()
        except Exception as e:
            raise SchemaBuildException(path, "Constraint exception while creating int type") from e

    @classmethod
    def _resolve_float(cls, t, path: typing.Tuple[str]) -> schema.FloatType:
        try:
            return schema.FloatType()
        except Exception as e:
            raise SchemaBuildException(path, "Constraint exception while creating float type") from e

    @classmethod
    def _resolve_float_type(cls, t, path: typing.Tuple[str]) -> schema.FloatType:
        try:
            return schema.FloatType()
        except Exception as e:
            raise SchemaBuildException(path, "Constraint exception while creating float type") from e

    @classmethod
    def _resolve_annotated(cls, t, path: typing.Tuple[str]):
        args = get_args(t)
        if len(args) < 2:
            raise SchemaBuildException(
                path,
                "At least one validation parameter required for typing.Annotated"
            )
        new_path = list(path)
        new_path.append("typing.Annotated")
        path = tuple(new_path)
        underlying_t = cls._resolve(args[0], path)
        for i in range(1, len(args)):
            new_path = list(path)
            new_path.append(str(i))
            if not isinstance(args[i], typing.Callable):
                raise SchemaBuildException(tuple(new_path), "Annotation is not callable")
            try:
                underlying_t = args[i](underlying_t)
            except Exception as e:
                raise SchemaBuildException(
                    tuple(new_path),
                    "Failed to execute Annotated argument: {}".format(e.__str__()),
                ) from e
        return underlying_t

    @classmethod
    def _resolve_list(cls, t, path: typing.Tuple[str]) -> schema.AbstractType:
        raise SchemaBuildException(
            path,
            "List type without item type definition encountered, please declare your lists like this: "
            "typing.List[str]"
        )

    @classmethod
    def _resolve_list_type(cls, t, path: typing.Tuple[str]) -> schema.AbstractType:
        raise SchemaBuildException(
            path,
            "List type without item type definition encountered, please declare your lists like this: "
            "typing.List[str]"
        )

    @classmethod
    def _resolve_list_annotation(cls, t, path: typing.Tuple[str]):
        args = get_args(t)
        if len(args) != 1:
            raise SchemaBuildException(
                path,
                "List type without item type definition encountered, please declare your lists like this: "
                "typing.List[str]"
            )
        new_path = list(path)
        new_path.append("items")
        try:
            return schema.ListType(
                cls._resolve_abstract_type(args[0], tuple(new_path))
            )
        except Exception as e:
            raise SchemaBuildException(path, "Failed to create list type") from e

    @classmethod
    def _resolve_dict(cls, t, path: typing.Tuple[str]) -> schema.AbstractType:
        raise SchemaBuildException(
            path,
            "Dict type without item type definition encountered, please declare your dicts like this: "
            "typing.Dict[str, int]"
        )

    @classmethod
    def _resolve_dict_type(cls, t, path: typing.Tuple[str]) -> schema.AbstractType:
        raise SchemaBuildException(
            path,
            "Dict type without item type definition encountered, please declare your dicts like this: "
            "typing.Dict[str, int]"
        )

    @classmethod
    def _resolve_dict_annotation(cls, t, path: typing.Tuple[str]):
        args = get_args(t)
        if len(args) != 2:
            raise SchemaBuildException(
                path,
                "Dict type without item type definition encountered, please declare your dicts like this: "
                "typing.Dict[str, int]"
            )
        keys_path = list(path)
        keys_path.append("keys")
        key_schema: schema.AbstractType = cls._resolve_abstract_type(args[0], tuple(keys_path))

        values_path = list(path)
        values_path.append("values")
        value_schema = cls._resolve_abstract_type(args[1], tuple(values_path))

        try:
            return schema.MapType(
                key_schema,
                value_schema,
            )
        except Exception as e:
            raise SchemaBuildException(path, "Failed to create map type") from e

    @classmethod
    def _resolve_union(cls, t, path: typing.Tuple[str]) -> schema.OneOfType:
        args = get_args(t)
        try:
            if isinstance(None, args[0]):
                raise SchemaBuildException(path, "None types are not supported.")
        except TypeError:
            pass
        try:
            if isinstance(None, args[1]):
                new_path = list(path)
                new_path.append("typing.Optional")
                result = cls._resolve_field(args[0], tuple(path))
                result.required = False
                return result
        except TypeError:
            pass
        result = schema.OneOfType(
            "_type",
            schema.StringType(),
            {}
        )
        for i in range(len(args)):
            new_path = list(path)
            new_path.append("typing.Union")
            new_path.append(str(i))
            f = cls._resolve_field(args[i], tuple(new_path))
            if not f.required:
                raise SchemaBuildException(
                    tuple(new_path),
                    "Union types cannot contain optional values."
                )
            if len(f.required_if) != 0:
                raise SchemaBuildException(
                    tuple(new_path),
                    "Union types cannot simultaneously contain require_if fields"
                )
            if len(f.required_if_not) != 0:
                raise SchemaBuildException(
                    tuple(new_path),
                    "Union types cannot simultaneously contain require_if_not fields"
                )
            if len(f.conflicts) != 0:
                raise SchemaBuildException(
                    tuple(new_path),
                    "Union types cannot simultaneously contain conflicts fields"
                )
            if f.type.type_id() != TypeID.OBJECT:
                raise SchemaBuildException(
                    tuple(new_path),
                    "Union types can only contain objects, {} found".format(f.type.type_id())
                )
            t:schema.ObjectType = f.type
            result.one_of[t.type_class().__name__] = t
        return result

    @classmethod
    def _resolve_pattern(cls, t, path):
        try:
            return schema.PatternType()
        except Exception as e:
            raise SchemaBuildException(path, "Failed to create pattern type") from e


class _ObjectTypeWrapper(schema.ObjectType):
    def unserialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> ObjectT:
        new_path = list(path)
        new_path.append(self.cls.__name__)
        return super().unserialize(data, tuple(new_path))

    def validate(self, data: TypeT, path: typing.Tuple[str] = tuple([])):
        new_path = list(path)
        new_path.append(self.cls.__name__)
        return super().validate(data, tuple(new_path))

    def serialize(self, data: ObjectT, path: typing.Tuple[str] = tuple([])) -> Any:
        new_path = list(path)
        new_path.append(self.cls.__name__)
        return super().serialize(data, tuple(new_path))


def build_object_schema(t, _skip_object_wrapping: bool = False) -> schema.ObjectType:
    """
    This function builds a schema for a single object. This is useful when serializing input parameters into a file
    for underlying tools to use, or unserializing responses from underlying tools into output data types.

    :param t: the type to build a schema for.
    :param _skip_object_wrapping: skip adding a wrapper object to include the type name in the path of any errors.
    :return: the built object schema
    """
    r = _Resolver.resolve(t)
    if not isinstance(r, schema.ObjectType):
        raise SchemaBuildException(tuple([]), "Response type is not an object.")
    if _skip_object_wrapping:
        return r
    return _ObjectTypeWrapper(r.cls, r.properties)


def run(
        s: schema.Schema,
        argv: List[str] = tuple(argv),
        stdin: io.TextIOWrapper = stdin,
        stdout: io.TextIOWrapper = stdout,
        stderr: io.TextIOWrapper = stderr
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
            "--json-schema",
            dest="json_schema",
            help="Print JSON schema for either the input or the output.",
            metavar="KIND",
        )
        parser.add_option(
            "--http",
            dest="http",
            help="Run a HTTP microservice.",
            metavar="LISTEN_PORT",
        )
        parser.add_option(
            "-s",
            "--step",
            dest="step",
            help="Which step to run? One of: " + ', '.join(s.steps.keys()),
            metavar="STEPID",
        )
        parser.add_option(
            "-d",
            "--debug",
            action="store_true",
            dest="debug",
            help="Enable debug mode (print step output and stack traces)."
        )
        (options, remaining_args) = parser.parse_args(list(argv[1:]))
        if len(remaining_args) > 0:
            raise _ExitException(
                64,
                "Unable to parse arguments: [" + ', '.join(remaining_args) + "]\n" + parser.get_usage()
            )
        action = None
        if options.filename is not None:
            action = "file"
        if options.json_schema is not None:
            if action is not None:
                raise _ExitException(
                    64,
                    "--file and --json-schema cannot be used together"
                )
            action = "json-schema"
        if options.http is not None:
            if action is not None:
                raise _ExitException(
                    64,
                    "--{} and --http cannot be used together".format(action)
                )
            action = "http"
        if action is None:
            raise _ExitException(
                64,
                "At least one of --file, --json-schema, or --http must be specified"
            )

        if action == "file" or action == "json-schema":
            if len(s.steps) > 1 and options.step is None:
                raise _ExitException(64, "-s|--step is required\n" + parser.get_usage())
            if options.step is not None:
                step_id = options.step
            else:
                step_id = list(s.steps.keys())[0]
        if action == "file":
            return _execute_file(step_id, s, options, stdin, stdout, stderr)
        elif action == "json-schema":
            return _print_json_schema(step_id, s, options, stdout)
        elif action == "http":
            return _run_server(options.http, s, stdin, stdout, stderr)
    except serialization.LoadFromFileException as e:
        stderr.write(e.msg + '\n')
        return 64
    except _ExitException as e:
        stderr.write(e.msg + '\n')
        return e.exit_code


def build_schema(*args: schema.StepSchema) -> schema.Schema:
    """
    This function takes functions annotated with @plugin.step and creates a schema from them.
    :param args: the steps to be added to the schema
    :return: a callable schema
    """
    steps_by_id: Dict[str, schema.StepSchema] = {}
    for step in args:
        if step.id in steps_by_id:
            raise BadArgumentException("Duplicate step ID %s" % step.id)
        steps_by_id[step.id] = step
    return schema.Schema(
        steps_by_id
    )


def _execute_file(
        step_id: str,
        s: schema.StepSchema,
        options,
        stdin: io.TextIOWrapper,
        stdout: io.TextIOWrapper,
        stderr: io.TextIOWrapper
) -> int:
    filename: str = options.filename
    if filename == "-":
        data = serialization.load_from_stdin(stdin)
    else:
        data = serialization.load_from_file(filename)
    original_stdout = sys.stdout
    original_stderr = sys.stderr
    if options.debug:
        # Redirect stdout to stderr for debug logging
        sys.stdout = stderr
        sys.stderr = stderr
    else:
        out_buffer = io.StringIO()
        sys.stdout = out_buffer
        sys.stderr = out_buffer
    try:
        output_id, output_data = s(step_id, data)
        output = {
            "output_id": output_id,
            "output_data": output_data
        }
        stdout.write(yaml.dump(output, sort_keys=False))
        return 0
    except InvalidInputException as e:
        stderr.write(
            "Invalid input encountered while executing step '{}' from file '{}':\n  {}\n\n".format(
                step_id,
                filename,
                e.__str__()
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
                step_id,
                filename,
                e.__str__()
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
    if options.json_schema == "input":
        data = jsonschema.step_input(s.steps[step_id])
    elif options.json_schema == "output":
        data = jsonschema.step_outputs(s.steps[step_id])
    else:
        raise _ExitException(64, "--json-schema must be one of 'input' or 'output'")
    stdout.write(json.dumps(data, indent="  "))
    return 0


def _run_server(listen, s, stdin, stdout, stderr):
    stdout.write("Starting HTTP server at {}...\n".format(listen))
    stdout.write("Warning! This mode is experimental and may change or be discontinued at any time!\n")
    http.run(listen, s)


def test_object_serialization(
        dc,
        fail: typing.Optional[Callable[[str], None]] = None,
        t: typing.Optional[schema.ObjectType] = None
):
    """
    This function aids serialization by first serializing, then unserializing the passed parameter according to the
    passed schema. It then compares that the two objects are equal.
    :param dc: the dataclass to use for tests.
    :param t: the schema for the dataclass. If none is passed, the schema is built automatically using
    plugin.build_object_schema()
    """
    try:
        if t is None:
            t = build_object_schema(dc.__class__)
        path: typing.Tuple[str] = tuple([dc.__class__.__name__])
        t.validate(dc, path)
        serialized_data = t.serialize(dc, path)
        unserialized_data = t.unserialize(serialized_data, path)
        if unserialized_data != dc:
            raise Exception(
                "After serializing and unserializing {}, the data mismatched. Serialized data was: {}".format(
                    dc.__name__,
                    serialized_data
                )
            )
    except Exception as e:
        result = "Your object serialization test for {} failed.\n\n" \
                 "This means that your object cannot be properly serialized by the SDK. There are three possible " \
                 "reasons for this:\n\n" \
                 "1. Your has a field type in it that the SDK doesn't support\n" \
                 "2. Your sample data is invalid according to your own rules\n" \
                 "3. There is a bug in the SDK (please report it)\n\n" \
                 "Check the error message below for details.\n\n" \
                 "---\n\n".format(type(dc).__name__, traceback.extract_stack())
        result += "Error message:\n" + e.__str__() + "\n\n"
        result += "Input:\n" + pprint.pformat(dataclasses.asdict(dc)) + "\n\n"
        result += "---\n\n"
        result += "Your object serialization test for {} failed. Please scroll up for details.\n\n".format(
            type(dc).__name__
        )
        if fail is None:
            print(result)
            sys.exit(1)
        fail(result)
