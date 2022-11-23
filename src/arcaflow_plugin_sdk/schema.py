"""
This module provides the tools to build a schema, either by hand, or automatically from type hints and annotations.

Using
=====

For using this module please check the documentation located at
https://arcalot.github.io/arcaflow/creating-plugins/python/

Contributing
============

This module is structured by using region folding delimiters (e.g. # region SomeRegion). You can use an editor that
supports these regions to collapse them together for easier access. The module has the following regions:

Exceptions
----------

This region defines a number of exceptions that can be raised either while building a schema or while working with
input and output.

Annotations
-----------

Annotations can be applied to dataclass fields to convey extra information (e.g. minimum or maximum values). These
annotations are used when building a schema. Note that, unlike tools like Pydantic we do not validate the dataclass
fields in-situ. Instead, we only add metadata information which is verified when the data is serialized or unserialized.
This late verification is done to avoid confusing error messages when using third party code that may be constructing
the dataclasses with intermediate states that are temporarily invalid.

Type aliases
------------

This section defines a number of type aliases for use in the schema itself.

Schema
------

This section contains the data model for the Arcaflow schema definition itself. The schema is self-describing and
can be serialized and unserialized with the same tools that you use to serialize or unserialize data that conforms to
the schema. The plugin system uses these dataclasses to construct the ``SCOPE_SCHEMA`` and ``SCHEMA_SCHEMA`` variables,
which you can use to serialize and unserialize a scope definition or an entire schema.

Types
-----

Types are the implementations of a schema. They add serialization, validation, and unserialization to the schema itself.
When working with schemas in practice you will want to work with types unless you don't need to unserialize data.

Build
-----

This region holds the tools to automatically build a schema from annotations. Use ``build_object_schema`` to build a schema
from a dataclass.

Note that the built schema will be a type, not a schema, so it is possible to use these schemas for serializing and
unserializing data.

Schema schemas
--------------

This region holds two variables, ``SCOPE_SCHEMA`` and ``SCHEMA_SCHEMA``. You can use these variables to work with
schemas themselves. For example, you can create a client that calls Arcaflow plugins and use these classes to
unserialize schemas that the plugins send.
"""
import collections
import dataclasses
import enum
import inspect
import json
import math
import pprint
import re
import sys
import traceback
import types
import typing
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from re import Pattern
from typing import Any, Callable, Dict, Generic, List, Optional, Type, TypeVar

_issue_url = "https://github.com/arcalot/arcaflow-plugin-sdk-python/issues"


# region Exceptions


@dataclass
class ConstraintException(Exception):
    """
    ``ConstraintException`` indicates that the passed data violated one or more constraints defined in the schema.
    The message holds the exact path of the problematic field, as well as a message explaining the error.
    If this error is not easily understood, please open an issue on the Arcaflow plugin SDK.
    """

    path: typing.Tuple[str] = tuple([])
    msg: str = ""

    def __str__(self):
        if len(self.path) == 0:
            return "Validation failed: {}".format(self.msg)
        return "Validation failed for '{}': {}".format(" -> ".join(self.path), self.msg)


@dataclass
class NoSuchStepException(Exception):
    """
    ``NoSuchStepException`` indicates that the given step is not supported by the plugin.
    """

    step: str

    def __str__(self):
        return "No such step: %s" % self.step


@dataclass
class BadArgumentException(Exception):
    """
    BadArgumentException indicates that an invalid configuration was passed to a schema component. The message will
    explain what exactly the problem is, but may not be able to locate the exact error as the schema may be manually
    built.
    """

    msg: str

    def __str__(self):
        return self.msg


@dataclass
class InvalidAnnotationException(Exception):
    """
    ``InvalidAnnotationException`` indicates that an annotation was used on a type it does not support.
    """

    annotation: str
    msg: str

    def __str__(self):
        return "Invalid {} annotation: {}".format(self.annotation, self.msg)


class SchemaBuildException(Exception):
    """
    SchemaBuildException indicates an error while building the schema using type inspection. This exception holds the
    path to the parameter that caused the problem. The message should be easily understood, if you are having trouble
    with an error message, please open a ticket on the Arcaflow plugin SDK.
    """

    def __init__(self, path: typing.Tuple[str], msg: str):
        self.path = path
        self.msg = msg

    def __str__(self) -> str:
        if len(self.path) == 0:
            return "Invalid schema definition: %s" % self.msg
        return "Invalid schema definition for %s: %s" % (
            " -> ".join(self.path),
            self.msg,
        )


class InvalidInputException(Exception):
    """
    This exception indicates that the input data for a given step didn't match the schema. The embedded
    ``ConstraintException`` holds the details of this failure.
    """

    constraint: ConstraintException

    def __init__(self, cause: ConstraintException):
        self.constraint = cause

    def __str__(self):
        return self.constraint.__str__()


class InvalidOutputException(Exception):
    """
    This exception indicates that the output of a schema was invalid. This is always a bug in the plugin and should
    be reported to the plugin author.
    """

    constraint: ConstraintException

    def __init__(self, cause: ConstraintException):
        self.constraint = cause

    def __str__(self):
        return self.constraint.__str__()


class UnitParseException(Exception):
    """
    This exception indicates that it failed to parse a unit string.
    """

    msg: str

    def __init__(self, msg: str):
        self.msg = msg

    def __str__(self) -> str:
        return self.msg


# endregion

# region Annotations


def id(id: str):
    """
    The id annotation can be used to change the serialized name of an object field. This is useful when a field
    must be serialized to a name that is not a valid Python field.

    **Example:**

    Imports:

    >>> from dataclasses import dataclass
    >>> from arcaflow_plugin_sdk import schema

    Define your dataclass:

    >>> @dataclass
    ... class YourDataClass:
    ...     some_field: typing.Annotated[str, schema.id("some-field")]

    Build your schema:

    >>> s = schema.build_object_schema(YourDataClass)

    Now unserialize your data:

    >>> unserialized_data = s.unserialize({"some-field": "foo"})

    This should now print "foo":

    >>> print(unserialized_data.some_field)
    foo

    :param id: The field to use
    :return: Callable
    """

    def call(t):
        t.__id = id
        return t

    return call


_id = id


def name(name: str):
    """
    The name annotation can be applied on any dataclass field, or on Union types to add a human-readable name to the
    field. It is used as a form field or as part of a dropdown box in a form.

    **Example:**

    Imports:

    >>> from dataclasses import dataclass
    >>> from arcaflow_plugin_sdk import schema

    Define your dataclass:

    >>> @dataclass
    ... class YourDataClass:
    ...     some_field: typing.Annotated[str, schema.name("Some field")]

    Build your schema:

    >>> s = schema.build_object_schema(YourDataClass)

    Print field name:

    >>> s["YourDataClass"].properties["some_field"].display.name
    'Some field'

    :param name: The name to apply
    :return: Callable
    """

    def call(t):
        t.__name = name
        return t

    return call


_name = name


def description(description: str):
    """
    The description annotation can be applied on any dataclass field, or on Union types to add a human-readable
    description to the field. It can contain line breaks and links for formatting. It is used as a form field
    description text or as part of a dropdown box in a form.

    **Example:**

    Imports:

    >>> from dataclasses import dataclass
    >>> from arcaflow_plugin_sdk import schema

    Define your dataclass:

    >>> @dataclass
    ... class YourDataClass:
    ...     some_field: typing.Annotated[str, schema.description("This is a string field")]

    Build your schema:

    >>> s = schema.build_object_schema(YourDataClass)

    Print field description:

    >>> s["YourDataClass"].properties["some_field"].display.description
    'This is a string field'

    :param description: The description to apply
    :return: Callable
    """

    def call(t):
        t.__description = description
        return t

    return call


_description = description


def icon(icon: str):
    """
    The icon annotation can be applied to any dataclass field, or on Union types to add a 64x64 pixel SVG icon to the
    item on display. However, the SVG must not contain any external sources or namespaces in order to work correctly.

    **Example:**

    Imports:

    >>> from dataclasses import dataclass
    >>> from arcaflow_plugin_sdk import schema

    Define your dataclass:

    >>> @dataclass
    ... class YourDataClass:
    ...     some_field: typing.Annotated[str, schema.icon("<svg></svg>")]

    Build your schema:

    >>> s = schema.build_object_schema(YourDataClass)

    Print field icon:

    >>> s["YourDataClass"].properties["some_field"].display.icon
    '<svg></svg>'
    """

    def call(t):
        t.__icon = icon
        return t

    return call


_icon = icon


def units(units: typing.ForwardRef("Units")):
    """
    This annotation lets you add unit definitions to int and float fields. This helps with determining how to treat that
    number, but also contains scaling information for creating a nicely formatted string, such as 5m30s.

    **Example:**

    Imports:

    >>> from dataclasses import dataclass
    >>> from arcaflow_plugin_sdk import schema

    Define your dataclass:

    >>> @dataclass
    ... class YourDataClass:
    ...     some_field: typing.Annotated[int, schema.units(schema.UNIT_BYTE)]

    Build your schema:

    >>> s = schema.build_object_schema(YourDataClass)

    Print format data:

    >>> s["YourDataClass"].properties["some_field"].type.units.format_short(5)
    '5B'
    """

    def call(t):
        """
        :param typing.Union[IntSchema, FloatSchema] t:
        :return typing.Union[IntSchema, FloatSchema]:
        """
        effective_t = t
        if isinstance(t, PropertySchema):
            effective_t = t.type
        if not isinstance(effective_t, IntSchema) and not isinstance(
            effective_t, FloatSchema
        ):
            raise InvalidAnnotationException(
                "units",
                "expected int or float schema, found {}".format(type(t).__name__),
            )
        effective_t.units = units
        return t

    return call


_units = units


def example(
    example: typing.Any,
) -> typing.Callable[
    [typing.ForwardRef("PropertySchema")], typing.ForwardRef("PropertySchema")
]:
    """
    This annotation provides the option to add an example to a type.
    :param example: the example as raw type, serializable by json.dumps. Do not use dataclasses

    **Example:**

    Imports:

    >>> from arcaflow_plugin_sdk import schema
    >>> from dataclasses import dataclass
    >>> import typing

    Your dataclass:

    >>> @dataclass
    ... class YourDataClass:
    ...     some_field: typing.Annotated[
    ...         typing.Dict[str, str],
    ...         schema.example({"foo":"bar"})
    ...     ]

    Build your schema:

    >>> s = schema.build_object_schema(YourDataClass)

    Print field description:

    >>> s["YourDataClass"].properties["some_field"].examples
    ['{"foo": "bar"}']
    """
    try:
        marshalled_example = json.dumps(example)
    except Exception as e:
        raise InvalidAnnotationException(
            "example", "expected a JSON-serializable type, {}".format(e.__str__())
        ) from e

    def call(t):
        if isinstance(t, PropertyType):
            if t.examples is None:
                t.examples = list()
            t.examples.append(marshalled_example)
        else:
            if not hasattr(t, "__examples") or t.__examples is None:
                t.__examples = list()
            t.__examples.append(marshalled_example)
        return t

    return call


_example = example

discriminatorT = typing.TypeVar(
    "discriminatorT",
    bound=typing.Union[
        typing.ForwardRef("OneOfStringSchema"), typing.ForwardRef("OneOfIntSchema")
    ],
)
discriminatorFunc = typing.Callable[
    [
        typing.Union[
            typing.ForwardRef("OneOfStringSchema"), typing.ForwardRef("OneOfIntSchema")
        ]
    ],
    typing.Union[
        typing.ForwardRef("OneOfStringSchema"), typing.ForwardRef("OneOfIntSchema")
    ],
]


def discriminator(discriminator_field_name: str) -> discriminatorFunc:
    """
    This annotation is used to manually set the discriminator field on a Union type.
    :param discriminator_field_name: the name of the discriminator field.
    :return: the callable decorator

    **Example:**

    Imports:

    >>> from arcaflow_plugin_sdk import schema
    >>> from dataclasses import dataclass
    >>> import typing

    Your dataclasses:

    >>> @dataclass
    ... class A:
    ...     a: str
    >>> @dataclass
    ... class B:
    ...     b: str
    >>> @dataclass
    ... class YourDataClass:
    ...     some_field: typing.Annotated[
    ...         typing.Union[A, B],
    ...         schema.discriminator("foo")
    ...     ]

    Build your schema:

    >>> s = schema.build_object_schema(YourDataClass)

    You can now deserialize a dataset:

    >>> unserialized_data = s.unserialize({"some_field": {"foo": "A", "a": "Hello world!"}})
    >>> unserialized_data.some_field.a
    'Hello world!'
    """

    def call(t):
        """
        :param typing.Union[OneOfStringSchema, OneOfIntSchema] t:
        :return typing.Union[OneOfStringSchema, OneOfIntSchema]:
        """
        if not isinstance(t, OneOfStringSchema) and not isinstance(t, OneOfIntSchema):
            raise InvalidAnnotationException(
                "discriminator",
                "expected a property or object type with union member, found {}".format(
                    type(t).__name__,
                ),
            )
        oneof: typing.Union[OneOfStringSchema, OneOfIntSchema] = t

        one_of: typing.Dict[typing.Union[int, str], RefSchema] = {}
        if isinstance(t, OneOfStringSchema):
            discriminator_field_schema = StringType()
        elif isinstance(t, OneOfIntSchema):
            discriminator_field_schema = IntType()
        else:
            raise BadArgumentException(
                "Unsupported discriminator type: {}".format(type(t))
            )
        for key, item in oneof.types.items():
            if hasattr(item, "__discriminator_value"):
                one_of[item.__discriminator_value] = item
            else:
                one_of[key] = item

        oneof.discriminator_field_name = discriminator_field_name

        for key, item in oneof.types.items():
            try:
                discriminator_field_schema.validate(key)
            except ConstraintException as e:
                raise BadArgumentException(
                    "The discriminator value has an invalid value: {}. "
                    "Please check your annotations.".format(e.__str__())
                ) from e

        return oneof

    return call


_discriminator = discriminator


def discriminator_value(discriminator_value: typing.Union[str, int, enum.Enum]):
    """
    This annotation adds a custom value for an instance of a discriminator. The value must match the discriminator field
     This annotation works only when used in conjunction with discriminator().

    :param discriminator_value: The value for the discriminator field.
    :return: The callable decorator

    **Example:**

    Imports:

    >>> from arcaflow_plugin_sdk import schema
    >>> from dataclasses import dataclass
    >>> import typing

    Your dataclasses:

    >>> @dataclass
    ... class A:
    ...     a: str
    >>> @dataclass
    ... class B:
    ...     b: str
    >>> @dataclass
    ... class YourDataClass:
    ...     some_field: typing.Union[
    ...         typing.Annotated[A, schema.discriminator_value("Foo")],
    ...         typing.Annotated[B, schema.discriminator_value("Bar")]
    ...     ]

    Build your schema:

    >>> s = schema.build_object_schema(YourDataClass)

    You can now deserialize a dataset:

    >>> unserialized_data = s.unserialize({"some_field": {"_type": "Foo", "a": "Hello world!"}})
    >>> unserialized_data.some_field.a
    'Hello world!'
    """

    def call(t):
        if (
            not isinstance(t, ObjectSchema)
            and not isinstance(t, RefSchema)
            and not isinstance(t, OneOfStringSchema)
            and not isinstance(t, OneOfIntSchema)
        ):
            raise InvalidAnnotationException(
                "discriminator_value",
                "discriminator_value is only valid for object types, not {}".format(
                    type(t).__name__
                ),
            )
        t.__discriminator_value = discriminator_value
        return t

    return call


_discriminator_value = discriminator_value

ValidatorT = TypeVar(
    "ValidatorT",
    bound=typing.Union[
        typing.ForwardRef("IntSchema"),
        typing.ForwardRef("FloatSchema"),
        typing.ForwardRef("StringSchema"),
        typing.ForwardRef("ListSchema"),
        typing.ForwardRef("MapSchema"),
        typing.ForwardRef("PropertySchema"),
    ],
)

Validator = Callable[[ValidatorT], ValidatorT]


def min(param: typing.Union[int, float]) -> Validator:
    """
    This decorator creates a minimum length (strings), minimum number (int, float), or minimum element count (lists and
    maps) validation.

    :param: The minimum number
    :return: the validator

    **Example:**

    Imports:

    >>> from arcaflow_plugin_sdk import schema
    >>> from dataclasses import dataclass
    >>> import typing

    Your dataclasses:

    >>> @dataclass
    ... class YourDataClass:
    ...     some_field: typing.Annotated[
    ...         int,
    ...         schema.min(5)
    ...     ]

    Build your schema:

    >>> s = schema.build_object_schema(YourDataClass)

    The unserialization will validate the field:

    >>> unserialized_data = s.unserialize({"some_field": 4})
    Traceback (most recent call last):
    ...
    arcaflow_plugin_sdk.schema.ConstraintException: Validation failed for 'YourDataClass -> some_field': Must be at least 5
    >>> unserialized_data = s.unserialize({"some_field": 42})
    >>> unserialized_data.some_field
    42
    """

    def call(t):
        effective_t = t
        if isinstance(t, PropertySchema):
            effective_t = t.type
        if hasattr(effective_t, "min"):
            effective_t.min = param
        else:
            raise BadArgumentException(
                "min is valid only for STRING, INT, FLOAT, LIST, and MAP types, not for {} types.".format(
                    t.__name__
                )
            )
        if isinstance(t, PropertySchema):
            t.type = effective_t
        return t

    return call


_min = min


def max(param: int) -> Validator:
    """
    This decorator creates a maximum length (strings), maximum number (int, float), or maximum element count (lists and
    maps) validation.

    :param param: The maximum number
    :return: the validator

    **Example:**

    Imports:

    >>> from arcaflow_plugin_sdk import schema
    >>> from dataclasses import dataclass
    >>> import typing

    Your dataclasses:

    >>> @dataclass
    ... class YourDataClass:
    ...     some_field: typing.Annotated[
    ...         int,
    ...         schema.max(5)
    ...     ]

    Build your schema:

    >>> s = schema.build_object_schema(YourDataClass)

    The unserialization will validate the field:

    >>> unserialized_data = s.unserialize({"some_field": 6})
    Traceback (most recent call last):
    ...
    arcaflow_plugin_sdk.schema.ConstraintException: Validation failed for 'YourDataClass -> some_field': Must be at \
most 5
    >>> unserialized_data = s.unserialize({"some_field": 4})
    >>> unserialized_data.some_field
    4
    """

    def call(t):
        """
        :param typing.Union[IntSchema, FloatSchema, StringSchema, ListSchema, MapSchema, PropertySchema] t:
        :return typing.Union[IntSchema, FloatSchema, StringSchema, ListSchema, MapSchema, PropertySchema]:
        """
        effective_t = t
        if isinstance(t, PropertySchema):
            effective_t = t.type
        if hasattr(effective_t, "max"):
            effective_t.max = param
        else:
            raise BadArgumentException(
                "max is valid only for STRING, INT, FLOAT, LIST, and MAP types, not for {} types.".format(
                    t.__name__
                )
            )
        if isinstance(t, PropertySchema):
            t.type = effective_t
        return t

    return call


_max = max


def pattern(pattern: Pattern) -> Validator:
    """
    This decorator creates a regular expression pattern validation for strings.

    :param pattern: The regular expression.
    :return: the validator

    **Example:**

    Imports:

    >>> from arcaflow_plugin_sdk import schema
    >>> from dataclasses import dataclass
    >>> import typing

    Your dataclasses:

    >>> @dataclass
    ... class YourDataClass:
    ...     some_field: typing.Annotated[
    ...         str,
    ...         schema.pattern(re.compile("^[a-z]+$"))
    ...     ]

    Build your schema:

    >>> s = schema.build_object_schema(YourDataClass)

    The unserialization will validate the field:

    >>> unserialized_data = s.unserialize({"some_field": "asdf1234"})
    Traceback (most recent call last):
    ...
    arcaflow_plugin_sdk.schema.ConstraintException: Validation failed for 'YourDataClass -> some_field': String must \
match the pattern ^[a-z]+$
    >>> unserialized_data = s.unserialize({"some_field": "asdf"})
    >>> unserialized_data.some_field
    'asdf'
    """

    def call(t):
        """
        :param typing.Union[StringSchema,PropertySchema] t:
        :return typing.Union[StringSchema,PropertySchema]:
        """
        effective_t = t
        if isinstance(t, PropertySchema):
            effective_t = t.type
        if hasattr(effective_t, "pattern"):
            effective_t.pattern = pattern
        else:
            raise BadArgumentException(
                "pattern is valid only for STRING types, not for {} types.".format(
                    t.__name__
                )
            )
        if isinstance(t, PropertySchema):
            t.type = effective_t
        return t

    return call


_pattern = pattern


def required_if(required_if: str) -> Validator:
    """
    This decorator creates a that marks the current field as required if the specified field is set.

    :param required_if: The other field to use.
    :return: the validator

    **Example:**

    Imports:

    >>> from arcaflow_plugin_sdk import schema
    >>> from dataclasses import dataclass
    >>> import typing

    Your dataclasses:

    >>> @dataclass
    ... class YourDataClass:
    ...     some_field: typing.Annotated[
    ...         typing.Optional[str],
    ...         schema.required_if("some_other_field")
    ...     ] = None
    ...     some_other_field: typing.Optional[str] = None

    Build your schema:

    >>> s = schema.build_object_schema(YourDataClass)

    This is a valid unserialization because both fields are unset:

    >>> unserialized_data = s.unserialize({})
    >>> unserialized_data.some_field
    >>> unserialized_data.some_other_field

    This is not valid because only some_other_field is set:

    >>> unserialized_data = s.unserialize({"some_other_field":"foo"})
    Traceback (most recent call last):
    ...
    arcaflow_plugin_sdk.schema.ConstraintException: Validation failed for 'YourDataClass -> some_field': This field is \
required because 'some_other_field' is set

    This is valid again:

    >>> unserialized_data = s.unserialize({"some_other_field":"foo", "some_field": "bar"})
    >>> unserialized_data.some_field
    'bar'
    >>> unserialized_data.some_other_field
    'foo'
    """

    def call(t: PropertySchema) -> PropertySchema:
        if not isinstance(t, PropertySchema):
            raise BadArgumentException(
                "required_if is only valid for properties on object types."
            )
        if t.required_if is None:
            require_if_list = list()
        else:
            require_if_list = list(t.required_if)
        require_if_list.append(required_if)
        t.required_if = require_if_list
        return t

    return call


_required_if = required_if


def required_if_not(required_if_not: str) -> Validator:
    """
    This decorator creates a validation that marks the current field as required if the specified field is not set. If
    there are multiple of these validators, the current field is only marked as required if none of the specified fields
    are provided.

    :param required_if_not: The other field to use.
    :return: the validator

    **Example:**

    Imports:

    >>> from arcaflow_plugin_sdk import schema
    >>> from dataclasses import dataclass
    >>> import typing

    Your dataclasses:

    >>> @dataclass
    ... class YourDataClass:
    ...     some_field: typing.Annotated[
    ...         typing.Optional[str],
    ...         schema.required_if_not("some_other_field")
    ...     ] = None
    ...     some_other_field: typing.Optional[str] = None

    Build your schema:

    >>> s = schema.build_object_schema(YourDataClass)

    This is not valid because neither of the fields are set:

    >>> unserialized_data = s.unserialize({})
    Traceback (most recent call last):
    ...
    arcaflow_plugin_sdk.schema.ConstraintException: Validation failed for 'YourDataClass -> some_field': This field is \
required because 'some_other_field' is not set

    This is valid because the some_other_field is set:

    >>> unserialized_data = s.unserialize({"some_other_field":"foo"})
    >>> unserialized_data.some_other_field
    'foo'

    This is also valid because some_field is set:

    >>> unserialized_data = s.unserialize({"some_field": "bar"})
    >>> unserialized_data.some_field
    'bar'

    Both fields can also be set:

    >>> unserialized_data = s.unserialize({"some_field": "bar", "some_other_field":"foo"})
    >>> unserialized_data.some_other_field
    'foo'
    >>> unserialized_data.some_field
    'bar'
    """

    def call(t: PropertySchema) -> PropertySchema:
        if not isinstance(t, PropertySchema):
            raise BadArgumentException(
                "required_if_not is only valid for fields on object types."
            )
        if t.required_if_not is None:
            required_if_not_list = list()
        else:
            required_if_not_list = list(t.required_if_not)
        required_if_not_list.append(required_if_not)
        t.required_if_not = required_if_not_list
        return t

    return call


_required_if_not = required_if_not


def conflicts(conflicts: str) -> Validator:
    """
    This decorator creates a validation that triggers if the current field on an object is set in parallel with the
    specified field.

    :param conflicts: The field to conflict with.
    :return: the validator

    **Example:**

    Imports:

    >>> from arcaflow_plugin_sdk import schema
    >>> from dataclasses import dataclass
    >>> import typing

    Your dataclasses:

    >>> @dataclass
    ... class YourDataClass:
    ...     some_field: typing.Annotated[
    ...         typing.Optional[str],
    ...         schema.conflicts("some_other_field")
    ...     ] = None
    ...     some_other_field: typing.Optional[str] = None

    Build your schema:

    >>> s = schema.build_object_schema(YourDataClass)

    This is valid because neither field is set:

    >>> unserialized_data = s.unserialize({})

    This is not valid because both fields are set:

    >>> unserialized_data = s.unserialize({"some_field": "bar", "some_other_field":"foo"})
    Traceback (most recent call last):
    ...
    arcaflow_plugin_sdk.schema.ConstraintException: Validation failed for 'YourDataClass -> some_field': Field \
conflicts 'some_other_field', set one of the two, not both

    This is valid because only the some_other_field is set:

    >>> unserialized_data = s.unserialize({"some_other_field":"foo"})
    >>> unserialized_data.some_other_field
    'foo'

    This is also valid because some_field is set:
    >>> unserialized_data = s.unserialize({"some_field": "bar"})
    >>> unserialized_data.some_field
    'bar'
    """

    def call(t):
        """
        :param PropertySchema t:
        :return PropertySchema:
        """
        if not isinstance(t, PropertySchema):
            raise BadArgumentException(
                "conflicts is only valid for fields on object types."
            )
        if t.conflicts is not None:
            conflicts_list = list(t.conflicts)
        else:
            conflicts_list = []
        conflicts_list.append(conflicts)
        t.conflicts = conflicts_list
        return t

    return call


_conflicts = conflicts

# endregion

# region Type aliases

ANY_TYPE = typing.Union[
    typing.List[typing.ForwardRef("ANY_TYPE")],
    typing.Dict[typing.ForwardRef("ANY_TYPE"), typing.ForwardRef("ANY_TYPE")],
    int,
    float,
    bool,
    str,
    type(None),
]
VALUE_TYPE = typing.Annotated[
    typing.Union[
        typing.Annotated[
            typing.ForwardRef("StringEnumSchema"),
            discriminator_value("enum_string"),
            name("String enum"),
        ],
        typing.Annotated[
            typing.ForwardRef("IntEnumSchema"),
            discriminator_value("enum_integer"),
            name("Integer enum"),
        ],
        typing.Annotated[
            typing.ForwardRef("StringSchema"),
            discriminator_value("string"),
            name("String"),
        ],
        typing.Annotated[
            typing.ForwardRef("PatternSchema"),
            discriminator_value("pattern"),
            name("Pattern"),
        ],
        typing.Annotated[
            typing.ForwardRef("IntSchema"),
            discriminator_value("integer"),
            name("Integer"),
        ],
        typing.Annotated[
            typing.ForwardRef("FloatSchema"),
            discriminator_value("float"),
            name("Float"),
        ],
        typing.Annotated[
            typing.ForwardRef("BoolSchema"), discriminator_value("bool"), name("Bool")
        ],
        typing.Annotated[
            typing.ForwardRef("ListSchema"), discriminator_value("list"), name("List")
        ],
        typing.Annotated[
            typing.ForwardRef("MapSchema"), discriminator_value("map"), name("Map")
        ],
        typing.Annotated[
            typing.ForwardRef("ScopeSchema"),
            discriminator_value("scope"),
            name("Scope"),
        ],
        typing.Annotated[
            typing.ForwardRef("ObjectSchema"),
            discriminator_value("object"),
            name("Object"),
        ],
        typing.Annotated[
            typing.ForwardRef("OneOfStringSchema"),
            discriminator_value("one_of_string"),
            name("Multiple with string key"),
        ],
        typing.Annotated[
            typing.ForwardRef("OneOfIntSchema"),
            discriminator_value("one_of_int"),
            name("Multiple with int key"),
        ],
        typing.Annotated[
            typing.ForwardRef("RefSchema"),
            discriminator_value("ref"),
            name("Object reference"),
        ],
        typing.Annotated[
            typing.ForwardRef("AnySchema"), discriminator_value("any"), name("Any")
        ],
    ],
    discriminator("type_id"),
]
MAP_KEY_TYPE = typing.Annotated[
    typing.Union[
        typing.Annotated[
            typing.ForwardRef("StringEnumSchema"),
            discriminator_value("enum_string"),
            name("String enum"),
        ],
        typing.Annotated[
            typing.ForwardRef("IntEnumSchema"),
            discriminator_value("enum_integer"),
            name("Integer enum"),
        ],
        typing.Annotated[
            typing.ForwardRef("StringSchema"),
            discriminator_value("string"),
            name("String"),
        ],
        typing.Annotated[
            typing.ForwardRef("IntSchema"),
            discriminator_value("integer"),
            name("Integer"),
        ],
    ],
    discriminator("type_id"),
]
ID_TYPE = typing.Annotated[
    str, min(1), max(255), pattern(re.compile("^[$@a-zA-Z0-9-_]+$"))
]
DISPLAY_TYPE = typing.Annotated[
    Optional[typing.ForwardRef("DisplayValue")],
    _name("Display options"),
    _description("Name, description and icon."),
]
DEFAULT_TYPE = typing.Annotated[
    Optional[str],
    _name("Default"),
    _description(
        "Default value for this property in JSON encoding. The value must be unserializable by the type specified "
        "in the type field. "
    ),
]
EXAMPLES_TYPE = typing.Annotated[
    List[str],
    _name("Examples"),
    _description("Example values for this property, encoded as JSON."),
]
_OBJECT_LIKE = typing.Union[
    typing.Annotated[typing.ForwardRef("RefSchema"), discriminator_value("ref")],
    typing.Annotated[typing.ForwardRef("ScopeSchema"), discriminator_value("scope")],
    typing.Annotated[typing.ForwardRef("ObjectSchema"), discriminator_value("object")],
]

_id_type_inverse_re = re.compile("[^$@a-zA-Z0-9-_]")


def _id_typeize(input: str) -> str:
    """
    This function creates an ID-safe representation of a string.

    **Example:**

    >>> from arcaflow_plugin_sdk.schema import _id_typeize
    >>> _id_typeize("Hello world!")
    'Hello_world_'
    >>> _id_typeize('Hello\\nworld')
    'Hello_world'
    """
    return re.sub(_id_type_inverse_re, "_", input)


# endregion

# region Schema


@dataclass
class _OpenAPIComponents:
    components: Dict[str, Dict]

    def __init__(self):
        self.components = {}


class _OpenAPIGenerator(ABC):
    """
    This class describes a method to generate OpenAPI 3.0 documents.
    """

    @abstractmethod
    def _to_openapi_fragment(
        self, scope: typing.ForwardRef("ScopeSchema"), defs: _OpenAPIComponents
    ) -> any:
        pass


@dataclass
class _JSONSchemaDefs:
    defs: Dict[str, Dict]

    def __init__(self):
        self.defs = {}


class _JSONSchemaGenerator(ABC):
    """
    This class prescribes a method to generate JSON schema documents.
    """

    @abstractmethod
    def _to_jsonschema_fragment(
        self, scope: typing.ForwardRef("ScopeSchema"), defs: _JSONSchemaDefs
    ) -> any:
        """
        This method creates a JSON schema version 2020-12 representation of this schema object.
        :return:
        """
        pass


@dataclass
class Unit:
    """
    A unit is a description of a single scale of measurement, such as a "second". If there are multiple scales, such as
    "minute", "second", etc. then multiple of these unit classes can be composed into units.

    **Example:**

    >>> from arcaflow_plugin_sdk import schema
    >>> u = schema.Unit(
    ...     "ns",
    ...     "ns",
    ...     "nanosecond",
    ...     "nanoseconds"
    ... )
    """

    name_short_singular: typing.Annotated[
        str,
        name("Short name (singular)"),
        description(
            "Short name that can be printed in a few characters, singular form."
        ),
        example("B"),
        example("char"),
    ]
    name_short_plural: typing.Annotated[
        str,
        name("Short name (plural)"),
        description("Short name that can be printed in a few characters, plural form."),
        example("B"),
        example("chars"),
    ]
    name_long_singular: typing.Annotated[
        str,
        name("Long name (singular)"),
        description("Longer name for this unit in singular form."),
        example("byte"),
        example("character"),
    ]
    name_long_plural: typing.Annotated[
        str,
        name("Long name (plural)"),
        description("Longer name for this unit in plural form."),
        example("bytes"),
        example("characters"),
    ]

    def format_short(
        self, amount: typing.Union[int, float], display_zero: bool = True
    ) -> str:
        """
        This function formats an amount according to this unit.

        **Example:**

        >>> from arcaflow_plugin_sdk import schema
        >>> u = schema.Unit(
        ...     "ns",
        ...     "ns",
        ...     "nanosecond",
        ...     "nanoseconds"
        ... )
        >>> u.format_short(42)
        '42ns'
        """
        if amount > 1:
            return str(amount) + self.name_short_plural
        elif amount == 1:
            return str(amount) + self.name_short_singular
        elif display_zero:
            return str(amount) + self.name_short_plural
        else:
            return ""

    def format_long(
        self, amount: typing.Union[int, float], display_zero: bool = True
    ) -> str:
        """
        This function formats an amount according to this unit.

        **Example:**

        >>> from arcaflow_plugin_sdk import schema
        >>> u = schema.Unit(
        ...     "ns",
        ...     "ns",
        ...     "nanosecond",
        ...     "nanoseconds"
        ... )
        >>> u.format_long(42)
        '42 nanoseconds'
        >>> u.format_long(1)
        '1 nanosecond'
        >>> u.format_long(0)
        '0 nanoseconds'
        """
        if amount > 1:
            return str(amount) + " " + self.name_long_plural
        elif amount == 1:
            return str(amount) + " " + self.name_long_singular
        elif display_zero:
            return str(amount) + " " + self.name_long_plural
        else:
            return ""


@dataclass
class Units:
    """
    Units holds several scales of magnitude of the same unit, for example 5m30s.

    **Example:**

    Imports:

    >>> from arcaflow_plugin_sdk import schema

    Define your unit:

    >>> t = schema.Units(
    ...     schema.Unit(
    ...         "ns",
    ...         "ns",
    ...         "nanosecond",
    ...         "nanoseconds"
    ...     ),
    ...     {
    ...         1000: schema.Unit(
    ...             "ms",
    ...             "ms",
    ...             "microsecond",
    ...             "microseconds"
    ...         ),
    ...         1000000: schema.Unit(
    ...             "s",
    ...             "s",
    ...             "second",
    ...             "seconds"
    ...         ),
    ...         60000000: schema.Unit(
    ...             "m",
    ...             "m",
    ...             "minute",
    ...             "minutes"
    ...         ),
    ...         3600000000: schema.Unit(
    ...             "H",
    ...             "H",
    ...             "hour",
    ...             "hours"
    ...         )
    ...     }
    ... )

    Format your time:

    >>> t.format_short(305000000)
    '5m5s'

    """

    base_unit: typing.Annotated[
        Unit,
        _name("Base unit"),
        _description(
            "The base unit is the smallest unit of scale for this set of units."
        ),
        _example(
            {
                "name_short_singular": "B",
                "name_short_plural": "B",
                "name_long_singular": "byte",
                "name_long_plural": "bytes",
            }
        ),
    ]
    multipliers: typing.Annotated[
        Optional[Dict[int, Unit]],
        _name("Multipliers"),
        _description("A set of multiplies that describe multiple units of scale."),
        _example(
            {
                1024: {
                    "name_short_singular": "kB",
                    "name_short_plural": "kB",
                    "name_long_singular": "kilobyte",
                    "name_long_plural": "kilobytes",
                },
                1048576: {
                    "name_short_singular": "MB",
                    "name_short_plural": "MB",
                    "name_long_singular": "megabyte",
                    "name_long_plural": "megabytes",
                },
            }
        ),
    ] = None

    def __init__(self, base_unit: Unit, multipliers: Optional[Dict[int, Unit]] = None):
        self.base_unit = base_unit
        self.multipliers = multipliers
        self.__unit_re_cache = None

    def parse(self, data: str) -> typing.Union[int, float]:
        """
        This function parses a string of units into the base number representation.

        :raises UnitParseException: if the data string fails to parse.

        **Example:**

        Imports:

        >>> from arcaflow_plugin_sdk import schema

        Parse a time string:

        >>> schema.UNIT_TIME.parse("5m5s")
        305000000

        Parse percentages:

        >>> schema.UNIT_PERCENT.parse("1.1%")
        1.1

        Parse bytes:

        >>> schema.UNIT_BYTE.parse("5MB")
        5242880
        """
        if data.strip() == "":
            raise UnitParseException(
                "Empty string cannot be parsed as " + self.base_unit.name_long_plural
            )
        if self.__unit_re_cache is None:
            parts = []
            if self.multipliers is not None:
                for multiplier in reversed(self.multipliers.keys()):
                    unit = self.multipliers[multiplier]
                    parts.append(
                        "(?:|(?P<g{}>[0-9]+)\\s*({}|{}|{}|{}))".format(
                            re.escape(str(multiplier)),
                            re.escape(unit.name_short_singular),
                            re.escape(unit.name_short_plural),
                            re.escape(unit.name_long_singular),
                            re.escape(unit.name_long_plural),
                        )
                    )
            parts.append(
                "(?:|(?P<g1>[0-9]+(|.[0-9]+))\\s*(|{}|{}|{}|{}))".format(
                    re.escape(self.base_unit.name_short_singular),
                    re.escape(self.base_unit.name_short_plural),
                    re.escape(self.base_unit.name_long_singular),
                    re.escape(self.base_unit.name_long_plural),
                )
            )
            regex = "^\\s*" + "\\s*".join(parts) + "\\s*$"
            self.__unit_re_cache = re.compile(regex)
        match = self.__unit_re_cache.match(data)
        if match is None:
            valid_units: typing.List[str] = []
            valid_units.append(self.base_unit.name_long_plural)
            valid_units.append(self.base_unit.name_long_singular)
            valid_units.append(self.base_unit.name_short_plural)
            valid_units.append(self.base_unit.name_short_singular)
            if self.multipliers is not None:
                for multiplier in self.multipliers.values():
                    valid_units.append(multiplier.name_long_plural)
                    valid_units.append(multiplier.name_long_singular)
                    valid_units.append(multiplier.name_short_plural)
                    valid_units.append(multiplier.name_short_singular)
            raise UnitParseException(
                "Cannot parse '{}' as '{}': invalid format, valid unit types are: '{}'".format(
                    data,
                    self.base_unit.name_long_plural,
                    "', '".join(collections.OrderedDict.fromkeys(valid_units).keys()),
                )
            )
        number = 0
        if self.multipliers is not None:
            for multiplier in self.multipliers.keys():
                match_group = match.group("g" + str(multiplier))
                if match_group is not None:
                    number += int(match_group) * multiplier
        base_match_group = match.group("g1")
        if base_match_group is not None:
            if "." in base_match_group:
                number += float(base_match_group)
            else:
                number += int(base_match_group)
        return number

    def format_short(self, data: typing.Union[int, float]) -> str:
        """
        This function takes an integer and formats it so that it is the most readable based on the current set of
        units.

        **Example:**

        Imports:

        >>> from arcaflow_plugin_sdk import schema

        Use the pre-defined units to format a number:

        >>> schema.UNIT_BYTE.format_short(1025)
        '1kB1B'

        You can also format floats:

        >>> schema.UNIT_PERCENT.format_short(1.1)
        '1.1%'
        """
        if data == 0:
            return self.base_unit.format_short(0)
        remainder = data
        output = ""
        if self.multipliers is not None:
            for i in reversed(self.multipliers.keys()):
                base = math.floor(remainder / i)
                remainder = remainder - (base * i)
                output += self.multipliers[i].format_short(base, False)
        output += self.base_unit.format_short(remainder, False)
        return output

    def format_long(self, data: typing.Union[int, float]) -> str:
        """
        This function takes an integer and formats it so that it is the most readable based on the current set of
        units.

        **Example:**

        Imports:

        >>> from arcaflow_plugin_sdk import schema

        Use the pre-defined units to format a number:

        >>> schema.UNIT_BYTE.format_long(1025)
        '1 kilobyte 1 byte'

        You can also format floats:

        >>> schema.UNIT_PERCENT.format_long(1.1)
        '1.1 percent'
        """
        if data == 0:
            return self.base_unit.format_short(0)
        remainder = data
        parts = []
        if self.multipliers is not None:
            for i in reversed(self.multipliers.keys()):
                base = math.floor(remainder / i)
                remainder = remainder - (base * i)
                part = self.multipliers[i].format_long(base, False)
                if part != "":
                    parts.append(part)
        part = self.base_unit.format_long(remainder, False)
        if part != "":
            parts.append(part)
        return " ".join(parts)


UNIT_BYTE = Units(
    Unit("B", "B", "byte", "bytes"),
    {
        1024: Unit("kB", "kB", "kilobyte", "kilobytes"),
        1048576: Unit("MB", "MB", "megabyte", "megabytes"),
        1073741824: Unit("GB", "GB", "gigabyte", "gigabytes"),
        1099511627776: Unit("TB", "TB", "terabyte", "terabytes"),
        1125899906842624: Unit("PB", "PB", "petabyte", "petabytes"),
    },
)
UNIT_TIME = Units(
    Unit("ns", "ns", "nanosecond", "nanoseconds"),
    {
        1000: Unit("ms", "ms", "microsecond", "microseconds"),
        1000000: Unit("s", "s", "second", "seconds"),
        60000000: Unit("m", "m", "minute", "minutes"),
        3600000000: Unit("H", "H", "hour", "hours"),
        86400000000: Unit("d", "d", "day", "days"),
    },
)
UNIT_CHARACTER = Units(Unit("char", "chars", "character", "characters"))
UNIT_PERCENT = Units(Unit("%", "%", "percent", "percent"))


@dataclass
class DisplayValue:
    """
    This class holds the fields related to displaying an item in a user interface.

    **Example:**

    >>> d = DisplayValue(
    ...     name="Foo",
    ...     description="This is a foo",
    ...     icon="<svg></svg>"
    ... )
    """

    name: typing.Annotated[
        typing.Optional[str],
        _name("Name"),
        _description("Short text serving as a name or title for this item."),
        _example("Fruit"),
        _min(1),
    ] = None
    description: typing.Annotated[
        Optional[str],
        _name("Description"),
        _description("Description for this item if needed."),
        _example("Please select the fruit you would like."),
        _min(1),
    ] = None
    icon: typing.Annotated[
        Optional[str],
        _name("Icon"),
        _description(
            "SVG icon for this item. Must have the declared size of 64x64, must not include "
            "additional namespaces, and must not reference external resources."
        ),
        _example("<svg ...></svg>"),
        _min(1),
    ] = None


@dataclass
class StringEnumSchema(_JSONSchemaGenerator, _OpenAPIGenerator):
    """
    This class specifically holds an enum that has string values. The values field maps the underlying values to
    display values for easy presentation.

    This dataclass only has the ability to hold the configuration but cannot serialize, unserialize or validate. For
    that functionality please use StringEnumType.

    **Example:**

    >>> from arcaflow_plugin_sdk import schema
    >>> s = schema.StringEnumSchema({
    ...     "apple": schema.DisplayValue("Apple"),
    ...     "banana": schema.DisplayValue("Banana"),
    ...     "orange": schema.DisplayValue("Orange"),
    ... })
    >>> s.valid_values()
    ['apple', 'banana', 'orange']
    """

    values: typing.Annotated[
        Dict[str, DisplayValue],
        _min(1),
        _name("Values"),
        _description(
            "Mapping where the left side of the map holds the possible value and the right side holds the display "
            "value for forms, etc."
        ),
        _example({"apple": {"name": "Apple"}, "orange": {"name": "Orange"}}),
    ]

    def valid_values(self) -> List[str]:
        return list(self.values.keys())

    def _to_jsonschema_fragment(
        self, scope: typing.ForwardRef("ScopeSchema"), defs: _JSONSchemaDefs
    ) -> any:
        """
        This method generates an JSON schema fragment for enumerated strings.
        See: https://json-schema.org/understanding-json-schema/reference/generic.html#enumerated-values
        """
        return {"type": "string", "enum": list(self.values.keys())}

    def _to_openapi_fragment(
        self, scope: typing.ForwardRef("ScopeSchema"), defs: _OpenAPIComponents
    ) -> any:
        """
        This method generates an OpenAPI fragment for string enums.
        See: https://spec.openapis.org/oas/v3.1.0#data-types
        """
        return {"type": "string", "enum": list(self.values.keys())}


@dataclass
class IntEnumSchema(_JSONSchemaGenerator, _OpenAPIGenerator):
    """
    This class specifically holds an enum that has integer values.

    This dataclass only has the ability to hold the configuration but cannot serialize, unserialize or validate. For
    that functionality please use :class:`IntEnumType`.

    **Example:**

    >>> from arcaflow_plugin_sdk import schema
    >>> s = schema.IntEnumSchema({
    ...     1: schema.DisplayValue("Apple"),
    ...     2: schema.DisplayValue("Banana"),
    ...     3: schema.DisplayValue("Orange"),
    ... })
    >>> s.valid_values()
    [1, 2, 3]
    """

    values: typing.Annotated[
        Dict[int, DisplayValue],
        min(1),
        name("Values"),
        description("Possible values for this field."),
        example({1024: {"name": "kB"}, 1048576: {"name": "MB"}}),
    ]
    units: Optional[Units] = None

    def valid_values(self) -> List[int]:
        return list(self.values.keys())

    def _to_jsonschema_fragment(
        self, scope: typing.ForwardRef("ScopeSchema"), defs: _JSONSchemaDefs
    ) -> any:
        """
        This method generates an JSON schema fragment for enumerated integers.
        See: https://json-schema.org/understanding-json-schema/reference/generic.html#enumerated-values
        """
        return {"type": "integer", "enum": list(self.values.keys())}

    def _to_openapi_fragment(
        self, scope: typing.ForwardRef("ScopeSchema"), defs: _OpenAPIComponents
    ) -> any:
        """
        This method generates an OpenAPI fragment for string enums.
        See: https://spec.openapis.org/oas/v3.1.0#data-types
        """
        return {"type": "integer", "format": "int64", "enum": list(self.values.keys())}


@dataclass
class StringSchema(_JSONSchemaGenerator, _OpenAPIGenerator):
    """
    This class holds schema information for strings. This dataclass only has the ability to hold the configuration but
    cannot serialize, unserialize or validate. For that functionality please use :class:`StringType`.

    **Example:**

    >>> from arcaflow_plugin_sdk import schema
    >>> import re
    >>> s = schema.StringSchema(
    ...     min=3,
    ...     max=5,
    ...     pattern=re.compile("^[a-z]+$")
    ... )
    """

    min: typing.Annotated[
        Optional[int],
        _min(0),
        _name("Minimum length"),
        _description("Minimum length for this string (inclusive)."),
        _units(UNIT_CHARACTER),
        _example(5),
    ] = None
    max: typing.Annotated[
        Optional[int],
        _min(0),
        _name("Maximum length"),
        _description("Maximum length for this string (inclusive)."),
        _units(UNIT_CHARACTER),
        _example(16),
    ] = None
    pattern: typing.Annotated[
        Optional[re.Pattern],
        _name("Pattern"),
        _description("Regular expression this string must match."),
        _example("^[a-zA-Z]+$"),
    ] = None

    def _to_jsonschema_fragment(
        self, scope: typing.ForwardRef("ScopeSchema"), defs: _JSONSchemaDefs
    ) -> any:
        """
        This method generates an JSON schema fragment for strings.
        See: https://json-schema.org/understanding-json-schema/reference/string.html
        """
        result = {"type": "string"}
        if self.min is not None:
            result["minLength"] = self.min
        if self.max is not None:
            result["maxLength"] = self.max
        if self.pattern is not None:
            result["pattern"] = self.pattern.pattern
        return result

    def _to_openapi_fragment(
        self, scope: typing.ForwardRef("ScopeSchema"), defs: _OpenAPIComponents
    ) -> any:
        """
        This method generates an OpenAPI fragment for strings.
        See: https://swagger.io/docs/specification/data-models/data-types/#string
        """
        result = {"type": "string"}
        if self.min is not None:
            result["minLength"] = self.min
        if self.max is not None:
            result["maxLength"] = self.max
        if self.pattern is not None:
            result["pattern"] = self.pattern.pattern
        return result


@dataclass
class PatternSchema(_JSONSchemaGenerator, _OpenAPIGenerator):
    """
    This class holds the schema information for regular expression patterns. This dataclass only has the ability to
    hold the configuration but cannot serialize, unserialize or validate. For that functionality please use
    :class:`PatternType`.

    **Example:**

    >>> from arcaflow_plugin_sdk import schema
    >>> s = schema.PatternSchema()
    """

    def _to_jsonschema_fragment(
        self, scope: typing.ForwardRef("ScopeSchema"), defs: _JSONSchemaDefs
    ) -> any:
        return {"type": "string", "format": "regex"}

    def _to_openapi_fragment(
        self, scope: typing.ForwardRef("ScopeSchema"), defs: _OpenAPIComponents
    ) -> any:
        return {"type": "string", "format": "regex"}


@dataclass
class IntSchema(_JSONSchemaGenerator, _OpenAPIGenerator):
    """
    This class holds the schema information for 64-bit integers. This dataclass only has the ability to hold the
    configuration but cannot serialize, unserialize or validate. For that functionality please use :class:`IntType`.

    **Example:**

    >>> from arcaflow_plugin_sdk import schema
    >>> s = schema.IntSchema(
    ...     min=3,
    ...     max=5,
    ...     units=schema.UNIT_BYTE
    ... )
    """

    min: typing.Annotated[
        Optional[int],
        _name("Minimum value"),
        _description("Minimum value for this int (inclusive)."),
        _example(5),
    ] = None
    max: typing.Annotated[
        Optional[int],
        _name("Maximum value"),
        _description("Maximum value for this int (inclusive)."),
        _example(16),
    ] = None
    units: typing.Annotated[
        Optional[Units],
        _name("Units"),
        _description("Units this number represents."),
        _example(
            {
                "base_unit": {
                    "name_short_singular": "char",
                    "name_short_plural": "chars",
                    "name_long_singular": "character",
                    "name_long_plural": "characters",
                }
            }
        ),
    ] = None

    def _to_jsonschema_fragment(
        self, scope: typing.ForwardRef("ScopeSchema"), defs: _JSONSchemaDefs
    ) -> any:
        result = {"type": "integer"}
        if self.min is not None:
            result["minimum"] = self.min
        if self.max is not None:
            result["maximum"] = self.max
        return result

    def _to_openapi_fragment(
        self, scope: typing.ForwardRef("ScopeSchema"), defs: _OpenAPIComponents
    ) -> any:
        result = {"type": "integer"}
        if self.min is not None:
            result["minimum"] = self.min
        if self.max is not None:
            result["maximum"] = self.max
        return result


@dataclass
class FloatSchema(_JSONSchemaGenerator, _OpenAPIGenerator):
    """
    This class holds the schema information for 64-bit floating point numbers. This dataclass only has the ability to
    hold the configuration but cannot serialize, unserialize or validate. For that functionality please use
    :class:`FloatType`.

    **Example:**

    >>> from arcaflow_plugin_sdk import schema
    >>> s = schema.FloatSchema(
    ...     min=0,
    ...     max=100,
    ...     units=schema.UNIT_PERCENT
    ... )
    """

    min: typing.Annotated[
        Optional[float],
        _name("Minimum value"),
        _description("Minimum value for this float (inclusive)."),
        _example(5.0),
    ] = None
    max: typing.Annotated[
        Optional[float],
        _name("Maximum value"),
        _description("Maximum value for this float (inclusive)."),
        _example(16.0),
    ] = None
    units: typing.Annotated[
        Optional[Units],
        _name("Units"),
        _description("Units this number represents."),
        _example(
            {
                "base_unit": {
                    "name_short_singular": "%",
                    "name_short_plural": "%",
                    "name_long_singular": "percent",
                    "name_long_plural": "percent",
                }
            }
        ),
    ] = None

    def _to_jsonschema_fragment(
        self, scope: typing.ForwardRef("ScopeSchema"), defs: _JSONSchemaDefs
    ) -> any:
        result = {"type": "number"}
        if self.min is not None:
            result["minimum"] = self.min
        if self.max is not None:
            result["maximum"] = self.max
        return result

    def _to_openapi_fragment(
        self, scope: typing.ForwardRef("ScopeSchema"), defs: _OpenAPIComponents
    ) -> any:
        result = {"type": "number"}
        if self.min is not None:
            result["minimum"] = self.min
        if self.max is not None:
            result["maximum"] = self.max
        return result


@dataclass
class BoolSchema(_JSONSchemaGenerator, _OpenAPIGenerator):
    """
    This class holds the schema information for boolean types. This dataclass only has the ability to hold the
    configuration but cannot serialize, unserialize or validate. For that functionality please use :class:`BoolType`.

    **Example:**

    >>> from arcaflow_plugin_sdk import schema
    >>> s = schema.FloatSchema(
    ...     min=0,
    ...     max=100,
    ...     units=schema.UNIT_PERCENT
    ... )
    """

    def _to_jsonschema_fragment(
        self, scope: typing.ForwardRef("ScopeSchema"), defs: _JSONSchemaDefs
    ) -> any:
        return {
            "anyOf": [
                {"title": "Boolean", "type": "boolean"},
                {
                    "title": "String",
                    "type": "string",
                    "enum": [
                        "yes",
                        "y",
                        "true",
                        "on",
                        "enable",
                        "enabled",
                        "1",
                        "no",
                        "n",
                        "false",
                        "off",
                        "disable",
                        "disabled",
                        "0",
                    ],
                },
                {
                    "title": "Integer",
                    "type": "integer",
                    "maximum": 1,
                    "minimum": 0,
                },
            ]
        }

    def _to_openapi_fragment(
        self, scope: typing.ForwardRef("ScopeSchema"), defs: _OpenAPIComponents
    ) -> any:
        return {
            "anyOf": [
                {"title": "Boolean", "type": "boolean"},
                {
                    "title": "String",
                    "type": "string",
                    "enum": [
                        "yes",
                        "y",
                        "true",
                        "on",
                        "enable",
                        "enabled",
                        "1",
                        "no",
                        "n",
                        "false",
                        "off",
                        "disable",
                        "disabled",
                        "0",
                    ],
                },
                {
                    "title": "Integer",
                    "type": "integer",
                    "maximum": 1,
                    "minimum": 0,
                },
            ]
        }


@dataclass
class ListSchema(_JSONSchemaGenerator, _OpenAPIGenerator):
    """
    This class holds the schema definition for lists. This dataclass only has the ability to hold the configuration but
    cannot serialize, unserialize or validate. For that functionality please use :class:`ListType`.

    **Example:**

    >>> from arcaflow_plugin_sdk import schema
    >>> s = schema.ListSchema(
    ...     items=schema.StringSchema(),
    ...     min=2,
    ...     max=3,
    ... )
    """

    items: typing.Annotated[
        VALUE_TYPE,
        _name("Items"),
        _description("Type definition for items in this list."),
    ]
    min: typing.Annotated[
        Optional[int],
        _min(0),
        _name("Minimum items"),
        _description("Minimum number of items in this list."),
        _example(5),
    ] = None
    max: typing.Annotated[
        Optional[int],
        _min(0),
        _name("Maximum items"),
        _description("Maximum number of items in this list."),
        _example(16),
    ] = None

    def _to_jsonschema_fragment(
        self, scope: typing.ForwardRef("ScopeSchema"), defs: _JSONSchemaDefs
    ) -> any:
        # noinspection PyProtectedMember
        result = {
            "type": "array",
            "items": self.items._to_jsonschema_fragment(scope, defs),
        }
        if self.min is not None:
            result["minItems"] = self.min
        if self.max is not None:
            result["maxItems"] = self.max
        return result

    def _to_openapi_fragment(
        self, scope: typing.ForwardRef("ScopeSchema"), defs: _OpenAPIComponents
    ) -> any:
        # noinspection PyProtectedMember
        result = {
            "type": "array",
            "items": self.items._to_openapi_fragment(scope, defs),
        }
        if self.min is not None:
            result["minItems"] = self.min
        if self.max is not None:
            result["maxItems"] = self.max
        return result


@dataclass
class MapSchema(_JSONSchemaGenerator, _OpenAPIGenerator):
    """
    This class holds the schema definition for key-value associations. This dataclass only has the ability to hold the
    configuration but cannot serialize, unserialize or validate. For that functionality please use :class:`MapType`.

    **Example:**

    >>> from arcaflow_plugin_sdk import schema
    >>> s = schema.MapSchema(
    ...     keys=schema.StringSchema(),
    ...     values=schema.IntSchema(),
    ...     min=2,
    ...     max=3,
    ... )
    """

    keys: typing.Annotated[
        MAP_KEY_TYPE,
        _name("Keys"),
        _description("Type definition for map keys."),
    ]
    values: typing.Annotated[
        VALUE_TYPE,
        _name("Values"),
        _description("Type definition for map values."),
    ]
    min: typing.Annotated[
        Optional[int],
        _min(0),
        _name("Minimum items"),
        _description("Minimum number of items in this list."),
        _example(5),
    ] = None
    max: typing.Annotated[
        Optional[int],
        _min(0),
        _name("Maximum items"),
        _description("Maximum number of items in this list."),
        _example(16),
    ] = None

    def _to_jsonschema_fragment(
        self, scope: typing.ForwardRef("ScopeSchema"), defs: _JSONSchemaDefs
    ) -> any:
        # noinspection PyProtectedMember
        result = {
            "type": "object",
            "propertyNames": self.keys._to_jsonschema_fragment(scope, defs),
            "additionalProperties": self.values._to_jsonschema_fragment(scope, defs),
        }

        # Sadly, these properties are not supported by JSON schema
        if "pattern" not in result["propertyNames"]:
            if result["propertyNames"]["type"] == "integer":
                result["propertyNames"]["pattern"] = "^[0-9]+$"
        result["propertyNames"].pop("type", None)
        result["propertyNames"].pop("minLength", None)
        result["propertyNames"].pop("maxLength", None)
        result["propertyNames"].pop("minimum", None)
        result["propertyNames"].pop("maximum", None)
        result["propertyNames"].pop("enum", None)

        if self.min is not None:
            result["minProperties"] = self.min
        if self.max is not None:
            result["maxProperties"] = self.max
        return result

    def _to_openapi_fragment(
        self, scope: typing.ForwardRef("ScopeSchema"), defs: _OpenAPIComponents
    ) -> any:
        # noinspection PyProtectedMember
        result = {
            "type": "object",
            "propertyNames": self.keys._to_openapi_fragment(scope, defs),
            "additionalProperties": self.values._to_openapi_fragment(scope, defs),
        }

        # Sadly, these properties are not supported by JSON schema
        if "pattern" not in result["propertyNames"]:
            if result["propertyNames"]["type"] == "integer":
                result["propertyNames"]["pattern"] = "^[0-9]+$"
        result["propertyNames"].pop("type", None)
        result["propertyNames"].pop("minLength", None)
        result["propertyNames"].pop("maxLength", None)
        result["propertyNames"].pop("minimum", None)
        result["propertyNames"].pop("maximum", None)
        result["propertyNames"].pop("enum", None)

        if self.min is not None:
            result["minProperties"] = self.min
        if self.max is not None:
            result["maxProperties"] = self.max
        return result


@dataclass
class PropertySchema(_JSONSchemaGenerator, _OpenAPIGenerator):
    """
    This class holds the schema definition for a single object property. It is usable in conjunction with ``ObjectSchema``.
    This dataclass only has the ability to hold the configuration but cannot serialize, unserialize or validate. For
    that functionality please use :class:`PropertyType`.

    **Example:**

    >>> from arcaflow_plugin_sdk import schema
    >>> s = schema.ObjectSchema(
    ...     "MyObject",
    ...     {
    ...         "foo": schema.PropertySchema(
    ...             type=schema.StringSchema(),
    ...             # Set the display settings for the property.
    ...             display=schema.DisplayValue(
    ...                 name="Foo",
    ...             ),
    ...             # Set the default value in JSON-encoded format.
    ...             default="'bar'",
    ...             # Add examples in JSON-encoded format.
    ...             examples=['baz'],
    ...             # Mark the field as optional.
    ...             required=False,
    ...             # ...
    ...         )
    ...     }
    ... )
    """

    type: typing.Annotated[
        VALUE_TYPE, _name("Type"), _description("Type definition for this field.")
    ]
    display: DISPLAY_TYPE = None
    default: DEFAULT_TYPE = None
    examples: EXAMPLES_TYPE = None
    required: typing.Annotated[
        bool,
        _name("Required"),
        _description(
            "When set to true, the value for this field must be provided under all circumstances."
        ),
    ] = True
    required_if: typing.Annotated[
        Optional[List[str]],
        _name("Required if"),
        _description(
            "Sets the current property to required if any of the properties in this list are set."
        ),
    ] = None
    required_if_not: typing.Annotated[
        Optional[List[str]],
        _name("Required if not"),
        _description(
            "Sets the current property to be required if none of the properties in this list are set."
        ),
    ] = None
    conflicts: typing.Annotated[
        Optional[List[str]],
        _name("Conflicts"),
        _description(
            "The current property cannot be set if any of the listed properties are set."
        ),
    ] = None

    def _to_jsonschema_fragment(
        self, scope: typing.ForwardRef("ScopeSchema"), defs: _JSONSchemaDefs
    ):
        # noinspection PyProtectedMember
        data = self.type._to_jsonschema_fragment(scope, defs)
        if self.examples is not None:
            data["examples"] = []
            for example in self.examples:
                data["examples"].append(json.loads(example))
        if self.default is not None:
            data["default"] = json.loads(self.default)
        if self.display is not None:
            if self.display.name is not None:
                data["title"] = self.display.name
            if self.display.description is not None:
                data["description"] = self.display.description
        return data

    def _to_openapi_fragment(
        self, scope: typing.ForwardRef("ScopeSchema"), defs: _OpenAPIComponents
    ) -> any:
        # noinspection PyProtectedMember
        data = self.type._to_openapi_fragment(scope, defs)
        if self.examples is not None:
            for example in self.examples:
                data["example"] = json.loads(example)
                break

        if self.default is not None:
            data["default"] = json.loads(self.default)
        if self.display is not None:
            if self.display.name is not None:
                data["title"] = self.display.name
            if self.display.description is not None:
                data["description"] = self.display.description
        return data


@dataclass
class ObjectSchema(_JSONSchemaGenerator, _OpenAPIGenerator):
    """
    This class holds the definition for objects comprised of defined fields. This dataclass only has the ability to hold
    the configuration but cannot serialize, unserialize or validate. For that functionality please use
    :class:`PropertyType`.

    **Example:**

    >>> from arcaflow_plugin_sdk import schema
    >>> s = schema.ObjectSchema(
    ...     "MyObject",
    ...     {
    ...         "foo": schema.PropertySchema(
    ...             type=schema.StringSchema(),
    ...             # Set the display settings for the property.
    ...             display=schema.DisplayValue(
    ...                 name="Foo",
    ...             ),
    ...             # Set the default value in JSON-encoded format.
    ...             default="'bar'",
    ...             # Add examples in JSON-encoded format.
    ...             examples=['baz'],
    ...             # Mark the field as optional.
    ...             required=False,
    ...             # ...
    ...         )
    ...     }
    ... )
    """

    id: typing.Annotated[
        ID_TYPE,
        _name("ID"),
        _description("Unique identifier for this object within the current scope."),
    ]
    properties: typing.Annotated[
        Dict[str, PropertySchema],
        _name("Properties"),
        _description("Properties of this object."),
    ]

    def _to_jsonschema_fragment(
        self, scope: typing.ForwardRef("ScopeSchema"), defs: _JSONSchemaDefs
    ):
        if self.id in defs.defs:
            return {"$ref": "#/$defs/" + self.id}

        # Add the object in the beginning to avoid an endless loop. Properties will be filled up below
        defs.defs[self.id] = {
            "type": "object",
            "properties": {},
            "required": [],
            "additionalProperties": False,
            "dependentRequired": {},
        }

        for property_id, property in self.properties.items():
            if property.required:
                defs.defs[self.id]["required"].append(property_id)
            if property.required_if is not None:
                defs.defs[self.id]["dependentRequired"][
                    property_id
                ] = property.required_if
            # noinspection PyProtectedMember
            defs.defs[self.id]["properties"][
                property_id
            ] = property._to_jsonschema_fragment(scope, defs)

        return {"$ref": "#/$defs/" + self.id}

    def _to_openapi_fragment(
        self, scope: typing.ForwardRef("ScopeSchema"), defs: _OpenAPIComponents
    ) -> any:
        if self.id in defs.components:
            return {"$ref": "#/components/schemas/" + self.id}

        # Add the object in the beginning to avoid an endless loop. Properties will be filled up below
        defs.components[self.id] = {
            "type": "object",
            "properties": {},
            "required": [],
        }

        for property_id, property in self.properties.items():
            if property.required:
                defs.components[self.id]["required"].append(property_id)
            # noinspection PyProtectedMember
            defs.components[self.id]["properties"][
                property_id
            ] = property._to_openapi_fragment(scope, defs)

        return {"$ref": "#/components/schemas/" + self.id}


@dataclass
class OneOfStringSchema(_JSONSchemaGenerator, _OpenAPIGenerator):
    """
    This class holds the definition of variable types with a string discriminator. This type acts as a split for a case
    where multiple possible object types can be present in a field. This type requires that there be a common field
    (the discriminator) which tells a parsing party which type it is. The field type in this case is a string.

    This dataclass only has the ability to hold the configuration but cannot serialize, unserialize or validate. For
    that functionality please use :class:`OneOfStringType`.

    **Example:**

    >>> from arcaflow_plugin_sdk import schema
    >>> a = schema.ObjectSchema(
    ...     "A",
    ...     {
    ...         "a": schema.PropertySchema(
    ...             type=schema.StringSchema(),
    ...         )
    ...     }
    ... )
    >>> b = schema.ObjectSchema(
    ...     "B",
    ...     {
    ...         "b": schema.PropertySchema(
    ...             type=schema.IntSchema(),
    ...         )
    ...     }
    ... )
    >>> one_of = schema.OneOfStringSchema(
    ...     {
    ...         "a": schema.RefSchema("A_ref"),
    ...         "b": schema.RefSchema("B_ref")
    ...     }
    ... )
    >>> c = schema.ObjectSchema(
    ...     "C",
    ...     {
    ...         "o": schema.PropertySchema(
    ...             one_of,
    ...         )
    ...     }
    ... )
    >>> s = schema.ScopeSchema(
    ...     {
    ...         "A_ref": a,
    ...         "B_ref": b,
    ...         "C_ref": c,
    ...     },
    ...     "C",
    ... )

    Instead of RefSchema, you can also add other object-like types, such as the ObjectSchema or the ScopeSchema
    directly:
    >>> one_of = schema.OneOfStringSchema(
    ...     {
    ...         "a": schema.RefSchema("A_ref"),
    ...         "b": schema.ObjectSchema(
    ...             "B",
    ...             {
    ...                 "b": schema.PropertySchema(
    ...                     type=schema.IntSchema(),
    ...                 )
    ...             }
    ...         )
    ...     }
    ... )
    """

    types: Dict[str, typing.Annotated[_OBJECT_LIKE, discriminator("type_id")]]
    discriminator_field_name: typing.Annotated[
        str,
        _name("Discriminator field name"),
        _description(
            "Name of the field used to discriminate between possible values. If this field is"
            "present on any of the component objects it must also be a string."
        ),
    ] = "_type"

    def _to_jsonschema_fragment(
        self, scope: typing.ForwardRef("ScopeSchema"), defs: _JSONSchemaDefs
    ) -> any:
        one_of = []
        for k, v in self.types.items():
            # noinspection PyProtectedMember
            scope.objects[v.id]._to_jsonschema_fragment(scope, defs)

            discriminated_object = defs.defs[v.id]

            discriminated_object["properties"][self.discriminator_field_name] = {
                "type": "string",
                "const": k,
            }
            discriminated_object["required"].insert(0, self.discriminator_field_name)
            if v.display is not None:
                if v.display.name is not None:
                    discriminated_object["title"] = v.display.name
                if v.display.description is not None:
                    discriminated_object["description"] = v.display.description
            name = v.id + "_discriminated_string_" + _id_typeize(k)
            defs.defs[name] = discriminated_object
            one_of.append({"$ref": "#/$defs/" + name})
        return {"oneOf": one_of}

    def _to_openapi_fragment(
        self, scope: typing.ForwardRef("ScopeSchema"), defs: _OpenAPIComponents
    ) -> any:
        one_of = []
        discriminator_mapping = {}
        for k, v in self.types.items():
            # noinspection PyProtectedMember
            scope.objects[v.id]._to_openapi_fragment(scope, defs)
            name = v.id + "_discriminated_string_" + _id_typeize(k)
            discriminator_mapping[k] = "#/components/schemas/" + name

            discriminated_object = defs.components[v.id]

            discriminated_object["properties"][self.discriminator_field_name] = {
                "type": "string",
            }
            discriminated_object["required"].insert(0, self.discriminator_field_name)
            if v.display is not None:
                if v.display.name is not None:
                    discriminated_object["title"] = v.display.name
                if v.display.description is not None:
                    discriminated_object["description"] = v.display.description

            defs.components[name] = discriminated_object
            one_of.append({"$ref": "#/components/schemas/" + name})
        return {
            "oneOf": one_of,
            "discriminator": {
                "propertyName": self.discriminator_field_name,
                "mapping": discriminator_mapping,
            },
        }


@dataclass
class OneOfIntSchema(_JSONSchemaGenerator, _OpenAPIGenerator):
    """
    This class holds the definition of variable types with an integer discriminator. This type acts as a split for a
    case where multiple possible object types can be present in a field. This type requires that there be a common field
    (the discriminator) which tells a parsing party which type it is. The field type in this case is a string.

    This dataclass only has the ability to hold the configuration but cannot serialize, unserialize or validate. For
    that functionality please use :class:`OneOfIntType`.

    **Example:**

    >>> from arcaflow_plugin_sdk import schema
    >>> a = schema.ObjectSchema(
    ...     "A",
    ...     {
    ...         "a": schema.PropertySchema(
    ...             type=schema.StringSchema(),
    ...         )
    ...     }
    ... )
    >>> b = schema.ObjectSchema(
    ...     "B",
    ...     {
    ...         "b": schema.PropertySchema(
    ...             type=schema.IntSchema(),
    ...         )
    ...     }
    ... )
    >>> one_of = schema.OneOfIntSchema(
    ...     {
    ...         1: schema.RefSchema("A_ref"),
    ...         2: schema.RefSchema("B_ref")
    ...     }
    ... )
    >>> c = schema.ObjectSchema(
    ...     "C",
    ...     {
    ...         "o": schema.PropertySchema(
    ...             one_of,
    ...         )
    ...     }
    ... )
    >>> s = schema.ScopeSchema(
    ...     {
    ...         "A_ref": a,
    ...         "B_ref": b,
    ...         "C_ref": c,
    ...     },
    ...     "C",
    ... )
    """

    types: Dict[int, typing.Annotated[_OBJECT_LIKE, discriminator("type_id")]]
    discriminator_field_name: typing.Annotated[
        str,
        _name("Discriminator field name"),
        _description(
            "Name of the field used to discriminate between possible values. If this field is"
            "present on any of the component objects it must also be an int."
        ),
    ] = "_type"

    def _to_jsonschema_fragment(
        self, scope: typing.ForwardRef("ScopeSchema"), defs: _JSONSchemaDefs
    ) -> any:
        one_of = []
        for k, v in self.types.items():
            # noinspection PyProtectedMember
            scope.objects[v.id]._to_jsonschema_fragment(scope, defs)

            discriminated_object = defs.defs[v.id]

            discriminated_object["properties"][self.discriminator_field_name] = {
                "type": "string",
                "const": str(k),
            }
            discriminated_object["required"].insert(0, self.discriminator_field_name)
            if v.display is not None:
                if v.display.name is not None:
                    discriminated_object["title"] = v.display.name
                if v.display.description is not None:
                    discriminated_object["description"] = v.display.description
            name = v.id + "_discriminated_int_" + str(k)
            defs.defs[name] = discriminated_object
            one_of.append({"$ref": "#/$defs/" + name})
        return {"oneOf": one_of}

    def _to_openapi_fragment(
        self, scope: typing.ForwardRef("ScopeSchema"), defs: _OpenAPIComponents
    ) -> any:
        one_of = []
        discriminator_mapping = {}
        for k, v in self.types.items():
            # noinspection PyProtectedMember
            scope.objects[v.id]._to_openapi_fragment(scope, defs)
            name = v.id + "_discriminated_int_" + str(k)
            discriminator_mapping[k] = "#/components/schemas/" + name

            discriminated_object = defs.components[v.id]

            discriminated_object["properties"][self.discriminator_field_name] = {
                "type": "string",
            }
            discriminated_object["required"].insert(0, self.discriminator_field_name)
            if v.display is not None:
                if v.display.name is not None:
                    discriminated_object["title"] = v.display.name
                if v.display.description is not None:
                    discriminated_object["description"] = v.display.description

            defs.components[name] = discriminated_object
            one_of.append({"$ref": "#/components/schemas/" + name})
        return {
            "oneOf": one_of,
            "discriminator": {
                "propertyName": self.discriminator_field_name,
                "mapping": discriminator_mapping,
            },
        }


@dataclass
class RefSchema(_JSONSchemaGenerator, _OpenAPIGenerator):
    """
    This class holds the definition of a reference to a scope-wide object. The ref must always be inside a scope,
    either directly or indirectly. If several scopes are embedded within each other, the Ref references the object
    in the current scope.

    This dataclass only has the ability to hold the configuration but cannot serialize, unserialize or validate. For
    that functionality please use :class:`RefType`.

    **Example:**

    Imports:

    >>> from arcaflow_plugin_sdk import schema

    Create a scope with an object:

    >>> s = schema.ScopeSchema(
    ...     {
    ...         "a": schema.ObjectSchema("A", {})
    ...     },
    ...     "a"
    ... )

    Create a ref:

    >>> ref = schema.RefSchema("a")

    Use ref in an object:

    >>> s.objects["b"] = schema.ObjectSchema(
    ...     "B",
    ...    {
    ...        "a": schema.PropertySchema(ref)
    ...    }
    ... )
    """

    id: typing.Annotated[
        ID_TYPE,
        _name("ID"),
        _description("Referenced object ID."),
    ]
    display: DISPLAY_TYPE = None

    def _to_jsonschema_fragment(
        self, scope: typing.ForwardRef("ScopeSchema"), defs: _JSONSchemaDefs
    ) -> any:
        return dict({"$ref": "#/$defs/" + self.id})

    def _to_openapi_fragment(
        self, scope: typing.ForwardRef("ScopeSchema"), defs: _OpenAPIComponents
    ) -> any:
        return dict({"$ref": "#/components/schemas/" + self.id})


@dataclass
class AnySchema(_JSONSchemaGenerator, _OpenAPIGenerator):
    """
    This class stores the details of the "any" type, which allows all lists, dicts, integers, floats, strings, and bools
    in a value.

    **Example:**

    Imports:

    >>> from arcaflow_plugin_sdk import schema

    Create an any schema:

    >>> ref = schema.AnySchema()
    """

    def _to_jsonschema_fragment(
        self, scope: typing.ForwardRef("ScopeSchema"), defs: _JSONSchemaDefs
    ) -> any:
        return dict({})

    def _to_openapi_fragment(
        self, scope: typing.ForwardRef("ScopeSchema"), defs: _OpenAPIComponents
    ) -> any:
        return dict({})


@dataclass
class ScopeSchema(_JSONSchemaGenerator, _OpenAPIGenerator):
    """
    A scope is a container for holding objects that can be referenced. It also optionally holds a reference to the
    root object of the current scope. References within the scope must always reference IDs in a scope. Scopes can be
    embedded into other objects, and scopes can have subscopes. Each RefSchema will reference objects in its current
    scope.

    This dataclass only has the ability to hold the configuration but cannot serialize, unserialize or validate. For
    that functionality please use :class:`ScopeType`.

    **Example:**

    Imports:

    >>> from arcaflow_plugin_sdk import schema

    Create a scope with an object:

    >>> s = schema.ScopeSchema(
    ...     {
    ...         "a": schema.ObjectSchema("A", {})
    ...     },
    ...     "a",
    ... )

    Create a ref:

    >>> ref = schema.RefSchema("a")

    Use ref in an object:

    >>> s.objects["b"] = schema.ObjectSchema(
    ...     "B",
    ...    {
    ...        "a": schema.PropertySchema(ref)
    ...    }
    ... )
    """

    objects: typing.Annotated[
        Dict[ID_TYPE, ObjectSchema],
        _name("Objects"),
        _description(
            "A set of referenceable objects. These objects may contain references themselves."
        ),
    ]
    root: typing.Annotated[
        str,
        _name("Root object"),
        _description("Reference to the root object of this scope"),
    ]

    def __getitem__(self, item):
        try:
            return self.objects[item]
        except KeyError:
            raise BadArgumentException(
                "Referenced object is not defined: {}".format(item)
            )

    def to_jsonschema(self):
        return self._to_jsonschema_fragment(self, _JSONSchemaDefs())

    def _to_jsonschema_fragment(
        self, scope: typing.ForwardRef("ScopeSchema"), defs: _JSONSchemaDefs
    ) -> any:
        result = {"$defs": {}}
        for k, v in self.objects.items():
            # noinspection PyProtectedMember
            _ = v._to_jsonschema_fragment(scope, defs)
        result["$defs"] = defs.defs
        for k, v in defs.defs.items():
            if k == self.root:
                for root_k, root_v in defs.defs[k].items():
                    result[root_k] = root_v

        return result

    def to_openapi(self):
        return self._to_openapi_fragment(self, _OpenAPIComponents())

    def _to_openapi_fragment(
        self, scope: typing.ForwardRef("ScopeSchema"), defs: _OpenAPIComponents
    ) -> any:
        result = {"components": {"schemas": {}}}
        for k, v in self.objects.items():
            # noinspection PyProtectedMember
            _ = v._to_openapi_fragment(scope, defs)
        result["components"]["schemas"] = defs.components
        result["$ref"] = "#/components/schemas/{}".format(self.root)

        return result


@dataclass
class StepOutputSchema(_JSONSchemaGenerator, _OpenAPIGenerator):
    """
    This class holds the possible outputs of a step and the metadata information related to these outputs.

    This dataclass only has the ability to hold the configuration but cannot serialize, unserialize or validate. For
    that functionality please use :class:`StepOutputType`.

    **Example:**

    Imports:

    >>> from arcaflow_plugin_sdk import schema

    Create scope:

    >>> scope = schema.ScopeSchema(
    ...     {
    ...         "a": schema.ObjectSchema(
    ...             "a",
    ...             {},
    ...         )
    ...     },
    ...     "a",
    ... )

    Create output schema:

    >>> output = schema.StepOutputSchema(
    ...     scope,
    ...     schema.DisplayValue("Test output"),
    ...     error=False,
    ... )
    """

    schema: typing.Annotated[
        ScopeSchema,
        _name("Schema"),
        _description("Data schema for this particular output."),
    ]
    display: DISPLAY_TYPE = None
    error: typing.Annotated[
        bool,
        _name("Error"),
        _description("If set to true, this output will be treated as an error output."),
    ] = False

    def to_jsonschema(self):
        """
        Creates a JSON schema fragment for this output. This is useful when combining the output with other outputs into
        a comprehensive JSON schema output.

        **Example:**

        Imports:

        >>> from arcaflow_plugin_sdk import schema

        Create scope:

        >>> scope = schema.ScopeSchema(
        ...     {
        ...         "a": schema.ObjectSchema(
        ...             "a",
        ...             {
        ...                 "foo": schema.PropertySchema(schema.StringSchema())
        ...             },
        ...         )
        ...     },
        ...     "a",
        ... )

        Create output schema:

        >>> output = schema.StepOutputSchema(
        ...     scope,
        ...     schema.DisplayValue("Test output"),
        ...     error=False,
        ... )

        Dump JSON schema:

        >>> json_schema = output.to_jsonschema()
        >>> json_schema["type"]
        'object'
        >>> json_schema["properties"]["foo"]["type"]
        'string'
        """
        return self._to_jsonschema_fragment(self.schema, _JSONSchemaDefs())

    def to_openapi(self):
        """
        Creates a OpenAPI fragment for this output. This is useful when combining the output with other outputs into
        a comprehensive OpenAPI document.

        **Example:**

        Imports:

        >>> from arcaflow_plugin_sdk import schema

        Create scope:

        >>> scope = schema.ScopeSchema(
        ...     {
        ...         "a": schema.ObjectSchema(
        ...             "a",
        ...             {
        ...                 "foo": schema.PropertySchema(schema.StringSchema())
        ...             },
        ...         )
        ...     },
        ...     "a",
        ... )

        Create output schema:

        >>> output = schema.StepOutputSchema(
        ...     scope,
        ...     schema.DisplayValue("Test output"),
        ...     error=False,
        ... )

        Dump OpenAPI schema:

        >>> openapi = output.to_openapi()
        >>> openapi["$ref"]
        '#/components/schemas/a'
        >>> openapi["components"]["schemas"]["a"]["properties"]["foo"]["type"]
        'string'
        """
        return self._to_openapi_fragment(self.schema, _OpenAPIComponents())

    def _to_jsonschema_fragment(self, scope: ScopeSchema, defs: _JSONSchemaDefs) -> any:
        # noinspection PyProtectedMember
        return self.schema._to_jsonschema_fragment(scope, defs)

    def _to_openapi_fragment(
        self, scope: typing.ForwardRef("ScopeSchema"), defs: _OpenAPIComponents
    ) -> any:
        # noinspection PyProtectedMember
        return self.schema._to_openapi_fragment(scope, defs)


@dataclass
class StepSchema:
    """
    This class holds the definition for a single step, it's input and output definitions.

    **Example:**

    Imports:

    >>> from arcaflow_plugin_sdk import schema

    Define input:

    >>> input_scope = schema.ScopeSchema(
    ...     {
    ...         "input": schema.ObjectSchema(
    ...             "input",
    ...             {
    ...                 "name": schema.PropertySchema(schema.StringSchema())
    ...             },
    ...         )
    ...     },
    ...     "input",
    ... )

    Create output schema:

    >>> output_scope = schema.ScopeSchema(
    ...     {
    ...         "greeting": schema.ObjectSchema(
    ...             "greeting",
    ...             {
    ...                 "text": schema.PropertySchema(schema.StringSchema())
    ...             },
    ...         )
    ...     },
    ...     "greeting",
    ... )
    >>> output = schema.StepOutputSchema(
    ...     output_scope,
    ...     schema.DisplayValue("Test output"),
    ...     error=False,
    ... )

    Create step schema:

    >>> step_schema = schema.StepSchema(
    ...     "hello_world",
    ...     input_scope,
    ...     {"success": output},
    ...     schema.DisplayValue("Hello world!")
    ... )
    """

    id: typing.Annotated[
        ID_TYPE, _name("ID"), _description("Machine identifier for this step.")
    ]
    input: typing.Annotated[
        ScopeSchema, _name("Input"), _description("Input data schema")
    ]
    outputs: typing.Annotated[
        Dict[
            ID_TYPE,
            StepOutputSchema,
        ],
        _name("Outputs"),
        _description("Possible outputs from this step."),
    ]
    display: DISPLAY_TYPE = None


@dataclass
class Schema:
    """
    This is a collection of steps supported by a plugin.
    """

    steps: typing.Annotated[
        Dict[ID_TYPE, StepSchema],
        _name("Steps"),
        _description("Steps this schema supports."),
    ]


# endregion

# region Types


TypeT = TypeVar("TypeT")


class AbstractType(Generic[TypeT]):
    """
    This class is an abstract class describing the methods needed to implement a type.
    """

    @abstractmethod
    def unserialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> TypeT:
        """
        This function takes the underlying raw data and decodes it into the underlying advanced data type (e.g.
        dataclass) for usage.

        :param data: the raw data.
        :param path: the list of structural elements that lead to this point for error messages.
        :return: the advanced datatype.
        :raise ConstraintException: if the passed data was not valid.
        """
        pass

    @abstractmethod
    def validate(self, data: TypeT, path: typing.Tuple[str] = tuple([])):
        """
        This function validates an already unserialized data type and raises an exception if it does not match
        the type definition.

        :param data: the unserialized data.
        :param path: the path that lead to this validation call, in order to produce a nice error message
        :raise ConstraintException: if the passed data was not valid.
        """

    @abstractmethod
    def serialize(self, data: TypeT, path: typing.Tuple[str] = tuple([])) -> Any:
        """
        This function serializes the passed data into it's raw form for transport, e.g. string, int, dicts, list.

        :param data: the underlying data type to be serialized.
        :param path: the list of structural elements that lead to this point for error messages.
        :return: the raw datatype.
        :raise ConstraintException: if the passed data was not valid.
        """
        pass


EnumT = TypeVar("EnumT", bound=Enum)


class _EnumType(AbstractType, Generic[EnumT]):
    """
    StringEnumType is an implementation of StringEnumSchema.
    """

    _type: Type[EnumT]

    def unserialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> EnumT:
        if isinstance(data, Enum):
            if data not in self._type:
                raise ConstraintException(
                    path,
                    "'{}' is not a valid value for the enum '{}'".format(
                        data, self._type.__name__
                    ),
                )
            return data
        else:
            for v in self._type:
                if v == data or v.value == data:
                    return v
            raise ConstraintException(
                path,
                "'{}' is not a valid value for '{}'".format(data, self._type.__name__),
            )

    def validate(self, data: TypeT, path: typing.Tuple[str] = tuple([])):
        if isinstance(data, Enum):
            if data not in self._type:
                raise ConstraintException(
                    path,
                    "'{}' is not a valid value for the enum '{}'".format(
                        data, self._type.__name__
                    ),
                )
        else:
            for v in self._type:
                if v == data or v.value == data:
                    return
            raise ConstraintException(
                path,
                "'{}' is not a valid value for '{}'".format(data, self._type.__name__),
            )

    def serialize(self, data: EnumT, path: typing.Tuple[str] = tuple([])) -> Any:
        if data not in self._type:
            raise ConstraintException(
                path,
                "'{}' is not a valid value for the enum '{}'".format(
                    data, self._type.__name__
                ),
            )
        return data.value


class StringEnumType(_EnumType, StringEnumSchema):
    """
    This class represents an enum type that is a string.

    **Example:**

    Imports:

    >>> from arcaflow_plugin_sdk import schema
    >>> from enum import Enum

    Create an enum:

    >>> class Fruits(Enum):
    ...    APPLE="apple"
    ...    ORANGE="orange"

    Create the type:

    >>> fruit_type = schema.StringEnumType(Fruits)

    Unserialize a value:

    >>> fruit_type.unserialize("apple")
    <Fruits.APPLE: 'apple'>

    Serialize a value:

    >>> fruit_type.serialize(Fruits.ORANGE)
    'orange'

    Validate a value:

    >>> fruit_type.validate("plum")
    Traceback (most recent call last):
    ...
    arcaflow_plugin_sdk.schema.ConstraintException: Validation failed: 'plum' is not a valid value for 'Fruits'
    """

    def __init__(self, t: Type[EnumT]):
        self._type = t
        values: Dict[str, DisplayValue] = {}
        try:
            for value in self._type:
                if not isinstance(value.value, str):
                    raise BadArgumentException(
                        "{} on {} is not a string".format(value, t.__name__)
                    )
                values[value.value] = DisplayValue(
                    value.name,
                )
            self.values = values
        except TypeError as e:
            raise BadArgumentException(
                "{} is not a valid enum, not iterable".format(t.__name__)
            ) from e


class IntEnumType(_EnumType, IntEnumSchema):
    """
    This class represents an enum type that is an integer.

    **Example:**

    Imports:

    >>> from arcaflow_plugin_sdk import schema
    >>> from enum import Enum

    Create an enum:

    >>> class PrimeNumbers(Enum):
    ...    FIRST=2
    ...    SECOND=3
    ...    THIRD=5

    Create the type:

    >>> prime_numbers_type = schema.IntEnumType(PrimeNumbers)

    Unserialize a value:

    >>> prime_numbers_type.unserialize(2)
    <PrimeNumbers.FIRST: 2>

    Serialize a value:

    >>> prime_numbers_type.serialize(PrimeNumbers.FIRST)
    2

    Validate a value:

    >>> prime_numbers_type.validate(4)
    Traceback (most recent call last):
    ...
    arcaflow_plugin_sdk.schema.ConstraintException: Validation failed: '4' is not a valid value for 'PrimeNumbers'
    """

    def __init__(self, t: Type[EnumT]):
        self._type = t
        values: Dict[int, DisplayValue] = {}
        try:
            for value in self._type:
                if not isinstance(value.value, int):
                    raise BadArgumentException(
                        "{} on {} is not a string".format(value, t.__name__)
                    )
                values[value.value] = DisplayValue(
                    value.name,
                )
            self.values = values
        except TypeError as e:
            raise BadArgumentException(
                "{} is not a valid enum, not iterable".format(t.__name__)
            ) from e


class BoolType(BoolSchema, AbstractType):
    """
    This type represents a boolean value with a multitude of unserialization options.

    **Example:**

    Imports:

    >>> from arcaflow_plugin_sdk import schema

    Create bool type:

    >>> bool_type = schema.BoolType()

    Now you can use the type to unseralize, validate, or serialize values.
    """

    def unserialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> TypeT:
        """
        This function unserializes a bool value from a variety of types.

        **Example:**

        Imports:

        >>> from arcaflow_plugin_sdk import schema

        Create bool type:

        >>> bool_type = schema.BoolType()

        Unserialize a bool iun various ways:

        >>> bool_type.unserialize(True)
        True
        >>> bool_type.unserialize(1)
        True
        >>> bool_type.unserialize("true")
        True
        >>> bool_type.unserialize("yes")
        True
        >>> bool_type.unserialize("y")
        True
        >>> bool_type.unserialize("on")
        True
        >>> bool_type.unserialize("enable")
        True
        >>> bool_type.unserialize("enabled")
        True
        >>> bool_type.unserialize(False)
        False
        >>> bool_type.unserialize(0)
        False
        >>> bool_type.unserialize("false")
        False
        >>> bool_type.unserialize("no")
        False
        >>> bool_type.unserialize("n")
        False
        >>> bool_type.unserialize("off")
        False
        >>> bool_type.unserialize("disable")
        False
        >>> bool_type.unserialize("disabled")
        False

        This will throw an error:

        >>> bool_type.unserialize("")
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed: Boolean value expected, string found ()
        """
        if isinstance(data, bool):
            return data
        if isinstance(data, int):
            if data == 0:
                return False
            if data == 1:
                return True
            raise ConstraintException(
                path, "Boolean value expected, integer found ({})".format(data)
            )
        if isinstance(data, str):
            lower_str = data.lower()
            if (
                lower_str == "yes"
                or lower_str == "y"
                or lower_str == "on"
                or lower_str == "true"
                or lower_str == "enable"
                or lower_str == "enabled"
                or lower_str == "1"
            ):
                return True
            if (
                lower_str == "no"
                or lower_str == "n"
                or lower_str == "off"
                or lower_str == "false"
                or lower_str == "disable"
                or lower_str == "disabled"
                or lower_str == "0"
            ):
                return False
            raise ConstraintException(
                path, "Boolean value expected, string found ({})".format(data)
            )

        raise ConstraintException(
            path, "Boolean value expected, {} found".format(type(data))
        )

    def validate(self, data: TypeT, path: typing.Tuple[str] = tuple([])):
        """
        This function validates a bool value.

        **Example:**

        Imports:

        >>> from arcaflow_plugin_sdk import schema

        Create bool type:

        >>> bool_type = schema.BoolType()

        Validate:

        >>> bool_type.validate(True)
        >>> bool_type.validate(False)

        This will throw an error:

        >>> bool_type.validate(1)
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed: Boolean value expected, <class 'int'> found
        """
        if not isinstance(data, bool):
            raise ConstraintException(
                path, "Boolean value expected, {} found".format(type(data))
            )

    def serialize(self, data: TypeT, path: typing.Tuple[str] = tuple([])) -> Any:
        """
        This function serializes a bool value.

        **Example:**

        Imports:

        >>> from arcaflow_plugin_sdk import schema

        Create bool type:

        >>> bool_type = schema.BoolType()

        Validate:

        >>> bool_type.serialize(True)
        True
        >>> bool_type.serialize(False)
        False

        This will throw an error:

        >>> bool_type.serialize(1)
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed: Boolean value expected, <class 'int'> found
        """
        if isinstance(data, bool):
            return data
        raise ConstraintException(
            path, "Boolean value expected, {} found".format(type(data))
        )


@dataclass
class StringType(StringSchema, AbstractType):
    """
    StringType represents a string of characters for human consumption.

    **Example:**

    >>> from arcaflow_plugin_sdk import schema
    >>> import re
    >>> s = schema.StringType(
    ...     min=3,
    ...     max=5,
    ...     pattern=re.compile("^[a-z]+$")
    ... )

    You can now unserialize, validate, or serialize the data.
    """

    def unserialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> str:
        """
        Unserializes the current string from an integer or string.

        **Example:**

        >>> from arcaflow_plugin_sdk import schema
        >>> import re
        >>> s = schema.StringType(
        ...     min=3,
        ...     max=5,
        ...     pattern=re.compile("^[0-9]+$")
        ... )
        >>> s.unserialize(123)
        '123'
        """
        if isinstance(data, int):
            data = str(data)
        self.validate(data, path)
        return data

    def validate(self, data: str, path: typing.Tuple[str] = tuple([])):
        """
        Validates the given string for conformance with the rules set up in this object.

        **Example:**

        >>> from arcaflow_plugin_sdk import schema
        >>> import re
        >>> s = schema.StringType(
        ...     min=3,
        ...     max=5,
        ...     pattern=re.compile("^[a-z]+$")
        ... )
        >>> s.validate("as")
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed: String must be at least 3 characters, 2 given

        >>> s.validate("asd")

        >>> s.validate("123")
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed: String must match the pattern ^[a-z]+$

        >>> s.validate("asdfgh")
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed: String must be at most 5 characters, 6 given
        """
        if not isinstance(data, str):
            raise ConstraintException(
                path, "Must be a string, {} given".format(type(data))
            )
        string: str = data
        if self.min is not None and len(string) < self.min:
            raise ConstraintException(
                path,
                "String must be at least {} characters, {} given".format(
                    self.min, len(string)
                ),
            )
        if self.max is not None and len(string) > self.max:
            raise ConstraintException(
                path,
                "String must be at most {} characters, {} given".format(
                    self.max, len(string)
                ),
            )
        if self.pattern is not None and not self.pattern.match(string):
            raise ConstraintException(
                path, "String must match the pattern {}".format(self.pattern.pattern)
            )

    def serialize(self, data: str, path: typing.Tuple[str] = tuple([])) -> any:
        """
        This function returns the string as-is after validating its contents.
        """
        self.validate(data, path)
        return data


class PatternType(PatternSchema, AbstractType):
    """
    PatternType represents a regular expression.

    **Example:**

    >>> from arcaflow_plugin_sdk import schema
    >>> pattern_type = schema.PatternType()

    You can now unserialize, validate, or serialize the data.
    """

    def unserialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> re.Pattern:
        """
        This function unserializes a regular expression from a string.

        **Example:**

        Initialize:

        >>> from arcaflow_plugin_sdk import schema
        >>> pattern_type = schema.PatternType()

        Unserialize:

        >>> regexp = pattern_type.unserialize("^[a-z]+$")
        >>> regexp.pattern
        '^[a-z]+$'

        This will throw an error:

        >>> regexp = pattern_type.unserialize("[")
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed: Invalid regular expression (unterminated \
character set at position 0)
        """
        if not isinstance(data, str):
            raise ConstraintException(path, "Must be a string")
        try:
            return re.compile(str(data))
        except re.error as e:
            raise ConstraintException(
                path, "Invalid regular expression ({})".format(e.__str__())
            )
        except TypeError as e:
            raise ConstraintException(
                path, "Invalid regular expression ({})".format(e.__str__())
            )
        except ValueError as e:
            raise ConstraintException(
                path, "Invalid regular expression ({})".format(e.__str__())
            )

    def validate(self, data: re.Pattern, path: typing.Tuple[str] = tuple([])):
        """
        This function validates a regular expression as such.

        **Example:**

        Initialize:

        >>> from arcaflow_plugin_sdk import schema
        >>> from re import compile
        >>> pattern_type = schema.PatternType()

        Validate:

        >>> pattern_type.validate(compile("^[a-z]+$"))

        This will fail:

        >>> pattern_type.validate("^[a-z]+$")
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed: Not a regular expression
        """
        if not isinstance(data, re.Pattern):
            raise ConstraintException(path, "Not a regular expression")

    def serialize(self, data: re.Pattern, path: typing.Tuple[str] = tuple([])) -> Any:
        if not isinstance(data, re.Pattern):
            raise ConstraintException(path, "Must be a re.Pattern")
        return data.pattern


class IntType(IntSchema, AbstractType):
    """
    ``IntType`` represents an integer type, both positive or negative. It is designed to take a 64 bit value.

    **Example:**

    Imports:

    >>> from arcaflow_plugin_sdk import schema

    Initialize int type:

    >>> int_type = schema.IntType(
    ...     min=3,
    ...     max=5,
    ...     units=schema.UNIT_TIME,
    ... )

    Now you can use this type to unserialize, validate, or serialize.
    """

    def unserialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> int:
        """
        This function can unserialize a number for a integers or strings. If the passed data is a string, it can take
        the unit of the current type into account.

        **Example:**

        Imports:

        >>> from arcaflow_plugin_sdk import schema

        Initialize int type:

        >>> int_type = schema.IntType(
        ...     min=1000000,
        ...     max=5000000,
        ...     units=schema.UNIT_TIME,
        ... )

        Unserialize:

        >>> int_type.unserialize("2s30ms")
        2030000
        >>> int_type.unserialize(3000000)
        3000000

        These will fail:

        >>> int_type.unserialize("4k")
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed: Cannot parse '4k' as 'nanoseconds': invalid \
format, valid unit types are: 'nanoseconds', 'nanosecond', 'ns', 'microseconds', 'microsecond', 'ms', 'seconds', \
'second', 's', 'minutes', 'minute', 'm', 'hours', 'hour', 'H', 'days', 'day', 'd'
        >>> int_type.unserialize("6s")
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed: Must be at most 5s (5000000ns)
        >>> int_type.unserialize("500ms")
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed: Must be at least 1s (1000000ns)
        >>> int_type.unserialize(500000)
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed: Must be at least 1s (1000000ns)
        """
        if isinstance(data, str):
            if self.units is not None:
                try:
                    data = self.units.parse(data)
                except UnitParseException as e:
                    raise ConstraintException(path, e.__str__()) from e
            else:
                try:
                    data = int(data)
                except ValueError as e:
                    raise ConstraintException(path, "Must be an integer") from e

        self.validate(data, path)
        return data

    def validate(self, data: int, path: typing.Tuple[str] = tuple([])):
        """
        This function validates the passed number for conformity with the schema.

        **Example:**

        Imports:

        >>> from arcaflow_plugin_sdk import schema

        Initialize int type:

        >>> int_type = schema.IntType(
        ...     min=1000000,
        ...     max=5000000,
        ...     units=schema.UNIT_TIME,
        ... )

        This will work:

        >>> int_type.validate(3000000)

        These will fail:

        >>> int_type.validate("4s")
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed: Must be an integer, str given
        >>> int_type.validate(6000000)
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed: Must be at most 5s (5000000ns)
        >>> int_type.validate(500000)
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed: Must be at least 1s (1000000ns)
        """
        if not isinstance(data, int):
            raise ConstraintException(
                path, "Must be an integer, {} given".format(type(data).__name__)
            )
        integer = int(data)
        if self.min is not None and integer < self.min:
            num = self.min
            if self.units is not None:
                num = (
                    self.units.format_short(num)
                    + " ("
                    + self.units.base_unit.format_short(num)
                    + ")"
                )
            raise ConstraintException(path, "Must be at least {}".format(num))
        if self.max is not None and integer > self.max:
            num = self.max
            if self.units is not None:
                num = (
                    self.units.format_short(num)
                    + " ("
                    + self.units.base_unit.format_short(num)
                    + ")"
                )
            raise ConstraintException(path, "Must be at most {}".format(num))

    def serialize(self, data: int, path: typing.Tuple[str] = tuple([])) -> Any:
        """
        This function will return an integer for the base unit of this value.

        **Example:**

        Imports:

        >>> from arcaflow_plugin_sdk import schema

        Initialize int type:

        >>> int_type = schema.IntType(
        ...     min=1000000,
        ...     max=5000000,
        ...     units=schema.UNIT_TIME,
        ... )

        This will work:

        >>> int_type.serialize(3000000)
        3000000

        These will fail:

        >>> int_type.serialize("4s")
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed: Must be an integer, str given
        >>> int_type.serialize(6000000)
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed: Must be at most 5s (5000000ns)
        >>> int_type.serialize(500000)
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed: Must be at least 1s (1000000ns)
        """
        self.validate(data, path)
        return data


@dataclass
class FloatType(FloatSchema, AbstractType):
    """
    This type represents a 64-bit floating point / real number.

    **Example:**

    Imports:

    >>> from arcaflow_plugin_sdk import schema

    Initialize float type:

    >>> float_type = schema.FloatType(
    ...     min=3.0,
    ...     max=5.0,
    ...     units=schema.UNIT_TIME,
    ... )

    Now you can use this type to unserialize, validate, or serialize.
    """

    def unserialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> float:
        """
        This function can unserialize a number for a integers or strings. If the passed data is a string, it can take
        the unit of the current type into account.

        **Example:**

        Imports:

        >>> from arcaflow_plugin_sdk import schema

        Initialize float type:

        >>> float_type = schema.FloatType(
        ...     min=1000000.0,
        ...     max=5000000.0,
        ...     units=schema.UNIT_TIME,
        ... )

        Unserialize:

        >>> float_type.unserialize("2s30ms")
        2030000.0
        >>> float_type.unserialize("2s30ms1.1ns")
        2030001.1
        >>> float_type.unserialize(3000000)
        3000000.0
        >>> float_type.unserialize(3000000.0)
        3000000.0

        These will fail:

        >>> float_type.unserialize("4k")
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed: Cannot parse '4k' as 'nanoseconds': invalid \
format, valid unit types are: 'nanoseconds', 'nanosecond', 'ns', 'microseconds', 'microsecond', 'ms', 'seconds', \
'second', 's', 'minutes', 'minute', 'm', 'hours', 'hour', 'H', 'days', 'day', 'd'
        >>> float_type.unserialize("6s")
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed: Must be at most 5s (5000000.0ns)
        >>> float_type.unserialize("500ms")
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed: Must be at least 1s (1000000.0ns)
        >>> float_type.unserialize(500000)
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed: Must be at least 1s (1000000.0ns)
        """
        if isinstance(data, str):
            if self.units is not None:
                try:
                    data = self.units.parse(data)
                except UnitParseException as e:
                    raise ConstraintException(path, e.__str__()) from e
            else:
                try:
                    data = float(data)
                except ValueError as e:
                    raise ConstraintException(path, "Must be a float") from e
        if isinstance(data, int):
            data = float(data)
        self.validate(data, path)
        return data

    def validate(self, data: float, path: typing.Tuple[str] = tuple([])):
        """
        This function validates the passed number for conformity with the schema.

        **Example:**

        Imports:

        >>> from arcaflow_plugin_sdk import schema

        Initialize float type:

        >>> float_type = schema.FloatType(
        ...     min=1000000.0,
        ...     max=5000000.0,
        ...     units=schema.UNIT_TIME,
        ... )

        This will work:

        >>> float_type.validate(3000000.0)

        These will fail:

        >>> float_type.validate(3000000)
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed: Must be an float, int given
        >>> float_type.validate("4s")
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed: Must be an float, str given
        >>> float_type.validate(6000000.0)
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed: Must be at most 5s (5000000.0ns)
        >>> float_type.validate(500000.0)
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed: Must be at least 1s (1000000.0ns)
        """
        if not isinstance(data, float):
            raise ConstraintException(
                path, "Must be an float, {} given".format(type(data).__name__)
            )
        f = float(data)
        if self.min is not None and f < self.min:
            num = self.min
            if self.units is not None:
                num = (
                    self.units.format_short(num)
                    + " ("
                    + self.units.base_unit.format_short(num)
                    + ")"
                )
            raise ConstraintException(path, "Must be at least {}".format(num))
        if self.max is not None and f > self.max:
            num = self.max
            if self.units is not None:
                num = (
                    self.units.format_short(num)
                    + " ("
                    + self.units.base_unit.format_short(num)
                    + ")"
                )
            raise ConstraintException(path, "Must be at most {}".format(num))

    def serialize(self, data: float, path: typing.Tuple[str] = tuple([])) -> Any:
        """
        This function will return a float for the base unit of this value.

        **Example:**

        Imports:

        >>> from arcaflow_plugin_sdk import schema

        Initialize float type:

        >>> float_type = schema.FloatType(
        ...     min=1000000.0,
        ...     max=5000000.0,
        ...     units=schema.UNIT_TIME,
        ... )

        This will work:

        >>> float_type.serialize(3000000.0)
        3000000.0

        These will fail:

        >>> float_type.serialize("4s")
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed: Must be an float, str given
        >>> float_type.serialize(6000000.0)
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed: Must be at most 5s (5000000.0ns)
        >>> float_type.serialize(500000.0)
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed: Must be at least 1s (1000000.0ns)
        """
        self.validate(data, path)
        return data


ListT = TypeVar("ListT", bound=List)


@dataclass
class ListType(ListSchema, AbstractType, Generic[ListT]):
    """
    ``ListType`` is a strongly typed list that can have elements of only one type. The typical Python equivalent would be
    ``typing.List[sometype]``.

    **Example:**

    Imports:

    >>> from arcaflow_plugin_sdk import schema

    Create a list type:

    >>> list_type = schema.ListType(
    ...     schema.StringType(),
    ...     min=1,
    ...     max=5,
    ... )

    Now you can use the list type to unserialize, validate, and serialize.
    """

    def unserialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> ListT:
        """
        This function unserializes the list itself, and also unserializes the underlying type.

        **Example:**

        Imports:

        >>> from arcaflow_plugin_sdk import schema

        Create a list type:

        >>> list_type = schema.ListType(
        ...     schema.StringType(min=1),
        ...     min=1,
        ...     max=3,
        ... )

        Unserialize data:

        >>> list_type.unserialize(["Hello world!"])
        ['Hello world!']

        These will fail:

        >>> list_type.unserialize([])
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed: Must have at least 1 items, 0 given
        >>> list_type.unserialize(["a","b","c","d"])
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed: Must have at most 3 items, 4 given

        Underlying types are also validated:

        >>> list_type.unserialize([""])
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed for 'item 0': String must be at least 1 \
characters, 0 given
        """
        if not isinstance(data, list):
            raise ConstraintException(
                path, "Must be a list, {} given".format(type(data).__name__)
            )
        for i in range(len(data)):
            new_path = list(path)
            new_path.append("item " + str(i))
            data[i] = self.items.unserialize(data[i], tuple(new_path))
        self._validate(data, path)
        return data

    def validate(self, data: TypeT, path: typing.Tuple[str] = tuple([])):
        """
        This function validates the data type. It also validates the underlying data type.

        **Example:**

        Imports:

        >>> from arcaflow_plugin_sdk import schema

        Create a list type:

        >>> list_type = schema.ListType(
        ...     schema.StringType(min=1),
        ...     min=1,
        ...     max=3,
        ... )

        Validate the data:

        >>> list_type.validate(["Hello world!"])

        These will fail:

        >>> list_type.validate([])
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed: Must have at least 1 items, 0 given
        >>> list_type.validate(["a","b","c","d"])
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed: Must have at most 3 items, 4 given

        Underlying types are also validated:

        >>> list_type.validate([""])
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed for 'item 0': String must be at least 1 \
characters, 0 given
        """
        self._validate(data, path)
        for i in range(len(data)):
            new_path = list(path)
            new_path.append("item " + str(i))
            self.items.validate(data[i], tuple(new_path))

    def serialize(self, data: ListT, path: typing.Tuple[str] = tuple([])) -> Any:
        """
        This function serializes the list elements into a list for transport.

        **Example:**


        Imports:

        >>> from arcaflow_plugin_sdk import schema

        Create a list type:

        >>> list_type = schema.ListType(
        ...     schema.StringType(min=1),
        ...     min=1,
        ...     max=3,
        ... )

        Serialize the data:

        >>> list_type.serialize(["a"])
        ['a']
        """
        self._validate(data, path)
        result = []
        for i in range(len(data)):
            new_path = list(path)
            new_path.append("item " + str(i))
            result.append(self.items.serialize(data[i], tuple(new_path)))
        return result

    def _validate(self, data, path):
        if not isinstance(data, list):
            raise ConstraintException(
                path, "Must be a list, {} given".format(type(data).__name__)
            )
        if self.min is not None and len(data) < self.min:
            raise ConstraintException(
                path,
                "Must have at least {} items, {} given".format(self.min, len(data)),
            )
        if self.max is not None and len(data) > self.max:
            raise ConstraintException(
                path, "Must have at most {} items, {} given".format(self.max, len(data))
            )


MapT = TypeVar("MapT", bound=Dict)


@dataclass
class MapType(MapSchema, AbstractType, Generic[MapT]):
    """
    MapType is a key-value dict with fixed types for both.

    **Example:**

    Imports:

    >>> from arcaflow_plugin_sdk import schema

    Create a map type:

    >>> map_type = schema.MapType(
    ...     keys=schema.StringType(min=1),
    ...     values=schema.IntType(),
    ...     min=1,
    ...     max=2,
    ... )

    Now you can use the map type to unserialize, validate, or serialize data.
    """

    def unserialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> MapT:
        """
        Unserialize a map (dict) type as defined with the underlying types.

        **Example:**

        Imports:

        >>> from arcaflow_plugin_sdk import schema

        Create a map type:

        >>> map_type = schema.MapType(
        ...     keys=schema.StringType(min=2),
        ...     values=schema.IntType(min=3),
        ...     min=1,
        ...     max=2,
        ... )

        Unserialize data:

        >>> map_type.unserialize({"foo": 5})
        {'foo': 5}

        This will not work due to underlying types failing validation:

        >>> map_type.unserialize({"a": 5})
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed for 'a -> key': String must be at least 2 \
characters, 1 given
        >>> map_type.unserialize({"foo":1})
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed for 'foo -> value': Must be at least 3

        This will also fail because the map does not have enough elements:

        >>> map_type.unserialize({})
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed: Must have at least 1 elements, 0 given.
        """
        entries = self._validate(data, path)
        result: MapT = {}
        for key in entries.keys():
            value = entries[key]
            new_path: List[str] = list(path)
            new_path.append(key)
            key_path: List[str] = list(tuple(new_path))
            key_path.append("key")
            unserialized_key = self.keys.unserialize(key, tuple(key_path))
            if unserialized_key in result:
                raise ConstraintException(
                    tuple(key_path), "Key already exists in result dict"
                )
            value_path = list(tuple(new_path))
            value_path.append("value")
            result[unserialized_key] = self.values.unserialize(value, tuple(value_path))
        return result

    def validate(self, data: TypeT, path: typing.Tuple[str] = tuple([])):
        """
        This function validates the map and its underlying types.

        **Example:**

        Imports:

        >>> from arcaflow_plugin_sdk import schema

        Create a map type:

        >>> map_type = schema.MapType(
        ...     keys=schema.StringType(min=2),
        ...     values=schema.IntType(min=3),
        ...     min=1,
        ...     max=2,
        ... )

        This is valid:

        >>> map_type.validate({"foo": 5})

        This will not work due to underlying types failing validation:

        >>> map_type.validate({"a": 5})
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed for 'a -> key': String must be at least 2 \
characters, 1 given
        >>> map_type.validate({"foo":1})
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed for 'foo -> value': Must be at least 3

        This will also fail because the map does not have enough elements:

        >>> map_type.validate({})
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed: Must have at least 1 elements, 0 given.
        """
        self._validate(data, path)
        for key in data.keys():
            value = data[key]
            new_path = list(path)
            new_path.append(key)
            key_path = list(tuple(new_path))
            key_path.append("key")
            self.keys.validate(key, tuple(key_path))
            value_path = list(tuple(new_path))
            value_path.append("value")
            self.values.validate(value, tuple(value_path))

    def serialize(self, data: MapT, path: typing.Tuple[str] = tuple([])) -> Any:
        """
        This function serializes the data into the transportable system.

        **Example:**

        Imports:

        >>> from arcaflow_plugin_sdk import schema

        Create a map type:

        >>> map_type = schema.MapType(
        ...     keys=schema.StringType(min=2),
        ...     values=schema.IntType(min=3),
        ...     min=1,
        ...     max=2,
        ... )

        This is valid:

        >>> map_type.serialize({"foo": 5})
        {'foo': 5}

        This will not work due to underlying types failing validation:

        >>> map_type.serialize({"a": 5})
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed for 'a -> key': String must be at least 2 \
characters, 1 given
        >>> map_type.serialize({"foo":1})
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed for 'foo -> value': Must be at least 3

        This will also fail because the map does not have enough elements:

        >>> map_type.serialize({})
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed: Must have at least 1 elements, 0 given.
        """
        entries = self._validate(data, path)
        result = {}
        for key in entries.keys():
            key_path = list(path)
            key_path.append(str(key))
            key_path.append("key")
            serialized_key = self.keys.serialize(key, tuple(key_path))
            value_path = list(path)
            value_path.append(str(key))
            value_path.append("value")
            value = self.values.serialize(data[key], tuple(value_path))
            result[serialized_key] = value
        entries = self._validate(result, path)
        return entries

    def _validate(self, data, path):
        if not isinstance(data, dict):
            raise ConstraintException(
                path, "Must be a dict, {} given".format(type(data).__name__)
            )
        entries = dict(data)
        if self.min is not None and len(entries) < self.min:
            raise ConstraintException(
                path,
                "Must have at least {} elements, {} given.".format(
                    self.min, len(entries)
                ),
            )
        if self.max is not None and len(entries) > self.max:
            raise ConstraintException(
                path,
                "Must have at most {} elements, {} given.".format(
                    self.max, len(entries)
                ),
            )
        return entries


PropertyT = TypeVar("PropertyT")


class PropertyType(PropertySchema, Generic[PropertyT]):
    """
    This class holds the schema definition for a single object property . It is usable in conjunction with ``ObjectType``.

    **Example:**

    Imports:

    >>> from arcaflow_plugin_sdk import schema
    >>> from dataclasses import dataclass

    Define your dataclass

    >>> @dataclass
    ... class ExampleData:
    ...    a: str

    Create a schema:

    >>> object_type = schema.build_object_schema(ExampleData)

    This will result in a scope object, containing the dataclass with the property as a root object:

    >>> object_type.properties["a"].type
    StringType(min=None, max=None, pattern=None)

    Alternatively, you can construct the type by hand:

    >>> object_type = schema.ObjectType(
    ...     ExampleData,
    ...     {
    ...         "a": schema.PropertyType(schema.StringType())
    ...     }
    ... )

    Now you can query the type as before:

    >>> object_type.properties["a"].type
    StringType(min=None, max=None, pattern=None)
    """

    field_override: str = ""

    def __init__(
        self,
        type: VALUE_TYPE,
        display: DISPLAY_TYPE = None,
        default: DEFAULT_TYPE = None,
        examples: EXAMPLES_TYPE = None,
        required: Optional[bool] = True,
        required_if: Optional[List[str]] = None,
        required_if_not: Optional[List[str]] = None,
        conflicts: Optional[List[str]] = None,
        field_override: str = "",
    ):
        # noinspection PyArgumentList
        PropertySchema.__init__(
            self,
            type,
            display,
            default,
            examples,
            required,
            required_if,
            required_if_not,
            conflicts,
        )
        self.field_override = field_override


ObjectT = TypeVar("ObjectT", bound=object)


@dataclass
class ObjectType(ObjectSchema, AbstractType, Generic[ObjectT]):
    """
    ``ObjectType`` represents an object with predefined fields. The property declaration must match the fields in the class.
    The type currently does not validate if the properties match the provided class.

    **Example:**

    Imports:

    >>> from arcaflow_plugin_sdk import schema
    >>> from dataclasses import dataclass

    Define your dataclass

    >>> @dataclass
    ... class ExampleData:
    ...    a: str

    Create a schema:

    >>> object_type = schema.build_object_schema(ExampleData)

    This will result in a scope object, containing the dataclass with the property as a root object:

    >>> object_type.properties["a"].type
    StringType(min=None, max=None, pattern=None)

    Alternatively, you can construct the type by hand:

    >>> object_type = schema.ObjectType(
    ...     ExampleData,
    ...     {
    ...         "a": schema.PropertyType(schema.StringType())
    ...     }
    ... )

    Now you can query the type as before:

    >>> object_type.properties["a"].type
    StringType(min=None, max=None, pattern=None)

    You can now use the object_type to unserialize, validate, and serialize properties.
    """

    _cls: Type[ObjectT]
    properties: Dict[str, PropertyType]

    def __init__(self, cls: Type[ObjectT], properties: Dict[str, PropertyType]):
        super().__init__(cls.__name__, properties)
        self._cls = cls
        self._validate_config(cls, properties)

    @property
    def cls(self) -> Type[ObjectT]:
        return self._cls

    @staticmethod
    def _validate_config(cls_type: Type[ObjectT], properties: Dict[str, PropertyType]):
        if not isinstance(cls_type, type):
            raise BadArgumentException(
                "The passed class argument '{}' is not a type. Please pass a type.".format(
                    type(cls_type).__name__
                )
            )
        if not isinstance(properties, dict):
            raise BadArgumentException(
                "The properties parameter to 'ObjectType' must be a 'dict', '{}' given".format(
                    type(properties).__name__
                )
            )
        try:
            # noinspection PyDataclass
            dataclasses.fields(cls_type)
        except Exception as e:
            raise BadArgumentException(
                "The passed class '{}' is not a dataclass. Please use a dataclass.".format(
                    cls_type.__name__
                )
            ) from e

        class_type_hints = typing.get_type_hints(cls_type)
        init_type_hints = typing.get_type_hints(cls_type.__init__)
        if (
            len(init_type_hints) == len(properties) + 1
            and "return" in init_type_hints
            and "return" not in properties
        ):
            del init_type_hints["return"]
        if len(properties) != len(init_type_hints):
            raise BadArgumentException(
                "The '{}' class has an invalid number of parameters in the '__init__' function. Expected: {} got: {}\n"
                "The declared '__init__' parameters are the following: '{}'\n"
                "The '__init__' parameters must match your declared parameters exactly so the Arcaflow plugin SDK can "
                "inject the data values.".format(
                    cls_type.__name__,
                    len(init_type_hints),
                    len(properties),
                    "', '".join(init_type_hints.keys()),
                )
            )

        if len(properties) > 0:
            for property_id, property in properties.items():
                # Determine what the field is called we are supposed to access. (This may be different from the
                # serialized representation.)
                field_id = property_id
                if property.field_override != "":
                    field_id = property.field_override

                # Check if the property can be properly set:
                if field_id not in init_type_hints:
                    raise BadArgumentException(
                        "The '__init__' function for the class '{}' does not contain a parameter called '{}' as "
                        "required by the property '{}'. Please make sure all declared properties are settable from the"
                        "'__init__' function so the Arcaflow SDK can fill your dataclass with the unserialized data.\n"
                        "Init parameters: '{}'".format(
                            cls_type.__name__,
                            field_id,
                            property_id,
                            "', '".join(init_type_hints.keys()),
                        )
                    )
                # TODO in the future we should check if the data types of the constructor match with the property.
                # This can be a source of bugs when the code assumes the data is of one type, but the actual data
                # type is different. This is not a problem if the schema is auto-generated, but hand-written schemas
                # can accidentally introduce this issue.
                #
                # See: https://github.com/arcalot/arcaflow-plugin-sdk-python/issues/46

                # Check if the property can be read:
                if field_id not in class_type_hints:
                    raise BadArgumentException(
                        "The '{}' class does not contain a property called '{}' as "
                        "required by the declared property '{}'. Please make sure all declared properties are "
                        "gettable from the class so the Arcaflow SDK can fill your dataclass with the unserialized "
                        "data.\n"
                        "Class properties: '{}'".format(
                            cls_type.__name__,
                            field_id,
                            property_id,
                            "', '".join(class_type_hints.keys()),
                        )
                    )
                # TODO in the future we should check if the data types of the properties match with the property.
                # This can be a source of bugs when the code assumes the data is of one type, but the actual data
                # type is different. This is not a problem if the schema is auto-generated, but hand-written schemas
                # can accidentally introduce this issue.
                #
                # See: https://github.com/arcalot/arcaflow-plugin-sdk-python/issues/46

    @classmethod
    def _resolve_class_type_hints(
        cls, cls_type: type, level: int = 0
    ) -> Dict[str, inspect.Parameter]:
        if level > 64:
            raise BadArgumentException(
                "Too many parent classes of {}".format(cls_type.__name__)
            )
        try:
            class_type_hints = {}
            if hasattr(cls_type, "__dict__"):
                class_type_hints = typing.get_type_hints(cls_type)
            if hasattr(cls_type, "__bases__"):
                for base in cls_type.__bases__:
                    base_class_type_hints = cls._resolve_class_type_hints(
                        base, level + 1
                    )
                    for k, v in base_class_type_hints.items():
                        if k not in class_type_hints:
                            class_type_hints[k] = v
            return class_type_hints
        except BadArgumentException:
            raise BadArgumentException(
                "Too many parent classes of {}".format(cls_type.__name__)
            )

    def unserialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> ObjectT:
        """
        This function unserializes a dict into a dataclass.

        **Example:**

        Imports:

        >>> from arcaflow_plugin_sdk import schema
        >>> from dataclasses import dataclass

        Define a dataclass:

        >>> @dataclass
        ... class TestData:
        ...     a: str

        Build a schema:

        >>> object_type = schema.build_object_schema(TestData)

        Unserialize data:

        >>> object_type.unserialize({"a":"Hello world!"})
        TestData(a='Hello world!')

        This will fail:

        >>> object_type.unserialize("Hello world!")
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed for 'TestData': Must be a dict, got str
        """
        if not isinstance(data, dict):
            raise ConstraintException(
                path, "Must be a dict, got {}".format(type(data).__name__)
            )
        kwargs = {}
        for key in data.keys():
            if key not in self.properties:
                raise ConstraintException(
                    path,
                    "Invalid parameter '{}', expected one of: {}".format(
                        key, ", ".join(self.properties.keys())
                    ),
                )
        for property_id in self.properties.keys():
            object_property: PropertyType = self.properties[property_id]
            property_value: Optional[any] = None
            try:
                property_value = data[property_id]
            except KeyError:
                pass
            new_path = list(path)
            new_path.append(property_id)
            if property_value is not None:
                field_id = property_id
                if object_property.field_override != "":
                    field_id = object_property.field_override
                kwargs[field_id] = object_property.type.unserialize(
                    property_value, tuple(new_path)
                )

                if object_property.conflicts is not None:
                    for conflict in object_property.conflicts:
                        if conflict in data:
                            raise ConstraintException(
                                tuple(new_path),
                                "Field conflicts '{}', set one of the two, not both".format(
                                    conflict
                                ),
                            )
            else:
                self._validate_not_set(data, object_property, tuple(new_path))
        return self._cls(**kwargs)

    def validate(self, data: TypeT, path: typing.Tuple[str] = tuple([])):
        """
        This function will validate the dataclass and all underlying types as well.

        **Example:**

        Imports:

        >>> from arcaflow_plugin_sdk import schema
        >>> from dataclasses import dataclass

        Define a dataclass:

        >>> @dataclass
        ... class TestData:
        ...     a: typing.Annotated[str, schema.min(1)]

        Build a schema:

        >>> object_type = schema.build_object_schema(TestData)

        Validate a data class:

        >>> object_type.validate(TestData("Hello world!"))

        These will fail:

        >>> object_type.validate(TestData(""))
        Traceback (most recent call last):
        ...
        arcaflow_plugin_sdk.schema.ConstraintException: Validation failed for 'TestData -> a': String must be at least \
1 characters, 0 given
        """
        if not isinstance(data, self._cls):
            raise ConstraintException(
                path,
                "Must be an instance of {}, {} given".format(
                    self._cls.__name__, type(data).__name__
                ),
            )
        values = {}
        for property_id in self.properties.keys():
            property_field: PropertyType = self.properties[property_id]
            field_id = property_id
            if property_field.field_override != "":
                field_id = property_field.field_override
            new_path, value = self._validate_property(data, path, field_id, property_id)
            if value is not None:
                property_field.type.validate(value, tuple(new_path))
                values[property_id] = value
        for property_id in self.properties.keys():
            property_field: PropertyType = self.properties[property_id]
            new_path = list(path)
            new_path.append(property_id)
            if property_id in values.keys():
                if property_field.conflicts is not None:
                    for conflicts in property_field.conflicts:
                        if conflicts in values.keys():
                            raise ConstraintException(
                                tuple(new_path),
                                "Field conflicts with {}".format(conflicts),
                            )
            else:
                if property_field.required:
                    raise ConstraintException(
                        tuple(new_path), "Field is required but not set"
                    )
                if (
                    property_field.required_if_not is not None
                    and len(property_field.required_if_not) > 0
                ):
                    found = False
                    for required_if_not in property_field.required_if_not:
                        if required_if_not in values.keys():
                            found = True
                            break
                    if not found:
                        raise ConstraintException(
                            tuple(new_path),
                            "Field is required because none of '{}' are set".format(
                                "', '".join(property_field.required_if_not)
                            ),
                        )

                if property_field.required_if is not None:
                    for required_if in property_field.required_if:
                        if required_if in values.keys():
                            raise ConstraintException(
                                tuple(new_path),
                                "Field is required because none of '{}' are set".format(
                                    "', '".join(property_field.required_if_not)
                                ),
                            )

    def serialize(self, data: ObjectT, path: typing.Tuple[str] = tuple([])) -> Any:
        if not isinstance(data, self._cls):
            raise ConstraintException(
                path,
                "Must be an instance of {}, {} given".format(
                    self._cls.__name__, type(data).__name__
                ),
            )
        result = {}
        for property_id in self.properties.keys():
            field_id = property_id
            property_field: PropertyType = self.properties[property_id]
            if property_field.field_override != "":
                field_id = property_field.field_override
            new_path, value = self._validate_property(data, path, field_id, property_id)
            if value is not None:
                result[property_id] = property_field.type.serialize(
                    getattr(data, field_id), tuple(new_path)
                )
        return result

    def _validate_property(
        self, data: TypeT, path: typing.Tuple[str], field_id: str, property_id: str
    ):
        new_path = list(path)
        new_path.append(property_id)
        value = getattr(data, field_id)
        property_field: PropertyType = self.properties[property_id]
        if value is None:
            self._validate_not_set(data, property_field, tuple(new_path))
        return new_path, value

    @staticmethod
    def _validate_not_set(data, object_property: PropertyType, path: typing.Tuple[str]):
        if object_property.required:
            raise ConstraintException(path, "This field is required")
        if object_property.required_if is not None:
            for required_if in object_property.required_if:
                if (isinstance(data, dict) and required_if in data) or (
                    hasattr(data, required_if) and getattr(data, required_if) is None
                ):
                    raise ConstraintException(
                        path,
                        "This field is required because '{}' is set".format(
                            required_if
                        ),
                    )
        if (
            object_property.required_if_not is not None
            and len(object_property.required_if_not) > 0
        ):
            none_set = True
            for required_if_not in object_property.required_if_not:
                if (isinstance(data, dict) and required_if_not in data) or (
                    hasattr(data, required_if_not)
                    and getattr(data, required_if_not) is not None
                ):
                    none_set = False
                    break
            if none_set:
                if len(object_property.required_if_not) == 1:
                    raise ConstraintException(
                        path,
                        "This field is required because '{}' is not set".format(
                            object_property.required_if_not[0]
                        ),
                    )
                raise ConstraintException(
                    path,
                    "This field is required because none of '{}' are set".format(
                        "', '".join(object_property.required_if_not)
                    ),
                )


OneOfT = TypeVar("OneOfT")
DiscriminatorT = TypeVar("DiscriminatorT")


class _OneOfType(AbstractType[OneOfT], Generic[OneOfT, DiscriminatorT]):
    discriminator_field_name: str
    types: Dict[DiscriminatorT, typing.ForwardRef("RefType")]
    _t: any
    _scope: typing.ForwardRef("ScopeType")

    def __init__(
        self,
        types: Dict[DiscriminatorT, typing.ForwardRef("RefType")],
        t: Type[DiscriminatorT],
        scope: typing.ForwardRef("ScopeType"),
        discriminator_field_name: str,
    ):
        if not isinstance(scope, ScopeType):
            raise BadArgumentException(
                "The 'scope' parameter for OneOf*Type must be a ScopeType, {} given".format(
                    type(scope).__name__
                )
            )
        if not isinstance(types, dict):
            raise BadArgumentException(
                "The 'scope' parameter for OneOf*Type must be a ScopeType, {} given".format(
                    type(scope).__name__
                )
            )
        for k, v in types.items():
            if (
                not isinstance(v, RefType)
                and not isinstance(v, ObjectType)
                and not isinstance(v, ScopeType)
            ):
                raise BadArgumentException(
                    "The 'types' parameter of OneOf*Type must contain RefTypes, ObjectTypes, or ScopeTypes, "
                    "{} found for key {}".format(type(v).__name__, v)
                )
        self._t = t
        self._scope = scope
        self.discriminator_field_name = discriminator_field_name

    def unserialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> OneOfT:
        if not isinstance(data, dict):
            raise ConstraintException(
                path, "Must be a dict, got {}".format(type(data).__name__)
            )
        new_path: List[str] = list(path)
        new_path.append(self.discriminator_field_name)
        if self.discriminator_field_name not in data:
            raise ConstraintException(
                tuple(new_path), "Required discriminator field not found"
            )
        unserialized_discriminator_field: str = data[self.discriminator_field_name]
        if not isinstance(unserialized_discriminator_field, self._t):
            raise ConstraintException(
                tuple(new_path),
                "{} required, {} found".format(
                    self._t.__name__, type(unserialized_discriminator_field).__name__
                ),
            )
        if unserialized_discriminator_field not in self.types:
            raise ConstraintException(
                tuple(new_path),
                "Invalid value for field: '{}' expected one of: '{}'".format(
                    unserialized_discriminator_field,
                    "', '".join(list(self.types.keys())),
                ),
            )
        sub_type = self.types[unserialized_discriminator_field]
        if self.discriminator_field_name not in sub_type.properties:
            del data[self.discriminator_field_name]
        return sub_type.unserialize(data, path)

    def validate(self, data: OneOfT, path: typing.Tuple[str] = tuple([])):
        types = []
        for discriminator, item_schema in self.types.items():
            item_schema: typing.ForwardRef("RefType")
            types.append(item_schema.id)
            object_schema: ObjectType = self._scope[item_schema.id]

            if isinstance(data, object_schema.cls):
                item_schema.validate(data)
                if self.discriminator_field_name in object_schema.properties:
                    if getattr(data, self.discriminator_field_name) != discriminator:
                        new_path = list(path)
                        new_path.append(self.discriminator_field_name)
                        raise ConstraintException(
                            tuple(new_path),
                            "Invalid value for '{}' on '{}', should be: '{}'".format(
                                self.discriminator_field_name,
                                object_schema.cls.__name__,
                                discriminator,
                            ),
                        )
                return
        raise ConstraintException(
            tuple(path),
            "Invalid type: '{}', expected one of '{}'".format(
                type(data).__name__, "', '".join(types)
            ),
        )

    def serialize(self, data: OneOfT, path: typing.Tuple[str] = tuple([])) -> Any:
        types = []
        for discriminator, item_schema in self.types.items():
            item_schema: RefType
            types.append(item_schema.id)
            object_schema: ObjectType = self._scope[item_schema.id]
            if isinstance(data, object_schema.cls):
                serialized_data = item_schema.serialize(data)
                if self.discriminator_field_name in object_schema.properties:
                    if getattr(data, self.discriminator_field_name) != discriminator:
                        new_path = list(path)
                        new_path.append(self.discriminator_field_name)
                        raise ConstraintException(
                            tuple(new_path),
                            "Invalid value for '{}' on '{}', should be: '{}'".format(
                                self.discriminator_field_name,
                                object_schema.cls.__name__,
                                discriminator,
                            ),
                        )
                else:
                    serialized_data[self.discriminator_field_name] = discriminator
                return serialized_data
        raise ConstraintException(
            tuple(path),
            "Invalid type: '{}', expected one of '{}'".format(
                type(data).__name__, "', '".join(types)
            ),
        )


class OneOfStringType(OneOfStringSchema, _OneOfType[OneOfT, str], Generic[OneOfT]):
    def __init__(
        self,
        types: Dict[str, typing.Annotated[_OBJECT_LIKE, discriminator("type_id")]],
        scope: typing.ForwardRef("ScopeType"),
        discriminator_field_name: str = "_type",
    ):
        # noinspection PyArgumentList
        OneOfStringSchema.__init__(self, types, discriminator_field_name)
        _OneOfType.__init__(
            self,
            types,
            t=str,
            scope=scope,
            discriminator_field_name=discriminator_field_name,
        )


class OneOfIntType(OneOfIntSchema, _OneOfType[OneOfT, int], Generic[OneOfT]):
    types: Dict[
        int,
        typing.Union[
            typing.ForwardRef("RefSchema"),
            typing.ForwardRef("ScopeSchema"),
            typing.ForwardRef("ObjectSchema"),
        ],
    ]

    def __init__(
        self,
        types: Dict[
            int,
            typing.Union[
                typing.ForwardRef("RefSchema"),
                typing.ForwardRef("ScopeSchema"),
                typing.ForwardRef("ObjectSchema"),
            ],
        ],
        scope: typing.ForwardRef("ScopeType"),
        discriminator_field_name: str = "_type",
    ):
        # noinspection PyArgumentList
        OneOfIntSchema.__init__(self, types, discriminator_field_name)
        _OneOfType.__init__(
            self,
            types,
            t=int,
            scope=scope,
            discriminator_field_name=discriminator_field_name,
        )


class ScopeType(ScopeSchema, AbstractType):
    """
    A scope is a container object for an object structure. Its main purpose is to hold objects that can be referenced,
    even in cases where circular references are desired. It mimics the ObjectType and provides several properties that
    proxy through to the underlying root object if set.
    """

    objects: Dict[ID_TYPE, ObjectType]

    def __init__(
        self,
        objects: Dict[ID_TYPE, ObjectType],
        root: typing.Optional[str],
    ):
        # noinspection PyArgumentList
        ScopeSchema.__init__(self, objects, root)

    def __getitem__(self, item):
        try:
            return self.objects[item]
        except KeyError:
            raise BadArgumentException(
                "Referenced object is not defined: {}".format(item)
            )

    @property
    def properties(self) -> Dict[str, PropertyType]:
        if self.root is None:
            path: typing.Tuple[str] = tuple()
            raise ConstraintException(
                path, "Cannot get properties, root object is not set on scope."
            )
        return self.objects[self.root].properties

    @property
    def cls(self) -> Type[ObjectT]:
        if self.root is None:
            path: typing.Tuple[str] = tuple()
            raise ConstraintException(
                path, "Cannot get cls, root object is not set on scope."
            )
        return self.objects[self.root].cls

    @property
    def id(self) -> str:
        if self.root is None:
            path: typing.Tuple[str] = tuple()
            raise ConstraintException(
                path, "Cannot get id, root object is not set on scope."
            )
        return self.objects[self.root].id

    def unserialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> TypeT:
        if self.root is None:
            raise ConstraintException(
                path, "Cannot unserialize, root object is not set on scope."
            )
        root_object: ObjectType = self.objects[self.root]
        new_path: List[str] = list(path)
        new_path.append(root_object.cls.__name__)
        return root_object.unserialize(data, tuple(new_path))

    def validate(self, data: TypeT, path: typing.Tuple[str] = tuple([])):
        if self.root is None:
            raise ConstraintException(
                path, "Cannot validate, root object is not set on scope."
            )
        root_object: ObjectType = self.objects[self.root]
        new_path: List[str] = list(path)
        new_path.append(root_object.cls.__name__)
        return root_object.validate(data, tuple(new_path))

    def serialize(self, data: TypeT, path: typing.Tuple[str] = tuple([])) -> Any:
        if self.root is None:
            raise ConstraintException(
                path, "Cannot serialize, root object is not set on scope."
            )
        root_object: ObjectType = self.objects[self.root]
        new_path: List[str] = list(path)
        new_path.append(root_object.cls.__name__)
        return root_object.serialize(data, tuple(new_path))


class RefType(RefSchema, AbstractType):
    """
    A ref is a reference to an object in a Scope.
    """

    _scope: ScopeType

    def __init__(self, id: str, scope: ScopeType):
        super().__init__(id)
        self._scope = scope

    @property
    def properties(self):
        return self._scope[self.id].properties

    def unserialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> TypeT:
        return self._scope.objects[self.id].unserialize(data, path)

    def validate(self, data: TypeT, path: typing.Tuple[str] = tuple([])):
        return self._scope.objects[self.id].validate(data, path)

    def serialize(self, data: TypeT, path: typing.Tuple[str] = tuple([])) -> Any:
        return self._scope.objects[self.id].serialize(data, path)


class AnyType(AnySchema, AbstractType):
    """
    This type is a serializable version of the "any" type.

    **Examples:**

    Import:

    >>> from arcaflow_plugin_sdk import schema

    Create an AnyType:
    >>> t = schema.AnyType()

    Unserialize a value:
    >>> t.unserialize("Hello world!")
    'Hello world!'
    >>> t.unserialize(1)
    1
    >>> t.unserialize(1.0)
    1.0
    >>> t.unserialize(True)
    True
    >>> t.unserialize({"message": "Hello world!"})
    {'message': 'Hello world!'}
    >>> t.unserialize(["Hello world!"])
    ['Hello world!']

    Validate a value:
    >>> t.validate("Hello world!")
    >>> t.validate(1)
    >>> t.validate(1.0)
    >>> t.validate(True)
    >>> t.validate({"message": "Hello world!"})
    >>> t.validate(["Hello world!"])

    Serialize a value:
    >>> t.serialize("Hello world!")
    'Hello world!'
    >>> t.serialize(1)
    1
    >>> t.serialize(1.0)
    1.0
    >>> t.serialize(True)
    True
    >>> t.serialize({"message": "Hello world!"})
    {'message': 'Hello world!'}
    >>> t.serialize(["Hello world!"])
    ['Hello world!']

    Not everything is accepted:
    >>> from dataclasses import dataclass
    >>> @dataclass
    ... class TestClass:
    ...     pass
    >>> t.unserialize(TestClass())
    Traceback (most recent call last):
    ...
    arcaflow_plugin_sdk.schema.ConstraintException: Validation failed: Unsupported data type for 'any' type: TestClass
    """

    def unserialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> Any:
        self._check(data, path)
        return data

    def validate(self, data: Any, path: typing.Tuple[str] = tuple([])):
        self._check(data, path)

    def serialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> Any:
        self._check(data, path)
        return data

    def _check(self, data: Any, path: typing.Tuple[str] = tuple([])):
        if isinstance(data, list):
            for i in range(len(data)):
                new_path = list(path)
                new_path.append("[{}]".format(i))
                self._check(data[i], tuple(new_path))
            return
        elif isinstance(data, dict):
            for k in data.keys():
                new_path = list(path)
                new_path.append("{{{}}}".format(k))
                self._check(k, tuple(new_path))

                new_path = list(path)
                new_path.append("[{}]".format(k))
                self._check(data[k], tuple(new_path))
            return
        elif isinstance(data, str):
            return
        elif isinstance(data, int):
            return
        elif isinstance(data, float):
            return
        elif isinstance(data, bool):
            return
        elif isinstance(data, type(None)):
            return
        else:
            raise ConstraintException(
                path,
                "Unsupported data type for 'any' type: {}".format(
                    data.__class__.__name__
                ),
            )


class StepOutputType(StepOutputSchema, AbstractType):
    """
    This class holds the possible outputs of a step and the metadata information related to these outputs.
    """

    schema: ScopeType
    display: Optional[DisplayValue] = None
    error: bool = False

    def unserialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> TypeT:
        return self.schema.unserialize(data, path)

    def validate(self, data: TypeT, path: typing.Tuple[str] = tuple([])):
        return self.schema.validate(data, path)

    def serialize(self, data: TypeT, path: typing.Tuple[str] = tuple([])) -> Any:
        return self.schema.serialize(data, path)


StepInputT = TypeVar("StepInputT", bound=object)
StepOutputT = TypeVar("StepOutputT", bound=object)


class StepType(StepSchema):
    """
    StepSchema describes the schema for a single step. The input is always one ObjectType, while there are multiple
    possible outputs identified by a string.
    """

    _handler: Callable[[StepInputT], typing.Tuple[str, StepOutputT]]
    input: ScopeType
    outputs: Dict[ID_TYPE, StepOutputType]

    def __init__(
        self,
        id: str,
        handler: Callable[[StepInputT], typing.Tuple[str, StepOutputT]],
        input: ScopeType,
        outputs: Dict[ID_TYPE, StepOutputType],
        display: Optional[DisplayValue] = None,
    ):
        super().__init__(id, input, outputs, display)
        self._handler = handler

    def __call__(
        self,
        params: StepInputT,
        skip_input_validation: bool = False,
        skip_output_validation: bool = False,
    ) -> typing.Tuple[str, StepOutputT]:
        """
        :param params: Input parameter for the step.
        :param skip_input_validation: Do not perform input data type validation. Use at your own risk.
        :param skip_output_validation: Do not validate returned output data. Use at your own risk.
        :return: The ID for the output datatype, and the output itself.
        """
        input: ScopeType = self.input
        if not skip_input_validation:
            input.validate(params, tuple(["input"]))
        result = self._handler(params)
        if len(result) != 2:
            raise BadArgumentException(
                "The step returned {} results instead of 2. Did your step return the correct results?".format(
                    len(result)
                )
            )
        output_id, output_data = result
        if output_id not in self.outputs:
            raise BadArgumentException(
                "The step returned an undeclared output ID: %s, please return one of: '%s'"
                % (output_id, "', '".join(self.outputs.keys()))
            )
        output: StepOutputType = self.outputs[output_id]
        if not skip_output_validation:
            output.validate(output_data, tuple(["output", output_id]))
        return output_id, output_data


class SchemaType(Schema):
    """
    A schema is a definition of one or more steps that can be executed. The step has a defined input and output
    """

    steps: Dict[str, StepType]

    def unserialize_input(self, step_id: str, data: Any) -> Any:
        """
        This function unserializes the input from a raw data to data structures, such as dataclasses. This function is
        automatically called by ``__call__`` before running the step with the unserialized input.

        :param step_id: The step ID to use to look up the schema for unserialization.
        :param data: The raw data to unserialize.
        :return: The unserialized data in the structure the step expects it.
        """
        if step_id not in self.steps:
            raise NoSuchStepException(step_id)
        step = self.steps[step_id]
        return self._unserialize_input(step, data)

    @staticmethod
    def _unserialize_input(step: StepType, data: Any) -> Any:
        try:
            return step.input.unserialize(data)
        except ConstraintException as e:
            raise InvalidInputException(e) from e

    def call_step(self, step_id: str, input_param: Any) -> typing.Tuple[str, Any]:
        """
        This function calls a specific step with the input parameter that has already been unserialized. It expects the
        data to be already valid, use unserialize_input to produce a valid input. This function is automatically called
        by ``__call__`` after unserializing the input.

        :param step_id: The ID of the input step to run.
        :param input_param: The unserialized data structure the step expects.
        :return: The ID of the output, and the data structure returned from the step.
        """
        if step_id not in self.steps:
            raise NoSuchStepException(step_id)
        step = self.steps[step_id]
        return self._call_step(step, input_param)

    @staticmethod
    def _call_step(
        step: StepType,
        input_param: Any,
        skip_input_validation: bool = False,
        skip_output_validation: bool = False,
    ) -> typing.Tuple[str, Any]:
        return step(
            input_param,
            skip_input_validation=skip_input_validation,
            skip_output_validation=skip_output_validation,
        )

    def serialize_output(self, step_id: str, output_id: str, output_data: Any) -> Any:
        """
        This function takes an output ID (e.g. "error") and structured output_data and serializes them into a format
        suitable for wire transport. This function is automatically called by ``__call__`` after the step is run.

        :param step_id: The step ID to use to look up the schema for serialization.
        :param output_id: The string identifier for the output data structure.
        :param output_data: The data structure returned from the step.
        :return:
        """
        if step_id not in self.steps:
            raise NoSuchStepException(step_id)
        step = self.steps[step_id]
        return self._serialize_output(step, output_id, output_data)

    @staticmethod
    def _serialize_output(step, output_id: str, output_data: Any) -> Any:
        try:
            return step.outputs[output_id].serialize(output_data)
        except ConstraintException as e:
            raise InvalidOutputException(e) from e

    def __call__(
        self, step_id: str, data: Any, skip_serialization: bool = False
    ) -> typing.Tuple[str, Any]:
        """
        This function takes the input data, unserializes it for the specified step, calls the specified step, and,
        unless skip_serialization is set, serializes the return data.

        :param step_id: the step to execute
        :param data: input data
        :param skip_serialization: skip result serialization to basic types
        :return: the result ID, and the resulting data in the structure matching the result ID
        """
        if step_id not in self.steps:
            raise NoSuchStepException(step_id)
        step = self.steps[step_id]
        input_param = self._unserialize_input(step, data)
        output_id, output_data = self._call_step(
            step,
            input_param,
            # Skip duplicate verification
            skip_input_validation=True,
            skip_output_validation=True,
        )
        if skip_serialization:
            step.outputs[output_id].validate(output_data)
            return output_id, output_data
        serialized_output_data = self._serialize_output(step, output_id, output_data)
        return output_id, serialized_output_data


# endregion

# region Build


class _SchemaBuilder:
    @classmethod
    def resolve(cls, t: any, scope: ScopeType) -> AbstractType:
        path: typing.List[str] = []
        if hasattr(t, "__name__"):
            path.append(t.__name__)

        return cls._resolve_abstract_type(t, t, tuple(path), scope)

    @classmethod
    def _resolve_abstract_type(
        cls,
        t: any,
        type_hints: type,
        path: typing.Tuple[str],
        scope: ScopeType,
    ) -> AbstractType:
        result = cls._resolve(t, type_hints, path, scope)
        if isinstance(result, PropertyType):
            res: PropertyType = result
            new_path: List[str] = list(path)
            name = res.display.name
            if name is None:
                raise SchemaBuildException(path, "BUG: resolved property with no name")
            new_path.append(name)
            raise SchemaBuildException(
                tuple(new_path),
                "Unsupported attribute combination, you can only use typing.Optional, etc. in classes, but not in "
                "lists, dicts, etc.",
            )
        res: AbstractType = result
        return res

    @classmethod
    def _resolve_field(
        cls, t: any, type_hints: type, path: typing.Tuple[str], scope: ScopeType
    ) -> PropertyType:
        result = cls._resolve(t, type_hints, path, scope)
        if not isinstance(result, PropertyType):
            result = PropertyType(result)
        return result

    @classmethod
    def _resolve(
        cls,
        t: any,
        type_hints: type,
        path: typing.Tuple[str],
        scope: ScopeType,
    ) -> typing.Union[AbstractType, PropertyType]:
        if t == typing.Any:
            return cls._resolve_any()
        elif isinstance(t, type):
            return cls._resolve_type(t, type_hints, path, scope)
        elif isinstance(t, str):
            return cls._resolve_string(t, type_hints, path, scope)
        elif isinstance(t, bool):
            return cls._resolve_bool(t, type_hints, path, scope)
        elif isinstance(t, int):
            return cls._resolve_int(t, type_hints, path, scope)
        elif isinstance(t, float):
            return cls._resolve_float(t, type_hints, path, scope)
        elif isinstance(t, list):
            return cls._resolve_list(t, type_hints, path, scope)
        elif isinstance(t, dict):
            return cls._resolve_dict(t, type_hints, path, scope)
        elif isinstance(t, typing.ForwardRef):
            return cls._resolve_forward(t, type_hints, path, scope)
        elif typing.get_origin(t) == list:
            return cls._resolve_list_annotation(t, type_hints, path, scope)
        elif typing.get_origin(t) == dict:
            return cls._resolve_dict_annotation(t, type_hints, path, scope)
        elif typing.get_origin(t) == typing.Union:
            return cls._resolve_union(t, type_hints, path, scope)
        elif typing.get_origin(t) == typing.Annotated:
            return cls._resolve_annotated(t, type_hints, path, scope)
        else:
            raise SchemaBuildException(
                path, "Unable to resolve underlying type: %s" % type(t).__name__
            )

    @classmethod
    def _resolve_type(
        cls, t, type_hints: type, path: typing.Tuple[str], scope: ScopeType
    ):
        if t == ANY_TYPE:
            return cls._resolve_any()
        elif issubclass(t, Enum):
            return cls._resolve_enum(t, type_hints, path, scope)
        elif t == re.Pattern:
            return cls._resolve_pattern(t, type_hints, path, scope)
        elif t == str:
            return cls._resolve_string_type(t, type_hints, path, scope)
        elif t == bool:
            return cls._resolve_bool_type(t, type_hints, path, scope)
        elif t == int:
            return cls._resolve_int_type(t, type_hints, path, scope)
        elif t == float:
            return cls._resolve_float_type(t, type_hints, path, scope)
        elif t == list:
            return cls._resolve_list_type(t, type_hints, path, scope)
        elif typing.get_origin(t) == dict:
            return cls._resolve_dict_annotation(t, type_hints, path, scope)
        elif t == dict:
            return cls._resolve_dict_type(t, type_hints, path, scope)
        return cls._resolve_class(t, type_hints, path, scope)

    @classmethod
    def _resolve_any(cls):
        return AnyType()

    @classmethod
    def _resolve_enum(
        cls,
        t,
        type_hints: type,
        path: typing.Tuple[str],
        scope: ScopeType,
    ) -> AbstractType:
        try:
            try:
                return StringEnumType(t)
            except BadArgumentException:
                return IntEnumType(t)
        except Exception as e:
            raise SchemaBuildException(
                path, "Constraint exception while creating enum type"
            ) from e

    @classmethod
    def _resolve_dataclass_field(
        cls,
        t: dataclasses.Field,
        type_hints: type,
        path: typing.Tuple[str],
        scope: ScopeType,
    ) -> typing.Tuple[str, PropertyType]:
        underlying_type = cls._resolve_field(t.type, type_hints, path, scope)
        base_type = underlying_type.type
        if underlying_type.display is None:
            underlying_type.display = DisplayValue()

        if hasattr(underlying_type, "__name"):
            underlying_type.display.name = getattr(underlying_type, "__name")
        elif hasattr(base_type, "__name"):
            underlying_type.display.name = getattr(base_type, "__name")
        if underlying_type.display.name == "" or underlying_type.display.name is None:
            meta_name = t.metadata.get("name")
            if meta_name != "" and meta_name is not None:
                underlying_type.display.name = meta_name

        if hasattr(underlying_type, "__description"):
            underlying_type.display.name = getattr(underlying_type, "__description")
        elif hasattr(base_type, "__description"):
            underlying_type.display.description = getattr(base_type, "__description")
        if (
            underlying_type.display.description == ""
            or underlying_type.display.description is None
        ):
            meta_description = t.metadata.get("description")
            if meta_description != "" and meta_description is not None:
                underlying_type.display.description = meta_description

        if hasattr(underlying_type, "__icon"):
            underlying_type.display.icon = getattr(underlying_type, "__icon")
        elif hasattr(base_type, "__icon"):
            underlying_type.display.icon = getattr(base_type, "__icon")
        if underlying_type.display.icon == "" or underlying_type.display.icon is None:
            meta_icon = t.metadata.get("icon")
            if meta_icon != "" and meta_icon is not None:
                underlying_type.display.icon = meta_icon

        if hasattr(underlying_type, "__examples"):
            underlying_type.examples = getattr(underlying_type, "__examples")
        elif hasattr(base_type, "__examples"):
            underlying_type.examples = getattr(base_type, "__examples")
        if (
            underlying_type.examples == ""
            or underlying_type.examples is None
            or underlying_type.examples == []
        ):
            meta_examples = t.metadata.get("examples")
            if (
                meta_examples != ""
                and meta_examples is not None
                and meta_examples != []
            ):
                underlying_type.examples = meta_examples

        meta_id = t.name
        if hasattr(underlying_type, "__id"):
            meta_id = getattr(underlying_type, "__id")
        elif hasattr(base_type, "__id"):
            meta_id = getattr(base_type, "__id")
        elif t.metadata.get("id") is not None:
            meta_id = t.metadata.get("id")
        if meta_id is not None and meta_id != t.name:
            underlying_type.field_override = t.name

        if t.default != dataclasses.MISSING or t.default_factory != dataclasses.MISSING:
            underlying_type.required = False
            if t.default != dataclasses.MISSING:
                default = t.default
            else:
                default = t.default_factory()
            if default is not None:
                try:
                    underlying_type.default = json.dumps(
                        underlying_type.type.serialize(default)
                    )
                except ConstraintException as e:
                    raise SchemaBuildException(
                        path,
                        "Failed to serialize default value: {}".format(e.__str__()),
                    )
        elif not underlying_type.required:
            raise SchemaBuildException(
                path,
                "Field is marked as optional, but does not have a default value set. "
                "Please set a default value for this field.",
            )
        return meta_id, underlying_type

    @classmethod
    def _resolve_class(
        cls,
        t,
        type_hints: type,
        path: typing.Tuple[str],
        scope: ScopeType,
    ) -> AbstractType:
        if t.__name__ in scope.objects:
            return RefType(t.__name__, scope)
        final_fields: Dict[str, PropertyType] = {}

        try:
            fields_list = dataclasses.fields(t)
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
                types.GenericAlias: "generic aliases",
                types.ModuleType: "modules",
            }
            for unsupported_type, unsupported_type_name in unsupported_types.items():
                if isinstance(t, unsupported_type) or t == unsupported_type:
                    raise SchemaBuildException(
                        path,
                        "{} are not supported by the Arcaflow typing system and cannot be used in input or output data"
                        "types. Please use one of the supported types, or file an issue at {} with your use case to "
                        "get them included.".format(unsupported_type_name, _issue_url),
                    )
            raise SchemaBuildException(
                path,
                "{} is not a dataclass or a supported type. Please use the @dataclasses.dataclass decorator on your "
                "class or use a supported native type. If this is a native Python type and you want to request support "
                "for it in the Arcaflow SDK, please open an issue at {} to get it included.".format(
                    t.__name__, _issue_url
                ),
            ) from e

        # Add placeholder object to stop recursion
        # noinspection PyTypeChecker
        scope.objects[t.__name__] = None

        type_hints = typing.get_type_hints(t)
        for f in fields_list:
            new_path = list(path)
            new_path.append(f.name)
            name, final_field = cls._resolve_dataclass_field(
                f, type_hints[f.name], tuple(new_path), scope
            )
            final_fields[name] = final_field

        try:
            scope.objects[t.__name__] = ObjectType(
                t,
                final_fields,
            )

            return RefType(t.__name__, scope)
        except Exception as e:
            raise SchemaBuildException(
                path, "Failed to create object type: {}".format(e.__str__())
            ) from e

    @classmethod
    def _resolve_bool_type(
        cls,
        t,
        type_hints: type,
        path: typing.Tuple[str],
        scope: ScopeType,
    ) -> BoolType:
        try:
            return BoolType()
        except Exception as e:
            raise SchemaBuildException(
                path, "Constraint exception while creating bool type"
            ) from e

    @classmethod
    def _resolve_bool(
        cls,
        t,
        type_hints: type,
        path: typing.Tuple[str],
        scope: ScopeType,
    ) -> BoolType:
        try:
            return BoolType()
        except Exception as e:
            raise SchemaBuildException(
                path, "Constraint exception while creating bool type"
            ) from e

    @classmethod
    def _resolve_string_type(
        cls,
        t,
        type_hints: type,
        path: typing.Tuple[str],
        scope: ScopeType,
    ) -> StringType:
        try:
            return StringType()
        except Exception as e:
            raise SchemaBuildException(
                path, "Constraint exception while creating string type"
            ) from e

    @classmethod
    def _resolve_string(
        cls,
        t,
        type_hints: type,
        path: typing.Tuple[str],
        scope: ScopeType,
    ) -> StringType:
        try:
            return StringType()
        except Exception as e:
            raise SchemaBuildException(
                path, "Constraint exception while creating string type"
            ) from e

    @classmethod
    def _resolve_int(
        cls,
        t,
        type_hints: type,
        path: typing.Tuple[str],
        scope: ScopeType,
    ) -> IntType:
        try:
            return IntType()
        except Exception as e:
            raise SchemaBuildException(
                path, "Constraint exception while creating int type"
            ) from e

    @classmethod
    def _resolve_int_type(
        cls,
        t,
        type_hints: type,
        path: typing.Tuple[str],
        scope: ScopeType,
    ) -> IntType:
        try:
            return IntType()
        except Exception as e:
            raise SchemaBuildException(
                path, "Constraint exception while creating int type"
            ) from e

    @classmethod
    def _resolve_float(
        cls,
        t,
        type_hints: type,
        path: typing.Tuple[str],
        scope: ScopeType,
    ) -> FloatType:
        try:
            return FloatType()
        except Exception as e:
            raise SchemaBuildException(
                path, "Constraint exception while creating float type"
            ) from e

    @classmethod
    def _resolve_float_type(
        cls,
        t,
        type_hints: type,
        path: typing.Tuple[str],
        scope: ScopeType,
    ) -> FloatType:
        try:
            return FloatType()
        except Exception as e:
            raise SchemaBuildException(
                path, "Constraint exception while creating float type"
            ) from e

    @classmethod
    def _resolve_annotated(
        cls,
        t,
        type_hints: type,
        path: typing.Tuple[str],
        scope: ScopeType,
    ):
        args = typing.get_args(t)
        if typing.get_origin(type_hints) == typing.Annotated:
            args_hints = typing.get_args(type_hints)
        else:
            args_hints = (type_hints,)
        if len(args) < 2:
            raise SchemaBuildException(
                path, "At least one validation parameter required for typing.Annotated"
            )
        new_path = list(path)
        new_path.append("typing.Annotated")
        path = tuple(new_path)
        underlying_t = cls._resolve(args[0], args_hints[0], path, scope)
        for i in range(1, len(args)):
            new_path = list(path)
            new_path.append(str(i))
            if not isinstance(args[i], typing.Callable):
                raise SchemaBuildException(
                    tuple(new_path), "Annotation is not callable"
                )
            try:
                underlying_t = args[i](underlying_t)
            except Exception as e:
                raise SchemaBuildException(
                    tuple(new_path),
                    "Failed to execute Annotated argument: {}".format(e.__str__()),
                ) from e
        return underlying_t

    @classmethod
    def _resolve_list(
        cls,
        t,
        type_hints: type,
        path: typing.Tuple[str],
        scope: ScopeType,
    ) -> AbstractType:
        raise SchemaBuildException(
            path,
            "List type without item type definition encountered, please declare your lists like this: "
            "typing.List[str]",
        )

    @classmethod
    def _resolve_list_type(
        cls,
        t,
        type_hints: type,
        path: typing.Tuple[str],
        scope: ScopeType,
    ) -> AbstractType:
        raise SchemaBuildException(
            path,
            "List type without item type definition encountered, please declare your lists like this: "
            "typing.List[str]",
        )

    @classmethod
    def _resolve_list_annotation(
        cls,
        t,
        type_hints: type,
        path: typing.Tuple[str],
        scope: ScopeType,
    ):
        args = typing.get_args(t)
        if len(args) != 1:
            raise SchemaBuildException(
                path,
                "List type without item type definition encountered, please declare your lists like this: "
                "typing.List[str]",
            )
        new_path = list(path)
        new_path.append("items")
        try:
            return ListType(
                cls._resolve_abstract_type(args[0], type_hints, tuple(new_path), scope)
            )
        except Exception as e:
            raise SchemaBuildException(path, "Failed to create list type") from e

    @classmethod
    def _resolve_dict(
        cls,
        t,
        type_hints: type,
        path: typing.Tuple[str],
        scope: ScopeType,
    ) -> AbstractType:
        raise SchemaBuildException(
            path,
            "Dict type without item type definition encountered, please declare your dicts like this: "
            "typing.Dict[str, int]",
        )

    @classmethod
    def _resolve_forward(
        cls,
        t,
        type_hints: type,
        path: typing.Tuple[str],
        scope: ScopeType,
    ) -> AbstractType:
        t: typing.ForwardRef
        # TODO is there a better way to directly evaluate a forward ref?
        # noinspection PyArgumentList,PyProtectedMember
        resolved = t._evaluate(None, None, frozenset())
        return cls._resolve(resolved, resolved, path, scope)

    @classmethod
    def _resolve_dict_type(
        cls,
        t,
        type_hints: type,
        path: typing.Tuple[str],
        scope: ScopeType,
    ) -> AbstractType:
        raise SchemaBuildException(
            path,
            "Dict type without item type definition encountered, please declare your dicts like this: "
            "typing.Dict[str, int]",
        )

    @classmethod
    def _resolve_dict_annotation(
        cls,
        t,
        type_hints: type,
        path: typing.Tuple[str],
        scope: ScopeType,
    ):
        args = typing.get_args(t)
        arg_hints = typing.get_args(type_hints)
        if len(arg_hints) == 0:
            arg_hints = (type_hints,)
        if len(args) != 2:
            raise SchemaBuildException(
                path,
                "Dict type without item type definition encountered, please declare your dicts like this: "
                "typing.Dict[str, int]",
            )
        keys_path = list(path)
        keys_path.append("keys")
        key_schema: AbstractType = cls._resolve_abstract_type(
            args[0], arg_hints[0], tuple(keys_path), scope
        )

        values_path = list(path)
        values_path.append("values")
        value_schema = cls._resolve_abstract_type(
            args[1], arg_hints[1], tuple(values_path), scope
        )

        try:
            return MapType(
                key_schema,
                value_schema,
            )
        except Exception as e:
            raise SchemaBuildException(path, "Failed to create map type") from e

    @classmethod
    def _resolve_union(
        cls,
        t,
        type_hints: type,
        path: typing.Tuple[str],
        scope: ScopeType,
    ) -> typing.Union[AbstractType, PropertyType]:
        args = typing.get_args(t)
        arg_hints = typing.get_args(t)
        try:
            # noinspection PyTypeHints
            if isinstance(None, args[0]):
                raise SchemaBuildException(path, "None types are not supported.")
        except TypeError:
            pass
        try:
            # noinspection PyTypeHints
            if isinstance(None, args[1]):
                new_path = list(path)
                new_path.append("typing.Optional")
                result = cls._resolve_field(args[0], arg_hints[0], tuple(path), scope)
                result.required = False
                return result
        except TypeError:
            pass
        types = {}
        discriminator_type = None
        for i in range(len(args)):
            new_path = list(path)
            new_path.append("typing.Union")
            new_path.append(str(i))
            f = cls._resolve_field(args[i], arg_hints[i], tuple(new_path), scope)
            if not f.required:
                raise SchemaBuildException(
                    tuple(new_path), "Union types cannot contain optional values."
                )
            if f.required_if is not None and len(f.required_if) != 0:
                raise SchemaBuildException(
                    tuple(new_path),
                    "Union types cannot simultaneously contain require_if fields",
                )
            if f.required_if_not is not None and len(f.required_if_not) != 0:
                raise SchemaBuildException(
                    tuple(new_path),
                    "Union types cannot simultaneously contain require_if_not fields",
                )
            if f.conflicts is not None and len(f.conflicts) != 0:
                raise SchemaBuildException(
                    tuple(new_path),
                    "Union types cannot simultaneously contain conflicts fields",
                )
            if not isinstance(f.type, RefType):
                raise SchemaBuildException(
                    tuple(new_path),
                    "Union types can only contain objects, {} found".format(
                        type(f.type).__name__
                    ),
                )
            discriminator_value = f.type.id
            if hasattr(f.type, "__discriminator_value"):
                discriminator_value = getattr(f.type, "__discriminator_value")
            if (
                discriminator_type is not None
                and type(discriminator_value) != discriminator_type
            ):
                raise BadArgumentException(
                    "Invalid discriminator value type: {}, the value type has been previously set to {}. Please make "
                    "sure all your discriminator values have the same type.".format(
                        type(discriminator_value), discriminator_type
                    )
                )
            discriminator_type = type(discriminator_value)
            if hasattr(f.type, "__name"):
                if f.type.display is None:
                    f.type.display = DisplayValue()
                f.type.display.name = getattr(f.type, "__name")
            if hasattr(f.type, "__description"):
                if f.type.display is None:
                    f.type.display = DisplayValue()
                f.type.display.description = getattr(f.type, "__description")
            types[discriminator_value] = f.type
        if discriminator_type == str:
            return OneOfStringType(
                types,
                scope,
            )
        else:
            return OneOfIntType(types, scope)

    @classmethod
    def _resolve_pattern(cls, t, type_hints: type, path, scope: ScopeType):
        try:
            return PatternType()
        except Exception as e:
            raise SchemaBuildException(path, "Failed to create pattern type") from e


def build_object_schema(t, _skip_validation: bool = False) -> ScopeType:
    """
    This function builds a schema for a single object. This is useful when serializing input parameters into a file
    for underlying tools to use, or unserializing responses from underlying tools into output data types.

    :param t: the type to build a schema for.
    :param _skip_validation: Skip schema validation. For internal use only when constructing ``SCOPE_SCHEMA``.
    :return: the built object schema
    """
    scope = ScopeType({}, t.__name__)

    r = _SchemaBuilder.resolve(t, scope)
    if not isinstance(r, RefType):
        raise SchemaBuildException(tuple({}), "Response type is not an object.")

    if not _skip_validation:
        SCOPE_SCHEMA.validate(scope)

    return scope


# endregion

# region Schema schemas

SCOPE_SCHEMA = build_object_schema(ScopeSchema, True)
"""
This variable holds a constructed, serializable/unserializable schema for a scope. You can use it to send schemas to
the engine, or to build an engine replacement. (This is normally handled by the plugin module.)
"""

SCHEMA_SCHEMA = build_object_schema(Schema)
"""
This variable holds a constructed, serializable/unserializable schema for an entire schema. You can use it to send
schemas to the engine, or to build an engine replacement. (This is normally handled by the plugin module.)
"""


def test_object_serialization(
    dc,
    fail: typing.Optional[Callable[[str], None]] = None,
    t: typing.Optional[ObjectType] = None,
):
    """
    This function aids serialization by first serializing, then unserializing the passed parameter according to the
    passed schema. It then compares that the two objects are equal.

    :param dc: the dataclass to use for tests.
    :param t: the schema for the dataclass. If none is passed, the schema is built automatically using
              ``schema.build_object_schema()``
    """
    try:
        if t is None:
            t = build_object_schema(dc.__class__)
        path: typing.Tuple[str] = tuple(dc.__class__.__name__)
        t.validate(dc, path)
        serialized_data = t.serialize(dc, path)
        unserialized_data = t.unserialize(serialized_data, path)
        if unserialized_data != dc:
            raise Exception(
                "After serializing and unserializing {}, the data mismatched. Serialized data was: {}".format(
                    dc.__name__, serialized_data
                )
            )
    except Exception as e:
        result = (
            "Your object serialization test for {} failed.\n\n"
            "This means that your object cannot be properly serialized by the SDK. There are three possible "
            "reasons for this:\n\n"
            "1. Your data class has a field type in it that the SDK doesn't support\n"
            "2. Your sample data is invalid according to your own rules\n"
            "3. There is a bug in the SDK (please report it)\n\n"
            "Check the error message below for details.\n\n"
            "---\n\n{}".format(type(dc).__name__, traceback.extract_stack())
        )
        result += "Error message:\n" + e.__str__() + "\n\n"
        # noinspection PyDataclass
        result += "Input:\n" + pprint.pformat(dataclasses.asdict(dc)) + "\n\n"
        result += "---\n\n"
        result += "Your object serialization test for {} failed. Please scroll up for details.\n\n".format(
            type(dc).__name__
        )
        if fail is None:
            print(result)
            sys.exit(1)
        fail(result)


# endregion


if __name__ == "__main__":
    import doctest

    doctest.testmod()
