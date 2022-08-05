import dataclasses
import enum
import pprint
import re
import typing
import unittest
from typing import Dict, Tuple

from arcaflow_plugin_sdk import schema, jsonschema


class JSONSchemaTest(unittest.TestCase):
    def test_bool(self):
        s = jsonschema._JSONSchema.from_bool(schema.BoolType())
        self.assertEqual(
            s["anyOf"][0]["type"],
            "boolean"
        )
        self.assertEqual(
            s["anyOf"][1]["type"],
            "string"
        )
        self.assertEqual(
            s["anyOf"][2]["type"],
            "integer"
        )

    def test_string(self):
        test_cases: Dict[str, Tuple[schema.StringType, Dict]] = {
            "base": (schema.StringType(), {"type": "string"}),
            "min": (schema.StringType(min_length=5), {"type": "string", "minLength": 5}),
            "max": (schema.StringType(max_length=5), {"type": "string", "maxLength": 5}),
            "pattern": (schema.StringType(pattern=re.compile("^[a-z]+$")), {"type": "string", "pattern": "^[a-z]+$"})
        }

        for name in test_cases.keys():
            with self.subTest(name=name):
                input = test_cases[name][0]
                expected = test_cases[name][1]

                self.assertEqual(expected, jsonschema._JSONSchema.from_string(input))

    def test_int(self):
        test_cases: Dict[str, Tuple[schema.IntType, Dict]] = {
            "base": (schema.IntType(), {"type": "integer"}),
            "min": (schema.IntType(min=5), {"type": "integer", "minimum": 5}),
            "max": (schema.IntType(max=5), {"type": "integer", "maximum": 5}),
        }

        for name in test_cases.keys():
            with self.subTest(name=name):
                input = test_cases[name][0]
                expected = test_cases[name][1]

                self.assertEqual(expected, jsonschema._JSONSchema.from_int(input))

    def test_float(self):
        test_cases: Dict[str, Tuple[schema.FloatType, Dict]] = {
            "base": (schema.FloatType(), {"type": "number"}),
            "min": (schema.FloatType(min=5.0), {"type": "number", "minimum": 5.0}),
            "max": (schema.FloatType(max=5.0), {"type": "number", "maximum": 5.0}),
        }

        for name in test_cases.keys():
            with self.subTest(name=name):
                input = test_cases[name][0]
                expected = test_cases[name][1]

                self.assertEqual(expected, jsonschema._JSONSchema.from_float(input))

    def test_enum(self):
        class Color(enum.Enum):
            RED = "red"

        class Fibonacci(enum.Enum):
            FIRST = 1
            SECOND = 2

        test_cases: Dict[str, Tuple[schema.EnumType, Dict]] = {
            "string": (schema.EnumType(Color), {"type": "string", "enum": ["red"]}),
            "int": (schema.EnumType(Fibonacci), {"type": "integer", "enum": [1, 2]}),
        }

        for name in test_cases.keys():
            with self.subTest(name=name):
                input = test_cases[name][0]
                expected = test_cases[name][1]

                self.assertEqual(expected, jsonschema._JSONSchema.from_enum(input))

    def test_list(self):
        test_cases: Dict[str, Tuple[schema.ListType, Dict]] = {
            "base": (schema.ListType(schema.IntType()), {"type": "array", "items": {"type": "integer"}}),
            "min": (
                schema.ListType(schema.IntType(), min=3),
                {"type": "array", "items": {"type": "integer"}, "minItems": 3}
            ),
            "max": (
                schema.ListType(schema.IntType(), max=3),
                {"type": "array", "items": {"type": "integer"}, "maxItems": 3}
            ),
        }

        for name in test_cases.keys():
            with self.subTest(name=name):
                input = test_cases[name][0]
                expected = test_cases[name][1]

                self.assertEqual(expected, jsonschema._JSONSchema.from_list(input))

    def test_map(self):
        test_cases: Dict[str, Tuple[schema.MapType, Dict]] = {
            "base": (
                schema.MapType(schema.IntType(), schema.StringType()),
                {
                    "type": "object",
                    "propertyNames": {
                        "pattern": "^[0-9]+$",
                    },
                    "additionalProperties": {
                        "type": "string",
                    },
                }
            ),
            "min": (
                schema.MapType(schema.StringType(), schema.IntType(), min=3),
                {
                    "type": "object",
                    "propertyNames": {},
                    "additionalProperties": {
                        "type": "integer",
                    },
                    "minProperties": 3,
                }
            ),
            "max": (
                schema.MapType(schema.StringType(), schema.IntType(), max=3),
                {
                    "type": "object",
                    "propertyNames": {},
                    "additionalProperties": {
                        "type": "integer",
                    },
                    "maxProperties": 3,
                }
            ),
        }

        for name in test_cases.keys():
            with self.subTest(name=name):
                input = test_cases[name][0]
                expected = test_cases[name][1]

                self.assertEqual(expected, jsonschema._JSONSchema.from_map(input))

    def test_object(self):
        @dataclasses.dataclass
        class TestData:
            a: str

        test_cases: Dict[str, Tuple[schema.ObjectType, Dict]] = {
            "base": (
                schema.ObjectType(TestData, {
                    "a": schema.Field(
                        schema.StringType(),
                        "A",
                        "A string"
                    )
                }),
                {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "a": {
                            "title": "A",
                            "description": "A string",
                            "type": "string",
                        }
                    },
                    "required": ["a"]
                }
            ),
        }

        for name in test_cases.keys():
            with self.subTest(name=name):
                input = test_cases[name][0]
                expected = test_cases[name][1]

                self.assertEqual(expected, jsonschema._JSONSchema.from_object(input))

    def test_one_of(self):
        @dataclasses.dataclass
        class A:
            a: str

        @dataclasses.dataclass
        class B:
            b: str

        @dataclasses.dataclass
        class TestData:
            a: typing.Union[A, B]

        s = schema.ObjectType(
            TestData,
            {
                "a": schema.Field(
                    schema.OneOfType(
                        "_type",
                        schema.StringType(),
                        {
                            "a": schema.ObjectType(
                                A,
                                {
                                    "a": schema.Field(schema.StringType())
                                }
                            ),
                            "b": schema.ObjectType(
                                B,
                                {
                                    "b": schema.Field(schema.StringType())
                                }
                            )
                        }
                    )
                )
            }
        )

        json_schema = jsonschema._JSONSchema.from_object(s)
        self.assertEqual({
            'additionalProperties': False,
            'properties': {
                'a': {
                    'oneOf': [
                        {
                            'title': 'a',
                            'additionalProperties': False,
                            'properties': {
                                '_type': {'const': 'a', 'type': 'string'},
                                'a': {'type': 'string'},
                            },
                            'required': ['a', '_type'],
                            'type': 'object'
                        },
                        {
                            'title': 'b',
                            'additionalProperties': False,
                            'properties': {
                                'b': {'type': 'string'},
                                '_type': {'const': 'b', 'type': 'string'}
                            },
                            'required': ['b', '_type'],
                            'type': 'object'
                        }
                    ]
                }
            },
            'required': ['a'],
            'type': 'object'
        }, json_schema)

    def test_step_input(self):
        @dataclasses.dataclass
        class Request:
            a: str
            b: str

        @dataclasses.dataclass
        class Response:
            b: str

        def noop_handler(input: Request) -> Tuple[str, typing.Union[Response]]:
            pass

        step = schema.StepSchema(
            id="test",
            name="Test step",
            description="This is just a test",
            input=schema.ObjectType(
                Request,
                {
                    "a": schema.Field(
                        schema.StringType(),
                        "a",
                    ),
                    "field-b": schema.Field(
                        schema.IntType(),
                        "b",
                        field_override="b"
                    )
                }
            ),
            outputs={},
            handler=noop_handler,
        )

        s = jsonschema.step_input(step)
        self.assertEqual({
            '$id': 'test',
            '$schema': 'https://json-schema.org/draft/2020-12/schema',
            'type': 'object',
            'title': 'Test step input',
            'description': 'This is just a test',
            'properties': {'a': {'title': 'a', 'type': 'string'}, 'field-b': {'title': 'b', 'type': 'integer'}},
            'required': ['a', 'field-b'],
            'additionalProperties': False,
        }, s)

    def test_step_outputs(self):
        @dataclasses.dataclass
        class Request:
            a: str

        @dataclasses.dataclass
        class Response1:
            b: str

        @dataclasses.dataclass
        class Response2:
            c: str

        def noop_handler(input: Request) -> Tuple[str, typing.Union[Response1, Response2]]:
            pass

        step = schema.StepSchema(
            id="test",
            name="Test step",
            description="This is just a test",
            input=schema.ObjectType(
                Request,
                {
                    "a": schema.Field(
                        schema.StringType(),
                        "a",
                    )
                }
            ),
            outputs={
                "success": schema.ObjectType(
                    Response1,
                    {
                        "b": schema.Field(
                            schema.StringType(),
                            "b"
                        )
                    }
                ),
                "error": schema.ObjectType(
                    Response2,
                    {
                        "c": schema.Field(
                            schema.StringType(),
                            "c"
                        )
                    }
                )
            },
            handler=noop_handler,
        )

        s = jsonschema.step_outputs(step)
        self.assertEqual({
            '$id': 'test',
            '$schema': 'https://json-schema.org/draft/2020-12/schema',
            'title': 'Test step outputs',
            'description': 'This is just a test',
            'oneof': [
                {
                    'output_id': {'const': 'success'},
                    'output_data': {
                        'type': 'object',
                        'properties': {
                            'b': {
                                'type': 'string',
                                'title': 'b',
                            }
                        },
                        'required': ['b'],
                        'additionalProperties': False,
                    },
                },
                {
                    'output_id': {'const': 'error'},
                    'output_data': {
                        'type': 'object',
                        'properties': {
                            'c': {
                                'title': 'c',
                                'type': 'string',
                            }
                        },
                        'required': ['c'],
                        'additionalProperties': False,
                    },
                }
            ],
        }, s)


if __name__ == '__main__':
    unittest.main()
