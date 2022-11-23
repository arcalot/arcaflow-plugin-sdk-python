import dataclasses
import typing
import unittest
from typing import Tuple

from arcaflow_plugin_sdk import jsonschema, schema


class JSONSchemaTest(unittest.TestCase):
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

        step = schema.StepType(
            id="test",
            display=schema.DisplayValue(
                name="Test step",
                description="This is just a test",
            ),
            input=schema.ScopeType(
                {
                    "Request": schema.ObjectType(
                        Request,
                        {
                            "a": schema.PropertyType(
                                schema.StringType(),
                            ),
                            "field-b": schema.PropertyType(
                                schema.IntType(), field_override="b"
                            ),
                        },
                    )
                },
                "Request",
            ),
            outputs={},
            handler=noop_handler,
        )

        s = jsonschema.step_input(step)
        self.maxDiff = None
        self.assertEqual(
            {
                "$defs": {
                    "Request": {
                        "additionalProperties": False,
                        "dependentRequired": {},
                        "properties": {
                            "a": {"type": "string"},
                            "field-b": {"type": "integer"},
                        },
                        "required": ["a", "field-b"],
                        "type": "object",
                    }
                },
                "$id": "test",
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "title": "Test step input",
                "description": "This is just a test",
                "properties": {"a": {"type": "string"}, "field-b": {"type": "integer"}},
                "required": ["a", "field-b"],
                "dependentRequired": {},
                "additionalProperties": False,
            },
            s,
        )

    def test_step_outputs(self):
        input_scope = schema.ScopeSchema(
            {
                "Request": schema.ObjectSchema(
                    id="Request",
                    properties={"a": schema.PropertySchema(schema.StringSchema())},
                )
            },
            "Request",
        )

        outputs = {
            "success": schema.StepOutputSchema(
                schema=schema.ScopeSchema(
                    {
                        "Response1": schema.ObjectSchema(
                            id="Response1",
                            properties={
                                "b": schema.PropertySchema(schema.StringSchema())
                            },
                        )
                    },
                    "Response1",
                )
            ),
            "error": schema.StepOutputSchema(
                schema=schema.ScopeSchema(
                    {
                        "Response1": schema.ObjectSchema(
                            id="Response1",
                            properties={
                                "c": schema.PropertySchema(schema.StringSchema())
                            },
                        )
                    },
                    "Response1",
                ),
                error=True,
            ),
        }

        step = schema.StepSchema(
            id="test",
            display=schema.DisplayValue(
                name="Test step",
                description="This is just a test",
            ),
            input=input_scope,
            outputs=outputs,
        )

        s = jsonschema.step_outputs(step)
        self.maxDiff = None
        self.assertEqual(
            {
                "$id": "test",
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "title": "Test step outputs",
                "description": "This is just a test",
                "oneof": [
                    {
                        "output_id": {"type": "string", "const": "success"},
                        "output_data": {
                            "type": "object",
                            "properties": {"b": {"type": "string"}},
                            "required": ["b"],
                            "additionalProperties": False,
                            "dependentRequired": {},
                        },
                    },
                    {
                        "output_id": {
                            "type": "string",
                            "const": "error",
                        },
                        "output_data": {
                            "type": "object",
                            "properties": {"c": {"type": "string"}},
                            "required": ["c"],
                            "additionalProperties": False,
                            "dependentRequired": {},
                        },
                    },
                ],
                "$defs": {
                    "Response1": {
                        "type": "object",
                        "properties": {"c": {"type": "string"}},
                        "required": ["c"],
                        "additionalProperties": False,
                        "dependentRequired": {},
                    }
                },
            },
            s,
        )


if __name__ == "__main__":
    unittest.main()
