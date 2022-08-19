import dataclasses
import typing
from typing import Any, Dict

from arcaflow_plugin_sdk import schema
from arcaflow_plugin_sdk.schema import TypeID


class OpenAPI:
    @classmethod
    def from_schema(cls, t: schema.AbstractType) -> typing.Tuple[dict, dict]:
        if t.type_id() == TypeID.INT:
            return cls.from_int(t)
        elif t.type_id() == TypeID.FLOAT:
            return cls.from_float(t)
        elif t.type_id() == TypeID.STRING:
            return cls.from_string(t)
        elif t.type_id() == TypeID.BOOL:
            return cls.from_bool(t)
        elif t.type_id() == TypeID.ENUM:
            return cls.from_enum(t)
        elif t.type_id() == TypeID.MAP:
            return cls.from_map(t)
        elif t.type_id() == TypeID.ONEOF:
            return cls.from_oneof(t)
        elif t.type_id() == TypeID.LIST:
            return cls.from_list(t)
        elif t.type_id() == TypeID.OBJECT:
            return cls.from_object(t)
        elif t.type_id() == TypeID.PATTERN:
            return cls.from_pattern(t)

    @classmethod
    def from_int(cls, t: schema.IntType) -> typing.Tuple[dict, dict]:
        result = {
            "type": "integer"
        }
        if t.min is not None:
            result["minimum"] = t.min
        if t.max is not None:
            result["maximum"] = t.max
        return result, {}

    @classmethod
    def from_float(cls, t: schema.FloatType) -> typing.Tuple[dict, dict]:
        result = {
            "type": "number"
        }
        if t.min is not None:
            result["minimum"] = t.min
        if t.max is not None:
            result["maximum"] = t.max
        return result, {}

    @classmethod
    def from_string(cls, t: schema.StringType) -> typing.Tuple[dict, dict]:
        result = {
            "type": "string"
        }
        if t.min_length is not None:
            result["minLength"] = t.min_length
        if t.max_length is not None:
            result["maxLength"] = t.max_length
        if t.pattern is not None:
            result["pattern"] = t.pattern.pattern
        return result, {}

    @classmethod
    def from_bool(cls, t: schema.BoolType) -> typing.Tuple[dict, dict]:
        result = {
            "anyOf": [
                {
                    "type": "boolean"
                },
                {
                    "type": "string",
                    "enum": [
                        "yes", "true", "on", "enable", "enabled", "1",
                        "no", "false", "off", "disable", "disabled", "0",
                    ]
                },
                {
                    "type": "integer",
                    "maximum": 1,
                    "minumum": 0,
                }
            ]
        }
        return result, {}

    @classmethod
    def from_oneof(cls, t: schema.OneOfType) -> typing.Tuple[dict, dict]:
        result = {
            "oneOf": [
            ],
            "discriminator": {
                "propertyName": t.discriminator_field_name,
                "mapping": {}
            }
        }
        objects = {}
        for key, value in t.one_of.items():
            s, o = cls.from_object(value)

            x = o[value.type_class().__name__]
            if "title" not in x:
                x["title"] = key
            if t.discriminator_field_name not in value.properties:
                x["properties"][t.discriminator_field_name] = {"type": "string", "const": key}
                x["required"].append(t.discriminator_field_name)
            else:
                x["properties"][t.discriminator_field_name]["const"] = key
            o[value.type_class().__name__] = x

            result["oneOf"].append(s)
            result["discriminator"]["mapping"][key] = "#/components/schemas/{}".format(value.type_class().__name__)
            objects = {**objects, **o}
        return result, objects

    @classmethod
    def from_object(cls, t: schema.ObjectType) -> typing.Tuple[dict, dict]:
        result = {
            "type": "object",
            "properties": {},
            "additionalProperties": False,
            "required": []
        }
        objects = {}
        for property_id in list(t.properties.keys()):
            property = t.properties[property_id]
            result["properties"][property_id], o = cls.from_schema(property.type)
            objects = {**objects, **o}
            if property.name != "":
                result["properties"][property_id]["title"] = property.name
            if property.description != "":
                result["properties"][property_id]["description"] = property.description
            if property.required:
                result["required"].append(property_id)
        objects[t.type_class().__name__] = result

        return {
            "$ref": "#/components/schemas/{}".format(t.type_class().__name__)
        }, objects

    @classmethod
    def from_enum(cls, t: schema.EnumType) -> typing.Tuple[dict, dict]:
        result = {
        }
        if t.value_type == str:
            result["type"] = "string"
        else:
            result["type"] = "integer"
        values = []
        for value in t.type:
            values.append(value.value)
        result["enum"] = values
        return result, {}

    @classmethod
    def from_map(cls, t: schema.MapType) -> typing.Tuple[dict, dict]:
        objects = {}
        s, o = cls.from_schema(t.key_type)
        objects = {**objects, **o}
        a, o = cls.from_schema(t.value_type)
        objects = {**objects, **o}
        result = {
            "type": "object",
            "propertyNames": s,
            "additionalProperties": a,
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

        if t.min is not None:
            result["minProperties"] = t.min
        if t.max is not None:
            result["maxProperties"] = t.max
        return result, objects

    @classmethod
    def from_list(cls, t: schema.ListType) -> typing.Tuple[dict, dict]:
        s, o = cls.from_schema(t.type)
        result = {
            "type": "array",
            "items": s
        }
        if t.min is not None:
            result["minItems"] = t.min
        if t.max is not None:
            result["maxItems"] = t.max
        return result, o

    @classmethod
    def from_pattern(cls, t: schema.Pattern) -> typing.Tuple[dict, dict]:
        result = {
            "type": "string",
            "format": "regex"
        }
        return result, {}
