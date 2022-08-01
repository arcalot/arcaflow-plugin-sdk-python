from arcaflow_plugin_sdk import schema
from arcaflow_plugin_sdk.schema import TypeID


class _JSONSchema:
    @classmethod
    def from_schema(cls, t: schema.AbstractType) -> dict:
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
    def from_int(cls, t: schema.IntType) -> dict:
        result = {
            "type": "integer"
        }
        if t.min is not None:
            result["minimum"] = t.min
        if t.max is not None:
            result["maximum"] = t.max
        return result

    @classmethod
    def from_float(cls, t: schema.FloatType) -> dict:
        result = {
            "type": "number"
        }
        if t.min is not None:
            result["minimum"] = t.min
        if t.max is not None:
            result["maximum"] = t.max
        return result

    @classmethod
    def from_string(cls, t: schema.StringType) -> dict:
        result = {
            "type": "string"
        }
        if t.min_length is not None:
            result["minLength"] = t.min_length
        if t.max_length is not None:
            result["maxLength"] = t.max_length
        if t.pattern is not None:
            result["pattern"] = t.pattern.pattern
        return result

    @classmethod
    def from_bool(cls, t: schema.BoolType) -> dict:
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
        return result

    @classmethod
    def from_oneof(cls, t: schema.OneOfType) -> dict:
        result = {
            "oneOf": [
            ]
        }
        for key, value in t.one_of.items():
            s = cls.from_object(value)
            if "title" not in s:
                s["title"] = key
            if t.discriminator_field_name not in value.properties:
                s["properties"][t.discriminator_field_name] = {"type": "string", "const": key}
                s["required"].append(t.discriminator_field_name)
            else:
                s["properties"][t.discriminator_field_name]["const"] = key
            result["oneOf"].append(s)
        return result

    @classmethod
    def from_object(cls, t: schema.ObjectType) -> dict:
        result = {
            "type": "object",
            "properties": {},
            "additionalProperties": False,
            "required": []
        }
        for property_id in list(t.properties.keys()):
            property = t.properties[property_id]
            result["properties"][property_id] = cls.from_schema(property.type)
            if property.name != "":
                result["properties"][property_id]["title"] = property.name
            if property.description != "":
                result["properties"][property_id]["description"] = property.description
            if property.required:
                result["required"].append(property_id)
        return result

    @classmethod
    def from_enum(cls, t: schema.EnumType) -> dict:
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
        return result

    @classmethod
    def from_map(cls, t: schema.MapType):
        result = {
            "type": "object",
            "propertyNames": cls.from_schema(t.key_type),
            "additionalProperties": cls.from_schema(t.value_type)
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
        return result

    @classmethod
    def from_list(cls, t: schema.ListType):
        result = {
            "type": "array",
            "items": cls.from_schema(t.type)
        }
        if t.min is not None:
            result["minItems"] = t.min
        if t.max is not None:
            result["maxItems"] = t.max
        return result

    @classmethod
    def from_pattern(cls, t: schema.Pattern):
        result = {
            "type": "string",
            "format": "regex"
        }
        return result


def step_input(t: schema.StepSchema) -> dict:
    """
    This function takes a schema step and creates a JSON schema object from the input parameter.
    :return: the JSON schema represented as a dict.
    """
    result = _JSONSchema.from_object(t.input)
    result["title"] = t.name + " input"
    result["description"] = t.description
    result["$id"] = t.id
    result["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    return result


def step_outputs(t: schema.StepSchema):
    """
    This function takes a schema step and creates a JSON schema object from the output parameters. 
    :return: the JSON schema represented as a dict.
    """
    result = {
        "$id": t.id,
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": t.name + " outputs",
        "description": t.description,
        "oneof": [],
    }

    for output_id in list(t.outputs.keys()):
        result["oneof"].append({
            "output_id": {
                "const": output_id
            },
            "output_data": _JSONSchema.from_object(t.outputs[output_id])
        })
    return result
