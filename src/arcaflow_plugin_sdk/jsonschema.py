from arcaflow_plugin_sdk import schema


def step_input(t: schema.StepSchema) -> dict:
    """
    This function takes a schema step and creates a JSON schema object from the input parameter.
    :return: the JSON schema represented as a dict.
    """
    result = t.input.to_jsonschema()
    result["title"] = t.display.name + " input"
    result["description"] = t.display.description
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
        "title": t.display.name + " outputs",
        "description": t.display.description,
        "oneof": [],
        "$defs": {},
    }

    for output_id in list(t.outputs.keys()):
        output_data = t.outputs[output_id].to_jsonschema()
        for k, v in output_data["$defs"].items():
            result["$defs"][k] = v
        del output_data["$defs"]
        result["oneof"].append(
            {
                "output_id": {"type": "string", "const": output_id},
                "output_data": output_data,
            }
        )
    return result
