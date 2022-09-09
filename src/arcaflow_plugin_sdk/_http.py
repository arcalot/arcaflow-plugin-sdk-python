import json

from twisted.web import server, resource
from twisted.internet import reactor, endpoints
from twisted.web.resource import Resource

from arcaflow_plugin_sdk import schema
from arcaflow_plugin_sdk.schema import InvalidInputException, InvalidOutputException

_INDEX_HTML = """
<!DOCTYPE html>
<html>
  <head>
    <title>{}</title>
    <meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link href="https://fonts.googleapis.com/css?family=Montserrat:300,400,700|Roboto:300,400,700" rel="stylesheet">

    <style>
      body {{
        margin: 0;
        padding: 0;
      }}
    </style>
  </head>
  <body>
    <redoc spec-url='{}'></redoc>
    <script src="https://cdn.redoc.ly/redoc/latest/bundles/redoc.standalone.js"> </script>
  </body>
</html>
"""


class _StepHandler(resource.Resource):
    isLeaf = True

    def __init__(self, s: schema.SchemaType, step_id: str):
        super().__init__()
        self.schema = s
        self.step_id = step_id

    def render(self, request):
        input = request.content.read()
        try:
            input_data = json.loads(input)
        except BaseException as e:
            request.setResponseCode(400)
            return "Failed to load JSON from request: {}".format(e.__str__()).encode("utf-8")

        try:
            output_id, output_data = self.schema(self.step_id, input_data)
        except InvalidInputException as e:
            request.setResponseCode(400)
            return "Invalid input: {}".format(e.__str__()).encode("utf-8")
        except InvalidOutputException as e:
            request.setResponseCode(500)
            return "Invalid output: {}".format(e.__str__()).encode("utf-8")

        try:
            request.setHeader("Content-Type", "application/json")
            output_data["_output_id"] = output_id
            return json.dumps(output_data).encode("utf-8")
        except BaseException as e:
            request.setResponseCode(500)
            return "Failed to encode response JSON: {}".format(e.__str__()).encode("utf-8")


class _SchemaHandler(resource.Resource):
    isLeaf = True

    def __init__(self, s: schema.Schema, listen: str):
        super().__init__()
        self.schema = s
        self.listen = listen

    def render(self, request):
        request.setHeader("Content-Type", "application/json")

        paths = {}
        defs = {}
        for step_id, step in self.schema.steps.items():
            step_input_schema = step.input.to_openapi()
            defs = {**defs, **step_input_schema["components"]["schemas"]}
            del step_input_schema["components"]

            one_of = []
            discriminator_mapping = {}
            for output_id, output_schema in step.outputs.items():
                step_output_schema = output_schema.to_openapi()
                defs = {**defs, **step_output_schema["components"]["schemas"]}
                del step_output_schema["components"]

                ref_id = step_output_schema["$ref"].replace("#/components/schemas/", "")

                defs[ref_id + "_output_" + output_id] = defs[ref_id]
                defs[ref_id + "_output_" + output_id]["properties"]["_output_id"] = {
                    "type": "string",
                }
                defs[ref_id + "_output_" + output_id]["required"].append("_output_id")
                one_of.append({"$ref": "#/components/schemas/" + ref_id + "_output_" + output_id})

                discriminator_mapping[output_id] = "#/components/schemas/" + ref_id + "_output_" + output_id
            output_openapi = {
                "oneOf": one_of,
                "discriminator": {
                    "propertyName": "_output_id",
                    "mapping": discriminator_mapping,
                }
            }

            paths["/{}".format(step_id)] = {
                "post": {
                    "summary": step.display.name,
                    "description": step.display.description,
                    "operationId": step.id,
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": step_input_schema,
                            }
                        }
                    },
                    "responses": {
                        "default": {
                            "description": "Execution complete",
                            "content": {
                                "application/json": {
                                    "schema": output_openapi
                                }
                            }
                        }
                    }
                }
            }
        data = {
            "openapi": "3.0.3",
            "info": {
                "title": "HTTP API",
                "description": "This OpenAPI document describes there microservices exposed by this plugin.",
                "version": "0.0.0",
            },
            "paths": paths,
            "components": {
                "schemas": defs
            }
        }

        return json.dumps(data).encode("utf-8")


class _Handler(resource.Resource):
    isLeaf = True

    def __init__(self, s: schema.Schema, listen: str):
        super().__init__()
        self.schema = s
        self.listen = listen

    def render(self, request):
        request.setHeader("Content-Type", "text/html;charset=utf-8")
        return _INDEX_HTML.format(
            "Plugin API",
            "http://{}/api.json".format(request.getHeader("Host"))
        ).encode("utf-8")


def run(listen: str, s: schema.Schema):
    parts = listen.split(":", 1)

    handler = Resource()
    handler.putChild(b"api.json", _SchemaHandler(s, listen))
    for step_id, step in s.steps.items():
        handler.putChild("{}".format(step_id).encode("utf-8"), _StepHandler(s, step_id))
    handler.putChild(b"", _Handler(s, listen))

    site = server.Site(handler)
    endpoint = endpoints.TCP4ServerEndpoint(reactor, int(parts[1]), interface=parts[0])
    endpoint.listen(site)
    reactor.run()
