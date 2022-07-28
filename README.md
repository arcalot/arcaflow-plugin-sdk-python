# Python SDK for the Arcaflow workflow engine (WIP)

## How this SDK works

In order to create an Arcaflow plugin, you must specify a **schema** for each step you want to support. This schema describes two things:

1. What your input parameters are and what their type is
2. What your output parameters are and what their type is

Note, that you can specify **several possible outputs**, depending on what the outcome of your plugin execution is. You should, however, never raise exceptions that bubble outside your plugin. If you do, your plugin will crash and Arcaflow will not be able to retrieve the result data, including the error, from it.

With the schema, the plugin can run in the following modes:

1. CLI mode, where a file with the data is loaded and the plugin is executed
2. GRPC mode (under development) where the plugin works in conjunction with the Arcaflow Engine to enable more complex workflows

---

## Requirements

In order to use this SDK you need at least Python 3.9.

---

## Run the example plugin

In order to run the [example plugin](example_plugin.py) run the following steps:

1. Checkout this repository
2. Create a `venv` in the current directory with `python3 -m venv $(pwd)/venv`
3. Activate the `venv` by running `source venv/bin/activate`
4. Run `pip install -r requirements.txt`
5. Run `./example_plugin.py -f example.yaml`

This should result in the following placeholder result being printed:

```yaml
output_id: error
output_data:
  error: Cannot kill pod nginx-.* in namespace default, function not implemented
```

---

## Generating a JSON schema file

Arcaflow plugins can generate their own JSON schema for both the input and the output schema. You can run the schema generation by calling:

```
./example_plugin.py --json-schema input
./example_plugin.py --json-schema output
```

If your plugin defines more than one step, you may need to pass the `--step` parameter.

**Note:** The Arcaflow schema system supports a few features that cannot be represented in JSON schema. The generated schema is for editor integration only.

---

## Setting up a new project

Before you start, make sure you have Python 3.9 installed. The easiest  way to figure out  which python command to use is to try the following:

```
python3.10 --version
python3.9 --version
python3 --version
python --version
```

Please use whichever works for the following commands. (Assuming it's at least Python 3.9.)

To start a new plugin project, create an empty folder and add a `requirements.txt` file with the following line:

```
arcaflow-plugin-sdk
```

Next, create a `venv` and activate it using the following commands:

```
python3.9 -m venv venv
source venv/bin/activate
```

Now install your dependencies:

```
pip install -r requirements.txt
```

Finally, copy the [example_plugin.py](example_plugin.py) and [test_example_plugin.py](test_example_plugin.py) files to your repository and start editing them.

Test if your schema generation works and save the schema into a file:

```
./example_plugin.py --json-schema input >example.schema.json
```

Copy over the [example config](example.yaml) to your repository and test if it runs:

```
./example_plugin.py -f example.yaml 
```

**Tip:** If you are using the YAML plugin for VSCode, you should automatically get code completion on the config file from the schema you generated  before. On other editors you may have to set up the schema for the config file manually.

---

## Creating a plugin

A plugin is nothing but a list of functions with type-annotated parameters and decorators. For example, let's create a function:

```python
def pod_scenario(input_parameter):
    # Do pod scenario magic here
```

However, this SDK uses [Python type hints](https://docs.python.org/3/library/typing.html) and [decorators](https://peps.python.org/pep-0318/) to automatically generate the schema required for Arcaflow. Alternatively, you can also [build a schema by hand](#building-a-schema-by-hand). The current section describes the automated way, the [section below](#building-a-schema-by-hand) describes the manual way.

### Input parameters

Your step function must take exactly one input parameter. This parameter must be a [dataclass](https://docs.python.org/3/library/dataclasses.html). For example:

```python
import dataclasses
import re

@dataclasses.dataclass
class PodScenarioParams:
    namespace_pattern: re.Pattern = re.compile(".*")
    pod_name_pattern: re.Pattern = re.compile(".*")
```

As you can see, our dataclass has two fields, each of which is a `re.Pattern`. This SDK automatically reads the types of the fields to construct the schema. See the [Types](#types) section below for supported type patterns.

### Output parameters

Now that you have your input parameter class, you must create one or more output classes in a similar fashion:

```python
import dataclasses
import typing

@dataclasses.dataclass
class Pod:
    namespace: str
    name: str

@dataclasses.dataclass
class PodScenarioResults:
    pods_killed: typing.List[Pod]
```

As you can see, your input may incorporate other classes, which themselves have to be dataclasses. Read on for [more information on types](#types).

### Creating a step function

Now that we have both our input and output(s), let's go back to our initial `pod_scenario` function. Here we need to add a decorator to tell the SDK about metadata, and more importantly, what the return types are. (This is needed because Python does not support reading return types to an adequate level.)

```python
from arcaflow_plugin_sdk import plugin


@plugin.step(
    id="pod",
    name="Pod scenario",
    description="Kill one or more pods matching the criteria",
    outputs={"success": PodScenarioResults, "error": PodScenarioError},
)
def pod_scenario(params: PodScenarioParams):
    # Fail for now
    return "error", PodScenarioError("Not implemented")
```

As you can see, apart from the metadata, we also declare the type of the parameter object so the SDK can read it.

Let's go through the `@plugin.step` decorator parameters one by one:

- `id` indicates the identifier of this step. This must be globally unique
- `name` indicates a human-readable name for this step
- `description` indicates a longer description for this step
- `outputs` indicates which possible outputs the step can have, with their output identifiers as keys

The function must return the output identifier, along with the output object.

### Running the plugin

Finally, we need to call `plugin.run()` in order to actually run the plugin:

```python
if __name__ == "__main__":
    sys.exit(plugin.run(plugin.build_schema(
        # Pass one or more scenario functions here
        pod_scenario,
    )))
```

You can now call your plugin using `./yourscript.py -f path-to-parameters.yaml`. If you have defined more than one step, you also need to pass the `-s step-id` parameter.

**Keep in mind, you should always test your plugin.** See [Testing your plugin](#testing-your-plugin) below for details.

### Types

The SDK supports a wide range of types. Let's start with the basics:

- `str`
- `int`
- Enums
- `re.Pattern`
- `typing.List[othertype]` (you must specify the type for the contents of the list)
- `typing.Dict[keytype, valuetype]` (you must specify the type for the keys and values)
- Any dataclass

#### Optional parameters

You can also declare any parameter as optional like this:

```python
@dataclasses.dataclass
class MyClass:
    param: typing.Optional[int] = None
```

Note that adding `typing.Optional` is not enough, you *must* specify the default value.

#### Validation

You can also validate the values by using [`typing.Annotated`](https://docs.python.org/3/library/typing.html#typing.Annotated), such as this:

```python
class MyClass:
    param: typing.Annotated[int, validation.min(5)]
```

This will create a minimum-value validation for the parameter of 5. The following annotations are supported for validation:

- `validation.min()` for strings, ints, lists, and maps
- `validation.max()` for strings, ints, lists, and maps
- `validation.pattern()` for strings
- `validation.required_if()` for any field on an object
- `validation.required_if_not()` for any field on an object
- `validation.conflicts()` for any field on an object

#### Metadata

You can add metadata to your schema by using the `field()` parameter for dataclasses, for example:

```python
@dataclasses.dataclass
class MyClass:
    param: str = field(metadata={"id": "my-param", "name":"Parameter 1", "description": "This is a parameter"})
```

---

## Building a schema by hand

For performance reasons, or for the purposes of separation of concerns, you may want to create a schema by hand. This section walks you through declaring a schema by hand and then using it to call a function. Keep in mind, the SDK still primarily operates with dataclasses to transport structured data.

We start by defining a schema:

```python
from arcaflow_plugin_sdk import schema
from typing import Dict

steps: Dict[str, schema.StepSchema]

s = schema.Schema(
    steps,
)
```

The `steps` parameter here must be a dict, where the key is the step ID and the value is the step schema. So, let's create a step schema:

```python
from arcaflow_plugin_sdk import schema

step_schema = schema.StepSchema(
    id = "pod",
    name = "Pod scenario",
    description = "Kills pods",
    input = input_schema,
    outputs = outputs,
    handler = my_handler_func
)
```

Let's go in order:

- The `input` must be a schema of the type `schema.ObjectType`. This describes the single parameter that will be passed to `my_handler_func`.
- The `outputs` describe a `Dict[str, schema.ObjectType]`, where the key is the ID for the returned output type, while the value describes the output schema.
- The `handler` function takes one parameter, the object described in `input` and must return a tuple of a string and the output object. Here the ID uniquely identifies which output is intended, for example `success` and `error`, while  the second parameter in the tuple must match the `outputs` declaration.

That's it! Now all that's left is to define the `ObjectType` and any subobjects.

### ObjectType

The ObjectType is intended as a backing type for [dataclasses](https://docs.python.org/3/library/dataclasses.html). For example:

```python
t = schema.ObjectType(
    TestClass,
    {
        "a": schema.Field(
            type=schema.StringType(),
            required=True,
        ),
        "b": schema.Field(
            type=schema.IntType(),
            required=True,
        )
    }
)
```

The fields support the following parameters:

- `type`: underlying type schema for the field (required)
- `name`: name for the current field
- `description`: description for the current field
- `required`: marks the field as required
- `required_if`: a list of other fields that, if filled, will also cause the current field to be required
- `required_if_not`: a list of other fields that, if not set, will cause the current field to be required
- `conflicts`: a list of other fields that cannot be set together with the current field

### StringType

String types indicate that the underlying type is a string. 

```python
t = schema.StringType()
```

The string type supports the following parameters:

- `min_length`: minimum length for the string (inclusive)
- `max_length`: maximum length for the string (inclusive)
- `pattern`: regular expression the string must match

### PatternType

The pattern type indicates that the field must contain a regular expression. It will be decoded as `re.Pattern`.

```python
t = schema.PatternType()
```

The pattern type has no parameters.

### IntType

The int type indicates that the underlying type is an integer.

```python
t = schema.IntType()
```

The int type supports the following parameters:

- `min`: minimum value for the number (inclusive).
- `max`: minimum value for the number (inclusive).

### EnumType

The enum type creates a type from an existing enum:

```python
class MyEnum(Enum):
    A = "a"
    B = "b"

t = schema.EnumType(MyEnum)
```

The enum type has no further parameters.

### ListType

The list type describes a list of items. The item type must be described:

```python
t = schema.ListType(
    schema.StringType()
)
```

The list type supports the following extra parameters:

- `min`: The minimum number of items in the list (inclusive)
- `max`: The maximum number of items in the list (inclusive)

### MapType

The map type describes a key-value type (dict). You must specify both the key and the value type:

```python
t = schema.MapType(
    schema.StringType(),
    schema.StringType()
)
```

The map type supports the following extra parameters:

- `min`: The minimum number of items in the map (inclusive)
- `max`: The maximum number of items in the map (inclusive)

### Running the plugin

If you create the schema by hand, you can add the following code to your plugin: 

```python
if __name__ == "__main__":
    sys.exit(plugin.run(your_schema))
```

You can then run your plugin as described before.

## Testing your plugin

You should always make sure you have enough test coverage to prevent your plugin from breaking. To help you with testing, this SDK provides some tools for testing:

1. Serialization tests for your input and output to make sure your classes can be serialized for transport
2. Functional tests that call your plugin and make sure it works correctly

### Writing a serialization test

You can use any test framework you like for your serialization test, we'll demonstrate with [unittest](https://docs.python.org/3/library/unittest.html) as it is included directly in Python. The key to this test is to call `plugin.test_object_serialization()` with an instance of your dataclass that you want to test:

```python
class ExamplePluginTest(unittest.TestCase):
    def test_serialization(self):
        self.assertTrue(plugin.test_object_serialization(
            example_plugin.PodScenarioResults(
                [
                    example_plugin.Pod(
                        namespace="default",
                        name="nginx-asdf"
                    )
                ]
            )
        ))
```

Remember, you need to call this function with an **instance** containing actual data, not just the class name.

The test function will first serialize, then unserialize your data and check if it's the same. If you want to use a manually created schema, you can do so, too:


```python
class ExamplePluginTest(unittest.TestCase):
    def test_serialization(self):
        plugin.test_object_serialization(
            example_plugin.PodScenarioResults(
                #...
            ),
            schema.ObjectType(
                #...
            )
        )
```

### Functional tests

Functional tests don't have anything special about them. You can directly call your code with your dataclasses as parameters, and check the return. This works best on auto-generated schemas with the `@plugin.step` decorator. See below for manually created schemas.

```python
class ExamplePluginTest(unittest.TestCase):
    def test_functional(self):
        input = example_plugin.PodScenarioParams()

        output_id, output_data = example_plugin.pod_scenario(input)

        # Check if the output is always an error, as it is the case for the example plugin.
        self.assertEqual("error", output_id)
        self.assertEqual(
            output_data,
            example_plugin.PodScenarioError(
                "Cannot kill pod .* in namespace .*, function not implemented"
            )
        )
```

If you created your schema manually, the best way to write your tests is to include the schema in your test. This will automatically validate both the input and the output, making sure they conform to your schema. For example:

```python
class ExamplePluginTest(unittest.TestCase):
    def test_functional(self):
        step_schema = schema.StepSchema(
            #...
            handler = example_plugin.pod_scenario,
        )
        input = example_plugin.PodScenarioParams()

        output_id, output_data = step_schema(input)

        # Check if the output is always an error, as it is the case for the example plugin.
        self.assertEqual("error", output_id)
        self.assertEqual(
            output_data,
            example_plugin.PodScenarioError(
                "Cannot kill pod .* in namespace .*, function not implemented"
            )
        )
```

---

## Embedding your plugin

Instead of using your plugin as a standalone tool or in conjunction with Arcaflow, you can also embed your plugin into your existing Python application. To do that you simply build a schema using one of the methods described above and then call the schema yourself. You can pass raw data as an input, and you'll get the benefit of schema validation.

```python
# Build your schema using the schema builder from above with the step functions passed.
schema = plugin.build_schema(pod_scenario)

# Which step we want to execute
step_id = "pod"
# Input parameters. Note, these must be a dict, not a dataclass
step_params = {
    "pod_name_pattern": ".*",
    "pod_namespace_pattern": ".*",
}

# Execute the step
output_id, output_data = schema(step_id, step_params)

# Print which kind of result we have
pprint.pprint(output_id)
# Print the result data
pprint.pprint(output_data)
```

However, the example above requires you to provide the data as a `dict`, not a `dataclass`, and it will also return a `dict` as an output object. Sometimes, you may want to use a partial approach, where you only use part of the SDK. In this case, you can change your code to run any of the following functions, in order:

- `serialization.load_from_file()` to load a YAML or JSON file into a dict
- `yourschema.unserialize_input()` to turn a `dict` into a `dataclass` needed for your steps
- `yourschema.call_step()` to run a step with the unserialized `dataclass`
- `yourschema.serialize_output()` to turn the output `dataclass` into a `dict`

---

## FAQ

### How can I add a field with dashes, such as `my-field`?

Dataclasses don't support dashes in parameters. You can work around this by defining the `id` metadata field:

```python
@dataclasses.dataclass
class MyData:
    my_field: str = dataclasses.field(metadata={"id": "my-field"})
```

### How can I write a dataclass from a schema to a YAML or JSON file?
You can [extend Pythons JSON encoder](https://stackoverflow.com/questions/51286748/make-the-python-json-encoder-support-pythons-new-dataclasses) to support dataclasses. If that doesn't suit your needs, you can use this SDK to convert the dataclasses to their basic representations and then write that to your JSON or YAML file. First, add this outside of your step:

```python
my_object_schema = plugin.build_object_schema(YourDataclass)
```

Inside your step function you can then dump the data from your input

```python
def your_step(params: YourParams)
    yaml_contents = yaml.dump(my_object_schema.serialize(params.some_param))
```