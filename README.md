# Python SDK for the Arcaflow workflow engine (WIP)

## How this SDK works

In order to create an Arcaflow plugin, you must specify a **schema** for each step you want to support. This schema describes two things:

1. What your input parameters are and what their type is
2. What your output parameters are and what their type is

Note, that you can specify **several possible outputs**, depending on what the outcome of your plugin execution is. You should, however, never raise exceptions that bubble outside your plugin. If you do, your plugin will crash and Arcaflow will not be able to retrieve the result data, including the error, from it.

With the schema, the plugin can run in the following modes:

1. CLI mode, where a file with the data is loaded and the plugin is executed
2. GRPC mode (under development) where the plugin works in conjunction with the Arcaflow Engine to enable more complex workflows

For a detailed description please see [the Arcalot website](https://arcalot.github.io/arcaflow/creating-plugins/python/).

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
output_id: success
output_data:
  message: Hello, Arca Lot!
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


## Generating documentation
1. Checkout this repository
2. Create a `venv` in the current directory with `python3 -m venv $(pwd)/venv`
3. Activate the `venv` by running `source venv/bin/activate`
4. Run `pip install -r requirements.txt`
5. Run `pip install sphinx`
6. Run `pip install sphinx-rtd-theme`
7. Run `sphinx-apidoc -o docs/ -f -a -e src/ --doc-project "Python SDK for Arcaflow"`
8. Run `make -C docs html`


---

## Developing your plugin

We have a detailed guide on developing Python plugins on [the Arcalot website](https://arcalot.github.io/arcaflow/creating-plugins/python/).