from dataclasses import dataclass

from arcaflow_plugin_sdk import schema

cancel_signal_input_schema = schema.ScopeSchema(
    {
        "cancelInput": schema.ObjectSchema(
            "cancelInput",
            {},  # No fields/properties at the moment
        )
    },
    "cancelInput",
)

cancel_signal_schema = schema.SignalSchema(
    id="cancel",
    data_schema=cancel_signal_input_schema,
    display=schema.DisplayValue(
        name="Step Cancel",
        description=(
            "The signal that instructs the plugin to finish execution"
            " gracefully."
        ),
        icon=None,
    ),
)


@dataclass
class cancelInput:
    pass
