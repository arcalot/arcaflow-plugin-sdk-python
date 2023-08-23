from dataclasses import dataclass

from arcaflow_plugin_sdk import schema

cancel_signal_schema = schema.SignalSchema(
    id="cancelInput",
    data=schema.ScopeSchema(
        {
            "cancelInput": schema.ObjectSchema(
                "cancelInput",
                {}, # No fields/properties at the moment
            )
        },
        "cancelInput",
    ),
    display=schema.DisplayValue(
        name="Cancel input",
        description="The signal that instructs the plugin to finish execution gracefully.",
        icon=None,
    ),
)



@dataclass
class cancelInput:
    pass