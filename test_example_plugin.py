#!/usr/bin/env python3
import unittest

import example_plugin
from arcaflow_plugin_sdk import plugin


class ExamplePluginTest(unittest.TestCase):
    @staticmethod
    def test_serialization():
        plugin.test_object_serialization(
            example_plugin.InputParams(
                name=example_plugin.FullName("Arca", "Lot")
            )
        )

        plugin.test_object_serialization(
            example_plugin.SuccessOutput(message="Hello, Arca Lot!")
        )

        plugin.test_object_serialization(
            example_plugin.ErrorOutput(error="This is an error")
        )

    def test_functional(self):
        step_input = example_plugin.InputParams(
            name=example_plugin.FullName("Arca", "Lot")
        )

        # Note: The call to hello_world is to the output of the decorator, not
        # the function itself, so it's calling the StepType
        output_id, output_data = example_plugin.hello_world(
            self.id(), step_input
        )

        # The example plugin always returns an error:
        self.assertEqual("success", output_id)
        self.assertEqual(
            output_data, example_plugin.SuccessOutput("Hello, Arca Lot!")
        )


if __name__ == "__main__":
    unittest.main()
