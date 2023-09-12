import dataclasses
import os
import signal
import time
import unittest
from threading import Event
from typing import TextIO, Tuple, Union, List

from arcaflow_plugin_sdk import atp, plugin, schema


@dataclasses.dataclass
class Input:
    name: str


@dataclasses.dataclass
class Output:
    message: str


@plugin.step(
    id="hello-world",
    name="Hello world!",
    description="Says hello :)",
    outputs={"success": Output},
)
def hello_world(params: Input) -> Tuple[str, Union[Output]]:
    print("Hello world!")
    return "success", Output("Hello, {}!".format(params.name))


@dataclasses.dataclass
class StepTestInput:
    wait_time_seconds: float


@dataclasses.dataclass
class SignalTestInput:
    final: bool  # The last one will trigger the end of the step.
    value: int


@dataclasses.dataclass
class SignalTestOutput:
    signals_received: List[int]


class SignalTestStep:
    signal_values: List[int] = []
    exit_event = Event()

    @plugin.step_with_signals(
        id="signal_test_step",
        name="signal_test_step",
        description="waits for signal with timeout",
        outputs={"success": SignalTestOutput},
        signal_handler_method_names=["signal_test_signal_handler"],
        signal_emitters=[],
        step_object_constructor=lambda: SignalTestStep(),
    )
    def signal_test_step(self, params: StepTestInput) -> Tuple[str, Union[SignalTestOutput]]:
        self.exit_event.wait(params.wait_time_seconds)
        return "success", SignalTestOutput(self.signal_values)

    @plugin.signal_handler(
        id="record_value",
        name="record value",
        description="Records the value, and optionally ends the step.",
    )
    def signal_test_signal_handler(self, signal_input: SignalTestInput):
        self.signal_values.append(signal_input.value)
        if signal_input.final:
            self.exit_event.set()


test_schema = plugin.build_schema(hello_world)
test_signals_schema = plugin.build_schema(SignalTestStep.signal_test_step)


class ATPTest(unittest.TestCase):
    def _execute_plugin(self, schema) -> Tuple[int, TextIO, TextIO]:
        stdin_reader_fd, stdin_writer_fd = os.pipe()
        stdout_reader_fd, stdout_writer_fd = os.pipe()
        pid = os.fork()
        if pid == 0:
            os.close(stdin_writer_fd)
            os.close(stdout_reader_fd)

            stdin_reader = os.fdopen(stdin_reader_fd, "r")
            stdout_writer = os.fdopen(stdout_writer_fd, "w")

            atp_server = atp.ATPServer(
                stdin_reader.buffer.raw,
                stdout_writer.buffer.raw,
                stdout_writer.buffer.raw,
            )
            result = atp_server.run_plugin(schema)
            os.close(stdin_reader_fd)
            os.close(stdout_writer_fd)
            if result != 0:
                print("Plugin exited with non-zero status: {}".format(result))
                os._exit(1)
            os._exit(0)
        elif pid > 0:
            os.close(stdin_reader_fd)
            os.close(stdout_writer_fd)

            stdin_writer = os.fdopen(stdin_writer_fd, "w")
            stdout_reader = os.fdopen(stdout_reader_fd, "r")

            return pid, stdin_writer, stdout_reader
        else:
            self.fail("Fork failed")

    def _cleanup(self, pid, stdin_writer, stdout_reader):
        stdin_writer.close()
        stdout_reader.close()
        time.sleep(1)
        os.kill(pid, signal.SIGTERM)
        stop_info = os.waitpid(pid, 0)
        exit_status = os.waitstatus_to_exitcode(stop_info[1])
        if exit_status != 0:
            self.fail("Plugin exited with non-zero status: {}".format(exit_status))

    def test_full_simple_workflow(self):
        pid, stdin_writer, stdout_reader = self._execute_plugin(test_schema)

        try:
            client = atp.PluginClient(stdin_writer.buffer.raw, stdout_reader.buffer.raw)
            client.start_output()
            hello_message = client.read_hello()
            self.assertEqual(2, hello_message.version)

            self.assertEqual(
                schema.SCHEMA_SCHEMA.serialize(test_schema),
                schema.SCHEMA_SCHEMA.serialize(hello_message.schema),
            )

            client.start_work("hello-world", {"name": "Arca Lot"})

            output_id, output_data, debug_logs = client.read_results()
            client.send_client_done()
            self.assertEqual(output_id, "success")
            self.assertEqual("Hello world!\n", debug_logs)
        finally:
            self._cleanup(pid, stdin_writer, stdout_reader)

    def test_full_workflow_with_signals(self):
        pid, stdin_writer, stdout_reader = self._execute_plugin(test_signals_schema)

        try:
            client = atp.PluginClient(stdin_writer.buffer.raw, stdout_reader.buffer.raw)
            client.start_output()
            hello_message = client.read_hello()
            self.assertEqual(2, hello_message.version)

            self.assertEqual(
                schema.SCHEMA_SCHEMA.serialize(test_signals_schema),
                schema.SCHEMA_SCHEMA.serialize(hello_message.schema),
            )

            client.start_work("signal_test_step", {"wait_time_seconds": "0.5"})
            client.send_signal("signal_test_step", "record_value",
                               {"final": "false", "value": "1"},
                               )
            client.send_signal("signal_test_step", "record_value",
                               {"final": "false", "value": "2"},
                               )
            client.send_signal("signal_test_step", "record_value",
                               {"final": "true", "value": "3"},
                               )
            output_id, output_data, _ = client.read_results()
            client.send_client_done()
            self.assertEqual(output_id, "success")
            self.assertListEqual(output_data["signals_received"], [1, 2, 3])
        finally:
            self._cleanup(pid, stdin_writer, stdout_reader)


if __name__ == "__main__":
    unittest.main()
