import dataclasses
import os
import signal
import time
import unittest
from threading import Condition, Lock
from typing import List, TextIO, Tuple, Union

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
def hello_world(params: Input) -> Tuple[str, Output]:
    print("Hello world!")
    return "success", Output("Hello, {}!".format(params.name))


# noinspection PyTypeChecker
@plugin.step(
    id="hello-world-broken",
    name="Broken!",
    description="Throws an exception with the text 'abcde'",
    outputs={"success": Output},
)
def hello_world_broken(_: Input) -> Tuple[str, Output]:
    print("Hello world!")
    raise Exception("abcde")


@dataclasses.dataclass
class StepTestInput:
    wait_time_seconds: float
    expected_signal_count: int


@dataclasses.dataclass
class SignalTestInput:
    value: int


@dataclasses.dataclass
class SignalTestOutput:
    signals_received: List[int]


class SignalTestStep:
    signal_values: List[int]
    exit_condition: Condition
    lock: Lock

    def __init__(self):
        # Due to the way Python works, this MUST be done here, and not inlined
        # above, or else it will be shared by all objects, resulting in a
        # shared list and event, which would cause problems.
        self.signal_values = []
        self.lock = Lock()
        self.exit_condition = Condition(self.lock)

    @plugin.step_with_signals(
        id="signal_test_step",
        name="signal_test_step",
        description="waits for signal with timeout",
        outputs={"success": SignalTestOutput},
        signal_handler_method_names=["signal_test_signal_handler"],
        signal_emitters=[],
        step_object_constructor=lambda: SignalTestStep(),
    )
    def signal_test_step(
        self, params: StepTestInput
    ) -> Tuple[str, Union[SignalTestOutput]]:
        with self.exit_condition:
            self.exit_condition.wait_for(
                lambda:
                    len(self.signal_values) >= params.expected_signal_count,
                timeout=params.wait_time_seconds)
        return "success", SignalTestOutput(self.signal_values)

    @plugin.signal_handler(
        id="record_value",
        name="record value",
        description=(
            "Records the value, and optionally ends the step. Throws error if"
            " it's less than 0, for testing."
        ),
    )
    def signal_test_signal_handler(self, signal_input: SignalTestInput):
        with self.exit_condition:
            if signal_input.value < 0:
                raise Exception(f"Value below zero: {signal_input.value}")
            self.signal_values.append(signal_input.value)
            self.exit_condition.notify()


test_schema = plugin.build_schema(hello_world)
test_broken_schema = plugin.build_schema(hello_world_broken)
test_signals_schema = plugin.build_schema(SignalTestStep.signal_test_step)


class ATPTest(unittest.TestCase):
    def _execute_plugin(self, schema) -> Tuple[int, TextIO, TextIO]:
        stdin_reader_fd, stdin_writer_fd = os.pipe()
        stdout_reader_fd, stdout_writer_fd = os.pipe()
        pid = os.fork()
        if pid == 0:  # The forked process
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
        elif pid > 0:  # The original process
            os.close(stdin_reader_fd)
            os.close(stdout_writer_fd)

            stdin_writer = os.fdopen(stdin_writer_fd, "w")
            stdout_reader = os.fdopen(stdout_reader_fd, "r")

            return pid, stdin_writer, stdout_reader
        else:
            self.fail("Fork failed")

    def _cleanup(
        self, pid, stdin_writer, stdout_reader, can_fail: bool = False
    ):
        stdin_writer.close()
        stdout_reader.close()
        time.sleep(0.1)
        os.kill(pid, signal.SIGTERM)
        stop_info = os.waitpid(pid, 0)
        exit_status = os.waitstatus_to_exitcode(stop_info[1])
        if exit_status != 0 and not can_fail:
            self.fail(
                "Plugin exited with non-zero status: {}".format(exit_status)
            )

    def test_step_simple(self):
        pid, stdin_writer, stdout_reader = self._execute_plugin(test_schema)

        try:
            client = atp.PluginClient(
                stdin_writer.buffer.raw, stdout_reader.buffer.raw
            )
            client.start_output()
            hello_message = client.read_hello()
            self.assertEqual(3, hello_message.version)

            self.assertEqual(
                schema.SCHEMA_SCHEMA.serialize(test_schema),
                schema.SCHEMA_SCHEMA.serialize(hello_message.schema),
            )

            client.start_work(self.id(), "hello-world", {"name": "Arca Lot"})

            result = client.read_single_result()
            self.assertEqual(result.run_id, self.id())
            client.send_client_done()
            self.assertEqual(result.output_id, "success")
            self.assertEqual("Hello world!\n", result.debug_logs)
        finally:
            self._cleanup(pid, stdin_writer, stdout_reader)

    def test_step_with_signals(self):
        pid, stdin_writer, stdout_reader = self._execute_plugin(
            test_signals_schema
        )

        try:
            client = atp.PluginClient(
                stdin_writer.buffer.raw, stdout_reader.buffer.raw
            )
            client.start_output()
            hello_message = client.read_hello()
            self.assertEqual(3, hello_message.version)

            self.assertEqual(
                schema.SCHEMA_SCHEMA.serialize(test_signals_schema),
                schema.SCHEMA_SCHEMA.serialize(hello_message.schema),
            )

            client.start_work(
                self.id(), "signal_test_step",
                {"wait_time_seconds": 5.0, "expected_signal_count": 3}
            )
            client.send_signal(
                self.id(),
                "record_value",
                {"value": 1},
            )
            client.send_signal(
                self.id(),
                "record_value",
                {"value": 2},
            )
            client.send_signal(
                self.id(),
                "record_value",
                {"value": 3},
            )
            result = client.read_single_result()
            self.assertEqual(result.run_id, self.id())
            client.send_client_done()
            self.assertEqual(result.debug_logs, "")
            self.assertEqual(result.output_id, "success")
            self.assertListEqual(
                sorted(result.output_data["signals_received"]), [1, 2, 3]
            )
        finally:
            self._cleanup(pid, stdin_writer, stdout_reader)

    def test_multi_step_with_signals(self):
        """Starts two steps simultaneously, sends them separate data from
        signals, then verifies that each step got the dats intended for it."""
        pid, stdin_writer, stdout_reader = self._execute_plugin(
            test_signals_schema
        )

        try:
            client = atp.PluginClient(
                stdin_writer.buffer.raw, stdout_reader.buffer.raw
            )
            client.start_output()
            hello_message = client.read_hello()
            self.assertEqual(3, hello_message.version)

            self.assertEqual(
                schema.SCHEMA_SCHEMA.serialize(test_signals_schema),
                schema.SCHEMA_SCHEMA.serialize(hello_message.schema),
            )
            step_a_id = self.id() + "_a"
            step_b_id = self.id() + "_b"

            client.start_work(
                step_a_id, "signal_test_step",
                {"wait_time_seconds": 5.0, "expected_signal_count": 2}
            )
            client.start_work(
                step_b_id, "signal_test_step",
                {"wait_time_seconds": 5.0, "expected_signal_count": 1}
            )
            client.send_signal(
                step_a_id,
                "record_value",
                {"value": 1},
            )
            client.send_signal(
                step_b_id,
                "record_value",
                {"value": 2},
            )
            step_b_result = client.read_single_result()

            client.send_signal(
                step_a_id,
                "record_value",
                {"value": 3},
            )
            step_a_result = client.read_single_result()
            client.send_client_done()
            self.assertEqual(
                step_a_result.run_id, step_a_id, "Expected 'a' run ID"
            )
            self.assertEqual(
                step_b_result.run_id, step_b_id, "Expected 'b' run ID"
            )
            self.assertEqual(step_b_result.debug_logs, "")
            self.assertEqual(step_a_result.debug_logs, "")
            self.assertEqual(step_a_result.output_id, "success")
            self.assertEqual(step_b_result.output_id, "success")
            self.assertListEqual(
                step_a_result.output_data["signals_received"], [1, 3]
            )
            self.assertListEqual(
                step_b_result.output_data["signals_received"], [2]
            )
        finally:
            self._cleanup(pid, stdin_writer, stdout_reader)

    def test_broken_step(self):
        """Runs a step that throws an exception, which is something that should
        be caught by the plugin, but we need to test for it since the uncaught
        exceptions are the hardest to debug without proper handling."""
        pid, stdin_writer, stdout_reader = self._execute_plugin(
            test_broken_schema
        )

        try:
            client = atp.PluginClient(
                stdin_writer.buffer.raw, stdout_reader.buffer.raw
            )
            client.start_output()
            client.read_hello()

            client.start_work(
                self.id(), "hello-world-broken", {"name": "Arca Lot"}
            )

            with self.assertRaises(atp.PluginClientStateException) as context:
                _, _, _, _ = client.read_single_result()
            client.send_client_done()
            self.assertIn("abcde", str(context.exception))
        finally:
            self._cleanup(pid, stdin_writer, stdout_reader, True)

    def test_wrong_step(self):
        """Tests the error reporting due to an invalid step being called."""
        pid, stdin_writer, stdout_reader = self._execute_plugin(test_schema)

        try:
            client = atp.PluginClient(
                stdin_writer.buffer.raw, stdout_reader.buffer.raw
            )
            client.start_output()
            client.read_hello()

            client.start_work(self.id(), "WRONG", {"name": "Arca Lot"})

            with self.assertRaises(atp.PluginClientStateException) as context:
                _, _, _, _ = client.read_single_result()
            client.send_client_done()
            self.assertIn("No such step: WRONG", str(context.exception))
        finally:
            self._cleanup(pid, stdin_writer, stdout_reader, True)

    def test_invalid_runtime_message_id(self):
        """Tests the error reporting due to an invalid step being called."""
        pid, stdin_writer, stdout_reader = self._execute_plugin(test_schema)

        try:
            client = atp.PluginClient(
                stdin_writer.buffer.raw, stdout_reader.buffer.raw
            )
            client.start_output()
            client.read_hello()

            # noinspection PyTypeChecker
            client.send_runtime_message(1000, "", "")

            with self.assertRaises(atp.PluginClientStateException) as context:
                _, _, _, _ = client.read_single_result()
            client.send_client_done()
            self.assertIn(
                "Unknown runtime message ID: 1000", str(context.exception)
            )
        finally:
            self._cleanup(pid, stdin_writer, stdout_reader, True)

    def test_error_in_signal(self):
        pid, stdin_writer, stdout_reader = self._execute_plugin(
            test_signals_schema
        )

        try:
            client = atp.PluginClient(
                stdin_writer.buffer.raw, stdout_reader.buffer.raw
            )
            client.start_output()
            hello_message = client.read_hello()
            self.assertEqual(3, hello_message.version)

            self.assertEqual(
                schema.SCHEMA_SCHEMA.serialize(test_signals_schema),
                schema.SCHEMA_SCHEMA.serialize(hello_message.schema),
            )

            client.start_work(
                self.id(), "signal_test_step",
                {"wait_time_seconds": 5.0, "expected_signal_count": 1}
            )
            client.send_signal(
                self.id(),
                "record_value",
                {"value": -1},
            )

            # Note: The exception is raised after the step finishes in the test
            # class
            with self.assertRaises(atp.PluginClientStateException) as context:
                client.read_single_result()
            client.send_client_done()
            self.assertIn("Value below zero: -1", str(context.exception))

        finally:
            self._cleanup(pid, stdin_writer, stdout_reader, True)


if __name__ == "__main__":
    unittest.main()
