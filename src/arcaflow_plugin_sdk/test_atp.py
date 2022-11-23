import dataclasses
import os
import signal
import time
import unittest
from typing import TextIO, Tuple, Union

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


test_schema = plugin.build_schema(hello_world)


class ATPTest(unittest.TestCase):
    def _execute_plugin(self) -> Tuple[int, TextIO, TextIO]:
        stdin_reader_fd, stdin_writer_fd = os.pipe()
        stdout_reader_fd, stdout_writer_fd = os.pipe()
        pid = os.fork()
        if pid == 0:
            os.close(stdin_writer_fd)
            os.close(stdout_reader_fd)

            stdin_reader = os.fdopen(stdin_reader_fd, "r")
            stdout_writer = os.fdopen(stdout_writer_fd, "w")

            result = atp.run_plugin(
                test_schema,
                stdin_reader.buffer.raw,
                stdout_writer.buffer.raw,
                stdout_writer.buffer.raw,
            )
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

    def test_full_workflow(self):
        pid, stdin_writer, stdout_reader = self._execute_plugin()

        try:
            client = atp.PluginClient(stdin_writer.buffer.raw, stdout_reader.buffer.raw)
            client.start_output()
            hello_message = client.read_hello()
            self.assertEqual(1, hello_message.version)

            self.assertEqual(
                schema.SCHEMA_SCHEMA.serialize(test_schema),
                schema.SCHEMA_SCHEMA.serialize(hello_message.schema),
            )

            client.start_work("hello-world", {"name": "Arca Lot"})

            output_id, output_data, debug_logs = client.read_results()
            self.assertEqual("Hello world!\n", debug_logs)
        finally:
            self._cleanup(pid, stdin_writer, stdout_reader)


if __name__ == "__main__":
    unittest.main()
