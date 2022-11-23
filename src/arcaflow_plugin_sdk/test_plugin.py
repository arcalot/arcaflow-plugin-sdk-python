import dataclasses
import io
import tempfile
import typing
import unittest

from arcaflow_plugin_sdk import plugin


@dataclasses.dataclass
class StdoutTestInput:
    pass


@dataclasses.dataclass
class StdoutTestOutput:
    pass


@plugin.step(
    "stdout-test",
    "Stdout test",
    "A test for writing to stdout.",
    {"success": StdoutTestOutput},
)
def stdout_test_step(input: StdoutTestInput) -> typing.Tuple[str, StdoutTestOutput]:
    print("Hello world!")
    return "success", StdoutTestOutput()


class StdoutTest(unittest.TestCase):
    def test_capture_stdout(self):
        s = plugin.build_schema(stdout_test_step)
        tmp = tempfile.NamedTemporaryFile(suffix=".json")

        def cleanup():
            tmp.close()

        self.addCleanup(cleanup)
        tmp.write(bytes("{}", "utf-8"))
        tmp.flush()

        i = io.StringIO()
        o = io.StringIO()
        e = io.StringIO()
        exit_code = plugin.run(s, ["test.py", "-f", tmp.name, "--debug"], i, o, e)
        self.assertEqual(0, exit_code)
        self.assertEqual("Hello world!\n", e.getvalue())


if __name__ == "__main__":
    unittest.main()
