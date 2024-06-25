import dataclasses
import io
import tempfile
import typing
import unittest

from arcaflow_plugin_sdk import plugin, schema


@dataclasses.dataclass
class EmptyTestInput:
    pass


@dataclasses.dataclass
class EmptyTestOutput:
    pass


@plugin.step(
    "stdout-test",
    "Stdout test",
    "A test for writing to stdout.",
    {"success": EmptyTestOutput},
)
def stdout_test_step(
    _: EmptyTestInput,
) -> typing.Tuple[str, EmptyTestOutput]:
    print("Hello world!")
    return "success", EmptyTestOutput()


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
        exit_code = plugin.run(
            s, ["test.py", "-f", tmp.name, "--debug"], i, o, e
        )
        self.assertEqual(0, exit_code)
        self.assertEqual("Hello world!\n", e.getvalue())


@plugin.step(
    "incorrect-return",
    "Incorrect Return",
    "A step that returns a bad output which omits the output ID.",
    {"success": EmptyTestOutput},
)
def incorrect_return_step(
    _: EmptyTestInput,
) -> typing.Tuple[str, EmptyTestOutput]:
    # noinspection PyTypeChecker
    return EmptyTestOutput()


class CallStepTest(unittest.TestCase):
    def test_incorrect_return_args_count(self):
        s = plugin.build_schema(incorrect_return_step)

        with self.assertRaises(schema.BadArgumentException):
            s.call_step(self.id(), "incorrect-return", EmptyTestInput())


if __name__ == "__main__":
    unittest.main()
