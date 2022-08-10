import dataclasses
import io
import re
import tempfile
import typing
import unittest
from enum import Enum
from re import Pattern
from typing import List, Dict
from arcaflow_plugin_sdk import schema, validation, plugin, annotations
from arcaflow_plugin_sdk.plugin import SchemaBuildException, _Resolver
from arcaflow_plugin_sdk.schema import TypeID, OneOfType, ConstraintException


class ResolverTest(unittest.TestCase):
    def test_non_dataclass(self):
        with self.assertRaises(SchemaBuildException) as ctx:
            _Resolver.resolve(complex)
        self.assertIn("complex numbers are not supported", ctx.exception.msg)

    def test_regexp(self):
        resolved_type = _Resolver.resolve(Pattern)
        self.assertEqual(schema.TypeID.PATTERN, resolved_type.type_id())

    def test_string(self):
        test: str = "foo"
        resolved_type = _Resolver.resolve(type(test))
        self.assertEqual(schema.TypeID.STRING, resolved_type.type_id())
        resolved_type = _Resolver.resolve(test)
        self.assertEqual(schema.TypeID.STRING, resolved_type.type_id())

    def test_int(self):
        test: int = 5
        resolved_type = _Resolver.resolve(type(test))
        self.assertEqual(schema.TypeID.INT, resolved_type.type_id())
        resolved_type = _Resolver.resolve(test)
        self.assertEqual(schema.TypeID.INT, resolved_type.type_id())

    def test_float(self):
        test: float = 3.14
        resolved_type = _Resolver.resolve(type(test))
        self.assertEqual(schema.TypeID.FLOAT, resolved_type.type_id())
        resolved_type = _Resolver.resolve(test)
        self.assertEqual(schema.TypeID.FLOAT, resolved_type.type_id())

    def test_enum(self):
        class TestEnum(Enum):
            A = "a"
            B = "b"

        resolved_type = _Resolver.resolve(TestEnum)
        self.assertEqual(schema.TypeID.ENUM, resolved_type.type_id())

    def test_list(self):
        resolved_type: schema.ListType[str] = _Resolver.resolve(List[str])
        self.assertEqual(schema.TypeID.LIST, resolved_type.type_id())
        self.assertEqual(schema.TypeID.STRING, resolved_type.type.type_id())

        test: list = []
        with self.assertRaises(SchemaBuildException):
            _Resolver.resolve(type(test))

    def test_map(self):
        resolved_type: schema.MapType[str, str] = _Resolver.resolve(Dict[str, str])
        self.assertEqual(schema.TypeID.MAP, resolved_type.type_id())
        self.assertEqual(schema.TypeID.STRING, resolved_type.key_type.type_id())
        self.assertEqual(schema.TypeID.STRING, resolved_type.value_type.type_id())

        test: dict = {}
        with self.assertRaises(SchemaBuildException):
            _Resolver.resolve(type(test))

        resolved_type = _Resolver.resolve(dict[str, str])
        self.assertEqual(schema.TypeID.MAP, resolved_type.type_id())
        self.assertEqual(schema.TypeID.STRING, resolved_type.key_type.type_id())
        self.assertEqual(schema.TypeID.STRING, resolved_type.value_type.type_id())

    def test_class(self):
        class TestData:
            a: str
            b: int
            c: float
            d: bool

        with self.assertRaises(SchemaBuildException):
            _Resolver.resolve(TestData)

        @dataclasses.dataclass
        class TestData:
            a: str
            b: int
            c: float
            d: bool

        resolved_type: schema.ObjectType
        resolved_type = _Resolver.resolve(TestData)
        self.assertEqual(schema.TypeID.OBJECT, resolved_type.type_id())

        self.assertEqual("a", resolved_type.properties["a"].name)
        self.assertTrue(resolved_type.properties["a"].required)
        self.assertEqual(TypeID.STRING, resolved_type.properties["a"].type.type_id())
        self.assertEqual("b", resolved_type.properties["b"].name)
        self.assertTrue(resolved_type.properties["b"].required)
        self.assertEqual(TypeID.INT, resolved_type.properties["b"].type.type_id())
        self.assertEqual("c", resolved_type.properties["c"].name)
        self.assertTrue(resolved_type.properties["c"].required)
        self.assertEqual(TypeID.FLOAT, resolved_type.properties["c"].type.type_id())
        self.assertEqual("d", resolved_type.properties["d"].name)
        self.assertTrue(resolved_type.properties["d"].required)
        self.assertEqual(TypeID.BOOL, resolved_type.properties["d"].type.type_id())

        @dataclasses.dataclass
        class TestData:
            a: str = "foo"
            b: int = 5
            c: str = dataclasses.field(default="bar", metadata={"name": "C", "description": "A string"})
            d: bool = True

        resolved_type: schema.ObjectType
        resolved_type = _Resolver.resolve(TestData)
        self.assertEqual(schema.TypeID.OBJECT, resolved_type.type_id())

        self.assertEqual("a", resolved_type.properties["a"].name)
        self.assertFalse(resolved_type.properties["a"].required)
        self.assertEqual(TypeID.STRING, resolved_type.properties["a"].type.type_id())
        self.assertEqual("b", resolved_type.properties["b"].name)
        self.assertFalse(resolved_type.properties["b"].required)
        self.assertEqual(TypeID.INT, resolved_type.properties["b"].type.type_id())
        self.assertEqual("C", resolved_type.properties["c"].name)
        self.assertEqual("A string", resolved_type.properties["c"].description)
        self.assertFalse(resolved_type.properties["c"].required)
        self.assertEqual(TypeID.STRING, resolved_type.properties["c"].type.type_id())
        self.assertEqual("d", resolved_type.properties["d"].name)
        self.assertFalse(resolved_type.properties["d"].required)
        self.assertEqual(TypeID.BOOL, resolved_type.properties["d"].type.type_id())

    def test_union(self):
        @dataclasses.dataclass
        class A:
            a: str

        @dataclasses.dataclass
        class B:
            b: str

        @dataclasses.dataclass
        class TestData:
            a: typing.Union[A, B]

        resolved_type: schema.ObjectType
        resolved_type = _Resolver.resolve(TestData)
        self.assertEqual(TypeID.ONEOF, resolved_type.properties["a"].type.type_id())
        t: OneOfType = resolved_type.properties["a"].type
        self.assertEqual(t.discriminator_field_name, "_type")
        self.assertEqual(TypeID.STRING, t.discriminator_field_schema.type_id())
        self.assertEqual(A, t.one_of["A"].type_class())
        self.assertEqual(B, t.one_of["B"].type_class())

    def test_union_custom_discriminator(self):
        @dataclasses.dataclass
        class A:
            discriminator: int
            a: str

        @dataclasses.dataclass
        class B:
            discriminator: int
            b: str

        @dataclasses.dataclass
        class TestData:
            a: typing.Annotated[
                typing.Union[
                    typing.Annotated[A, annotations.discriminator_value(1)],
                    typing.Annotated[B, annotations.discriminator_value(2)],
                ],
                annotations.discriminator("discriminator")
            ]

        resolved_type: schema.ObjectType
        resolved_type = _Resolver.resolve(TestData)
        self.assertEqual(TypeID.ONEOF, resolved_type.properties["a"].type.type_id())
        t: OneOfType = resolved_type.properties["a"].type
        self.assertEqual(t.discriminator_field_name, "discriminator")
        self.assertEqual(t.discriminator_field_schema.type_id(), TypeID.INT)

    def test_optional(self):
        @dataclasses.dataclass
        class TestData:
            a: typing.Optional[str] = None

        resolved_type: schema.ObjectType
        resolved_type = _Resolver.resolve(TestData)
        self.assertEqual(schema.TypeID.OBJECT, resolved_type.type_id())
        self.assertFalse(resolved_type.properties["a"].required)
        self.assertEqual(TypeID.STRING, resolved_type.properties["a"].type.type_id())

    def test_annotated(self):
        resolved_type: schema.StringType
        resolved_type = _Resolver.resolve(typing.Annotated[str, validation.min(3)])
        self.assertEqual(schema.TypeID.STRING, resolved_type.type_id())
        self.assertEqual(3, resolved_type._min_length)

        @dataclasses.dataclass
        class TestData:
            a: typing.Annotated[typing.Optional[str], validation.min(3)] = None

        resolved_type2: schema.ObjectType
        resolved_type2 = _Resolver.resolve(TestData)
        a = resolved_type2.properties["a"]
        self.assertEqual(schema.TypeID.STRING, a.type.type_id())
        self.assertFalse(a.required)
        t: schema.StringType = a.type
        self.assertEqual(3, t._min_length)

        with self.assertRaises(SchemaBuildException):
            @dataclasses.dataclass
            class TestData:
                a: typing.Annotated[typing.Optional[str], "foo"] = None

            _Resolver.resolve(TestData)

    def test_annotated_required_if(self):
        @dataclasses.dataclass
        class TestData2:
            a: typing.Annotated[typing.Optional[str], validation.required_if("b")] = None
            b: typing.Optional[str] = None

        t: schema.ObjectType
        t = _Resolver.resolve(TestData2)
        a = t.properties["a"]
        b = t.properties["b"]

        self.assertFalse(a.required)
        self.assertFalse(b.required)
        self.assertEqual(["b"], a.required_if)

    def test_different_id(self):
        @dataclasses.dataclass
        class TestData:
            a: str = dataclasses.field(metadata={"id": "test-field"})

        t: schema.ObjectType = _Resolver.resolve(TestData)
        a = t.properties["test-field"]
        self.assertEqual(a.field_override, "a")

    def test_unclear_error_message(self):
        @dataclasses.dataclass
        class TestData:
            a: typing.Dict[str, str]
        t: schema.ObjectType = _Resolver.resolve(TestData)
        with self.assertRaises(ConstraintException):
            t.serialize(TestData(type(dict[str, str])))


class SerializationTest(unittest.TestCase):
    def test_serialization_cycle(self):
        @dataclasses.dataclass
        class TestData1:
            A: str
            B: int
            C: Dict[str, int]
            D: List[str]
            H: float
            E: typing.Optional[str] = None
            F: typing.Annotated[typing.Optional[str], validation.min(3)] = None
            G: typing.Optional[str] = dataclasses.field(default="", metadata={"id": "test-field", "name": "G"})

        plugin.test_object_serialization(
            TestData1(
                A="Hello world!",
                B=5,
                C={},
                D=[],
                H=3.14
            ),
            self.fail,
        )

        @dataclasses.dataclass
        class KillPodConfig:
            namespace_pattern: re.Pattern

            name_pattern: typing.Annotated[
                typing.Optional[re.Pattern],
                validation.required_if_not("label_selector")
            ] = None

            kill: typing.Annotated[int, validation.min(1)] = dataclasses.field(
                default=1,
                metadata={"name": "Number of pods to kill", "description": "How many pods should we attempt to kill?"}
            )

            label_selector: typing.Annotated[
                typing.Optional[str],
                validation.min(1),
                validation.required_if_not("name_pattern")
            ] = None

            kubeconfig_path: typing.Optional[str] = None

        plugin.test_object_serialization(
            KillPodConfig(
                namespace_pattern=re.compile(".*"),
                name_pattern=re.compile(".*")
            ),
            self.fail
        )

    def test_required_if(self):
        @dataclasses.dataclass
        class TestData1:
            A: typing.Annotated[typing.Optional[str], validation.required_if("B")] = None
            B: typing.Optional[int] = None

        s = plugin.build_object_schema(TestData1)

        unserialized = s.unserialize({})
        self.assertIsNone(unserialized.A)
        self.assertIsNone(unserialized.B)

        unserialized = s.unserialize({"A": "Foo"})
        self.assertEqual(unserialized.A, "Foo")
        self.assertIsNone(unserialized.B)

        with self.assertRaises(schema.ConstraintException):
            s.unserialize({"B": "Foo"})

    def test_int_optional(self):
        @dataclasses.dataclass
        class TestData1:
            A: typing.Optional[int] = None

        s = plugin.build_object_schema(TestData1)

        unserialized = s.unserialize({})
        self.assertIsNone(unserialized.A)

    def test_float_optional(self):
        @dataclasses.dataclass
        class TestData1:
            A: typing.Optional[float] = None

        s = plugin.build_object_schema(TestData1)

        unserialized = s.unserialize({})
        self.assertIsNone(unserialized.A)

    def test_build_object_schema_wrapping(self):
        @dataclasses.dataclass
        class TestData1:
            A: int
        s = plugin.build_object_schema(TestData1)

        with self.assertRaises(ConstraintException) as ctx:
            s.unserialize({})

        self.assertIn("TestData1", ctx.exception.__str__())


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
    {"success": StdoutTestOutput}
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
        tmp.write(bytes("{}", 'utf-8'))
        tmp.flush()

        i = io.StringIO()
        o = io.StringIO()
        e = io.StringIO()
        exit_code = plugin.run(
            s,
            ["test.py", "-f", tmp.name, "--debug"],
            i,
            o,
            e
        )
        self.assertEqual(0, exit_code)
        self.assertEqual("Hello world!\n", e.getvalue())


if __name__ == '__main__':
    unittest.main()
