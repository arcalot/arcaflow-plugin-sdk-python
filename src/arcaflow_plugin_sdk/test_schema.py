import dataclasses
import doctest
import enum
import re
import typing
import unittest
from dataclasses import dataclass
from re import Pattern

from arcaflow_plugin_sdk import schema
from arcaflow_plugin_sdk.schema import (
    BadArgumentException,
    ConstraintException,
    PropertyType,
    SchemaBuildException,
)


class Color(enum.Enum):
    GREEN = "green"
    RED = "red"


class EnumTest(unittest.TestCase):
    def test_unserialize(self):
        t = schema.StringEnumType(Color)
        self.assertEqual(Color.GREEN, t.unserialize("green"))
        self.assertEqual(Color.RED, t.unserialize("red"))
        self.assertEqual(Color.GREEN, t.unserialize(Color.GREEN))
        self.assertEqual(Color.RED, t.unserialize(Color.RED))
        try:
            t.unserialize("blue")
            self.fail("Invalid enum value didn't fail.")
        except schema.ConstraintException:
            pass

        class DifferentColor(enum.Enum):
            BLUE = "blue"

        try:
            t.unserialize(DifferentColor.BLUE)
            self.fail("Invalid enum value didn't fail.")
        except schema.ConstraintException:
            pass

        with self.assertRaises(schema.BadArgumentException):

            class BadEnum(enum.Enum):
                A = "foo"
                B = False

            schema.StringEnumType(BadEnum)


class BoolTest(unittest.TestCase):
    def test_unserialize(self):
        t = schema.BoolType()
        self.assertEqual(False, t.unserialize("false"))
        self.assertEqual(False, t.unserialize("no"))
        self.assertEqual(False, t.unserialize("off"))
        self.assertEqual(False, t.unserialize("disable"))
        self.assertEqual(False, t.unserialize("disabled"))
        self.assertEqual(False, t.unserialize("0"))
        self.assertEqual(False, t.unserialize(0))
        self.assertEqual(False, t.unserialize(False))

        self.assertEqual(True, t.unserialize("true"))
        self.assertEqual(True, t.unserialize("yes"))
        self.assertEqual(True, t.unserialize("Yes"))
        self.assertEqual(True, t.unserialize("YES"))
        self.assertEqual(True, t.unserialize("on"))
        self.assertEqual(True, t.unserialize("enable"))
        self.assertEqual(True, t.unserialize("enabled"))
        self.assertEqual(True, t.unserialize("1"))
        self.assertEqual(True, t.unserialize(1))
        self.assertEqual(True, t.unserialize(True))
        with self.assertRaises(ConstraintException):
            t.unserialize(3.14)
        with self.assertRaises(ConstraintException):
            t.unserialize("")

    def test_serialize(self):
        t = schema.BoolType()
        self.assertEqual(False, t.serialize(False))
        self.assertEqual(True, t.serialize(True))
        with self.assertRaises(ConstraintException):
            t.serialize(3.14)
        with self.assertRaises(ConstraintException):
            t.serialize("yes")

    def test_validate(self):
        t = schema.BoolType()
        t.validate(False)
        t.validate(True)
        with self.assertRaises(ConstraintException):
            t.validate(3.14)
        with self.assertRaises(ConstraintException):
            t.validate("yes")


class StringTest(unittest.TestCase):
    def test_validator_assignment(self):
        t = schema.StringType()
        t.min_length = 1
        t.max_length = 2
        t.pattern = re.compile("^[a-z]$")
        self.assertEqual(1, t.min_length)
        self.assertEqual(2, t.max_length)
        self.assertEqual("^[a-z]$", t.pattern.pattern)

    def test_validation(self):
        t = schema.StringType()
        t.unserialize("")
        t.unserialize("Hello world!")

    def test_validation_min_length(self):
        t = schema.StringType(
            min=1,
        )
        with self.assertRaises(schema.ConstraintException):
            t.unserialize("")
        t.unserialize("A")

    def test_validation_max_length(self):
        t = schema.StringType(
            max=1,
        )
        with self.assertRaises(schema.ConstraintException):
            t.unserialize("ab")
        t.unserialize("a")

    def test_validation_pattern(self):
        t = schema.StringType(pattern=re.compile("^[a-zA-Z]$"))
        with self.assertRaises(schema.ConstraintException):
            t.unserialize("ab1")
        t.unserialize("a")

    def test_unserialize(self):
        @dataclasses.dataclass
        class InvalidType:
            pass

        t = schema.StringType()
        self.assertEqual("asdf", t.unserialize("asdf"))
        with self.assertRaises(schema.ConstraintException):
            t.unserialize(InvalidType())


class IntTest(unittest.TestCase):
    def test_assignment(self):
        t = schema.IntType()
        t.min = 1
        t.max = 2
        self.assertEqual(1, t.min)
        self.assertEqual(2, t.max)

    def unserialize(self):
        t = schema.IntType()
        self.assertEqual(0, t.unserialize(0))
        self.assertEqual(-1, t.unserialize(-1))
        self.assertEqual(1, t.unserialize(1))
        with self.assertRaises(schema.ConstraintException):
            t.unserialize("1")

    def test_validation_min(self):
        t = schema.IntType(min=1)
        t.unserialize(2)
        t.unserialize(1)
        with self.assertRaises(schema.ConstraintException):
            t.unserialize(0)

    def test_validation_max(self):
        t = schema.IntType(max=1)
        t.unserialize(0)
        t.unserialize(1)
        with self.assertRaises(schema.ConstraintException):
            t.unserialize(2)

    def test_unserialize(self):
        t = schema.IntType()
        self.assertEqual(1, t.unserialize(1))
        with self.assertRaises(schema.ConstraintException):
            t.unserialize("asdf")


class ListTest(unittest.TestCase):
    def test_assignement(self):
        t = schema.ListType(schema.StringType())
        t.min = 1
        t.max = 2
        self.assertEqual(1, t.min)
        self.assertEqual(2, t.max)

    def test_validation(self):
        @dataclasses.dataclass
        class BadData:
            pass

        t = schema.ListType(schema.StringType())

        t.unserialize(["foo"])

        with self.assertRaises(schema.ConstraintException):
            t.unserialize([BadData()])

        with self.assertRaises(schema.ConstraintException):
            t.unserialize("5")
        with self.assertRaises(schema.ConstraintException):
            t.unserialize(BadData())

    def test_validation_elements(self):
        t = schema.ListType(schema.StringType(min=5))
        with self.assertRaises(schema.ConstraintException):
            t.unserialize(["foo"])

    def test_validation_min(self):
        t = schema.ListType(
            schema.StringType(),
            min=3,
        )
        with self.assertRaises(schema.ConstraintException):
            t.unserialize(["foo"])

    def test_validation_max(self):
        t = schema.ListType(
            schema.StringType(),
            max=0,
        )
        with self.assertRaises(schema.ConstraintException):
            t.unserialize(["foo"])


class MapTest(unittest.TestCase):
    def test_assignment(self):
        t = schema.MapType(schema.StringType(), schema.StringType())
        t.min = 1
        t.max = 2
        self.assertEqual(1, t.min)
        self.assertEqual(2, t.max)

    def test_type_validation(self):
        @dataclasses.dataclass(frozen=True)
        class InvalidData:
            a: str

        t = schema.MapType(schema.StringType(), schema.StringType())
        t.unserialize({})
        t.validate({})
        t.unserialize({"foo": "bar"})
        t.validate({"foo": "bar"})
        with self.assertRaises(schema.ConstraintException):
            t.unserialize({"foo": "bar", "baz": InvalidData("bar")})
        with self.assertRaises(schema.ConstraintException):
            t.validate({"foo": "bar", "baz": InvalidData("bar")})
        with self.assertRaises(schema.ConstraintException):
            t.unserialize({"foo": "bar", InvalidData("baz"): "baz"})
        with self.assertRaises(schema.ConstraintException):
            t.validate({"foo": "bar", InvalidData("baz"): "baz"})

    def test_validation_min(self):
        t = schema.MapType(
            schema.StringType(),
            schema.StringType(),
            min=1,
        )
        t.unserialize({"foo": "bar"})
        t.validate({"foo": "bar"})
        with self.assertRaises(schema.ConstraintException):
            t.unserialize({})
        with self.assertRaises(schema.ConstraintException):
            t.validate({})

    def test_validation_max(self):
        t = schema.MapType(
            schema.StringType(),
            schema.StringType(),
            max=1,
        )
        t.unserialize({"foo": "bar"})
        t.validate({"foo": "bar"})
        with self.assertRaises(schema.ConstraintException):
            t.unserialize({"foo": "bar", "baz": "Hello world!"})
        with self.assertRaises(schema.ConstraintException):
            t.validate({"foo": "bar", "baz": "Hello world!"})


class AnyTest(unittest.TestCase):
    def test_unserialize(self):
        t = schema.AnyType()
        self.assertEqual(1, t.unserialize(1))
        self.assertEqual(1.0, t.unserialize(1.0))
        self.assertEqual(True, t.unserialize(True))
        self.assertEqual("True", t.unserialize("True"))
        self.assertEqual(None, t.unserialize(None))
        self.assertEqual({"a": "b"}, t.unserialize({"a": "b"}))
        self.assertEqual([1, 1.0, None, "a"], t.unserialize([1, 1.0, None, "a"]))
        self.assertEqual([{0: ["a"]}], t.unserialize([{0: ["a"]}]))

        class IsAClass:
            pass

        with self.assertRaises(ConstraintException):
            t.unserialize(set())
        with self.assertRaises(ConstraintException):
            t.unserialize(IsAClass())

    def test_validate(self):
        t = schema.AnyType()
        t.validate("Hello world!")
        t.validate(1)
        t.validate(1.0)
        t.validate(True)
        t.validate({"message": "Hello world!"})
        t.validate(["Hello world!"])
        t.validate(None)


@dataclass
class TestClass:
    a: str
    b: int
    c: float
    d: bool


class ObjectTest(unittest.TestCase):
    t: schema.ObjectType[TestClass] = schema.ObjectType(
        TestClass,
        {
            "a": schema.PropertyType(
                schema.StringType(),
                required=True,
            ),
            "b": schema.PropertyType(
                schema.IntType(),
                required=True,
            ),
            "c": schema.PropertyType(schema.FloatType(), required=True),
            "d": schema.PropertyType(schema.BoolType(), required=True),
        },
    )

    def test_serialize(self):
        o = TestClass("foo", 5, 3.14, True)
        d = self.t.serialize(o)
        self.assertEqual({"a": "foo", "b": 5, "c": 3.14, "d": True}, d)

        o.b = None
        with self.assertRaises(schema.ConstraintException):
            self.t.serialize(o)

    def test_validate(self):
        o = TestClass("a", 5, 3.14, True)
        self.t.validate(o)
        o.b = None
        with self.assertRaises(schema.ConstraintException):
            self.t.validate(o)

    def test_unserialize(self):
        o = self.t.unserialize({"a": "foo", "b": 5, "c": 3.14, "d": True})
        self.assertEqual("foo", o.a)
        self.assertEqual(5, o.b)
        self.assertEqual(3.14, o.c)
        self.assertEqual(True, o.d)

        with self.assertRaises(schema.ConstraintException):
            self.t.unserialize(
                {
                    "a": "foo",
                }
            )

        with self.assertRaises(schema.ConstraintException):
            self.t.unserialize(
                {
                    "b": 5,
                }
            )

        with self.assertRaises(schema.ConstraintException):
            self.t.unserialize(
                {"a": "foo", "b": 5, "c": 3.14, "d": True, "e": complex(3.14)}
            )

    def test_field_override(self):
        @dataclasses.dataclass
        class TestData:
            a: str

        s: schema.ObjectType[TestData] = schema.ObjectType(
            TestData,
            {
                "test-data": schema.PropertyType(
                    schema.StringType(), required=True, field_override="a"
                )
            },
        )
        unserialized = s.unserialize({"test-data": "foo"})
        self.assertEqual(unserialized.a, "foo")
        serialized = s.serialize(unserialized)
        self.assertEqual("foo", serialized["test-data"])

    def test_init_mismatches(self):
        @dataclasses.dataclass
        class TestData2:
            a: str
            b: str

            def __init__(self, c: str, a: str):
                self.a = a
                self.b = c

        for name, cls in {"name-mismatch": TestData2}.items():
            with self.subTest(name):
                with self.assertRaises(BadArgumentException):
                    schema.ObjectType(
                        cls,
                        {
                            "a": schema.PropertyType(
                                schema.StringType(),
                            ),
                            "b": schema.PropertyType(
                                schema.StringType(),
                            ),
                        },
                    )

    def test_baseclass_field(self):
        @dataclasses.dataclass
        class TestParent:
            a: str

        @dataclasses.dataclass
        class TestSubclass(TestParent):
            pass

        # If a is missing from 'TestSubclass', it will fail.
        schema.ObjectType(
            TestSubclass,
            {
                "a": schema.PropertyType(
                    schema.StringType(),
                )
            },
        )


class OneOfTest(unittest.TestCase):
    def test_assignment(self):
        @dataclasses.dataclass
        class OneOfData1:
            a: str

        @dataclasses.dataclass
        class OneOfData2:
            b: int

        scope = schema.ScopeType(
            {
                "a": schema.ObjectType(
                    OneOfData1, {"a": PropertyType(schema.StringType())}
                ),
                "b": schema.ObjectType(
                    OneOfData2, {"b": PropertyType(schema.IntType())}
                ),
            },
            "a",
        )
        s_type = schema.OneOfStringType(
            {"a": schema.RefType("a", scope), "b": schema.RefType("b", scope)},
            scope,
            "_type",
        )
        s_type.discriminator_field_name = "foo"
        self.assertEqual("foo", s_type.discriminator_field_name)

        schema.OneOfIntType(
            {1: schema.RefType(1, scope), 2: schema.RefType(2, scope)},
            scope,
            "_type",
        )

    def test_unserialize(self):
        @dataclasses.dataclass
        class OneOfData1:
            a: str

        @dataclasses.dataclass
        class OneOfData2:
            b: int

        scope = schema.ScopeType(
            {
                "a": schema.ObjectType(
                    OneOfData1, {"a": PropertyType(schema.StringType())}
                ),
                "b": schema.ObjectType(
                    OneOfData2, {"b": PropertyType(schema.IntType())}
                ),
            },
            "a",
        )
        s_type = schema.OneOfStringType(
            {"a": schema.RefType("a", scope), "b": schema.RefType("b", scope)},
            scope,
            "_type",
        )

        # Incomplete values to unserialize
        with self.assertRaises(ConstraintException):
            s_type.unserialize({"a": "Hello world!"})
        with self.assertRaises(ConstraintException):
            s_type.unserialize({"b": 42})

        # Mismatching key value
        with self.assertRaises(ConstraintException):
            s_type.unserialize({"_type": "a", 1: "Hello world!"})
        # Invalid key value
        with self.assertRaises(ConstraintException):
            s_type.unserialize({"_type": 1, 1: "Hello world!"})

        unserialized_data: OneOfData1 = s_type.unserialize(
            {"_type": "a", "a": "Hello world!"}
        )
        self.assertIsInstance(unserialized_data, OneOfData1)
        self.assertEqual(unserialized_data.a, "Hello world!")

        unserialized_data2: OneOfData2 = s_type.unserialize({"_type": "b", "b": 42})
        self.assertIsInstance(unserialized_data2, OneOfData2)
        self.assertEqual(unserialized_data2.b, 42)

    def test_unserialize_embedded(self):
        @dataclasses.dataclass
        class OneOfData1:
            type: str
            a: str

        @dataclasses.dataclass
        class OneOfData2:
            b: int

        scope = schema.ScopeType(
            {
                "a": schema.ObjectType(
                    OneOfData1,
                    {
                        "type": PropertyType(
                            schema.StringType(),
                        ),
                        "a": PropertyType(schema.StringType()),
                    },
                ),
                "b": schema.ObjectType(
                    OneOfData2, {"b": PropertyType(schema.IntType())}
                ),
            },
            "a",
        )
        s = schema.OneOfStringType(
            {"a": schema.RefType("a", scope), "b": schema.RefType("b", scope)},
            scope,
            "type",
        )

        unserialized_data: OneOfData1 = s.unserialize(
            {"type": "a", "a": "Hello world!"}
        )
        self.assertIsInstance(unserialized_data, OneOfData1)
        self.assertEqual(unserialized_data.type, "a")
        self.assertEqual(unserialized_data.a, "Hello world!")

        unserialized_data2: OneOfData2 = s.unserialize({"type": "b", "b": 42})
        self.assertIsInstance(unserialized_data2, OneOfData2)
        self.assertEqual(unserialized_data2.b, 42)

    def test_validation(self):
        @dataclasses.dataclass
        class OneOfData1:
            type: str
            a: str

        @dataclasses.dataclass
        class OneOfData2:
            b: int

        scope = schema.ScopeType(
            {
                "a": schema.ObjectType(
                    OneOfData1,
                    {
                        "type": PropertyType(
                            schema.StringType(),
                        ),
                        "a": PropertyType(schema.StringType()),
                    },
                ),
                "b": schema.ObjectType(
                    OneOfData2, {"b": PropertyType(schema.IntType())}
                ),
            },
            "a",
        )
        s = schema.OneOfStringType[OneOfData1](
            {"a": schema.RefType("a", scope), "b": schema.RefType("b", scope)},
            scope,
            "type",
        )

        with self.assertRaises(ConstraintException):
            # noinspection PyTypeChecker
            s.validate(OneOfData1(None, "Hello world!"))

        with self.assertRaises(ConstraintException):
            s.validate(OneOfData1("b", "Hello world!"))

        s.validate(OneOfData1("a", "Hello world!"))

    def test_serialize(self):
        @dataclasses.dataclass
        class OneOfData1:
            type: str
            a: str

        @dataclasses.dataclass
        class OneOfData2:
            b: int

        scope = schema.ScopeType(
            {
                "a": schema.ObjectType(
                    OneOfData1,
                    {
                        "type": PropertyType(
                            schema.StringType(),
                        ),
                        "a": PropertyType(schema.StringType()),
                    },
                ),
                "b": schema.ObjectType(
                    OneOfData2, {"b": PropertyType(schema.IntType())}
                ),
            },
            "a",
        )
        s = schema.OneOfStringType(
            {"a": schema.RefType("a", scope), "b": schema.RefType("b", scope)},
            scope,
            "type",
        )

        self.assertEqual(
            s.serialize(OneOfData1("a", "Hello world!")),
            {"type": "a", "a": "Hello world!"},
        )

        self.assertEqual(s.serialize(OneOfData2(42)), {"type": "b", "b": 42})

    def test_object(self):
        @dataclasses.dataclass
        class OneOfData1:
            type: str
            a: str

        @dataclasses.dataclass
        class OneOfData2:
            b: int

        scope = schema.ScopeType({}, "")
        s = schema.OneOfStringType(
            {
                "a": schema.ObjectType(
                    OneOfData1,
                    {
                        "type": PropertyType(
                            schema.StringType(),
                        ),
                        "a": PropertyType(schema.StringType()),
                    },
                ),
                "b": schema.ObjectType(
                    OneOfData2, {"b": PropertyType(schema.IntType())}
                ),
            },
            scope,
            "type",
        )

        unserialized_data = s.unserialize({"type": "b", "b": 42})
        self.assertIsInstance(unserialized_data, OneOfData2)


class SerializationTest(unittest.TestCase):
    def test_serialization_cycle(self):
        @dataclasses.dataclass
        class TestData1:
            A: str
            B: int
            C: typing.Dict[str, int]
            D: typing.List[str]
            H: float
            E: typing.Optional[str] = None
            F: typing.Annotated[typing.Optional[str], schema.min(3)] = None
            G: typing.Optional[str] = dataclasses.field(
                default="", metadata={"id": "test-field", "name": "G"}
            )
            I: typing.Any = None

        schema.test_object_serialization(
            TestData1(A="Hello world!", B=5, C={}, D=[], H=3.14),
            self.fail,
        )

        @dataclasses.dataclass
        class KillPodConfig:
            namespace_pattern: re.Pattern

            name_pattern: typing.Annotated[
                typing.Optional[re.Pattern], schema.required_if_not("label_selector")
            ] = None

            kill: typing.Annotated[int, schema.min(1)] = dataclasses.field(
                default=1,
                metadata={
                    "name": "Number of pods to kill",
                    "description": "How many pods should we attempt to kill?",
                },
            )

            label_selector: typing.Annotated[
                typing.Optional[str],
                schema.min(1),
                schema.required_if_not("name_pattern"),
            ] = None

            kubeconfig_path: typing.Optional[str] = None

        schema.test_object_serialization(
            KillPodConfig(
                namespace_pattern=re.compile(".*"), name_pattern=re.compile(".*")
            ),
            self.fail,
        )

    def test_required_if(self):
        @dataclasses.dataclass
        class TestData1:
            A: typing.Annotated[typing.Optional[str], schema.required_if("B")] = None
            B: typing.Optional[int] = None

        s = schema.build_object_schema(TestData1)

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

        s = schema.build_object_schema(TestData1)

        unserialized = s.unserialize({})
        self.assertIsNone(unserialized.A)

    def test_float_optional(self):
        @dataclasses.dataclass
        class TestData1:
            A: typing.Optional[float] = None

        s = schema.build_object_schema(TestData1)

        unserialized = s.unserialize({})
        self.assertIsNone(unserialized.A)

    def test_build_object_schema_wrapping(self):
        @dataclasses.dataclass
        class TestData1:
            A: int

        s = schema.build_object_schema(TestData1)

        with self.assertRaises(ConstraintException) as ctx:
            s.unserialize({})

        self.assertIn("TestData1", ctx.exception.__str__())

    def test_default_value(self):
        class TestEnum(enum.Enum):
            A = "a"

        @dataclasses.dataclass
        class A:
            a: TestEnum = TestEnum.A

        s = schema.build_object_schema(A)

        data = s.unserialize({"a": "a"})
        s.validate(data)
        serialized_data = s.serialize(data)

        self.assertEqual(serialized_data, {"a": "a"})


class SchemaBuilderTest(unittest.TestCase):
    def test_any(self):
        scope = schema.ScopeType(
            {},
            "a",
        )
        resolved_type = schema._SchemaBuilder.resolve(typing.Any, scope)
        self.assertIsInstance(resolved_type, schema.AnyType)

    def test_non_dataclass(self):
        scope = schema.ScopeType(
            {},
            "a",
        )
        with self.assertRaises(SchemaBuildException) as ctx:
            schema._SchemaBuilder.resolve(complex, scope)
        self.assertIn("complex numbers are not supported", ctx.exception.msg)

    def test_regexp(self):
        scope = schema.ScopeType(
            {},
            "a",
        )
        resolved_type = schema._SchemaBuilder.resolve(Pattern, scope)
        self.assertIsInstance(resolved_type, schema.PatternType)

    def test_string(self):
        scope = schema.ScopeType(
            {},
            "a",
        )
        test: str = "foo"
        resolved_type = schema._SchemaBuilder.resolve(type(test), scope)
        self.assertIsInstance(resolved_type, schema.StringType)
        resolved_type = schema._SchemaBuilder.resolve(test, scope)
        self.assertIsInstance(resolved_type, schema.StringType)

    def test_int(self):
        scope = schema.ScopeType(
            {},
            "a",
        )
        test: int = 5
        resolved_type = schema._SchemaBuilder.resolve(type(test), scope)
        self.assertIsInstance(resolved_type, schema.IntType)
        resolved_type = schema._SchemaBuilder.resolve(test, scope)
        self.assertIsInstance(resolved_type, schema.IntType)

    def test_float(self):
        scope = schema.ScopeType(
            {},
            "a",
        )
        test: float = 3.14
        resolved_type = schema._SchemaBuilder.resolve(type(test), scope)
        self.assertIsInstance(resolved_type, schema.FloatType)
        resolved_type = schema._SchemaBuilder.resolve(test, scope)
        self.assertIsInstance(resolved_type, schema.FloatType)

    def test_string_enum(self):
        scope = schema.ScopeType(
            {},
            "a",
        )

        class TestEnum(enum.Enum):
            A = "a"
            B = "b"

        resolved_type = schema._SchemaBuilder.resolve(TestEnum, scope)
        self.assertIsInstance(resolved_type, schema.StringEnumType)

    def test_int_enum(self):
        scope = schema.ScopeType(
            {},
            "a",
        )

        class TestEnum(enum.Enum):
            A = 1
            B = 2

        resolved_type = schema._SchemaBuilder.resolve(TestEnum, scope)
        self.assertIsInstance(resolved_type, schema.IntEnumType)

    def test_list(self):
        scope = schema.ScopeType(
            {},
            "a",
        )

        resolved_type = schema._SchemaBuilder.resolve(typing.List[str], scope)
        self.assertIsInstance(resolved_type, schema.ListType)
        self.assertIsInstance(resolved_type.items, schema.StringType)

        test: list = []
        with self.assertRaises(SchemaBuildException):
            schema._SchemaBuilder.resolve(type(test), scope)

    def test_map(self):
        scope = schema.ScopeType(
            {},
            "a",
        )
        resolved_type = schema._SchemaBuilder.resolve(typing.Dict[str, str], scope)
        self.assertIsInstance(resolved_type, schema.MapType)
        self.assertIsInstance(resolved_type.keys, schema.StringType)
        self.assertIsInstance(resolved_type.values, schema.StringType)

        test: dict = {}
        with self.assertRaises(SchemaBuildException):
            schema._SchemaBuilder.resolve(type(test), scope)

        resolved_type = schema._SchemaBuilder.resolve(dict[str, str], scope)
        self.assertIsInstance(resolved_type, schema.MapType)
        self.assertIsInstance(resolved_type.keys, schema.StringType)
        self.assertIsInstance(resolved_type.values, schema.StringType)

    def test_class(self):
        scope = schema.ScopeType(
            {},
            "TestData",
        )

        class TestData:
            a: str
            b: int
            c: float
            d: bool

        with self.assertRaises(SchemaBuildException):
            schema._SchemaBuilder.resolve(TestData, scope)

        @dataclasses.dataclass
        class TestData:
            a: str
            b: int
            c: float
            d: bool

        scope = schema.ScopeType(
            {},
            "TestData",
        )
        resolved_type = schema._SchemaBuilder.resolve(TestData, scope)
        self.assertIsInstance(resolved_type, schema.RefType)
        self.assertEqual(1, len(scope.objects))
        object_schema = scope.objects["TestData"]
        self.assertIsInstance(object_schema, schema.ObjectType)
        self.assertIsNone(object_schema.properties["a"].display.name)
        self.assertTrue(object_schema.properties["a"].required)
        self.assertIsInstance(object_schema.properties["a"].type, schema.StringType)
        self.assertIsNone(object_schema.properties["b"].display.name)
        self.assertTrue(object_schema.properties["b"].required)
        self.assertIsInstance(object_schema.properties["b"].type, schema.IntType)
        self.assertIsNone(object_schema.properties["c"].display.name)
        self.assertTrue(object_schema.properties["c"].required)
        self.assertIsInstance(object_schema.properties["c"].type, schema.FloatType)
        self.assertIsNone(object_schema.properties["d"].display.name)
        self.assertTrue(object_schema.properties["d"].required)
        self.assertIsInstance(object_schema.properties["d"].type, schema.BoolType)

        @dataclasses.dataclass
        class TestData:
            a: str = "foo"
            b: int = 5
            c: str = dataclasses.field(
                default="bar", metadata={"name": "C", "description": "A string"}
            )
            d: bool = True

        scope = schema.ScopeType(
            {},
            "TestData",
        )
        resolved_type = schema._SchemaBuilder.resolve(TestData, scope)
        self.assertIsInstance(resolved_type, schema.RefType)
        self.assertEqual(1, len(scope.objects))
        object_schema = scope.objects["TestData"]
        self.assertIsInstance(object_schema, schema.ObjectType)
        self.assertIsNone(object_schema.properties["a"].display.name)
        self.assertFalse(object_schema.properties["a"].required)
        self.assertIsInstance(object_schema.properties["a"].type, schema.StringType)
        self.assertIsNone(object_schema.properties["b"].display.name)
        self.assertFalse(object_schema.properties["b"].required)
        self.assertIsInstance(object_schema.properties["b"].type, schema.IntType)
        self.assertEqual("C", object_schema.properties["c"].display.name)
        self.assertEqual("A string", object_schema.properties["c"].display.description)
        self.assertFalse(object_schema.properties["c"].required)
        self.assertIsInstance(object_schema.properties["c"].type, schema.StringType)
        self.assertIsNone(object_schema.properties["d"].display.name)
        self.assertFalse(object_schema.properties["d"].required)
        self.assertIsInstance(object_schema.properties["d"].type, schema.BoolType)

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

        scope = schema.build_object_schema(TestData)
        self.assertEqual("TestData", scope.root)
        self.assertIsInstance(scope.objects["TestData"], schema.ObjectType)
        self.assertIsInstance(scope.objects["A"], schema.ObjectType)
        self.assertIsInstance(scope.objects["B"], schema.ObjectType)

        self.assertIsInstance(
            scope.objects["TestData"].properties["a"].type, schema.OneOfStringType
        )
        one_of_type: schema.OneOfStringType = (
            scope.objects["TestData"].properties["a"].type
        )
        self.assertEqual(one_of_type.discriminator_field_name, "_type")
        self.assertIsInstance(one_of_type.types["A"], schema.RefType)
        self.assertEqual(one_of_type.types["A"].id, "A")
        self.assertIsInstance(one_of_type.types["B"], schema.RefType)
        self.assertEqual(one_of_type.types["B"].id, "B")

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
                    typing.Annotated[A, schema.discriminator_value(1)],
                    typing.Annotated[B, schema.discriminator_value(2)],
                ],
                schema.discriminator("discriminator"),
            ]

        scope = schema.build_object_schema(TestData)
        self.assertEqual("TestData", scope.root)
        self.assertIsInstance(scope.objects["TestData"], schema.ObjectType)
        self.assertIsInstance(scope.objects["A"], schema.ObjectType)
        self.assertIsInstance(scope.objects["B"], schema.ObjectType)

        self.assertIsInstance(
            scope.objects["TestData"].properties["a"].type, schema.OneOfIntType
        )
        one_of_type: schema.OneOfIntType = (
            scope.objects["TestData"].properties["a"].type
        )
        self.assertEqual(one_of_type.discriminator_field_name, "discriminator")
        self.assertIsInstance(one_of_type.types[1], schema.RefType)
        self.assertEqual(one_of_type.types[1].id, "A")
        self.assertIsInstance(one_of_type.types[2], schema.RefType)
        self.assertEqual(one_of_type.types[2].id, "B")

    def test_optional(self):
        @dataclasses.dataclass
        class TestData:
            a: typing.Optional[str] = None

        scope = schema.build_object_schema(TestData)
        self.assertEqual("TestData", scope.root)
        self.assertIsInstance(scope.objects["TestData"], schema.ObjectType)
        resolved_type = scope.objects["TestData"]

        self.assertFalse(resolved_type.properties["a"].required)
        self.assertIsInstance(resolved_type.properties["a"].type, schema.StringType)

    def test_annotated(self):
        scope = schema.ScopeType(
            {},
            "TestData",
        )
        resolved_type = schema._SchemaBuilder.resolve(
            typing.Annotated[str, schema.min(3)], scope
        )
        self.assertIsInstance(resolved_type, schema.StringType)
        self.assertEqual(3, resolved_type.min)

        @dataclasses.dataclass
        class TestData:
            a: typing.Annotated[typing.Optional[str], schema.min(3)] = None

        scope = schema.ScopeType(
            {},
            "TestData",
        )
        schema._SchemaBuilder.resolve(TestData, scope)
        resolved_type2 = scope.objects["TestData"]

        a = resolved_type2.properties["a"]
        self.assertIsInstance(a.type, schema.StringType)
        self.assertFalse(a.required)
        self.assertEqual(3, a.type.min)

        with self.assertRaises(SchemaBuildException):

            @dataclasses.dataclass
            class TestData:
                a: typing.Annotated[typing.Optional[str], "foo"] = None

            scope = schema.ScopeType(
                {},
                "TestData",
            )
            schema._SchemaBuilder.resolve(TestData, scope)

    def test_annotated_required_if(self):
        @dataclasses.dataclass
        class TestData2:
            a: typing.Annotated[typing.Optional[str], schema.required_if("b")] = None
            b: typing.Optional[str] = None

        scope = schema.ScopeType(
            {},
            "TestData2",
        )
        schema._SchemaBuilder.resolve(TestData2, scope)
        t = scope.objects["TestData2"]
        a = t.properties["a"]
        b = t.properties["b"]

        self.assertFalse(a.required)
        self.assertFalse(b.required)
        self.assertEqual(["b"], a.required_if)

    def test_different_id(self):
        @dataclasses.dataclass
        class TestData:
            a: str = dataclasses.field(metadata={"id": "test-field"})

        scope = schema.ScopeType(
            {},
            "TestData",
        )
        schema._SchemaBuilder.resolve(TestData, scope)
        t = scope.objects["TestData"]
        a = t.properties["test-field"]
        self.assertEqual(a.field_override, "a")

    def test_unclear_error_message(self):
        @dataclasses.dataclass
        class TestData:
            a: typing.Dict[str, str]

        scope = schema.ScopeType(
            {},
            "TestData",
        )
        schema._SchemaBuilder.resolve(TestData, scope)
        t = scope.objects["TestData"]
        with self.assertRaises(ConstraintException):
            # noinspection PyTypeChecker
            t.serialize(TestData(type(dict[str, str])))


class JSONSchemaTest(unittest.TestCase):
    def _execute_test_cases(self, test_cases):
        for name in test_cases.keys():
            defs = schema._JSONSchemaDefs()
            scope = schema.ScopeType(
                {},
                "a",
            )
            with self.subTest(name=name):
                input = test_cases[name][0]
                expected = test_cases[name][1]

                self.assertEqual(expected, input._to_jsonschema_fragment(scope, defs))

    def test_bool(self):
        defs = schema._JSONSchemaDefs()
        scope = schema.ScopeType(
            {},
            "a",
        )
        s = schema.BoolType()._to_jsonschema_fragment(scope, defs)
        self.assertEqual(s["anyOf"][0]["type"], "boolean")
        self.assertEqual(s["anyOf"][1]["type"], "string")
        self.assertEqual(s["anyOf"][2]["type"], "integer")

    def test_string(self):
        test_cases: typing.Dict[str, typing.Tuple[schema.StringType, typing.Dict]] = {
            "base": (schema.StringType(), {"type": "string"}),
            "min": (schema.StringType(min=5), {"type": "string", "minLength": 5}),
            "max": (schema.StringType(max=5), {"type": "string", "maxLength": 5}),
            "pattern": (
                schema.StringType(pattern=re.compile("^[a-z]+$")),
                {"type": "string", "pattern": "^[a-z]+$"},
            ),
        }

        self._execute_test_cases(test_cases)

    def test_int(self):
        test_cases: typing.Dict[str, typing.Tuple[schema.IntType, typing.Dict]] = {
            "base": (schema.IntType(), {"type": "integer"}),
            "min": (schema.IntType(min=5), {"type": "integer", "minimum": 5}),
            "max": (schema.IntType(max=5), {"type": "integer", "maximum": 5}),
        }

        self._execute_test_cases(test_cases)

    def test_float(self):
        test_cases: typing.Dict[str, typing.Tuple[schema.FloatType, typing.Dict]] = {
            "base": (schema.FloatType(), {"type": "number"}),
            "min": (schema.FloatType(min=5.0), {"type": "number", "minimum": 5.0}),
            "max": (schema.FloatType(max=5.0), {"type": "number", "maximum": 5.0}),
        }

        self._execute_test_cases(test_cases)

    def test_enum(self):
        class Color(enum.Enum):
            RED = "red"

        class Fibonacci(enum.Enum):
            FIRST = 1
            SECOND = 2

        test_cases: typing.Dict[str, typing.Tuple[schema._EnumType, typing.Dict]] = {
            "string": (
                schema.StringEnumType(Color),
                {"type": "string", "enum": ["red"]},
            ),
            "int": (schema.IntEnumType(Fibonacci), {"type": "integer", "enum": [1, 2]}),
        }

        self._execute_test_cases(test_cases)

    def test_list(self):
        test_cases: typing.Dict[str, typing.Tuple[schema.ListType, typing.Dict]] = {
            "base": (
                schema.ListType(schema.IntType()),
                {"type": "array", "items": {"type": "integer"}},
            ),
            "min": (
                schema.ListType(schema.IntType(), min=3),
                {"type": "array", "items": {"type": "integer"}, "minItems": 3},
            ),
            "max": (
                schema.ListType(schema.IntType(), max=3),
                {"type": "array", "items": {"type": "integer"}, "maxItems": 3},
            ),
        }
        self._execute_test_cases(test_cases)

    def test_map(self):
        test_cases: typing.Dict[str, typing.Tuple[schema.MapType, typing.Dict]] = {
            "base": (
                schema.MapType(schema.IntType(), schema.StringType()),
                {
                    "type": "object",
                    "propertyNames": {
                        "pattern": "^[0-9]+$",
                    },
                    "additionalProperties": {
                        "type": "string",
                    },
                },
            ),
            "min": (
                schema.MapType(schema.StringType(), schema.IntType(), min=3),
                {
                    "type": "object",
                    "propertyNames": {},
                    "additionalProperties": {
                        "type": "integer",
                    },
                    "minProperties": 3,
                },
            ),
            "max": (
                schema.MapType(schema.StringType(), schema.IntType(), max=3),
                {
                    "type": "object",
                    "propertyNames": {},
                    "additionalProperties": {
                        "type": "integer",
                    },
                    "maxProperties": 3,
                },
            ),
        }
        self._execute_test_cases(test_cases)

    def test_object(self):
        @dataclasses.dataclass
        class TestData:
            a: str

        scope = schema.ScopeType(
            {},
            "TestData",
        )
        scope.objects = {
            "TestData": schema.ObjectType(
                TestData,
                {
                    "a": schema.PropertyType(
                        schema.StringType(),
                        display=schema.DisplayValue("A", "A string"),
                    )
                },
            )
        }
        defs = schema._JSONSchemaDefs()
        expected = {
            "$defs": {
                "TestData": {
                    "type": "object",
                    "properties": {
                        "a": {"type": "string", "title": "A", "description": "A string"}
                    },
                    "required": ["a"],
                    "additionalProperties": False,
                    "dependentRequired": {},
                }
            },
            "type": "object",
            "properties": {
                "a": {"type": "string", "title": "A", "description": "A string"}
            },
            "required": ["a"],
            "additionalProperties": False,
            "dependentRequired": {},
        }
        result = scope._to_jsonschema_fragment(scope, defs)
        self.assertEqual(expected, result)

    def test_one_of(self):
        @dataclasses.dataclass
        class A:
            a: str

        @dataclasses.dataclass
        class B:
            b: str

        @dataclasses.dataclass
        class TestData:
            a: typing.Union[A, B]

        scope = schema.ScopeType(
            {},
            "TestData",
        )
        scope.objects = {
            "TestData": schema.ObjectType(
                TestData,
                {
                    "a": schema.PropertyType(
                        schema.OneOfStringType(
                            {
                                "a": schema.RefType("A", scope),
                                "b": schema.RefType("B", scope),
                            },
                            scope,
                            "_type",
                        )
                    )
                },
            ),
            "A": schema.ObjectType(A, {"a": schema.PropertyType(schema.StringType())}),
            "B": schema.ObjectType(B, {"b": schema.PropertyType(schema.StringType())}),
        }
        defs = schema._JSONSchemaDefs()
        json_schema = scope._to_jsonschema_fragment(scope, defs)
        self.assertEqual(
            {
                "$defs": {
                    "TestData": {
                        "type": "object",
                        "properties": {
                            "a": {
                                "oneOf": [
                                    {"$ref": "#/$defs/A_discriminated_string_a"},
                                    {"$ref": "#/$defs/B_discriminated_string_b"},
                                ]
                            }
                        },
                        "required": ["a"],
                        "additionalProperties": False,
                        "dependentRequired": {},
                    },
                    "A": {
                        "type": "object",
                        "properties": {
                            "a": {"type": "string"},
                            "_type": {"type": "string", "const": "a"},
                        },
                        "required": ["_type", "a"],
                        "additionalProperties": False,
                        "dependentRequired": {},
                    },
                    "A_discriminated_string_a": {
                        "type": "object",
                        "properties": {
                            "a": {"type": "string"},
                            "_type": {"type": "string", "const": "a"},
                        },
                        "required": ["_type", "a"],
                        "additionalProperties": False,
                        "dependentRequired": {},
                    },
                    "B": {
                        "type": "object",
                        "properties": {
                            "b": {"type": "string"},
                            "_type": {"type": "string", "const": "b"},
                        },
                        "required": ["_type", "b"],
                        "additionalProperties": False,
                        "dependentRequired": {},
                    },
                    "B_discriminated_string_b": {
                        "type": "object",
                        "properties": {
                            "b": {"type": "string"},
                            "_type": {"type": "string", "const": "b"},
                        },
                        "required": ["_type", "b"],
                        "additionalProperties": False,
                        "dependentRequired": {},
                    },
                },
                "type": "object",
                "properties": {
                    "a": {
                        "oneOf": [
                            {"$ref": "#/$defs/A_discriminated_string_a"},
                            {"$ref": "#/$defs/B_discriminated_string_b"},
                        ]
                    }
                },
                "required": ["a"],
                "additionalProperties": False,
                "dependentRequired": {},
            },
            json_schema,
        )


def load_tests(loader, tests, ignore):
    """
    This function adds the doctests to the discovery process.
    """
    tests.addTests(doctest.DocTestSuite(schema))
    return tests


if __name__ == "__main__":
    unittest.main()
