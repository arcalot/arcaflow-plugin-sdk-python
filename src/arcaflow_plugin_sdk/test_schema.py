import dataclasses
import re
from dataclasses import dataclass

from arcaflow_plugin_sdk import schema
import enum
import unittest

from arcaflow_plugin_sdk.plugin import SchemaBuildException
from arcaflow_plugin_sdk.schema import Field, ConstraintException, BadArgumentException, TypeID


class Color(enum.Enum):
    GREEN = "green"
    RED = "red"


class EnumTest(unittest.TestCase):
    def test_unserialize(self):
        t = schema.EnumType(Color)
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

            schema.EnumType(BadEnum)


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
            min_length=1,
        )
        with self.assertRaises(schema.ConstraintException):
            t.unserialize("")
        t.unserialize("A")

    def test_validation_max_length(self):
        t = schema.StringType(
            max_length=1,
        )
        with self.assertRaises(schema.ConstraintException):
            t.unserialize("ab")
        t.unserialize("a")

    def test_validation_pattern(self):
        t = schema.StringType(
            pattern=re.compile("^[a-zA-Z]$")
        )
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

        t = schema.ListType(
            schema.StringType()
        )

        t.unserialize(["foo"])

        with self.assertRaises(schema.ConstraintException):
            t.unserialize([BadData()])

        with self.assertRaises(schema.ConstraintException):
            t.unserialize("5")
        with self.assertRaises(schema.ConstraintException):
            t.unserialize(BadData())

    def test_validation_elements(self):
        t = schema.ListType(
            schema.StringType(
                min_length=5
            )
        )
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
        t = schema.MapType(
            schema.StringType(),
            schema.StringType()
        )
        t.min = 1
        t.max = 2
        self.assertEqual(1, t.min)
        self.assertEqual(2, t.max)

    def test_type_validation(self):
        @dataclasses.dataclass(frozen=True)
        class InvalidData:
            a: str

        t = schema.MapType(
            schema.StringType(),
            schema.StringType()
        )
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
            "a": schema.Field(
                schema.StringType(),
                required=True,
            ),
            "b": schema.Field(
                schema.IntType(),
                required=True,
            ),
            "c": schema.Field(
                schema.FloatType(),
                required=True
            ),
            "d": schema.Field(
                schema.BoolType(),
                required=True
            )
        }
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
            self.t.unserialize({
                "a": "foo",
            })

        with self.assertRaises(schema.ConstraintException):
            self.t.unserialize({
                "b": 5,
            })

        with self.assertRaises(schema.ConstraintException):
            self.t.unserialize({
                "a": "foo",
                "b": 5,
                "c": 3.14,
                "d": True,
                "e": complex(3.14)
            })

    def test_field_override(self):
        @dataclasses.dataclass
        class TestData:
            a: str

        s: schema.ObjectType[TestData] = schema.ObjectType(
            TestData,
            {
                "test-data": schema.Field(
                    schema.StringType(),
                    required=True,
                    field_override="a"
                )
            }
        )
        unserialized = s.unserialize({"test-data": "foo"})
        self.assertEqual(unserialized.a, "foo")
        serialized = s.serialize(unserialized)
        self.assertEqual("foo", serialized["test-data"])

    def test_init_mismatches(self):
        @dataclasses.dataclass
        class TestData1:
            a: str
            b: str

            def __init__(self, b: str, a: str):
                self.a = a
                self.b = b

        @dataclasses.dataclass
        class TestData2:
            a: str
            b: str

            def __init__(self, c: str, a: str):
                self.a = a
                self.b = c

        @dataclasses.dataclass
        class TestData3:
            a: str
            b: str

            def __init__(self, a: str, b: int):
                self.a = a
                self.b = str(b)

        for name, cls in {"order-mismatch": TestData1, "name-mismatch": TestData2, "type-mismatch": TestData3}.items():
            with self.subTest(name):
                with self.assertRaises(BadArgumentException):
                    schema.ObjectType(
                        cls,
                        {
                            "a": schema.Field(
                                schema.StringType(),
                            ),
                            "b": schema.Field(
                                schema.StringType(),
                            )
                        }
                    )

    def test_baseclass_field(self):
        @dataclasses.dataclass
        class TestParent:
            a: str

        @dataclasses.dataclass
        class TestSubclass(TestParent):
            pass

        # If a is missing from 'TestSubclass', it will fail.
        s = schema.ObjectType(
            TestSubclass,
            {
                "a": schema.Field(
                    schema.StringType(),
                )
            }
        )


class OneOfTest(unittest.TestCase):
    def test_assignment(self):
        @dataclasses.dataclass
        class OneOfData1:
            a: str

        @dataclasses.dataclass
        class OneOfData2:
            b: int

        s = schema.OneOfType(
            "_type",
            schema.StringType(),
            {
                "a": schema.ObjectType(
                    OneOfData1,
                    {
                        "a": Field(
                            schema.StringType()
                        )
                    }
                ),
                "b": schema.ObjectType(
                    OneOfData2,
                    {
                        "b": Field(
                            schema.IntType()
                        )
                    }
                )
            }
        )
        s.discriminator_field_name = "foo"
        self.assertEqual("foo", s.discriminator_field_name)

    def test_unserialize(self):
        @dataclasses.dataclass
        class OneOfData1:
            a: str

        @dataclasses.dataclass
        class OneOfData2:
            b: int

        s = schema.OneOfType(
            "_type",
            schema.StringType(),
            {
                "a": schema.ObjectType(
                    OneOfData1,
                    {
                        "a": Field(
                            schema.StringType()
                        )
                    }
                ),
                "b": schema.ObjectType(
                    OneOfData2,
                    {
                        "b": Field(
                            schema.IntType()
                        )
                    }
                )
            }
        )

        with self.assertRaises(ConstraintException):
            s.unserialize({"a": "Hello world!"})
        with self.assertRaises(ConstraintException):
            s.unserialize({"b": 42})

        unserialized_data: OneOfData1 = s.unserialize({"_type": "a", "a": "Hello world!"})
        self.assertIsInstance(unserialized_data, OneOfData1)
        self.assertEqual(unserialized_data.a, "Hello world!")

        unserialized_data2: OneOfData2 = s.unserialize({"_type": "b", "b": 42})
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

        s = schema.OneOfType(
            "type",
            schema.StringType(),
            {
                "a": schema.ObjectType(
                    OneOfData1,
                    {
                        "type": Field(
                            schema.StringType(),
                        ),
                        "a": Field(
                            schema.StringType()
                        )
                    }
                ),
                "b": schema.ObjectType(
                    OneOfData2,
                    {
                        "b": Field(
                            schema.IntType()
                        )
                    }
                )
            }
        )

        unserialized_data: OneOfData1 = s.unserialize({"type": "a", "a": "Hello world!"})
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

        s = schema.OneOfType(
            "type",
            schema.StringType(),
            {
                "a": schema.ObjectType(
                    OneOfData1,
                    {
                        "type": Field(
                            schema.StringType(),
                        ),
                        "a": Field(
                            schema.StringType()
                        )
                    }
                ),
                "b": schema.ObjectType(
                    OneOfData2,
                    {
                        "b": Field(
                            schema.IntType()
                        )
                    }
                )
            }
        )

        with self.assertRaises(ConstraintException):
            s.validate(OneOfData1(
                None,
                "Hello world!"
            ))

        with self.assertRaises(ConstraintException):
            s.validate(OneOfData1(
                "b",
                "Hello world!"
            ))

        s.validate(OneOfData1(
            "a",
            "Hello world!"
        ))

    def test_serialize(self):
        @dataclasses.dataclass
        class OneOfData1:
            type: str
            a: str

        @dataclasses.dataclass
        class OneOfData2:
            b: int

        s = schema.OneOfType(
            "type",
            schema.StringType(),
            {
                "a": schema.ObjectType(
                    OneOfData1,
                    {
                        "type": Field(
                            schema.StringType(),
                        ),
                        "a": Field(
                            schema.StringType()
                        )
                    }
                ),
                "b": schema.ObjectType(
                    OneOfData2,
                    {
                        "b": Field(
                            schema.IntType()
                        )
                    }
                )
            }
        )

        self.assertEqual(s.serialize(OneOfData1(
            "a",
            "Hello world!"
        )), {"type": "a", "a": "Hello world!"})

        self.assertEqual(s.serialize(OneOfData2(
            42
        )), {"type": "b", "b": 42})


if __name__ == '__main__':
    unittest.main()
