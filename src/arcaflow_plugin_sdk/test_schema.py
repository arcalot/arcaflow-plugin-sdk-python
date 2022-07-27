import dataclasses
import re
from dataclasses import dataclass

from arcaflow_plugin_sdk import schema
import enum
import unittest


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


class StringTest(unittest.TestCase):
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
        t.validate({"foo":"bar"})
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
            )
        }
    )

    def test_serialize(self):
        o = TestClass("foo", 5)
        d = self.t.serialize(o)
        self.assertEqual({"a": "foo", "b": 5}, d)

        o.b = None
        with self.assertRaises(schema.ConstraintException):
            self.t.serialize(o)

    def test_validate(self):
        o = TestClass("a", 5)
        self.t.validate(o)
        o.b = None
        with self.assertRaises(schema.ConstraintException):
            self.t.validate(o)

    def test_unserialize(self):
        o = self.t.unserialize({"a": "foo", "b": 5})
        self.assertEqual("foo", o.a)
        self.assertEqual(5, o.b)

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
        unserialized = s.unserialize({"test-data":"foo"})
        self.assertEqual(unserialized.a, "foo")
        serialized = s.serialize(unserialized)
        self.assertEqual("foo", serialized["test-data"])


if __name__ == '__main__':
    unittest.main()
