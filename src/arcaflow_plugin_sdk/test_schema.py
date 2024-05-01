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

# default discriminator field name used by the OneOfType
# when no discriminator field name is declared
default_discriminator = "_type"

# The string "type_" is the discriminator identifier that will
# be embedded in StrInline. It must match the OneOfType's
# discriminator field name.
discriminator_field_name = "type_"


@dataclasses.dataclass
class Basic:
    msg: str


@dataclasses.dataclass
class Basic2:
    msg2: str


@dataclasses.dataclass
class Basic3:
    b: int


@dataclasses.dataclass
class InlineStr:
    type_: str
    a: str


@dataclasses.dataclass
class InlineStr2:
    type_: str
    code: int


@dataclasses.dataclass
class InlineInt:
    type_: int
    msg: str
    code: int


@dataclasses.dataclass
class InlineInt2:
    type_: int
    msg2: str


@dataclasses.dataclass
class DoubleInlineStr:
    _type: str
    type_: str
    msg: str


@dataclasses.dataclass
class BasicUnion:
    union_basic: typing.Union[Basic, Basic2]


@dataclasses.dataclass
class InlineUnion:
    union: typing.Union[InlineStr, InlineStr2, DoubleInlineStr]


@dataclasses.dataclass
class IntInlineUnion:
    union: typing.Union[InlineInt, InlineInt2]


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
        t.validate({"foo": "bar"})
        with self.assertRaises(schema.ConstraintException):
            t.unserialize([1, 1.0, None, "a"])
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
    def setUp(self):
        self.obj_basic = schema.ObjectType(
            Basic,
            {"msg": PropertyType(schema.StringType())},
        )
        self.obj_basic_b = schema.ObjectType(
            Basic3, {"b": PropertyType(schema.IntType())}
        )
        self.obj_inline_str = schema.ObjectType(
            InlineStr,
            {
                discriminator_field_name: PropertyType(
                    schema.StringType(),
                ),
                "a": PropertyType(schema.StringType()),
            },
        )
        self.obj_inline_str2 = schema.ObjectType(
            InlineStr2,
            {
                discriminator_field_name: PropertyType(
                    schema.StringType(),
                ),
                "code": PropertyType(schema.IntType()),
            },
        )
        self.scope_basic = schema.ScopeType(
            {
                "a": self.obj_basic,
                "b": self.obj_basic_b,
            },
            "a",
        )
        self.obj_double_inline_str = schema.ObjectType(
            DoubleInlineStr,
            {
                discriminator_field_name: PropertyType(
                    schema.StringType(),
                ),
                default_discriminator: PropertyType(
                    schema.StringType(),
                ),
                "msg": PropertyType(schema.StringType()),
            },
        )
        self.scope_mixed_type = schema.ScopeType(
            {
                "a": self.obj_inline_str,
                "a2": self.obj_inline_str2,
                "basic": self.obj_basic,
                "basic_b": self.obj_basic_b,
            },
            "a",
        )
        self.scope_mixed_type2 = schema.ScopeType(
            {
                "a": self.obj_inline_str,
                "aa": self.obj_double_inline_str,
                "basic": self.obj_basic,
                "basic_b": self.obj_basic_b,
            },
            "a",
        )
        self.scope_inlined = schema.ScopeType(
            {
                "a": self.obj_inline_str,
                "b": self.obj_inline_str2,
                "a2": self.obj_double_inline_str,
            },
            "a",
        )
        self.scope_inlined_int = schema.ScopeType(
            {
                "a": schema.ObjectType(
                    InlineInt,
                    {
                        discriminator_field_name: PropertyType(
                            schema.IntType(),
                        ),
                        "msg": PropertyType(schema.StringType()),
                        "code": PropertyType(schema.IntType()),
                    },
                ),
                "b": schema.ObjectType(
                    InlineInt2,
                    {
                        discriminator_field_name: PropertyType(
                            schema.IntType(),
                        ),
                        "msg2": PropertyType(schema.StringType()),
                    },
                ),
            },
            "a",
        )

    def test_inline_discriminator_missing(self):
        with self.assertRaises(BadArgumentException) as cm:
            schema.OneOfStringType(
                {
                    "a": schema.RefType("a", self.scope_mixed_type),
                    "a2": schema.RefType("a2", self.scope_mixed_type),
                    "basic": schema.RefType("basic", self.scope_mixed_type),
                },
                scope=self.scope_mixed_type,
                discriminator_inlined=True,
                discriminator_field_name=discriminator_field_name,
            ).validate({})
        self.assertIn("needs discriminator field", str(cm.exception))

    def test_has_discriminator_error(self):
        # error when a schema should not have the given discriminator
        # as a property
        with self.assertRaises(BadArgumentException) as cm:
            schema.OneOfStringType(
                {
                    "a": schema.RefType("a", self.scope_mixed_type),
                    "basic": schema.RefType("basic", self.scope_mixed_type),
                    "basic_b": schema.RefType(
                        "basic_b", self.scope_mixed_type
                    ),
                },
                scope=self.scope_mixed_type,
                discriminator_field_name=discriminator_field_name,
                discriminator_inlined=False,
            ).validate({})
        self.assertIn(
            f'has conflicting field "{discriminator_field_name}"',
            str(cm.exception),
        )

        # error when a schema should not have the default discriminator
        # as a property
        with self.assertRaises(BadArgumentException) as cm:
            schema.OneOfStringType(
                {
                    "aa": schema.RefType("aa", self.scope_mixed_type2),
                    "basic": schema.RefType("basic", self.scope_mixed_type2),
                    "basic_b": schema.RefType(
                        "basic_b", self.scope_mixed_type2
                    ),
                },
                scope=self.scope_mixed_type2,
                discriminator_inlined=False,
            ).validate({})
        self.assertIn(
            f'has conflicting field "{default_discriminator}"',
            str(cm.exception),
        )

    def test_inline_discriminator_type_mismatch(self):
        with self.assertRaises(BadArgumentException) as cm:
            # noinspection PyTypeChecker
            schema.OneOfIntType(
                {
                    "a": schema.RefType("a", self.scope_inlined),
                    "b": schema.RefType("b", self.scope_inlined),
                },
                scope=self.scope_inlined,
                discriminator_field_name=discriminator_field_name,
                discriminator_inlined=True,
            ).validate({})
        self.assertIn(
            "does not match the discriminator type for the union",
            str(cm.exception),
        )

    def test_unserialize(self):
        s_type = schema.OneOfStringType(
            {
                "a": schema.RefType("a", self.scope_basic),
                "b": schema.RefType("b", self.scope_basic),
            },
            scope=self.scope_basic,
        )

        # Incomplete values to unserialize
        with self.assertRaises(ConstraintException) as cm:
            s_type.unserialize({"a": "Hello world!"})
        self.assertIn(
            f"'{s_type.discriminator_field_name}': Required discriminator"
            " field not found",
            str(cm.exception),
        )
        with self.assertRaises(ConstraintException) as cm:
            s_type.unserialize({"b": 42})
        self.assertIn(
            f"'{s_type.discriminator_field_name}': Required discriminator"
            " field not found",
            str(cm.exception),
        )

        # Key not in this OneOf union
        absent_key = "k"
        with self.assertRaises(ConstraintException) as cm:
            s_type.unserialize(
                {default_discriminator: absent_key, 1: "Hello world!"}
            )
        self.assertIn(
            f"Invalid value for field: '{absent_key}'", str(cm.exception)
        )

        # Invalid type for 'data' argument
        with self.assertRaises(ConstraintException) as cm:
            s_type.unserialize([])
        self.assertIn("Must be a dict", str(cm.exception))

        # Invalid parameter for the selected member type
        invalid_member_param = "b"
        with self.assertRaises(ConstraintException) as cm:
            s_type.unserialize(
                {
                    default_discriminator: "a",
                    invalid_member_param: "Hello world!",
                }
            )
        self.assertIn(
            f"Invalid parameter '{invalid_member_param}'", str(cm.exception)
        )

        # Invalid type for discriminator value
        discriminator_wrong_type = 1
        with self.assertRaises(ConstraintException) as cm:
            s_type.unserialize(
                {
                    default_discriminator: discriminator_wrong_type,
                    "a": "Hello world!",
                }
            )
        self.assertIn(
            f"{type(s_type.discriminator_field_name).__name__} required, "
            f"{type(discriminator_wrong_type).__name__} found",
            str(cm.exception),
        )

        unserialized_data: Basic = s_type.unserialize(
            {default_discriminator: "a", "msg": "Hello world!"}
        )
        self.assertIsInstance(unserialized_data, Basic)
        self.assertEqual(unserialized_data.msg, "Hello world!")
        unserialized_data2: Basic3 = s_type.unserialize(
            {default_discriminator: "b", "b": 42}
        )
        self.assertIsInstance(unserialized_data2, Basic3)
        self.assertEqual(unserialized_data2.b, 42)

        s_type_int = schema.OneOfIntType(
            {
                1: schema.RefType("a", self.scope_basic),
                2: schema.RefType("b", self.scope_basic),
            },
            scope=self.scope_basic,
        )
        unserialized_data3: Basic = s_type_int.unserialize(
            {default_discriminator: 1, "msg": "Hello world!"}
        )
        self.assertIsInstance(unserialized_data3, Basic)
        self.assertEqual(unserialized_data3.msg, "Hello world!")

    def test_unserialize_embedded(self):
        s = schema.OneOfStringType(
            {
                "a": schema.RefType("a", self.scope_inlined),
                "b": schema.RefType("b", self.scope_inlined),
                "a2": schema.RefType("a2", self.scope_inlined),
            },
            scope=self.scope_inlined,
            discriminator_field_name=discriminator_field_name,
            discriminator_inlined=True,
        )

        unserialized_data: InlineStr = s.unserialize(
            {discriminator_field_name: "a", "a": "Hello world!"}
        )
        self.assertIsInstance(unserialized_data, InlineStr)
        self.assertEqual(
            getattr(unserialized_data, discriminator_field_name), "a"
        )
        self.assertEqual(unserialized_data.a, "Hello world!")

        unserialized_data2: DoubleInlineStr = s.unserialize(
            {
                discriminator_field_name: "a2",
                default_discriminator: "a",
                "msg": "Hi again",
            }
        )
        self.assertIsInstance(unserialized_data2, DoubleInlineStr)
        self.assertEqual(unserialized_data2.msg, "Hi again")

        s = schema.OneOfIntType(
            {
                1: schema.RefType("a", self.scope_inlined_int),
                2: schema.RefType("b", self.scope_inlined_int),
            },
            scope=self.scope_inlined_int,
            discriminator_field_name=discriminator_field_name,
            discriminator_inlined=True,
        )
        unserialized_data: InlineInt = s.unserialize(
            {discriminator_field_name: 1, "msg": "Hi again", "code": 101}
        )
        self.assertIsInstance(unserialized_data, InlineInt)

    def test_validation(self):
        s = schema.OneOfStringType[Basic](
            {
                "a": schema.RefType("a", self.scope_basic),
                "b": schema.RefType("b", self.scope_basic),
            },
            scope=self.scope_basic,
            discriminator_field_name=discriminator_field_name,
            discriminator_inlined=False,
        )
        # attempt to validate a class not within the scope of
        # this union
        with self.assertRaises(ConstraintException) as cm:
            # noinspection PyTypeChecker
            s.validate(InlineStr("b", "Hello world!"))
        self.assertIn(
            f"Invalid type: '{InlineStr.__name__}'",
            str(cm.exception),
        )
        # validate with the class used as the generic type parameter
        # in constructing the OneOfStringType
        s.validate(Basic("Hello world!"))

    def test_validation_inline(self):
        s = schema.OneOfStringType[InlineStr](
            {
                "a": schema.RefType("a", self.scope_inlined),
                "b": schema.RefType("b", self.scope_inlined),
                "a2": schema.RefType("a2", self.scope_inlined),
            },
            scope=self.scope_inlined,
            discriminator_field_name=discriminator_field_name,
            discriminator_inlined=True,
        )
        with self.assertRaises(ConstraintException):
            # noinspection PyTypeChecker
            s.validate(InlineStr(None, "Hello world!"))
        with self.assertRaises(ConstraintException):
            s.validate(InlineStr("b", "Hello world!"))
        with self.assertRaises(ConstraintException):
            # noinspection PyTypeChecker
            s.validate(Basic("Hello world!"))
        s.validate(InlineStr("a", "Hello world!"))

    def test_serialize(self):
        s = schema.OneOfStringType(
            {
                "a": schema.RefType("a", self.scope_basic),
                "b": schema.RefType("b", self.scope_basic),
            },
            scope=self.scope_basic,
            discriminator_field_name=discriminator_field_name,
        )
        self.assertEqual(
            s.serialize(Basic("Hello world!")),
            {discriminator_field_name: "a", "msg": "Hello world!"},
        )
        self.assertEqual(
            s.serialize(Basic3(42)),
            {discriminator_field_name: "b", "b": 42},
        )
        # the InlineStr schema is not within the scope of this OneOf
        with self.assertRaises(ConstraintException) as cm:
            # noinspection PyTypeChecker
            s.serialize(InlineStr("a", "Hello world!"))
        self.assertIn(
            f"Invalid type: '{InlineStr.__name__}'",
            str(cm.exception),
        )

    def test_serialize_inline(self):
        s = schema.OneOfStringType(
            {
                "a": schema.RefType("a", self.scope_inlined),
                "b": schema.RefType("b", self.scope_inlined),
            },
            scope=self.scope_inlined,
            discriminator_field_name=discriminator_field_name,
            discriminator_inlined=True,
        )
        self.assertEqual(
            s.serialize(InlineStr("a", "Hello world!")),
            {discriminator_field_name: "a", "a": "Hello world!"},
        )
        with self.assertRaises(ConstraintException) as cm:
            s.serialize(InlineStr("b", "Hello world!"))
        self.assertIn(
            f"Invalid value for '{discriminator_field_name}' "
            f"on '{InlineStr.__name__}'",
            str(cm.exception),
        )
        with self.assertRaises(ConstraintException) as cm:
            # noinspection PyTypeChecker
            s.serialize(Basic("Hello world!"))
        self.assertIn(
            f"Invalid type: '{Basic.__name__}'",
            str(cm.exception),
        )

    def test_object(self):
        scope = schema.ScopeType({}, "")
        s = schema.OneOfStringType(
            {
                "a": schema.ObjectType(
                    InlineStr,
                    {
                        discriminator_field_name: PropertyType(
                            schema.StringType(),
                        ),
                        "a": PropertyType(schema.StringType()),
                    },
                ),
                "b": schema.ObjectType(
                    Basic3, {"b": PropertyType(schema.IntType())}
                ),
            },
            scope=scope,
            discriminator_field_name=discriminator_field_name,
        )
        unserialized_data = s.unserialize(
            {discriminator_field_name: "b", "b": 42}
        )
        self.assertIsInstance(unserialized_data, Basic3)


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
                typing.Optional[re.Pattern],
                schema.required_if_not("label_selector"),
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
                namespace_pattern=re.compile(".*"),
                name_pattern=re.compile(".*"),
            ),
            self.fail,
        )

    def test_required_if(self):
        @dataclasses.dataclass
        class TestData1:
            A: typing.Annotated[
                typing.Optional[str], schema.required_if("B")
            ] = None
            B: typing.Optional[int] = None

        s = schema.build_object_schema(TestData1)

        unserialized = s.unserialize({})
        self.assertIsNone(unserialized.A)
        self.assertIsNone(unserialized.B)

        unserialized = s.unserialize({"A": None, "B": None})
        self.assertIsNone(unserialized.A)
        self.assertIsNone(unserialized.B)

        unserialized = s.unserialize({"A": "Foo"})
        self.assertEqual(unserialized.A, "Foo")
        self.assertIsNone(unserialized.B)

        with self.assertRaises(schema.ConstraintException):
            s.unserialize({"B": "Foo"})

        with self.assertRaises(schema.ConstraintException):
            s.validate(TestData1(B="Foo"))

        with self.assertRaises(schema.ConstraintException):
            s.serialize(TestData1(B="Foo"))

    def test_required_if_not(self):
        @dataclasses.dataclass
        class TestData1:
            A: typing.Optional[str] = None
            B: typing.Annotated[
                typing.Optional[str], schema.required_if_not("A")
            ] = None

        s = schema.build_object_schema(TestData1)

        with self.assertRaises(schema.ConstraintException):
            s.unserialize({})

        with self.assertRaises(schema.ConstraintException):
            s.unserialize({"A": None, "B": None})

        unserialized = s.unserialize({"A": "Foo"})
        self.assertEqual(unserialized.A, "Foo")
        self.assertIsNone(unserialized.B)

        unserialized = s.unserialize({"B": "Foo"})
        self.assertEqual(unserialized.B, "Foo")
        self.assertIsNone(unserialized.A)

        s.validate(TestData1(B="Foo"))
        s.serialize(TestData1(B="Foo"))

        @dataclasses.dataclass
        class TestData2:
            A: typing.Optional[str] = None
            B: typing.Optional[str] = None
            C: typing.Annotated[
                typing.Optional[str],
                schema.required_if_not("A"),
                schema.required_if_not("B"),
            ] = None

        s = schema.build_object_schema(TestData2)

        with self.assertRaises(schema.ConstraintException):
            s.unserialize({"A": None, "B": None, "C": None})

        unserialized = s.unserialize({"C": "Foo"})
        self.assertIsNone(unserialized.A)
        self.assertIsNone(unserialized.B)
        self.assertEqual(unserialized.C, "Foo")

        td2_c = TestData2(C="Foo")
        s.validate(td2_c)
        s.serialize(td2_c)

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
        resolved_type = schema._SchemaBuilder.resolve(
            typing.Dict[str, str], scope
        )
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
        self.assertIsInstance(
            object_schema.properties["a"].type, schema.StringType
        )
        self.assertIsNone(object_schema.properties["b"].display.name)
        self.assertTrue(object_schema.properties["b"].required)
        self.assertIsInstance(
            object_schema.properties["b"].type, schema.IntType
        )
        self.assertIsNone(object_schema.properties["c"].display.name)
        self.assertTrue(object_schema.properties["c"].required)
        self.assertIsInstance(
            object_schema.properties["c"].type, schema.FloatType
        )
        self.assertIsNone(object_schema.properties["d"].display.name)
        self.assertTrue(object_schema.properties["d"].required)
        self.assertIsInstance(
            object_schema.properties["d"].type, schema.BoolType
        )

        @dataclasses.dataclass
        class TestData:
            a: str = "foo"
            b: int = 5
            c: str = dataclasses.field(
                default="bar",
                metadata={"name": "C", "description": "A string"},
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
        self.assertIsInstance(
            object_schema.properties["a"].type, schema.StringType
        )
        self.assertIsNone(object_schema.properties["b"].display.name)
        self.assertFalse(object_schema.properties["b"].required)
        self.assertIsInstance(
            object_schema.properties["b"].type, schema.IntType
        )
        self.assertEqual("C", object_schema.properties["c"].display.name)
        self.assertEqual(
            "A string", object_schema.properties["c"].display.description
        )
        self.assertFalse(object_schema.properties["c"].required)
        self.assertIsInstance(
            object_schema.properties["c"].type, schema.StringType
        )
        self.assertIsNone(object_schema.properties["d"].display.name)
        self.assertFalse(object_schema.properties["d"].required)
        self.assertIsInstance(
            object_schema.properties["d"].type, schema.BoolType
        )

    def test_union(self):
        scope = schema.build_object_schema(BasicUnion)
        self.assertEqual(BasicUnion.__name__, scope.root)
        self.assertIsInstance(
            scope.objects[BasicUnion.__name__], schema.ObjectType
        )
        self.assertIsInstance(scope.objects[Basic.__name__], schema.ObjectType)
        self.assertIsInstance(
            scope.objects[Basic2.__name__], schema.ObjectType
        )

        self.assertIsInstance(
            scope.objects[BasicUnion.__name__].properties["union_basic"].type,
            schema.OneOfStringType,
        )
        one_of_type: schema.OneOfStringType = (
            scope.objects[BasicUnion.__name__].properties["union_basic"].type
        )
        self.assertEqual(
            one_of_type.discriminator_field_name, default_discriminator
        )
        self.assertIsInstance(
            one_of_type.types[Basic.__name__], schema.RefType
        )
        self.assertEqual(one_of_type.types[Basic.__name__].id, "Basic")
        self.assertIsInstance(
            one_of_type.types[Basic2.__name__], schema.RefType
        )
        self.assertEqual(one_of_type.types[Basic2.__name__].id, "Basic2")

    def test_union_custom_discriminator(self):
        @dataclasses.dataclass
        class TestData:
            union: typing.Annotated[
                typing.Union[
                    typing.Annotated[InlineInt, schema.discriminator_value(1)],
                    typing.Annotated[
                        InlineInt2, schema.discriminator_value(2)
                    ],
                ],
                schema.discriminator(
                    discriminator_field_name, discriminator_inlined=True
                ),
            ]

        scope = schema.build_object_schema(TestData)
        self.assertEqual(TestData.__name__, scope.root)
        self.assertIsInstance(
            scope.objects[TestData.__name__], schema.ObjectType
        )
        self.assertIsInstance(
            scope.objects[InlineInt.__name__], schema.ObjectType
        )
        self.assertIsInstance(
            scope.objects[InlineInt2.__name__], schema.ObjectType
        )

        self.assertIsInstance(
            scope.objects[TestData.__name__].properties["union"].type,
            schema.OneOfIntType,
        )
        one_of_type: schema.OneOfIntType = (
            scope.objects[TestData.__name__].properties["union"].type
        )
        self.assertEqual(
            one_of_type.discriminator_field_name, discriminator_field_name
        )
        self.assertIsInstance(one_of_type.types[1], schema.RefType)
        self.assertEqual(one_of_type.types[1].id, InlineInt.__name__)
        self.assertIsInstance(one_of_type.types[2], schema.RefType)
        self.assertEqual(one_of_type.types[2].id, InlineInt2.__name__)

    def test_one_of_has_discriminator_error(self):
        @dataclasses.dataclass
        class TestData:
            union: typing.Annotated[
                typing.Union[Basic, Basic2, InlineStr],
                schema.discriminator(
                    discriminator_field_name=discriminator_field_name,
                    discriminator_inlined=False,
                ),
            ]

        with self.assertRaises(SchemaBuildException) as cm:
            schema.build_object_schema(TestData)
        self.assertIn(
            f'"{InlineStr.__name__}" has conflicting field '
            f'"{discriminator_field_name}"',
            str(cm.exception),
        )

    def test_one_of_inline_discriminator_missing_error(self):
        @dataclasses.dataclass
        class TestData:
            union: typing.Annotated[
                typing.Union[
                    typing.Annotated[
                        InlineStr, schema.discriminator_value("InlineStr")
                    ],
                    typing.Annotated[
                        InlineStr2, schema.discriminator_value("InlineStr2")
                    ],
                    typing.Annotated[
                        Basic, schema.discriminator_value("Basic")
                    ],
                ],
                schema.discriminator(
                    discriminator_field_name=discriminator_field_name,
                    discriminator_inlined=True,
                ),
            ]

        with self.assertRaises(SchemaBuildException) as cm:
            schema.build_object_schema(TestData)
        self.assertIn(
            f'"{Basic.__name__}" needs discriminator field', str(cm.exception)
        )

    def test_one_of_inline_discriminator_type_mismatch(self):
        discriminator_wrong_type: int = 1

        @dataclasses.dataclass
        class TestData:
            union: typing.Annotated[
                typing.Union[
                    typing.Annotated[
                        InlineStr, schema.discriminator_value("InlineStr")
                    ],
                    typing.Annotated[
                        InlineStr2, schema.discriminator_value("InlineStr2")
                    ],
                    typing.Annotated[
                        InlineInt,
                        schema.discriminator_value(discriminator_wrong_type),
                    ],
                ],
                schema.discriminator(
                    discriminator_field_name=discriminator_field_name,
                    discriminator_inlined=True,
                ),
            ]

        with self.assertRaises(BadArgumentException) as cm:
            schema.build_object_schema(TestData)
        self.assertIn(
            "Invalid discriminator value type: "
            f"{type(discriminator_wrong_type)}",
            str(cm.exception),
        )

    def test_optional(self):
        @dataclasses.dataclass
        class TestData:
            a: typing.Optional[str] = None

        scope = schema.build_object_schema(TestData)
        self.assertEqual("TestData", scope.root)
        self.assertIsInstance(scope.objects["TestData"], schema.ObjectType)
        resolved_type = scope.objects["TestData"]

        self.assertFalse(resolved_type.properties["a"].required)
        self.assertIsInstance(
            resolved_type.properties["a"].type, schema.StringType
        )

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
            a: typing.Annotated[
                typing.Optional[str], schema.required_if("b")
            ] = None
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

                self.assertEqual(
                    expected, input._to_jsonschema_fragment(scope, defs)
                )

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
        test_cases: typing.Dict[
            str, typing.Tuple[schema.StringType, typing.Dict]
        ] = {
            "base": (schema.StringType(), {"type": "string"}),
            "min": (
                schema.StringType(min=5),
                {"type": "string", "minLength": 5},
            ),
            "max": (
                schema.StringType(max=5),
                {"type": "string", "maxLength": 5},
            ),
            "pattern": (
                schema.StringType(pattern=re.compile("^[a-z]+$")),
                {"type": "string", "pattern": "^[a-z]+$"},
            ),
        }

        self._execute_test_cases(test_cases)

    def test_int(self):
        test_cases: typing.Dict[
            str, typing.Tuple[schema.IntType, typing.Dict]
        ] = {
            "base": (schema.IntType(), {"type": "integer"}),
            "min": (schema.IntType(min=5), {"type": "integer", "minimum": 5}),
            "max": (schema.IntType(max=5), {"type": "integer", "maximum": 5}),
        }

        self._execute_test_cases(test_cases)

    def test_float(self):
        test_cases: typing.Dict[
            str, typing.Tuple[schema.FloatType, typing.Dict]
        ] = {
            "base": (schema.FloatType(), {"type": "number"}),
            "min": (
                schema.FloatType(min=5.0),
                {"type": "number", "minimum": 5.0},
            ),
            "max": (
                schema.FloatType(max=5.0),
                {"type": "number", "maximum": 5.0},
            ),
        }

        self._execute_test_cases(test_cases)

    def test_enum(self):
        class Color(enum.Enum):
            RED = "red"

        class Fibonacci(enum.Enum):
            FIRST = 1
            SECOND = 2

        test_cases: typing.Dict[
            str, typing.Tuple[schema._EnumType, typing.Dict]
        ] = {
            "string": (
                schema.StringEnumType(Color),
                {"type": "string", "enum": ["red"]},
            ),
            "int": (
                schema.IntEnumType(Fibonacci),
                {"type": "integer", "enum": [1, 2]},
            ),
        }

        self._execute_test_cases(test_cases)

    def test_list(self):
        test_cases: typing.Dict[
            str, typing.Tuple[schema.ListType, typing.Dict]
        ] = {
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
        test_cases: typing.Dict[
            str, typing.Tuple[schema.MapType, typing.Dict]
        ] = {
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
                        "a": {
                            "type": "string",
                            "title": "A",
                            "description": "A string",
                        }
                    },
                    "required": ["a"],
                    "additionalProperties": False,
                    "dependentRequired": {},
                }
            },
            "type": "object",
            "properties": {
                "a": {
                    "type": "string",
                    "title": "A",
                    "description": "A string",
                }
            },
            "required": ["a"],
            "additionalProperties": False,
            "dependentRequired": {},
        }
        result = scope._to_jsonschema_fragment(scope, defs)
        self.assertEqual(expected, result)

    def test_one_of(self):
        scope = schema.ScopeType(
            {},
            BasicUnion.__name__,
        )
        scope.objects = {
            BasicUnion.__name__: schema.ObjectType(
                BasicUnion,
                {
                    "union_basic": schema.PropertyType(
                        schema.OneOfStringType(
                            {
                                "a": schema.RefType(Basic.__name__, scope),
                                "b": schema.RefType(Basic2.__name__, scope),
                            },
                            scope,
                            discriminator_inlined=False,
                        )
                    )
                },
            ),
            Basic.__name__: schema.ObjectType(
                Basic, {"msg": schema.PropertyType(schema.StringType())}
            ),
            Basic2.__name__: schema.ObjectType(
                Basic2, {"msg2": schema.PropertyType(schema.StringType())}
            ),
        }
        defs = schema._JSONSchemaDefs()
        json_schema = scope._to_jsonschema_fragment(scope, defs)
        self.assertEqual(
            {
                "$defs": {
                    BasicUnion.__name__: {
                        "type": "object",
                        "properties": {
                            "union_basic": {
                                "oneOf": [
                                    {
                                        "$ref": (
                                            f"#/$defs/{Basic.__name__}"
                                            "_discriminated_string_"
                                            "a"
                                        )
                                    },
                                    {
                                        "$ref": (
                                            f"#/$defs/{Basic2.__name__}"
                                            "_discriminated_string_"
                                            "b"
                                        )
                                    },
                                ]
                            }
                        },
                        "required": ["union_basic"],
                        "additionalProperties": False,
                        "dependentRequired": {},
                    },
                    Basic.__name__: {
                        "type": "object",
                        "properties": {
                            "msg": {"type": "string"},
                        },
                        "required": [default_discriminator, "msg"],
                        "additionalProperties": False,
                        "dependentRequired": {},
                    },
                    f"{Basic.__name__}_discriminated_string_a": {
                        "type": "object",
                        "properties": {
                            "msg": {"type": "string"},
                        },
                        "required": [default_discriminator, "msg"],
                        "additionalProperties": False,
                        "dependentRequired": {},
                    },
                    Basic2.__name__: {
                        "type": "object",
                        "properties": {
                            "msg2": {"type": "string"},
                        },
                        "required": [default_discriminator, "msg2"],
                        "additionalProperties": False,
                        "dependentRequired": {},
                    },
                    f"{Basic2.__name__}_discriminated_string_b": {
                        "type": "object",
                        "properties": {
                            "msg2": {"type": "string"},
                        },
                        "required": [default_discriminator, "msg2"],
                        "additionalProperties": False,
                        "dependentRequired": {},
                    },
                },
                "type": "object",
                "properties": {
                    "union_basic": {
                        "oneOf": [
                            {
                                "$ref": (
                                    f"#/$defs/{Basic.__name__}"
                                    "_discriminated_string_"
                                    "a"
                                )
                            },
                            {
                                "$ref": (
                                    f"#/$defs/{Basic2.__name__}"
                                    "_discriminated_string_"
                                    "b"
                                )
                            },
                        ]
                    }
                },
                "required": ["union_basic"],
                "additionalProperties": False,
                "dependentRequired": {},
            },
            json_schema,
        )

    def test_one_of_inline(self):
        scope = schema.ScopeType(
            {},
            InlineUnion.__name__,
        )
        scope.objects = {
            InlineUnion.__name__: schema.ObjectType(
                InlineUnion,
                {
                    "union": schema.PropertyType(
                        schema.OneOfStringType(
                            {
                                "a": schema.RefType(InlineStr.__name__, scope),
                                "b": schema.RefType(
                                    InlineStr2.__name__, scope
                                ),
                                "a2": schema.RefType(
                                    DoubleInlineStr.__name__, scope
                                ),
                            },
                            scope,
                            discriminator_inlined=True,
                            discriminator_field_name=discriminator_field_name,
                        )
                    )
                },
            ),
            InlineStr.__name__: schema.ObjectType(
                InlineStr,
                {
                    "a": schema.PropertyType(schema.StringType()),
                    discriminator_field_name: schema.PropertyType(
                        schema.StringType()
                    ),
                },
            ),
            InlineStr2.__name__: schema.ObjectType(
                InlineStr2,
                {
                    "code": schema.PropertyType(schema.IntType()),
                    discriminator_field_name: schema.PropertyType(
                        schema.StringType()
                    ),
                },
            ),
            DoubleInlineStr.__name__: schema.ObjectType(
                DoubleInlineStr,
                {
                    default_discriminator: schema.PropertyType(
                        schema.StringType()
                    ),
                    discriminator_field_name: schema.PropertyType(
                        schema.StringType()
                    ),
                    "msg": schema.PropertyType(schema.StringType()),
                },
            ),
        }

        defs = schema._JSONSchemaDefs()
        json_schema = scope._to_jsonschema_fragment(scope, defs)
        self.assertEqual(
            {
                "$defs": {
                    InlineUnion.__name__: {
                        "type": "object",
                        "properties": {
                            "union": {
                                "oneOf": [
                                    {
                                        "$ref": (
                                            f"#/$defs/{InlineStr.__name__}"
                                            "_discriminated_string_"
                                            "a"
                                        )
                                    },
                                    {
                                        "$ref": (
                                            f"#/$defs/{InlineStr2.__name__}"
                                            "_discriminated_string_"
                                            "b"
                                        )
                                    },
                                    {
                                        "$ref": (
                                            "#/$defs/"
                                            f"{DoubleInlineStr.__name__}"
                                            "_discriminated_string_"
                                            "a2"
                                        )
                                    },
                                ]
                            }
                        },
                        "required": ["union"],
                        "additionalProperties": False,
                        "dependentRequired": {},
                    },
                    InlineStr.__name__: {
                        "type": "object",
                        "properties": {
                            "a": {"type": "string"},
                            discriminator_field_name: {
                                "type": "string",
                                "const": "a",
                            },
                        },
                        "required": [discriminator_field_name, "a"],
                        "additionalProperties": False,
                        "dependentRequired": {},
                    },
                    f"{InlineStr.__name__}_discriminated_string_a": {
                        "type": "object",
                        "properties": {
                            "a": {"type": "string"},
                            discriminator_field_name: {
                                "type": "string",
                                "const": "a",
                            },
                        },
                        "required": [discriminator_field_name, "a"],
                        "additionalProperties": False,
                        "dependentRequired": {},
                    },
                    InlineStr2.__name__: {
                        "type": "object",
                        "properties": {
                            "code": {"type": "integer"},
                            discriminator_field_name: {
                                "type": "string",
                                "const": "b",
                            },
                        },
                        "required": [discriminator_field_name, "code"],
                        "additionalProperties": False,
                        "dependentRequired": {},
                    },
                    f"{InlineStr2.__name__}_discriminated_string_b": {
                        "type": "object",
                        "properties": {
                            "code": {"type": "integer"},
                            discriminator_field_name: {
                                "type": "string",
                                "const": "b",
                            },
                        },
                        "required": [discriminator_field_name, "code"],
                        "additionalProperties": False,
                        "dependentRequired": {},
                    },
                    DoubleInlineStr.__name__: {
                        "type": "object",
                        "properties": {
                            "msg": {"type": "string"},
                            discriminator_field_name: {
                                "type": "string",
                                "const": "a2",
                            },
                            default_discriminator: {"type": "string"},
                        },
                        "required": [
                            discriminator_field_name,
                            default_discriminator,
                            "msg",
                        ],
                        "additionalProperties": False,
                        "dependentRequired": {},
                    },
                    f"{DoubleInlineStr.__name__}_discriminated_string_a2": {
                        "type": "object",
                        "properties": {
                            "msg": {"type": "string"},
                            discriminator_field_name: {
                                "type": "string",
                                "const": "a2",
                            },
                            default_discriminator: {"type": "string"},
                        },
                        "required": [
                            discriminator_field_name,
                            default_discriminator,
                            "msg",
                        ],
                        "additionalProperties": False,
                        "dependentRequired": {},
                    },
                },
                "type": "object",
                "properties": {
                    "union": {
                        "oneOf": [
                            {
                                "$ref": (
                                    f"#/$defs/{InlineStr.__name__}"
                                    "_discriminated_string_"
                                    "a"
                                )
                            },
                            {
                                "$ref": (
                                    f"#/$defs/{InlineStr2.__name__}"
                                    "_discriminated_string_"
                                    "b"
                                )
                            },
                            {
                                "$ref": (
                                    f"#/$defs/{DoubleInlineStr.__name__}"
                                    "_discriminated_string_"
                                    "a2"
                                )
                            },
                        ]
                    }
                },
                "required": ["union"],
                "additionalProperties": False,
                "dependentRequired": {},
            },
            json_schema,
        )

    def test_oneof_int_inline(self):
        scope = schema.ScopeType(
            {},
            IntInlineUnion.__name__,
        )
        inline_int_key = 1
        inline_int2_key = 2

        scope.objects = {
            IntInlineUnion.__name__: schema.ObjectType(
                IntInlineUnion,
                {
                    "union": schema.PropertyType(
                        schema.OneOfIntType(
                            {
                                inline_int_key: schema.RefType(
                                    InlineInt.__name__, scope
                                ),
                                inline_int2_key: schema.RefType(
                                    InlineInt2.__name__, scope
                                ),
                            },
                            scope,
                            discriminator_inlined=True,
                            discriminator_field_name=discriminator_field_name,
                        )
                    )
                },
            ),
            InlineInt.__name__: schema.ObjectType(
                InlineInt,
                {
                    "msg": schema.PropertyType(schema.StringType()),
                    "code": schema.PropertyType(schema.IntType()),
                    discriminator_field_name: schema.PropertyType(
                        schema.IntType()
                    ),
                },
            ),
            InlineInt2.__name__: schema.ObjectType(
                InlineInt2,
                {
                    "msg2": schema.PropertyType(schema.StringType()),
                    discriminator_field_name: schema.PropertyType(
                        schema.IntType()
                    ),
                },
            ),
        }

        defs = schema._JSONSchemaDefs()
        json_schema = scope._to_jsonschema_fragment(scope, defs)
        self.assertEqual(
            {
                "$defs": {
                    IntInlineUnion.__name__: {
                        "type": "object",
                        "properties": {
                            "union": {
                                "oneOf": [
                                    {
                                        "$ref": (
                                            f"#/$defs/{InlineInt.__name__}"
                                            "_discriminated_int_"
                                            f"{inline_int_key}"
                                        )
                                    },
                                    {
                                        "$ref": (
                                            f"#/$defs/{InlineInt2.__name__}"
                                            "_discriminated_int_"
                                            f"{inline_int2_key}"
                                        )
                                    },
                                ]
                            }
                        },
                        "required": ["union"],
                        "additionalProperties": False,
                        "dependentRequired": {},
                    },
                    InlineInt.__name__: {
                        "type": "object",
                        "properties": {
                            "msg": {"type": "string"},
                            "code": {"type": "integer"},
                            discriminator_field_name: {
                                "type": "integer",
                                "const": str(inline_int_key),
                            },
                        },
                        "required": [discriminator_field_name, "msg", "code"],
                        "additionalProperties": False,
                        "dependentRequired": {},
                    },
                    InlineInt.__name__
                    + f"_discriminated_int_{inline_int_key}": {
                        "type": "object",
                        "properties": {
                            "msg": {"type": "string"},
                            "code": {"type": "integer"},
                            discriminator_field_name: {
                                "type": "integer",
                                "const": str(inline_int_key),
                            },
                        },
                        "required": [discriminator_field_name, "msg", "code"],
                        "additionalProperties": False,
                        "dependentRequired": {},
                    },
                    InlineInt2.__name__: {
                        "type": "object",
                        "properties": {
                            "msg2": {"type": "string"},
                            discriminator_field_name: {
                                "type": "integer",
                                "const": str(inline_int2_key),
                            },
                        },
                        "required": [discriminator_field_name, "msg2"],
                        "additionalProperties": False,
                        "dependentRequired": {},
                    },
                    InlineInt2.__name__
                    + f"_discriminated_int_{inline_int2_key}": {
                        "type": "object",
                        "properties": {
                            "msg2": {"type": "string"},
                            discriminator_field_name: {
                                "type": "integer",
                                "const": str(inline_int2_key),
                            },
                        },
                        "required": [discriminator_field_name, "msg2"],
                        "additionalProperties": False,
                        "dependentRequired": {},
                    },
                },
                "type": "object",
                "properties": {
                    "union": {
                        "oneOf": [
                            {
                                "$ref": (
                                    f"#/$defs/{InlineInt.__name__}"
                                    "_discriminated_int_"
                                    f"{inline_int_key}"
                                )
                            },
                            {
                                "$ref": (
                                    f"#/$defs/{InlineInt2.__name__}"
                                    "_discriminated_int_"
                                    f"{inline_int2_key}"
                                )
                            },
                        ]
                    }
                },
                "required": ["union"],
                "additionalProperties": False,
                "dependentRequired": {},
            },
            json_schema,
        )


def load_tests(loader, tests, ignore):
    """This function adds the doctests to the discovery process."""
    tests.addTests(doctest.DocTestSuite(schema))
    return tests


if __name__ == "__main__":
    unittest.main()
