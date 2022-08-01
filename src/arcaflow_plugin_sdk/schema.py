import enum
import pprint
import re
import typing
from re import Pattern
from abc import abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Any, Optional, TypeVar, Type, Generic, Callable


@dataclass
class ConstraintException(Exception):
    """
    ConstraintException indicates that the passed data violated one or more constraints defined in the schema.
    """
    path: typing.Tuple[str] = tuple([])
    msg: str = ""

    def __str__(self):
        if len(self.path) == 0:
            return "Validation failed: {}".format(self.msg)
        return "Validation failed for '{}': {}".format(" -> ".join(self.path), self.msg)


@dataclass
class NoSuchStepException(Exception):
    """
    NoSuchStepException indicates that the given step is not supported by a schema.
    """
    step: str

    def __str__(self):
        return "No such step: %s" % self.step


@dataclass
class BadArgumentException(Exception):
    """
    BadArgumentException indicates that an invalid configuration was passed to a schema component.
    """
    msg: str

    def __str__(self):
        return self.msg


class TypeID(enum.Enum):
    """
    TypeID is the enum of possible types supported by the protocol.
    """
    ENUM = "enum"
    STRING = "string"
    PATTERN = "pattern"
    INT = "integer"
    FLOAT = "float"
    BOOL = "boolean"
    LIST = "list"
    MAP = "map"
    OBJECT = "object"
    ONEOF = "oneof"

    def is_map_key(self) -> bool:
        """
        This function returns true if the current type can be used as a map key.

        :return: True if the current type can be used as map key.
        """
        return self in [
            TypeID.ENUM,
            TypeID.STRING,
            TypeID.INT
        ]


TypeT = TypeVar("TypeT")


class AbstractType(Generic[TypeT]):
    """
    This class is an abstract class describing the methods needed to implement a type.
    """

    @abstractmethod
    def type_id(self) -> TypeID:
        """
        This function returns an identifier for the data structure represented in this type.
        :return:
        """
        pass

    @abstractmethod
    def unserialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> TypeT:
        """
        This function takes the underlying raw data and decodes it into the underlying advanced data type (e.g.
        dataclass) for usage.
        :param data: the raw data.
        :param path: the list of structural elements that lead to this point for error messages.
        :return: the advanced datatype.
        :raise ConstraintException: if the passed data was not valid.
        """
        pass

    @abstractmethod
    def validate(self, data: TypeT, path: typing.Tuple[str] = tuple([])):
        """
        This function validates an already unserialized data type and raises an exception if it does not match
        the type definition.
        :param data: the unserialized data.
        :param path: the path that lead to this validation call, in order to produce a nice error message
        :raise ConstraintException: if the passed data was not valid.
        """

    @abstractmethod
    def serialize(self, data: TypeT, path: typing.Tuple[str] = tuple([])) -> Any:
        """
        This function serializes the passed data into it's raw form for transport, e.g. string, int, dicts, list.
        :param data: the underlying data type to be serialized.
        :param path: the list of structural elements that lead to this point for error messages.
        :return: the raw datatype.
        :raise ConstraintException: if the passed data was not valid.
        """
        pass


EnumT = TypeVar("EnumT", bound=Enum)


@dataclass
class EnumType(AbstractType, Generic[EnumT]):
    """
    EnumType is a type that can take only a limited set of values provided by a Python Enum. The validation and
    unserialization will take the enum itself, or the underlying basic value as a possible value.
    """

    type: Type[EnumT]
    value_type: object

    def __init__(self, type: Type[EnumT]):
        self.type = type
        try:
            found_type = None
            if len(self.type) == 0:
                BadArgumentException("Enum {} has no valid values.".format(type.__name__))
            for value in self.type:
                if (not isinstance(value.value, str)) and (not isinstance(value.value, int)):
                    raise BadArgumentException(
                        "{} on {} is not a valid enum value, must be str or int".format(value, type.__name__))
                if found_type is not None and value.value.__class__.__name__ != found_type:
                    raise BadArgumentException(
                        "Enum {} contains different value types. Please make all value types the same. (Found both {} "
                        "and {} values.)".format(type.__name__, value.value.__class__.__name__, found_type)
                    )
                found_type = value.value.__class__.__name__
                self.value_type = value.value.__class__
        except TypeError as e:
            raise BadArgumentException("{} is not a valid enum, not iterable".format(type.__name__)) from e

    def type_id(self) -> TypeID:
        return TypeID.ENUM

    def unserialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> EnumT:
        if isinstance(data, Enum):
            if data not in self.type:
                raise ConstraintException(
                    path,
                    "'{}' is not a valid value for the enum '{}'".format(data, self.type.__name__)
                )
            return data
        else:
            for v in self.type:
                if v == data or v.value == data:
                    return v
            raise ConstraintException(path, "'{}' is not a valid value for '{}'".format(data, self.type.__name__))

    def validate(self, data: TypeT, path: typing.Tuple[str] = tuple([])):
        if isinstance(data, Enum):
            if data not in self.type:
                raise ConstraintException(
                    path,
                    "'{}' is not a valid value for the enum '{}'".format(data, self.type.__name__)
                )
        else:
            for v in self.type:
                if v == data or v.value == data:
                    return
            raise ConstraintException(path, "'{}' is not a valid value for '{}'".format(data, self.type.__name__))

    def serialize(self, data: EnumT, path: typing.Tuple[str] = tuple([])) -> Any:
        if data not in self.type:
            raise ConstraintException(
                path,
                "'{}' is not a valid value for the enum '{}'".format(data, self.type.__name__)
            )
        return data.value


class BoolType(AbstractType):

    def type_id(self) -> TypeID:
        return TypeID.BOOL

    def unserialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> TypeT:
        if isinstance(data, bool):
            return data
        if isinstance(data, int):
            if data == 0:
                return False
            if data == 1:
                return True
            raise ConstraintException(path, "Boolean value expected, integer found ({})".format(data))
        if isinstance(data, str):
            lower_str = data.lower()
            if lower_str == "yes" or \
                    lower_str == "on" or \
                    lower_str == "true" or \
                    lower_str == "enable" or \
                    lower_str == "enabled" or \
                    lower_str == "1":
                return True
            if lower_str == "no" or \
                    lower_str == "off" or \
                    lower_str == "false" or \
                    lower_str == "disable" or \
                    lower_str == "disabled" or \
                    lower_str == "0":
                return False
            raise ConstraintException(path, "Boolean value expected, string found ({})".format(data))

        raise ConstraintException(path, "Boolean value expected, {} found".format(type(data)))

    def validate(self, data: TypeT, path: typing.Tuple[str] = tuple([])):
        if not isinstance(data, bool):
            raise ConstraintException(path, "Boolean value expected, {} found".format(type(data)))

    def serialize(self, data: TypeT, path: typing.Tuple[str] = tuple([])) -> Any:
        if isinstance(data, bool):
            return data
        raise ConstraintException(path, "Boolean value expected, {} found".format(type(data)))


@dataclass
class StringType(AbstractType):
    """
    StringType represents a string of characters for human consumption.
    """

    min_length: Optional[int] = None
    "Minimum length of the string (inclusive, optional)."

    max_length: Optional[int] = None
    "Maximum length of the string (inclusive, optional)."

    pattern: Optional[Pattern] = None
    "Regular expression the string must match (optional)."

    def type_id(self) -> TypeID:
        return TypeID.STRING

    def unserialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> str:
        if isinstance(data, int):
            data = str(data)
        self.validate(data, path)
        return data

    def validate(self, data: TypeT, path: typing.Tuple[str] = tuple([])):
        if not isinstance(data, str):
            raise ConstraintException(path, "Must be a string, {} given".format(type(data)))
        string: str = data
        if self.min_length is not None and len(string) < self.min_length:
            raise ConstraintException(
                path,
                "String must be at least {} characters, {} given".format(self.min_length, len(string))
            )
        if self.max_length is not None and len(string) > self.max_length:
            raise ConstraintException(
                path,
                "String must be at most {} characters, {} given".format(self.max_length, len(string))
            )
        if self.pattern is not None and not self.pattern.match(string):
            raise ConstraintException(
                path,
                "String must match the pattern {}".format(self.pattern.__str__())
            )

    def serialize(self, data: str, path: typing.Tuple[str] = tuple([])) -> any:
        self.validate(data, path)
        return data


@dataclass
class PatternType(AbstractType):
    """
    PatternType represents a regular expression.
    """

    def type_id(self) -> TypeID:
        return TypeID.PATTERN

    def unserialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> re.Pattern:
        if not isinstance(data, str):
            raise ConstraintException(path, "Must be a string")
        try:
            return re.compile(str(data))
        except TypeError as e:
            raise ConstraintException(path, "Invalid regular expression ({})".format(e.__str__()))
        except ValueError as e:
            raise ConstraintException(path, "Invalid regular expression ({})".format(e.__str__()))

    def validate(self, data: TypeT, path: typing.Tuple[str] = tuple([])):
        if not isinstance(data, re.Pattern):
            raise ConstraintException(path, "Not a regular expression")

    def serialize(self, data: re.Pattern, path: typing.Tuple[str] = tuple([])) -> Any:
        if not isinstance(data, re.Pattern):
            raise ConstraintException(path, "Must be a re.Pattern")
        return data.pattern


@dataclass
class IntType(AbstractType):
    """
    IntType represents an integer type, both positive or negative. It is designed to take a 64 bit value.
    """

    min: Optional[int] = None
    "Minimum value (inclusive) for this type."

    max: Optional[int] = None
    "Maximum value (inclusive) for this type."

    def type_id(self) -> TypeID:
        return TypeID.INT

    def unserialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> int:
        if isinstance(data, str):
            try:
                data = int(data)
            except ValueError as e:
                raise ConstraintException(path, "Must be an integer") from e

        self.validate(data, path)
        return data

    def validate(self, data: TypeT, path: typing.Tuple[str] = tuple([])):
        if not isinstance(data, int):
            raise ConstraintException(path, "Must be an integer, {} given".format(type(data).__name__))
        integer = int(data)
        if self.min is not None and integer < self.min:
            raise ConstraintException(path, "Must be at least {}".format(self.min))
        if self.max is not None and integer > self.max:
            raise ConstraintException(path, "Must be at most {}".format(self.max))

    def serialize(self, data: int, path: typing.Tuple[str] = tuple([])) -> Any:
        self.validate(data, path)
        return data


@dataclass
class FloatType(AbstractType):
    """
    IntType represents an integer type, both positive or negative. It is designed to take a 64 bit value.
    """

    min: Optional[float] = None
    "Minimum value (inclusive) for this type."

    max: Optional[float] = None
    "Maximum value (inclusive) for this type."

    def type_id(self) -> TypeID:
        return TypeID.FLOAT

    def unserialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> int:
        if isinstance(data, str):
            try:
                data = float(data)
            except ValueError as e:
                raise ConstraintException(path, "Must be an float") from e

        self.validate(data, path)
        return data

    def validate(self, data: TypeT, path: typing.Tuple[str] = tuple([])):
        if not isinstance(data, float):
            raise ConstraintException(path, "Must be a float, {} given".format(type(data).__name__))
        integer = float(data)
        if self.min is not None and integer < self.min:
            raise ConstraintException(path, "Must be at least {}".format(self.min))
        if self.max is not None and integer > self.max:
            raise ConstraintException(path, "Must be at most {}".format(self.max))

    def serialize(self, data: float, path: typing.Tuple[str] = tuple([])) -> Any:
        self.validate(data, path)
        return data


ListT = TypeVar("ListT", bound=List)


@dataclass
class ListType(AbstractType, Generic[ListT]):
    """
    ListType is a strongly typed list that can have elements of only one type.
    """

    type: AbstractType
    "The underlying type of the items in this list."

    min: Optional[int] = None
    "Minimum number of elements (inclusive) in this list."

    max: Optional[int] = None
    "Maximum number of elements (inclusive) in this list."

    def type_id(self) -> TypeID:
        return TypeID.LIST

    def unserialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> ListT:
        if not isinstance(data, list):
            raise ConstraintException(path, "Must be a list, {} given".format(type(data).__name__))
        for i in range(len(data)):
            new_path = list(path)
            new_path.append(str(i))
            data[i] = self.type.unserialize(data[i], tuple(new_path))
        self._validate(data, path)
        return data

    def validate(self, data: TypeT, path: typing.Tuple[str] = tuple([])):
        self._validate(data, path)
        for i in range(len(data)):
            new_path = list(path)
            new_path.append(str(i))
            self.type.validate(data[i], tuple(new_path))

    def serialize(self, data: ListT, path: typing.Tuple[str] = tuple([])) -> Any:
        self._validate(data, path)
        result = []
        for i in range(len(data)):
            new_path = list(path)
            new_path.append(str(i))
            result.append(self.type.serialize(data[i], tuple(new_path)))
        return result

    def _validate(self, data, path):
        if not isinstance(data, list):
            raise ConstraintException(path, "Must be a list, {} given".format(type(data).__name__))
        if self.min is not None and len(data) < self.min:
            raise ConstraintException(path, "Must have at least {} items, {} given".format(self.min, len(data)))
        if self.max is not None and len(data) > self.max:
            raise ConstraintException(path, "Must have at most {} items, {} given".format(self.max, len(data)))


MapT = TypeVar("MapT", bound=Dict)


@dataclass
class MapType(AbstractType, Generic[MapT]):
    """
    MapType is a key-value dict with fixed types for both.
    """

    key_type: AbstractType
    "Type definition for the keys in this map. Must be a type that can serve as a map key."

    value_type: AbstractType
    "Type definition for the values in this map."

    min: Optional[int] = None
    "Minimum number of elements (inclusive) in this map."

    max: Optional[int] = None
    "Maximum number of elements (inclusive) in this map."

    def __init__(self, key_type: AbstractType, value_type: AbstractType, min: Optional[int] = None,
                 max: Optional[int] = None):
        """
        :param key_type: Type definition for the keys in this map. Must be a type that can serve as a map key.
        :param value_type: Type definition for the values in this map.
        :param min: Minimum number of elements (inclusive) in this map.
        :param max: Maximum number of elements (inclusive) in this map.
        """
        self.key_type = key_type
        self.value_type = value_type
        self.min = min
        self.max = max
        if not self.key_type.type_id().is_map_key():
            raise Exception(self.key_type.type_id().__str__() + " is not a valid map key")

    def type_id(self) -> TypeID:
        return TypeID.MAP

    def _validate(self, data, path):
        if not isinstance(data, dict):
            raise ConstraintException(path, "Must be a dict, {} given".format(type(data).__name__))
        entries = dict(data)
        if self.min is not None and len(entries) < self.min:
            raise ConstraintException()
        if self.max is not None and len(entries) > self.max:
            raise ConstraintException()
        return entries

    def unserialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> MapT:
        entries = self._validate(data, path)
        result: MapT = {}
        for key in entries.keys():
            value = entries[key]
            new_path = list(path)
            new_path.append(key)
            key_path = list(tuple(new_path))
            key_path.append("key")
            unserialized_key = self.key_type.unserialize(key, tuple(key_path))
            if unserialized_key in result:
                raise ConstraintException(
                    tuple(key_path),
                    "Key already exists in result dict"
                )
            value_path = list(tuple(new_path))
            value_path.append("value")
            result[unserialized_key] = self.value_type.unserialize(value, tuple(value_path))
        return result

    def validate(self, data: TypeT, path: typing.Tuple[str] = tuple([])):
        self._validate(data, path)
        for key in data.keys():
            value = data[key]
            new_path = list(path)
            new_path.append(key)
            key_path = list(tuple(new_path))
            key_path.append("key")
            self.key_type.validate(key, tuple(key_path))
            value_path = list(tuple(new_path))
            value_path.append("value")
            self.value_type.validate(value, tuple(new_path))

    def serialize(self, data: MapT, path: typing.Tuple[str] = tuple([])) -> Any:
        result = {}
        for key in data.keys():
            key_path = list(path)
            key_path.append(str(key))
            key_path.append("key")
            serialized_key = self.key_type.serialize(key, tuple(key_path))
            value_path = list(path)
            value_path.append(str(key))
            value_path.append("value")
            value = self.value_type.serialize(data[key], tuple(value_path))
            result[serialized_key] = value
        entries = self._validate(result, path)
        return entries


FieldT = TypeVar("FieldT")


@dataclass
class Field(Generic[FieldT]):
    """
    Field is a field in an object and contains object-related validation information.
    """
    type: AbstractType[FieldT]
    name: str = ""
    description: str = ""
    required: bool = True
    required_if: List[str] = frozenset([])
    required_if_not: List[str] = frozenset([])
    conflicts: List[str] = frozenset([])
    field_override: str = ""


ObjectT = TypeVar("ObjectT", bound=object)


@dataclass
class ObjectType(AbstractType, Generic[ObjectT]):
    """
    ObjectType represents an object with predefined fields. The property declaration must match the fields in the class.
    The type currently does not validate if the properties match the provided class.
    """

    cls: Type[ObjectT]
    properties: Dict[str, Field]

    def type_class(self) -> Type[ObjectT]:
        return self.cls

    def type_id(self) -> TypeID:
        return TypeID.OBJECT

    def unserialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> ObjectT:
        if not isinstance(data, dict):
            raise ConstraintException(path, "Must be a dict, got {}".format(type(data).__name__))
        kwargs = {}
        for key in data.keys():
            if key not in self.properties:
                raise ConstraintException(
                    path,
                    "Invalid parameter '{}', expected one of: {}".format(key, ", ".join(self.properties.keys()))
                )
        for property_id in self.properties.keys():
            object_property = self.properties[property_id]
            property_value: Optional[any] = None
            try:
                property_value = data[property_id]
            except KeyError:
                pass
            new_path = list(path)
            new_path.append(property_id)
            if property_value is not None:
                field_id = property_id
                if object_property.field_override != "":
                    field_id = object_property.field_override
                kwargs[field_id] = object_property.type.unserialize(property_value, tuple(new_path))

                for conflict in object_property.conflicts:
                    if conflict in data:
                        raise ConstraintException(
                            tuple(new_path),
                            "Field conflicts '{}', set one of the two, not both".format(conflict)
                        )
            else:
                self._validate_not_set(data, object_property, tuple(new_path))
        return self.cls(**kwargs)

    def validate(self, data: TypeT, path: typing.Tuple[str] = tuple([])):
        if not isinstance(data, self.cls):
            raise ConstraintException(
                path,
                "Must be an instance of {}, {} given".format(self.cls.__name__, type(data).__name__)
            )
        values = {}
        for property_id in self.properties.keys():
            property_field = self.properties[property_id]
            field_id = property_id
            if property_field.field_override != "":
                field_id = property_field.field_override
            new_path, value = self._validate_property(data, path, field_id, property_id)
            if value is not None:
                property_field.type.validate(value, tuple(new_path))
                values[property_id] = value
        for property_id in self.properties.keys():
            property_field = self.properties[property_id]
            new_path = list(path)
            new_path.append(property_id)
            if property_id in values.keys():
                for conflicts in property_field.conflicts:
                    if conflicts in values.keys():
                        raise ConstraintException(
                            tuple(new_path),
                            "Field conflicts with {}".format(conflicts)
                        )
            else:
                if property_field.required:
                    raise ConstraintException(
                        tuple(new_path),
                        "Field is required but not set"
                    )
                if len(property_field.required_if_not) > 0:
                    found = False
                    for required_if_not in property_field.required_if_not:
                        if required_if_not in values.keys():
                            found = True
                            break
                    if not found:
                        raise ConstraintException(
                            tuple(new_path),
                            "Field is required because none of '{}' are set".format(
                                "', '".join(property_field.required_if_not))
                        )

                for required_if in property_field.required_if:
                    if required_if in values.keys():
                        raise ConstraintException(
                            tuple(new_path),
                            "Field is required because none of '{}' are set".format(
                                "', '".join(property_field.required_if_not))
                        )

    def serialize(self, data: ObjectT, path: typing.Tuple[str] = tuple([])) -> Any:
        if not isinstance(data, self.cls):
            raise ConstraintException(
                path,
                "Must be an instance of {}, {} given".format(self.cls.__name__, type(data).__name__)
            )
        result = {}
        for property_id in self.properties.keys():
            field_id = property_id
            property_field = self.properties[property_id]
            if property_field.field_override != "":
                field_id = property_field.field_override
            new_path, value = self._validate_property(data, path, field_id, property_id)
            if value is not None:
                result[property_id] = property_field.type.serialize(getattr(data, field_id), tuple(new_path))
        return result

    def _validate_property(self, data: TypeT, path: typing.Tuple[str], field_id: str, property_id: str):
        new_path = list(path)
        new_path.append(property_id)
        value = getattr(data, field_id)
        property_field = self.properties[property_id]
        if value is None:
            self._validate_not_set(data, property_field, tuple(new_path))
        return new_path, value

    @staticmethod
    def _validate_not_set(data, object_property: Field, path: typing.Tuple[str]):
        if object_property.required:
            raise ConstraintException(
                path,
                "This field is required"
            )
        for required_if in object_property.required_if:
            if (isinstance(data, dict) and required_if in data) or \
                    (hasattr(data, required_if) and getattr(data, required_if) is None):
                raise ConstraintException(
                    path,
                    "This field is required because '{}' is set".format(required_if)
                )
        if len(object_property.required_if_not) > 0:
            none_set = True
            for required_if_not in object_property.required_if_not:
                if (isinstance(data, dict) and required_if_not in data) or \
                        (hasattr(data, required_if_not) and getattr(data, required_if_not) is not None):
                    none_set = False
                    break
            if none_set:
                if len(object_property.required_if_not) == 1:
                    raise ConstraintException(
                        path,
                        "This field is required because '{}' is not set".format(
                            object_property.required_if_not[0]
                        )
                    )
                raise ConstraintException(
                    path,
                    "This field is required because none of '{}' are set".format(
                        "', '".join(object_property.required_if_not)
                    )
                )


OneOfT = TypeVar("OneOfT", bound=object)
DiscriminatorT = TypeVar("DiscriminatorT", bound=typing.Union[str, int, Enum])


@dataclass
class OneOfType(AbstractType[OneOfT], Generic[OneOfT, DiscriminatorT]):
    """
    OneOfType is a type that can have multiple types of underlying objects. It only supports object types, and the
    differentiation is done based on a special discriminator field.

    Important rules:

    - One object type must appear only once.
    - If the discriminator field appears in the object type, it must have the same type as declared here, and must not
      be optional.
    - The discriminator field must be a string, int, or an enum.
    """

    discriminator_field_name: str
    discriminator_field_schema: AbstractType[DiscriminatorT]
    one_of: Dict[DiscriminatorT, ObjectType[OneOfT]]

    def __init__(
            self,
            discriminator_field_name: str,
            discriminator_field_schema: AbstractType[DiscriminatorT],
            one_of: Dict[DiscriminatorT, ObjectType[OneOfT]]
    ):
        self.discriminator_field_name = discriminator_field_name
        self.discriminator_field_schema = discriminator_field_schema
        self.one_of = one_of

    def type_id(self) -> TypeID:
        return TypeID.ONEOF

    def unserialize(self, data: Any, path: typing.Tuple[str] = tuple([])) -> OneOfT:
        if not isinstance(data, dict):
            raise ConstraintException(path, "Must be a dict, got {}".format(type(data).__name__))
        new_path = list(path)
        new_path.append(self.discriminator_field_name)
        if self.discriminator_field_name not in data:
            raise ConstraintException(tuple(new_path), "Required discriminator field not found")
        unserialized_discriminator_field = self.discriminator_field_schema.unserialize(
            data[self.discriminator_field_name], tuple(new_path)
        )
        if unserialized_discriminator_field not in self.one_of:
            raise ConstraintException(
                tuple(new_path),
                "Invalid value for field: '{}' expected one of: '{}'".format(
                    unserialized_discriminator_field,
                    "', '".join(list(self.one_of.keys()))
                )
            )
        sub_type = self.one_of[unserialized_discriminator_field]
        if self.discriminator_field_name not in sub_type.properties:
            del data[self.discriminator_field_name]
        return sub_type.unserialize(data, path)

    def validate(self, data: OneOfT, path: typing.Tuple[str] = tuple([])):
        types = []
        for discriminator, item_schema in self.one_of.items():
            types.append(item_schema.type_class().__name__)
            if isinstance(data, item_schema.type_class()):
                item_schema.validate(data)
                if self.discriminator_field_name in item_schema.properties:
                    if getattr(data, self.discriminator_field_name) != discriminator:
                        new_path = list(path)
                        new_path.append(self.discriminator_field_name)
                        raise ConstraintException(
                            tuple(new_path),
                            "Invalid value for '{}' on '{}', should be: '{}'".format(
                                self.discriminator_field_name,
                                item_schema.type_class().__name__,
                                discriminator
                            )
                        )
                return
        raise ConstraintException(
            tuple(path),
            "Invalid type: '{}', expected one of '{}'".format(
                type(data).__name__,
                "', '".join(types)
            )
        )

    def serialize(self, data: OneOfT, path: typing.Tuple[str] = tuple([])) -> Any:
        types = []
        for discriminator, item_schema in self.one_of.items():
            types.append(item_schema.type_class().__name__)
            if isinstance(data, item_schema.type_class()):
                serialized_data = item_schema.serialize(data)
                if self.discriminator_field_name in item_schema.properties:
                    if getattr(data, self.discriminator_field_name) != discriminator:
                        new_path = list(path)
                        new_path.append(self.discriminator_field_name)
                        raise ConstraintException(
                            tuple(new_path),
                            "Invalid value for '{}' on '{}', should be: '{}'".format(
                                self.discriminator_field_name,
                                item_schema.type_class().__name__,
                                discriminator
                            )
                        )
                else:
                    serialized_data[self.discriminator_field_name] = self.discriminator_field_schema.serialize(
                        discriminator
                    )
                return serialized_data
        raise ConstraintException(
            tuple(path),
            "Invalid type: '{}', expected one of '{}'".format(
                type(data).__name__,
                "', '".join(types)
            )
        )


StepInputT = TypeVar("StepInputT", bound=object)
StepOutputT = TypeVar("StepOutputT", bound=object)


@dataclass
class StepSchema(Generic[StepInputT]):
    """
    StepSchema describes the schema for a single step. The input is always one ObjectType, while there are multiple
    possible outputs identified by a string.
    """

    id: str
    name: str
    description: str
    input: ObjectType[StepInputT]
    outputs: Dict[str, ObjectType]
    handler: Callable[[StepInputT], typing.Tuple[str, StepOutputT]]

    def __call__(
            self,
            params: StepInputT,
            skip_input_validation: bool = False,
            skip_output_validation: bool = False,
    ) -> typing.Tuple[str, StepOutputT]:
        """
        This function executes
        :param params: Input parameter for the step.
        :param skip_input_validation: Do not perform input data type validation. Use at your own risk.
        :param skip_output_validation: Do not validate returned output data. Use at your own risk.
        :return: The ID for the output datatype, and the output itself.
        """
        if not skip_input_validation:
            self.input.validate(params, tuple([self.name, "input"]))
        output_id, output_data = self.handler(params)
        if output_id not in self.outputs:
            raise BadArgumentException(
                "The step '%s' (%s) returned an undeclared output ID: %s, please return one of: '%s'" % (
                    self.name,
                    self.id,
                    output_id,
                    "', '".join(self.outputs.keys())
                )
            )
        if not skip_output_validation:
            self.outputs[output_id].validate(output_data, tuple([self.id, "output", output_id]))
        return output_id, output_data


class InvalidInputException(Exception):
    """
    This exception indicates that the input data for a given step didn't match the schema.
    """
    constraint: ConstraintException

    def __init__(self, cause: ConstraintException):
        self.constraint = cause

    def __str__(self):
        return self.constraint.__str__()


class InvalidOutputException(Exception):
    """
    This exception indicates that the output of a schema was invalid. This is always a bug in the plugin and should
    be reported to the plugin author.
    """
    constraint: ConstraintException

    def __init__(self, cause: ConstraintException):
        self.constraint = cause

    def __str__(self):
        return self.constraint.__str__()


@dataclass
class Schema:
    """
    A schema is a definition of one or more steps that can be executed. The step has a defined input and output schema.
    """
    steps: Dict[str, StepSchema]

    def unserialize_input(self, step_id: str, data: Any) -> Any:
        """
        This function unserializes the input from a raw data to data structures, such as dataclasses. This function is
        automatically called by __call__ before running the step with the unserialized input.
        :param step_id: The step ID to use to look up the schema for unserialization.
        :param data: The raw data to unserialize.
        :return: The unserialized data in the structure the step expects it.
        """
        if step_id not in self.steps:
            raise NoSuchStepException(step_id)
        step = self.steps[step_id]
        return self._unserialize_input(step, data)

    @staticmethod
    def _unserialize_input(step: StepSchema, data: Any) -> Any:
        try:
            return step.input.unserialize(data)
        except ConstraintException as e:
            raise InvalidInputException(e) from e

    def call_step(self, step_id: str, input_param: Any) -> typing.Tuple[str, Any]:
        """
        This function calls a specific step with the input parameter that has already been unserialized. It expects the
        data to be already valid, use unserialize_input to produce a valid input. This function is automatically called
        by __call__ after unserializing the input.
        :param step_id: The ID of the input step to run.
        :param input_param: The unserialized data structure the step expects.
        :return: The ID of the output, and the data structure returned from the step.
        """
        if step_id not in self.steps:
            raise NoSuchStepException(step_id)
        step = self.steps[step_id]
        return self._call_step(step, input_param)

    @staticmethod
    def _call_step(
            step: StepSchema,
            input_param: Any,
            skip_input_validation: bool = False,
            skip_output_validation: bool = False,
    ) -> typing.Tuple[str, Any]:
        return step(
            input_param,
            skip_input_validation=skip_input_validation,
            skip_output_validation=skip_output_validation,
        )

    def serialize_output(self, step_id: str, output_id: str, output_data: Any) -> Any:
        """
        This function takes an output ID (e.g. "error") and structured output_data and serializes them into a format
        suitable for wire transport. This function is automatically called by __call__ after the step is run.
        :param step_id: The step ID to use to look up the schema for serialization.
        :param output_id: The string identifier for the output data structure.
        :param output_data: The data structure returned from the step.
        :return:
        """
        if step_id not in self.steps:
            raise NoSuchStepException(step_id)
        step = self.steps[step_id]
        return self._serialize_output(step, output_id, output_data)

    @staticmethod
    def _serialize_output(step, output_id: str, output_data: Any) -> Any:
        try:
            return step.outputs[output_id].serialize(output_data)
        except ConstraintException as e:
            raise InvalidOutputException(e) from e

    def __call__(self, step_id: str, data: Any, skip_serialization: bool = False) -> typing.Tuple[str, Any]:
        """
        This function takes the input data, unserializes it for the specified step, calls the specified step, and,
        unless skip_serialization is set, serializes the return data.
        :param step_id: the step to execute
        :param data: input data
        :param skip_serialization: skip result serialization to basic types
        :return: the result ID, and the resulting data in the structure matching the result ID
        """
        if step_id not in self.steps:
            raise NoSuchStepException(step_id)
        step = self.steps[step_id]
        input_param = self._unserialize_input(step, data)
        output_id, output_data = self._call_step(
            step,
            input_param,
            # Skip duplicate verification
            skip_input_validation=True,
            skip_output_validation=True,
        )
        if skip_serialization:
            step.outputs[output_id].validate(output_data)
            return output_id, output_data
        serialized_output_data = self._serialize_output(step, output_id, output_data)
        return output_id, serialized_output_data
