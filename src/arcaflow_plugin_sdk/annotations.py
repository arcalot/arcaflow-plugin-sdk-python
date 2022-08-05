import pprint
import typing
from typing import TypeVar

from arcaflow_plugin_sdk import schema

from arcaflow_plugin_sdk.schema import BadArgumentException, OneOfType, ObjectType, OneOfT, ConstraintException

DiscriminatorT = TypeVar("DiscriminatorT", bound=OneOfType)
Discriminator = typing.Callable[[OneOfType], OneOfType]


def discriminator(discriminator_field_name: str) -> Discriminator:
    """
    This annotation is used to manually set the discriminator field on a Union type.

    For example:

    typing.Annotated[typing.Union[A, B], annotations.discriminator("my_discriminator")]

    :param discriminator_field_name: the name of the discriminator field.
    :return: the callable decorator
    """
    def call(t: OneOfType) -> OneOfType:
        if not isinstance(t, OneOfType):
            raise BadArgumentException("discriminator is only valid for fields on object types with union members")
        oneof: OneOfType = t

        one_of: typing.Dict[DiscriminatorT, ObjectType[OneOfT]] = {}
        discriminator_field_schema: typing.Optional[schema.AbstractType] = None
        for key, item in oneof.one_of.items():
            if discriminator_field_name in item.properties:
                if discriminator_field_schema is not None and item.properties[discriminator_field_name].type.type_id() != discriminator_field_schema.type_id():
                    raise BadArgumentException(
                        "Discriminator field mismatch, the discriminator field must have the same type across all "
                        "dataclasses in a Union type."
                    )
                discriminator_field_schema = item.properties[discriminator_field_name].type
            if hasattr(item, "__discriminator_value"):
                one_of[item.__discriminator_value] = item
            else:
                one_of[key] = item

        if discriminator_field_schema is None:
            discriminator_field_schema = t.discriminator_field_schema

        oneof = schema.OneOfType(
            discriminator_field_name,
            discriminator_field_schema,
            one_of,
        )
        for key, item in oneof.one_of.items():
            try:
                discriminator_field_schema.validate(key)
            except ConstraintException as e:
                raise BadArgumentException(
                    "The discriminator value for {} has an invalid value: {}. "
                    "Please check your annotations.".format(
                        item.cls.__name__,
                        e.__str__()
                    )
                ) from e

        return oneof

    return call


def discriminator_value(discriminator_value):
    """
    This annotation adds a custom value for an instance of a discriminator. The value must match the discriminator field
    schema. This annotation works only when used in conjunction with discriminator().

    For example:

    typing.Annotated[typing.Union[A
        typing.Annotated[A, annotations.discriminator_value("foo"),
        typing.Annotated[B, annotations.discriminator_value("bar")
    ], annotations.discriminator("my_discriminator")]

    :param discriminator_value: The value for the discriminator field.
    :return: The callable decorator
    """
    def call(t):
        if not isinstance(t, ObjectType):
            raise BadArgumentException(
                "discriminator_value is only valid for object types, not {}".format(type(t).__name__)
            )
        t.__discriminator_value = discriminator_value
        return t

    return call
