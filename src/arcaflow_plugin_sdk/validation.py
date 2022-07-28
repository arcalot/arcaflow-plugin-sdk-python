import typing
from re import Pattern
from typing import Callable, TypeVar
from arcaflow_plugin_sdk.schema import AbstractType, TypeID, BadArgumentException, StringType, IntType, ListType, \
    Field

ValidatorT = TypeVar("ValidatorT", bound=typing.Union[AbstractType, Field])

Validator = Callable[[ValidatorT], ValidatorT]


def min(param: int) -> Validator:
    """
    This decorator creates a minimum length (strings), minimum number (int, float), or minimum element count (lists and
    maps) validation.
    :param param: The minimum number
    :return: the validator
    """
    def call(t: typing.Union[AbstractType, Field]) -> typing.Union[AbstractType, Field]:
        effective_t: AbstractType = t
        if isinstance(t, Field):
            effective_t = t.type
        if effective_t.type_id() == TypeID.STRING:
            effective_t.min_length = param
        elif effective_t.type_id() == TypeID.INT:
            effective_t.min = param
        elif effective_t.type_id() == TypeID.FLOAT:
            effective_t.min = param
        elif effective_t.type_id() == TypeID.LIST:
            effective_t.min = param
        elif effective_t.type_id() == TypeID.MAP:
            effective_t.min = param
        else:
            raise BadArgumentException(
                "min is valid only for STRING, INT, FLOAT, LIST, and MAP types, not for %s types." % t.type_id()
            )
        if isinstance(t, Field):
            t.type = effective_t
        return t

    return call


def max(param: int) -> Validator:
    """
    This decorator creates a maximum length (strings), maximum number (int, float), or maximum element count (lists and
    maps) validation.
    :param param: The maximum number
    :return: the validator
    """
    def call(t: typing.Union[AbstractType, Field]) -> typing.Union[AbstractType, Field]:
        effective_t: AbstractType = t
        if isinstance(t, Field):
            effective_t = t.type
        if effective_t.type_id() == TypeID.STRING:
            effective_t.max_length = param
        elif effective_t.type_id() == TypeID.INT:
            effective_t.max = param
        elif effective_t.type_id() == TypeID.FLOAT:
            effective_t.max = param
        elif effective_t.type_id() == TypeID.LIST:
            effective_t.max = param
        elif effective_t.type_id() == TypeID.MAP:
            effective_t.max = param
        else:
            raise BadArgumentException(
                "max is valid only for STRING, INT, FLOAT, LIST, and MAP types, not for %s types." % t.type_id()
            )
        if isinstance(t, Field):
            t.type = effective_t
        return t

    return call


def pattern(pattern: Pattern) -> Validator:
    """
    This decorator creates a regular expression pattern validation for strings.
    :param pattern: The regular expression.
    :return: the validator
    """
    def call(t: typing.Union[AbstractType, Field]) -> typing.Union[AbstractType, Field]:
        effective_t: AbstractType = t
        if isinstance(t, Field):
            effective_t = t.type
        if effective_t.type_id() == TypeID.STRING:
            effective_t.pattern = pattern
        else:
            raise BadArgumentException("pattern is valid only for STRING types, not for %s types." % t.type_id())
        if isinstance(t, Field):
            t.type = effective_t
        return t

    return call


def required_if(required_if: str) -> Validator:
    """
    This decorator creates a that marks the current field as required if the specified field is set.
    :param required_if: The other field to use.
    :return: the validator
    """
    def call(t: typing.Union[AbstractType, Field]) -> typing.Union[AbstractType, Field]:
        if not isinstance(t, Field):
            raise BadArgumentException("required_if is only valid for fields on object types.")
        require_if_list = list(t.required_if)
        require_if_list.append(required_if)
        t.required_if = require_if_list
        return t

    return call


def required_if_not(required_if_not: str) -> Validator:
    """
    This decorator creates a validation that marks the current field as required if the specified field is not set. If
    there are multiple of these validators, the current field is only marked as required if none of the specified fields
    are provided.
    :param required_if_not: The other field to use.
    :return: the validator
    """
    def call(t: typing.Union[AbstractType, Field]) -> typing.Union[AbstractType, Field]:
        if not isinstance(t, Field):
            raise BadArgumentException("required_if_not is only valid for fields on object types.")
        required_if_not_list = list(t.required_if_not)
        required_if_not_list.append(required_if_not)
        t.required_if_not = required_if_not_list
        return t

    return call


def conflicts(conflicts: str) -> Validator:
    """
    This decorator creates a validation that triggers if the current field on an object is set in parallel with the
    specified field.
    :param conflicts: The field to conflict with.
    :return: the validator
    """
    def call(t: typing.Union[AbstractType, Field]) -> typing.Union[AbstractType, Field]:
        if not isinstance(t, Field):
            raise BadArgumentException("conflicts is only valid for fields on object types.")
        conflicts_list = list(t.conflicts)
        conflicts_list.append(conflicts)
        t.conflicts = conflicts_list
        return t

    return call
