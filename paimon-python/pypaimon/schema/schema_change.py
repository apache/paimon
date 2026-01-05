from abc import ABC
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Union

from pypaimon.common.json_util import json_field
from pypaimon.schema.data_types import DataType


class Actions:
    FIELD_ACTION = "action"
    SET_OPTION_ACTION = "setOption"
    REMOVE_OPTION_ACTION = "removeOption"
    UPDATE_COMMENT_ACTION = "updateComment"
    ADD_COLUMN_ACTION = "addColumn"
    RENAME_COLUMN_ACTION = "renameColumn"
    DROP_COLUMN_ACTION = "dropColumn"
    UPDATE_COLUMN_TYPE_ACTION = "updateColumnType"
    UPDATE_COLUMN_NULLABILITY_ACTION = "updateColumnNullability"
    UPDATE_COLUMN_COMMENT_ACTION = "updateColumnComment"
    UPDATE_COLUMN_DEFAULT_VALUE_ACTION = "updateColumnDefaultValue"
    UPDATE_COLUMN_POSITION_ACTION = "updateColumnPosition"


class SchemaChange(ABC):
    @staticmethod
    def set_option(key: str, value: str) -> "SetOption":
        return SetOption(key=key, value=value)

    @staticmethod
    def remove_option(key: str) -> "RemoveOption":
        return RemoveOption(key)

    @staticmethod
    def update_comment(comment: Optional[str]) -> "UpdateComment":
        return UpdateComment(comment)

    @staticmethod
    def add_column(field_name: Union[str, List[str]], data_type: DataType, comment: Optional[str] = None, move: Optional["Move"] = None) -> "AddColumn":
        if isinstance(field_name, str):
            return AddColumn(field_names=[field_name], data_type=data_type, comment=comment, move=move)
        else:
            return AddColumn(field_names=field_name, data_type=data_type, comment=comment, move=move)

    @staticmethod
    def rename_column(field_name: Union[str, List[str]], new_name: str) -> "RenameColumn":
        if isinstance(field_name, str):
            return RenameColumn([field_name], new_name)
        else:
            return RenameColumn(field_name, new_name)

    @staticmethod
    def drop_column(field_name: Union[str, List[str]]) -> "DropColumn":
        if isinstance(field_name, str):
            return DropColumn([field_name])
        else:
            return DropColumn(field_name)

    @staticmethod
    def update_column_type(field_name: Union[str, List[str]], new_data_type: DataType, keep_nullability: bool = False) -> "UpdateColumnType":
        if isinstance(field_name, str):
            return UpdateColumnType([field_name], new_data_type, keep_nullability)
        else:
            return UpdateColumnType(field_name, new_data_type, keep_nullability)

    @staticmethod
    def update_column_nullability(field_name: Union[str, List[str]], new_nullability: bool) -> "UpdateColumnNullability":
        if isinstance(field_name, str):
            return UpdateColumnNullability([field_name], new_nullability)
        else:
            return UpdateColumnNullability(field_name, new_nullability)

    @staticmethod
    def update_column_comment(field_name: Union[str, List[str]], comment: str) -> "UpdateColumnComment":
        if isinstance(field_name, str):
            return UpdateColumnComment([field_name], comment)
        else:
            return UpdateColumnComment(field_name, comment)

    @staticmethod
    def update_column_default_value(field_names: List[str], default_value: str) -> "UpdateColumnDefaultValue":
        return UpdateColumnDefaultValue(field_names, default_value)

    @staticmethod
    def update_column_position(move: "Move") -> "UpdateColumnPosition":
        return UpdateColumnPosition(move)


@dataclass
class SetOption(SchemaChange):
    FIELD_KEY = "key"
    FIELD_VALUE = "value"
    key: str = json_field(FIELD_KEY)
    value: str = json_field(FIELD_VALUE)
    action: str = json_field(Actions.FIELD_ACTION, default=Actions.SET_OPTION_ACTION)

    def __post_init__(self):
        if not hasattr(self, 'action') or self.action is None:
            self.action = Actions.SET_OPTION_ACTION


@dataclass
class RemoveOption(SchemaChange):
    FIELD_KEY = "key"
    key: str = json_field(FIELD_KEY)
    action: str = json_field(Actions.FIELD_ACTION, default=Actions.REMOVE_OPTION_ACTION)

    def __post_init__(self):
        if not hasattr(self, 'action') or self.action is None:
            self.action = Actions.REMOVE_OPTION_ACTION


@dataclass
class UpdateComment(SchemaChange):
    FIELD_COMMENT = "comment"
    comment: Optional[str] = json_field(FIELD_COMMENT)
    action: str = json_field(Actions.FIELD_ACTION, default=Actions.UPDATE_COMMENT_ACTION)

    def __post_init__(self):
        if not hasattr(self, 'action') or self.action is None:
            self.action = Actions.UPDATE_COMMENT_ACTION


@dataclass
class AddColumn(SchemaChange):
    FIELD_FIELD_NAMES = "fieldNames"
    FIELD_DATA_TYPE = "dataType"
    FIELD_COMMENT = "comment"
    FIELD_MOVE = "move"
    field_names: List[str] = json_field(FIELD_FIELD_NAMES)
    data_type: DataType = json_field(FIELD_DATA_TYPE)
    comment: Optional[str] = json_field(FIELD_COMMENT)
    move: Optional["Move"] = json_field(FIELD_MOVE)
    action: str = json_field(Actions.FIELD_ACTION, default=Actions.ADD_COLUMN_ACTION)

    def __post_init__(self):
        if not hasattr(self, 'action') or self.action is None:
            self.action = Actions.ADD_COLUMN_ACTION


@dataclass
class RenameColumn(SchemaChange):
    FIELD_FIELD_NAMES = "fieldNames"
    FIELD_NEW_NAME = "newName"
    field_names: List[str] = json_field(FIELD_FIELD_NAMES)
    new_name: str = json_field(FIELD_NEW_NAME)
    action: str = json_field(Actions.FIELD_ACTION, default=Actions.RENAME_COLUMN_ACTION)

    def __post_init__(self):
        if not hasattr(self, 'action') or self.action is None:
            self.action = Actions.RENAME_COLUMN_ACTION


@dataclass
class DropColumn(SchemaChange):
    FIELD_FIELD_NAMES = "fieldNames"
    field_names: List[str] = json_field(FIELD_FIELD_NAMES)
    action: str = json_field(Actions.FIELD_ACTION, default=Actions.DROP_COLUMN_ACTION)

    def __post_init__(self):
        if not hasattr(self, 'action') or self.action is None:
            self.action = Actions.DROP_COLUMN_ACTION


@dataclass
class UpdateColumnType(SchemaChange):
    FIELD_FIELD_NAMES = "fieldNames"
    FIELD_NEW_DATA_TYPE = "newDataType"
    FIELD_KEEP_NULLABILITY = "keepNullability"
    field_names: List[str] = json_field(FIELD_FIELD_NAMES)
    new_data_type: DataType = json_field(FIELD_NEW_DATA_TYPE)
    keep_nullability: bool = json_field(FIELD_KEEP_NULLABILITY)
    action: str = json_field(Actions.FIELD_ACTION, default=Actions.UPDATE_COLUMN_TYPE_ACTION)

    def __post_init__(self):
        if not hasattr(self, 'action') or self.action is None:
            self.action = Actions.UPDATE_COLUMN_TYPE_ACTION


@dataclass
class UpdateColumnNullability(SchemaChange):
    FIELD_FIELD_NAMES = "fieldNames"
    FIELD_NEW_NULLABILITY = "newNullability"
    field_names: List[str] = json_field(FIELD_FIELD_NAMES)
    new_nullability: bool = json_field(FIELD_NEW_NULLABILITY)
    action: str = json_field(Actions.FIELD_ACTION, default=Actions.UPDATE_COLUMN_NULLABILITY_ACTION)

    def __post_init__(self):
        if not hasattr(self, 'action') or self.action is None:
            self.action = Actions.UPDATE_COLUMN_NULLABILITY_ACTION


@dataclass
class UpdateColumnComment(SchemaChange):
    FIELD_FIELD_NAMES = "fieldNames"
    FIELD_NEW_COMMENT = "newComment"
    field_names: List[str] = json_field(FIELD_FIELD_NAMES)
    new_comment: str = json_field(FIELD_NEW_COMMENT)
    action: str = json_field(Actions.FIELD_ACTION, default=Actions.UPDATE_COLUMN_COMMENT_ACTION)

    def __post_init__(self):
        if not hasattr(self, 'action') or self.action is None:
            self.action = Actions.UPDATE_COLUMN_COMMENT_ACTION


@dataclass
class UpdateColumnDefaultValue(SchemaChange):
    FIELD_FIELD_NAMES = "fieldNames"
    FIELD_NEW_DEFAULT_VALUE = "newDefaultValue"
    field_names: List[str] = json_field(FIELD_FIELD_NAMES)
    new_default_value: str = json_field(FIELD_NEW_DEFAULT_VALUE)
    action: str = json_field(Actions.FIELD_ACTION, default=Actions.UPDATE_COLUMN_DEFAULT_VALUE_ACTION)

    def __post_init__(self):
        if not hasattr(self, 'action') or self.action is None:
            self.action = Actions.UPDATE_COLUMN_DEFAULT_VALUE_ACTION


@dataclass
class UpdateColumnPosition(SchemaChange):
    FIELD_MOVE = "move"
    move: "Move" = json_field(FIELD_MOVE)
    action: str = json_field(Actions.FIELD_ACTION, default=Actions.UPDATE_COLUMN_POSITION_ACTION)

    def __post_init__(self):
        if not hasattr(self, 'action') or self.action is None:
            self.action = Actions.UPDATE_COLUMN_POSITION_ACTION


class MoveType(Enum):
    FIRST = "FIRST"
    AFTER = "AFTER"
    BEFORE = "BEFORE"
    LAST = "LAST"


@dataclass
class Move:
    FIELD_FIELD_NAME = "fieldName"
    FIELD_REFERENCE_FIELD_NAME = "referenceFieldName"
    FIELD_TYPE = "type"

    @staticmethod
    def first(field_name: str) -> "Move":
        return Move(field_name, None, MoveType.FIRST)

    @staticmethod
    def after(field_name: str, reference_field_name: str) -> "Move":
        return Move(field_name, reference_field_name, MoveType.AFTER)

    @staticmethod
    def before(field_name: str, reference_field_name: str) -> "Move":
        return Move(field_name, reference_field_name, MoveType.BEFORE)

    @staticmethod
    def last(field_name: str) -> "Move":
        return Move(field_name, None, MoveType.LAST)

    field_name: str = json_field(FIELD_FIELD_NAME)
    reference_field_name: Optional[str] = json_field(FIELD_REFERENCE_FIELD_NAME)
    type: MoveType = json_field(FIELD_TYPE)

