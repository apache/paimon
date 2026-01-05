################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
from typing import Optional, List

from pypaimon.catalog.catalog_exception import ColumnAlreadyExistException, ColumnNotExistException
from pypaimon.common.file_io import FileIO
from pypaimon.common.json_util import JSON
from pypaimon.schema.data_types import AtomicInteger, DataField
from pypaimon.schema.schema import Schema
from pypaimon.schema.schema_change import (
    AddColumn, DropColumn, RemoveOption, RenameColumn,
    SchemaChange, SetOption, UpdateColumnComment,
    UpdateColumnNullability, UpdateColumnPosition,
    UpdateColumnType, UpdateComment
)
from pypaimon.schema.table_schema import TableSchema


def _find_field_index(fields: List[DataField], field_name: str) -> Optional[int]:
    for i, field in enumerate(fields):
        if field.name == field_name:
            return i
    return None


def _get_rename_mappings(changes: List[SchemaChange]) -> dict:
    rename_mappings = {}
    for change in changes:
        if isinstance(change, RenameColumn) and len(change.field_names) == 1:
            rename_mappings[change.field_names[0]] = change.new_name
    return rename_mappings


def _handle_update_column_comment(
        change: UpdateColumnComment, new_fields: List[DataField]
):
    field_name = change.field_names[-1]
    field_index = _find_field_index(new_fields, field_name)
    if field_index is None:
        raise ColumnNotExistException(field_name)
    field = new_fields[field_index]
    new_fields[field_index] = DataField(
        field.id, field.name, field.type, change.new_comment, field.default_value
    )


def _handle_update_column_nullability(
        change: UpdateColumnNullability, new_fields: List[DataField]
):
    field_name = change.field_names[-1]
    field_index = _find_field_index(new_fields, field_name)
    if field_index is None:
        raise ColumnNotExistException(field_name)
    field = new_fields[field_index]
    from pypaimon.schema.data_types import DataTypeParser
    field_type_dict = field.type.to_dict()
    new_type = DataTypeParser.parse_data_type(field_type_dict)
    new_type.nullable = change.new_nullability
    new_fields[field_index] = DataField(
        field.id, field.name, new_type, field.description, field.default_value
    )


def _handle_update_column_type(
        change: UpdateColumnType, new_fields: List[DataField]
):
    field_name = change.field_names[-1]
    field_index = _find_field_index(new_fields, field_name)
    if field_index is None:
        raise ColumnNotExistException(field_name)
    field = new_fields[field_index]
    from pypaimon.schema.data_types import DataTypeParser
    new_type_dict = change.new_data_type.to_dict()
    new_type = DataTypeParser.parse_data_type(new_type_dict)
    if change.keep_nullability:
        new_type.nullable = field.type.nullable
    new_fields[field_index] = DataField(
        field.id, field.name, new_type, field.description, field.default_value
    )


def _handle_drop_column(change: DropColumn, new_fields: List[DataField]):
    field_name = change.field_names[-1]
    field_index = _find_field_index(new_fields, field_name)
    if field_index is None:
        raise ColumnNotExistException(field_name)
    new_fields.pop(field_index)
    if not new_fields:
        raise ValueError("Cannot drop all fields in table")


def _assert_not_updating_partition_keys(
        schema: 'TableSchema', field_names: List[str], operation: str):
    if len(field_names) > 1:
        return
    field_name = field_names[0]
    if field_name in schema.partition_keys:
        raise ValueError(
            f"Cannot {operation} partition column: [{field_name}]"
        )


def _assert_not_updating_primary_keys(
        schema: 'TableSchema', field_names: List[str], operation: str):
    if len(field_names) > 1:
        return
    field_name = field_names[0]
    if field_name in schema.primary_keys:
        raise ValueError(f"Cannot {operation} primary key")


def _handle_rename_column(change: RenameColumn, new_fields: List[DataField]):
    field_name = change.field_names[-1]
    new_name = change.new_name
    field_index = _find_field_index(new_fields, field_name)
    if field_index is None:
        raise ColumnNotExistException(field_name)
    if _find_field_index(new_fields, new_name) is not None:
        raise ColumnAlreadyExistException(new_name)
    field = new_fields[field_index]
    new_fields[field_index] = DataField(
        field.id, new_name, field.type, field.description, field.default_value
    )


def _apply_move(fields: List[DataField], new_field: Optional[DataField], move):
    from pypaimon.schema.schema_change import MoveType

    if new_field:
        pass
    else:
        field_name = move.field_name
        new_field = None
        for i, field in enumerate(fields):
            if field.name == field_name:
                new_field = fields.pop(i)
                break
        if new_field is None:
            raise ColumnNotExistException(field_name)

    field_map = {field.name: i for i, field in enumerate(fields)}
    if move.type == MoveType.FIRST:
        fields.insert(0, new_field)
    elif move.type == MoveType.AFTER:
        if move.reference_field_name not in field_map:
            raise ColumnNotExistException(move.reference_field_name)
        fields.insert(field_map[move.reference_field_name] + 1, new_field)
    elif move.type == MoveType.BEFORE:
        if move.reference_field_name not in field_map:
            raise ColumnNotExistException(move.reference_field_name)
        fields.insert(field_map[move.reference_field_name], new_field)
    elif move.type == MoveType.LAST:
        fields.append(new_field)
    else:
        raise ValueError(f"Unsupported move type: {move.type}")


def _handle_add_column(
        change: AddColumn, new_fields: List[DataField], highest_field_id: AtomicInteger
):
    if not change.data_type.nullable:
        raise ValueError(
            f"Column {'.'.join(change.field_names)} cannot specify NOT NULL in the table."
        )
    field_id = highest_field_id.increment_and_get()
    field_name = change.field_names[-1]
    if _find_field_index(new_fields, field_name) is not None:
        raise ColumnAlreadyExistException(field_name)
    new_field = DataField(field_id, field_name, change.data_type, change.comment)
    if change.move:
        _apply_move(new_fields, new_field, change.move)
    else:
        new_fields.append(new_field)


class SchemaManager:

    def __init__(self, file_io: FileIO, table_path: str):
        self.schema_prefix = "schema-"
        self.file_io = file_io
        self.table_path = table_path
        self.schema_path = f"{table_path.rstrip('/')}/schema"
        self.schema_cache = {}

    def latest(self) -> Optional['TableSchema']:
        try:
            versions = self._list_versioned_files()
            if not versions:
                return None

            max_version = max(versions)
            return self.get_schema(max_version)
        except Exception as e:
            raise RuntimeError(f"Failed to load schema from path: {self.schema_path}") from e

    def create_table(self, schema: Schema) -> TableSchema:
        while True:
            latest = self.latest()
            if latest is not None:
                raise RuntimeError("Schema in filesystem exists, creation is not allowed.")

            table_schema = TableSchema.from_schema(schema_id=0, schema=schema)
            success = self.commit(table_schema)
            if success:
                return table_schema

    def commit(self, new_schema: TableSchema) -> bool:
        schema_path = self._to_schema_path(new_schema.id)
        try:
            result = self.file_io.try_to_write_atomic(schema_path, JSON.to_json(new_schema, indent=2))
            if result:
                self.schema_cache[new_schema.id] = new_schema
            return result
        except Exception as e:
            raise RuntimeError(f"Failed to commit schema: {e}") from e

    def _to_schema_path(self, schema_id: int) -> str:
        return f"{self.schema_path.rstrip('/')}/{self.schema_prefix}{schema_id}"

    def get_schema(self, schema_id: int) -> Optional[TableSchema]:
        if schema_id not in self.schema_cache:
            schema_path = self._to_schema_path(schema_id)
            if not self.file_io.exists(schema_path):
                return None
            schema = TableSchema.from_path(self.file_io, schema_path)
            self.schema_cache[schema_id] = schema
        return self.schema_cache[schema_id]

    def _list_versioned_files(self) -> List[int]:
        if not self.file_io.exists(self.schema_path):
            return []

        statuses = self.file_io.list_status(self.schema_path)
        if statuses is None:
            return []

        versions = []
        for status in statuses:
            name = status.path.split('/')[-1]
            if name.startswith(self.schema_prefix):
                try:
                    version = int(name[len(self.schema_prefix):])
                    versions.append(version)
                except ValueError:
                    continue
        return versions

    def commit_changes(self, changes: List[SchemaChange]) -> TableSchema:
        while True:
            old_table_schema = self.latest()
            if old_table_schema is None:
                raise RuntimeError(
                    f"Table schema does not exist at path: {self.table_path}. "
                    "This may happen if the table was deleted concurrently."
                )
            
            new_table_schema = self._generate_table_schema(old_table_schema, changes)
            try:
                success = self.commit(new_table_schema)
                if success:
                    return new_table_schema
            except Exception as e:
                raise RuntimeError(f"Failed to commit schema changes: {e}") from e

    def _generate_table_schema(
        self, old_table_schema: TableSchema, changes: List[SchemaChange]
    ) -> TableSchema:
        new_options = dict(old_table_schema.options)
        new_fields = []
        for field in old_table_schema.fields:
            from pypaimon.schema.data_types import DataTypeParser
            field_type_dict = field.type.to_dict()
            copied_type = DataTypeParser.parse_data_type(field_type_dict)
            new_fields.append(DataField(
                field.id,
                field.name,
                copied_type,
                field.description,
                field.default_value
            ))
        highest_field_id = AtomicInteger(old_table_schema.highest_field_id)
        new_comment = old_table_schema.comment

        for change in changes:
            if isinstance(change, SetOption):
                new_options[change.key] = change.value
            elif isinstance(change, RemoveOption):
                new_options.pop(change.key, None)
            elif isinstance(change, UpdateComment):
                new_comment = change.comment
            elif isinstance(change, AddColumn):
                _handle_add_column(change, new_fields, highest_field_id)
            elif isinstance(change, RenameColumn):
                _assert_not_updating_partition_keys(
                    old_table_schema, change.field_names, "rename"
                )
                _handle_rename_column(change, new_fields)
            elif isinstance(change, DropColumn):
                _handle_drop_column(change, new_fields)
            elif isinstance(change, UpdateColumnType):
                _assert_not_updating_partition_keys(
                    old_table_schema, change.field_names, "update"
                )
                _assert_not_updating_primary_keys(
                    old_table_schema, change.field_names, "update"
                )
                _handle_update_column_type(change, new_fields)
            elif isinstance(change, UpdateColumnNullability):
                if change.new_nullability:
                    _assert_not_updating_primary_keys(
                        old_table_schema, change.field_names, "change nullability of"
                    )
                _handle_update_column_nullability(change, new_fields)
            elif isinstance(change, UpdateColumnComment):
                _handle_update_column_comment(change, new_fields)
            elif isinstance(change, UpdateColumnPosition):
                _apply_move(new_fields, None, change.move)
            else:
                raise NotImplementedError(f"Unsupported change: {type(change)}")

        rename_mappings = _get_rename_mappings(changes)
        new_primary_keys = SchemaManager._apply_not_nested_column_rename(
            old_table_schema.primary_keys, rename_mappings
        )
        new_options = SchemaManager._apply_rename_columns_to_options(new_options, rename_mappings)

        new_schema = Schema(
            fields=new_fields,
            partition_keys=old_table_schema.partition_keys,
            primary_keys=new_primary_keys,
            options=new_options,
            comment=new_comment
        )

        return TableSchema(
            version=old_table_schema.version,
            id=old_table_schema.id + 1,
            fields=new_schema.fields,
            highest_field_id=highest_field_id.get(),
            partition_keys=new_schema.partition_keys,
            primary_keys=new_schema.primary_keys,
            options=new_schema.options,
            comment=new_schema.comment
        )

    @staticmethod
    def _apply_not_nested_column_rename(
        columns: List[str], rename_mappings: dict
    ) -> List[str]:
        return [rename_mappings.get(col, col) for col in columns]

    @staticmethod
    def _apply_rename_columns_to_options(
        options: dict, rename_mappings: dict
    ) -> dict:
        if not rename_mappings:
            return options
        new_options = dict(options)
        from pypaimon.common.options.core_options import CoreOptions

        bucket_key = CoreOptions.BUCKET_KEY.key()
        if bucket_key in new_options:
            bucket_columns = new_options[bucket_key].split(",")
            new_bucket_columns = SchemaManager._apply_not_nested_column_rename(
                bucket_columns, rename_mappings
            )
            new_options[bucket_key] = ",".join(new_bucket_columns)

        sequence_field = "sequence.field"
        if sequence_field in new_options:
            sequence_fields = new_options[sequence_field].split(",")
            new_sequence_fields = SchemaManager._apply_not_nested_column_rename(
                sequence_fields, rename_mappings
            )
            new_options[sequence_field] = ",".join(new_sequence_fields)

        return new_options
