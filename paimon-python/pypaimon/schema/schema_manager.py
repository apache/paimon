# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from typing import List, Optional

from pypaimon.catalog.catalog_exception import (ColumnAlreadyExistException,
                                                ColumnNotExistException)
from pypaimon.common.file_io import FileIO
from pypaimon.common.identifier import DEFAULT_MAIN_BRANCH
from pypaimon.common.json_util import JSON
from pypaimon.common.options import CoreOptions, Options
from pypaimon.schema.column_directive_utils import (
    apply_add_column_directive, apply_directives,
    remove_dropped_directive_options)
from pypaimon.schema.data_types import AtomicInteger, DataField
from pypaimon.schema.schema import Schema
from pypaimon.schema.schema_change import (AddColumn, DropColumn, RemoveOption,
                                           RenameColumn, SchemaChange,
                                           SetOption, UpdateColumnComment,
                                           UpdateColumnNullability,
                                           UpdateColumnPosition,
                                           UpdateColumnType, UpdateComment)
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


def _drop_column_validation(schema: 'TableSchema', change: DropColumn):
    if len(change.field_names) > 1:
        return
    column_to_drop = change.field_names[0]
    if column_to_drop in schema.partition_keys or column_to_drop in schema.primary_keys:
        raise ValueError(
            f"Cannot drop partition key or primary key: [{column_to_drop}]"
        )


def _handle_drop_column(change: DropColumn, new_fields: List[DataField],
                        new_options: dict):
    field_name = change.field_names[-1]
    field_index = _find_field_index(new_fields, field_name)
    if field_index is None:
        raise ColumnNotExistException(field_name)
    if len(change.field_names) == 1:
        field = new_fields[field_index]
        type_root = _get_type_root(field.type)
        remove_dropped_directive_options(field_name, type_root, new_options)
    new_fields.pop(field_index)
    if not new_fields:
        raise ValueError("Cannot drop all fields in table")


def _get_type_root(data_type) -> str:
    from pypaimon.schema.data_types import AtomicType, VectorType
    if isinstance(data_type, VectorType):
        return 'VECTOR'
    if isinstance(data_type, AtomicType) and data_type.type == 'BLOB':
        return 'BLOB'
    return getattr(data_type, 'type', '')


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


def _assert_not_renaming_blob_column(
        new_fields: List[DataField], field_names: List[str]):
    if len(field_names) > 1:
        return
    field_name = field_names[0]
    for field in new_fields:
        if field.name == field_name and str(field.type) == 'BLOB':
            raise ValueError(
                f"Cannot rename BLOB column: [{field_name}]"
            )


def _validate_blob_fields(fields: List[DataField], options: dict, primary_keys: List[str]):
    """Validate blob field configurations in the schema."""
    if options is None:
        options = {}

    blob_field_names = {
        field.name for field in fields
        if getattr(field.type, 'type', None) == 'BLOB'
    }

    if len(fields) <= len(blob_field_names):
        raise ValueError(
            "Table with BLOB type column must have other normal columns."
        )

    core_options = CoreOptions(Options(options))

    configured_blob_fields = core_options.blob_field()
    for field in configured_blob_fields:
        if field not in blob_field_names:
            raise ValueError(
                "Field '{}' in '{}' must be a BLOB field in table schema.".format(
                    field, CoreOptions.BLOB_FIELD.key()
                )
            )

    descriptor_fields = core_options.blob_descriptor_fields()
    view_fields = core_options.blob_view_fields()

    all_inline_fields = descriptor_fields.union(view_fields)
    non_blob_inline_fields = all_inline_fields.difference(blob_field_names)
    if non_blob_inline_fields:
        raise ValueError(
            "Fields in 'blob-descriptor-field' or 'blob-view-field' must be blob fields "
            "in schema. Non-BLOB fields: {}".format(sorted(non_blob_inline_fields))
        )

    overlapping_inline_fields = descriptor_fields.intersection(view_fields)
    if overlapping_inline_fields:
        raise ValueError(
            "Fields in 'blob-descriptor-field' and 'blob-view-field' must not overlap. "
            "Overlapping fields: {}".format(sorted(overlapping_inline_fields))
        )

    if blob_field_names:
        required_options = {
            CoreOptions.ROW_TRACKING_ENABLED.key(): 'true',
            CoreOptions.DATA_EVOLUTION_ENABLED.key(): 'true'
        }

        missing_options = []
        for key, expected_value in required_options.items():
            if key not in options or options[key] != expected_value:
                missing_options.append(f"{key}='{expected_value}'")

        if missing_options:
            raise ValueError(
                f"Schema contains Blob type but is missing required options: {', '.join(missing_options)}. "
                f"Please add these options to the schema."
            )

        if primary_keys:
            raise ValueError("Blob type is not supported with primary key.")


def _validate_blob_external_storage_fields(fields: List[DataField], options: dict):
    """Validate blob-external-storage-field configuration.

    Validation order aligned with Java's SchemaValidation.validateBlobExternalStorageFields():
    1. Field must be a BLOB type in the schema
    2. Field must be in blob-descriptor-field
    3. blob-external-storage-path must be configured
    """
    core_options = CoreOptions(Options(options))
    external_fields = core_options.blob_external_storage_fields()
    if not external_fields:
        return

    # 1. Configured fields must be BLOB type
    field_type_map = {f.name: f.type for f in fields}
    for field_name in external_fields:
        field_type = field_type_map.get(field_name)
        if field_type is None or getattr(field_type, 'type', None) != 'BLOB':
            raise ValueError(
                f"Field '{field_name}' in "
                f"'{CoreOptions.BLOB_EXTERNAL_STORAGE_FIELD.key()}' must be a BLOB type field."
            )

    # 2. Must be a subset of blob-descriptor-field
    descriptor_fields = core_options.blob_descriptor_fields()
    not_in_descriptor = external_fields - descriptor_fields
    if not_in_descriptor:
        raise ValueError(
            f"Fields {sorted(not_in_descriptor)} in "
            f"'{CoreOptions.BLOB_EXTERNAL_STORAGE_FIELD.key()}' must also be configured in "
            f"'{CoreOptions.BLOB_DESCRIPTOR_FIELD.key()}'."
        )

    # 3. Must configure external-storage-path
    external_path = core_options.blob_external_storage_path()
    if not external_path:
        raise ValueError(
            f"'{CoreOptions.BLOB_EXTERNAL_STORAGE_FIELD.key()}' is configured but "
            f"'{CoreOptions.BLOB_EXTERNAL_STORAGE_PATH.key()}' is not set."
        )


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
    change: AddColumn,
    new_fields: List[DataField],
    highest_field_id: AtomicInteger,
    partition_keys: List[str],
    add_column_before_partition: bool,
    new_options: dict
):
    if not change.data_type.nullable:
        raise ValueError(
            f"Column {'.'.join(change.field_names)} cannot specify NOT NULL in the table."
        )
    field_id = highest_field_id.increment_and_get()
    field_name = change.field_names[-1]
    if _find_field_index(new_fields, field_name) is not None:
        raise ColumnAlreadyExistException(field_name)

    data_type = change.data_type
    comment = change.comment
    converted = apply_add_column_directive(comment, field_name, data_type, new_options)
    if converted is not None:
        if len(change.field_names) > 1:
            raise ValueError(
                f"Comment directive cannot be used on a nested column "
                f"{'.'.join(change.field_names)}."
            )
        data_type = converted.type
        comment = converted.comment

    new_field = DataField(field_id, field_name, data_type, comment)
    if change.move:
        _apply_move(new_fields, new_field, change.move)
    elif (
        add_column_before_partition
        and partition_keys
        and len(change.field_names) == 1
    ):
        insert_index = len(new_fields)
        for i, field in enumerate(new_fields):
            if field.name in partition_keys:
                insert_index = i
                break
        new_fields.insert(insert_index, new_field)
    else:
        new_fields.append(new_field)


class SchemaManager:

    def __init__(self, file_io: FileIO, table_path: str, branch: str = DEFAULT_MAIN_BRANCH):
        from pypaimon.branch.branch_manager import BranchManager
        self.schema_prefix = "schema-"
        self.file_io = file_io
        self.table_path = table_path
        self.branch = BranchManager.normalize_branch(branch)
        self.schema_path = f"{self.branch_path}/schema"
        self.schema_cache = {}

    @property
    def branch_path(self) -> str:
        """Get the branch path for this schema manager."""
        from pypaimon.branch.branch_manager import BranchManager
        return BranchManager.branch_path(self.table_path, self.branch)

    def copy_with_branch(self, branch_name: str) -> 'SchemaManager':
        """Create a copy of SchemaManager for a different branch."""
        return SchemaManager(self.file_io, self.table_path, branch_name)

    def latest(self) -> Optional['TableSchema']:
        try:
            versions = self._list_versioned_files()
            if not versions:
                return None

            max_version = max(versions)
            return self.get_schema(max_version)
        except Exception as e:
            raise RuntimeError(f"Failed to load schema from path: {self.schema_path}") from e

    def list_all(self) -> List['TableSchema']:
        """Return every committed schema in ascending ID order.

        Missing IDs (deleted on disk after expiry, for instance) are
        skipped.
        """
        ids = sorted(self._list_versioned_files())
        schemas: List['TableSchema'] = []
        for schema_id in ids:
            schema = self.get_schema(schema_id)
            if schema is not None:
                schemas.append(schema)
        return schemas

    def create_table(self, schema: Schema) -> TableSchema:
        while True:
            latest = self.latest()
            if latest is not None:
                raise RuntimeError("Schema in filesystem exists, creation is not allowed.")

            fields = list(schema.fields)
            options = dict(schema.options)
            apply_directives(fields, options)
            schema = Schema(
                fields=fields,
                partition_keys=schema.partition_keys,
                primary_keys=schema.primary_keys,
                options=options,
                comment=schema.comment,
            )

            _validate_blob_fields(schema.fields, schema.options, schema.primary_keys)
            _validate_blob_external_storage_fields(schema.fields, schema.options)
            table_schema = TableSchema.from_schema(schema_id=0, schema=schema)
            success = self.commit(table_schema)
            if success:
                return table_schema

    def commit(self, new_schema: TableSchema) -> bool:
        _validate_blob_fields(new_schema.fields, new_schema.options, new_schema.primary_keys)
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

        # Get add_column_before_partition option
        add_column_before_partition = CoreOptions(Options(old_table_schema.options)).add_column_before_partition()
        partition_keys = old_table_schema.partition_keys

        for change in changes:
            if isinstance(change, SetOption):
                new_options[change.key] = change.value
            elif isinstance(change, RemoveOption):
                new_options.pop(change.key, None)
            elif isinstance(change, UpdateComment):
                new_comment = change.comment
            elif isinstance(change, AddColumn):
                _handle_add_column(
                    change, new_fields, highest_field_id,
                    partition_keys, add_column_before_partition,
                    new_options
                )
            elif isinstance(change, RenameColumn):
                _assert_not_updating_partition_keys(
                    old_table_schema, change.field_names, "rename"
                )
                _assert_not_renaming_blob_column(new_fields, change.field_names)
                _handle_rename_column(change, new_fields)
            elif isinstance(change, DropColumn):
                _drop_column_validation(old_table_schema, change)
                _handle_drop_column(change, new_fields, new_options)
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
