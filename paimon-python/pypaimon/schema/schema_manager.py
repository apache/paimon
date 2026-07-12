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
from pypaimon.casting.data_type_casts import can_execute_cast, supports_cast
from pypaimon.schema.data_types import (ArrayType, AtomicInteger, DataField,
                                        MapType, RowType, reassign_field_id)
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


def _extract_row_data_fields(data_type, out_fields: List[DataField],
                             field_names: List[str], token_pos: int) -> int:
    """Collect the immediate sub-fields reachable from *data_type* into
    *out_fields* and return the path depth consumed. A ROW contributes its
    fields (depth 1); an ARRAY/MAP is transparent and descends into its
    element/value, consuming the ``element``/``value`` path token -- the
    consumed token is validated so an unknown step cannot silently mutate
    the schema; anything else contributes nothing (depth 1)."""
    if isinstance(data_type, RowType):
        out_fields.extend(data_type.fields)
        return 1
    if isinstance(data_type, ArrayType):
        _assert_wrapper_token(field_names, token_pos, 'element')
        return _extract_row_data_fields(
            data_type.element, out_fields, field_names, token_pos + 1) + 1
    if isinstance(data_type, MapType):
        _assert_wrapper_token(field_names, token_pos, 'value')
        return _extract_row_data_fields(
            data_type.value, out_fields, field_names, token_pos + 1) + 1
    return 1


def _assert_wrapper_token(field_names: List[str], token_pos: int, expected: str):
    # A path that ends inside the wrappers (token_pos out of range) is the
    # update-the-wrapped-type-itself case, handled by the caller's overflow
    # branch; only a present-but-wrong token is rejected.
    if token_pos < len(field_names) and field_names[token_pos] != expected:
        raise ColumnNotExistException('.'.join(field_names))


def _wrap_new_row_type(data_type, nested_fields: List[DataField]):
    """Rebuild *data_type* substituting *nested_fields* at its innermost ROW,
    preserving any ARRAY/MAP wrappers."""
    if isinstance(data_type, RowType):
        return RowType(data_type.nullable, nested_fields)
    if isinstance(data_type, ArrayType):
        return ArrayType(data_type.nullable, _wrap_new_row_type(data_type.element, nested_fields))
    if isinstance(data_type, MapType):
        return MapType(
            data_type.nullable, data_type.key,
            _wrap_new_row_type(data_type.value, nested_fields))
    return data_type


def _get_root_type(data_type, curr_depth: int, max_depth: int):
    """Return the type sitting at ``max_depth`` when walking ARRAY/MAP wrappers
    from *data_type* (e.g. the INT in ARRAY<MAP<STRING, ARRAY<INT>>>)."""
    if curr_depth == max_depth - 1:
        return data_type
    if isinstance(data_type, ArrayType):
        return _get_root_type(data_type.element, curr_depth + 1, max_depth)
    if isinstance(data_type, MapType):
        return _get_root_type(data_type.value, curr_depth + 1, max_depth)
    return data_type


def _get_array_map_type_with_target_type_root(source, target, curr_depth: int, max_depth: int):
    """Rebuild *source* with *target* substituted at ``max_depth``, keeping the
    ARRAY/MAP wrappers around it intact."""
    if curr_depth == max_depth - 1:
        return target
    if isinstance(source, ArrayType):
        return ArrayType(
            source.nullable,
            _get_array_map_type_with_target_type_root(
                source.element, target, curr_depth + 1, max_depth))
    if isinstance(source, MapType):
        return MapType(
            source.nullable, source.key,
            _get_array_map_type_with_target_type_root(
                source.value, target, curr_depth + 1, max_depth))
    return target


def _update_intermediate_column(new_fields, previous_fields, depth, prev_depth,
                                field_names, update_last_fn):
    """Walk *field_names* into nested ROW (transparently through ARRAY/MAP),
    then run *update_last_fn(depth, fields, name)* on the field list that
    directly contains the final path element, rebuilding parent types upward."""
    if depth == len(field_names) - 1:
        update_last_fn(depth, new_fields, field_names[depth])
        return
    if depth >= len(field_names):
        # Path descended through ARRAY/MAP past the last ROW; operate on the
        # field that owns the wrapper at the previous depth.
        update_last_fn(prev_depth, previous_fields, field_names[prev_depth])
        return
    for i, field in enumerate(new_fields):
        if field.name != field_names[depth]:
            continue
        nested_fields: List[DataField] = []
        new_depth = depth + _extract_row_data_fields(
            field.type, nested_fields, field_names, depth + 1)
        _update_intermediate_column(
            nested_fields, new_fields, new_depth, depth, field_names, update_last_fn)
        field = new_fields[i]
        new_fields[i] = DataField(
            field.id, field.name,
            _wrap_new_row_type(field.type, nested_fields),
            field.description, field.default_value)
        return
    raise ColumnNotExistException('.'.join(field_names[:depth + 1]))


def _modify_nested_column(new_fields, field_names, update_last_fn):
    _update_intermediate_column(new_fields, new_fields, 0, 0, field_names, update_last_fn)


def _update_nested_column(new_fields, field_names, update_func):
    def update_last(depth, fields, field_name):
        idx = _find_field_index(fields, field_name)
        if idx is None:
            raise ColumnNotExistException('.'.join(field_names))
        fields[idx] = update_func(fields[idx], depth)
    _modify_nested_column(new_fields, field_names, update_last)


def _get_rename_mappings(changes: List[SchemaChange]) -> dict:
    rename_mappings = {}
    for change in changes:
        if isinstance(change, RenameColumn) and len(change.field_names) == 1:
            rename_mappings[change.field_names[0]] = change.new_name
    return rename_mappings


def _handle_update_column_comment(
    change: UpdateColumnComment, new_fields: List[DataField]
):
    def update_func(field: DataField, depth: int) -> DataField:
        return DataField(
            field.id, field.name, field.type, change.new_comment, field.default_value
        )
    _update_nested_column(new_fields, change.field_names, update_func)


def _assert_nullability_change(old_nullability: bool, new_nullability: bool,
                               field_name: str, disable_null_to_not_null: bool):
    if disable_null_to_not_null and old_nullability and not new_nullability:
        raise ValueError(
            "Cannot update column type from nullable to non nullable for {}. "
            "You can set table configuration option "
            "'alter-column-null-to-not-null.disabled' = 'false' "
            "to allow converting null columns to not null".format(field_name)
        )


def _handle_update_column_nullability(
    change: UpdateColumnNullability, new_fields: List[DataField],
    disable_null_to_not_null: bool
):
    from pypaimon.schema.data_types import DataTypeParser
    field_names = change.field_names
    max_depth = len(field_names)

    def update_func(field: DataField, depth: int) -> DataField:
        source_root = _get_root_type(field.type, depth, max_depth)
        _assert_nullability_change(
            source_root.nullable, change.new_nullability,
            '.'.join(field_names), disable_null_to_not_null)
        new_root = DataTypeParser.parse_data_type(source_root.to_dict())
        new_root.nullable = change.new_nullability
        new_type = _get_array_map_type_with_target_type_root(
            field.type, new_root, depth, max_depth)
        return DataField(
            field.id, field.name, new_type, field.description, field.default_value
        )
    _update_nested_column(new_fields, field_names, update_func)


def _handle_update_column_type(
    change: UpdateColumnType, new_fields: List[DataField],
    disable_null_to_not_null: bool
):
    from pypaimon.schema.data_types import DataTypeParser
    field_names = change.field_names
    max_depth = len(field_names)

    def update_func(field: DataField, depth: int) -> DataField:
        source_root = _get_root_type(field.type, depth, max_depth)
        target_root = DataTypeParser.parse_data_type(change.new_data_type.to_dict())
        if change.keep_nullability:
            target_root.nullable = source_root.nullable
        else:
            # A type change carries its own nullability; guard nullable ->
            # not null just like UpdateColumnNullability (mirrors Java
            # SchemaManager#updateColumnType).
            _assert_nullability_change(
                source_root.nullable, target_root.nullable,
                '.'.join(field_names), disable_null_to_not_null)
        if not supports_cast(source_root, target_root):
            raise ValueError(
                "Column type {}[{}] cannot be converted to {} without losing information."
                .format(field.name, source_root, target_root)
            )
        # Logical cast support is not enough: the read path materializes the
        # change via PyArrow when reading old files, so reject casts it cannot
        # execute (mirrors Java's CastExecutors.resolve(...) != null check).
        if not can_execute_cast(source_root, target_root):
            raise ValueError(
                "Column type {}[{}] cannot be converted to {}: the read path "
                "has no executable cast for this conversion."
                .format(field.name, source_root, target_root)
            )
        new_type = _get_array_map_type_with_target_type_root(
            field.type, target_root, depth, max_depth)
        return DataField(
            field.id, field.name, new_type, field.description, field.default_value
        )
    _update_nested_column(new_fields, field_names, update_func)


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
    field_names = change.field_names

    def update_last(depth, fields, field_name):
        field_index = _find_field_index(fields, field_name)
        if field_index is None:
            raise ColumnNotExistException(field_name)
        if len(field_names) == 1:
            field = fields[field_index]
            type_root = _get_type_root(field.type)
            remove_dropped_directive_options(field_name, type_root, new_options)
        fields.pop(field_index)
        if not fields:
            raise ValueError("Cannot drop all fields in table")
    _modify_nested_column(new_fields, field_names, update_last)


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


def _handle_rename_column(change: RenameColumn, new_fields: List[DataField]):
    new_name = change.new_name

    def update_last(depth, fields, field_name):
        field_index = _find_field_index(fields, field_name)
        if field_index is None:
            raise ColumnNotExistException(field_name)
        if _find_field_index(fields, new_name) is not None:
            raise ColumnAlreadyExistException(new_name)
        field = fields[field_index]
        fields[field_index] = DataField(
            field.id, new_name, field.type, field.description, field.default_value
        )
    _modify_nested_column(new_fields, change.field_names, update_last)


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
    # Reassign ids of any nested sub-fields the new column carries (a ROW/ARRAY/
    # MAP type) so they draw globally-unique ids from the running counter.
    data_type = reassign_field_id(change.data_type, highest_field_id)
    field_name = change.field_names[-1]

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

    def update_last(depth, fields, fname):
        if _find_field_index(fields, fname) is not None:
            raise ColumnAlreadyExistException(fname)
        if change.move:
            _apply_move(fields, new_field, change.move)
        elif (
            add_column_before_partition
            and partition_keys
            and len(change.field_names) == 1
        ):
            insert_index = len(fields)
            for i, field in enumerate(fields):
                if field.name in partition_keys:
                    insert_index = i
                    break
            fields.insert(insert_index, new_field)
        else:
            fields.append(new_field)
    _modify_nested_column(new_fields, change.field_names, update_last)


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
        # Converting a nullable column to NOT NULL is unsafe for existing
        # data and is disabled by default; the table option below opts in.
        disable_null_to_not_null = str(old_table_schema.options.get(
            'alter-column-null-to-not-null.disabled', 'true')).lower() != 'false'

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
                _handle_update_column_type(
                    change, new_fields, disable_null_to_not_null)
            elif isinstance(change, UpdateColumnNullability):
                if change.new_nullability:
                    _assert_not_updating_primary_keys(
                        old_table_schema, change.field_names, "change nullability of"
                    )
                _handle_update_column_nullability(
                    change, new_fields, disable_null_to_not_null)
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
