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

import ast
from typing import Dict, List, Optional, Sequence, Union, TYPE_CHECKING

import pyarrow

from pypaimon.common.predicate import Predicate
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.read.explain import ExplainResult
from pypaimon.read.push_down_utils import predicate_field_names, predicate_matches_physical_names
from pypaimon.read.table_read import ROW_KIND_COLUMN, TableRead
from pypaimon.read.table_scan import TableScan
from pypaimon.read.read_type import OutputProjection, output_fields, project_read_type, reader_adapter
from pypaimon.read.variant_read_type import with_variant_extractions
from pypaimon.schema.data_types import AtomicType, DataField, MapType
from pypaimon.table.special_fields import SpecialFields
from pypaimon.utils.projection import MapKey, Projection, is_row_type


if TYPE_CHECKING:
    from pypaimon.table.file_store_table import FileStoreTable

ProjectionPath = Sequence[Union[int, MapKey]]


def _string_literal(node: ast.AST) -> Optional[str]:
    if isinstance(node, ast.Str):
        return node.s
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _normalize_sql_literals(expression: str) -> str:
    """Decode SQL doubled quotes before validating the expression with AST."""
    normalized = []
    index = 0
    previous_was_literal = False
    while index < len(expression):
        quote = expression[index]
        if quote not in ("'", '"'):
            normalized.append(quote)
            if not quote.isspace():
                previous_was_literal = False
            index += 1
            continue
        if previous_was_literal:
            raise ValueError("Adjacent string literals are not supported")
        index += 1
        literal = []
        while index < len(expression):
            char = expression[index]
            if char == quote:
                if index + 1 < len(expression) and expression[index + 1] == quote:
                    literal.append(quote)
                    index += 2
                else:
                    index += 1
                    break
            else:
                literal.append(char)
                index += 1
        else:
            raise ValueError("Unterminated string literal in projection expression")
        normalized.append(repr(''.join(literal)))
        previous_was_literal = True
    return ''.join(normalized)


class _ReadPredicateBuilder(PredicateBuilder):

    def __init__(self, fields, unsupported_fields, paths=None):
        super().__init__(fields)
        self._unsupported_fields = unsupported_fields
        self._paths = paths or {}

    def _build_predicate(self, method, field, literals=None):
        predicate = super()._build_predicate(method, field, literals)
        path = self._paths.get(field)
        if path:
            predicate.field_path = list(path)
        return predicate

    def _get_field_index(self, field: str) -> int:
        if field in self._unsupported_fields:
            raise NotImplementedError(
                "Filtering projected MAP keys is not supported: {}".format(
                    field))
        return super()._get_field_index(field)


class ReadBuilder:
    """Implementation of ReadBuilder for native Python reading."""

    def __init__(self, table):
        self.table: "FileStoreTable" = table
        self._predicate: Optional[Predicate] = None
        self._read_type: Optional[List[DataField]] = None
        self._output_projection: Optional[OutputProjection] = None
        self._partition_filter: Optional[Predicate] = None
        self._limit: Optional[int] = None

    def with_filter(self, predicate: Predicate) -> 'ReadBuilder':
        self._predicate = predicate
        return self

    def with_partition_filter(self, partition_filter: Optional[Predicate]) -> 'ReadBuilder':
        """Replace scan-only partition selection; None restores derived pruning.

        As with Java's PartitionPredicate, only partition fields are allowed.
        Ordinary row predicates remain independent and are checked by the reader.
        """
        if partition_filter is not None and (
                not self.table.partition_keys
                or not predicate_matches_physical_names(partition_filter)
                or not predicate_field_names(partition_filter).issubset(self.table.partition_keys)):
            raise ValueError('Partition filter must reference only partition keys of a partitioned table')
        self._partition_filter = partition_filter
        return self

    def with_projection(
        self,
        projection: Union[List[str], Dict[str, str]],
    ) -> 'ReadBuilder':
        """Project columns, nested ROW fields, MAP values or VARIANT expressions.

        Lists retain column, nested ROW and MAP-key projection semantics.
        A mapping assigns output names to source columns or float32
        ``variant_get`` / ``try_variant_get`` expressions. Variant
        expressions require native reading.
        """
        if isinstance(projection, dict):
            name_paths, variants, outputs = self._parse_expression_projection(projection)
            output = OutputProjection(outputs, True)
        else:
            fields = self._table_read_fields()
            indexes = self._resolve_projection_paths(projection) if projection else []
            if projection is None:
                return self.with_read_type(self.table.fields)
            name_paths = Projection.of(indexes).to_name_paths(fields)
            output_indexes = (indexes if any(len(path) > 1 for path in indexes)
                              else [path[0] for path in indexes])
            flat_fields = Projection.of(output_indexes).project(fields)
            output = OutputProjection([(field.name, path) for field, path in zip(flat_fields, name_paths)])
            variants = None
        read_type = project_read_type(self._table_read_fields(), name_paths)
        self.with_read_type(with_variant_extractions(read_type, variants))
        self._output_projection = output
        return self

    def with_read_type(self, read_type: List[DataField]) -> 'ReadBuilder':
        """Set one complete reader request, resetting the result projection."""
        self._read_type = read_type
        self._output_projection = None
        return self

    def _table_read_fields(self):
        fields = self.table.fields
        return (SpecialFields.row_type_with_row_tracking(fields)
                if self.table.options.row_tracking_enabled() else fields)

    def with_limit(self, limit: int) -> 'ReadBuilder':
        self._limit = limit
        return self

    def new_scan(self) -> TableScan:
        self._validate_map_key_filter()
        scan = TableScan(
            table=self.table,
            predicate=self._predicate,
            limit=self._limit,
            partition_predicate=self._partition_filter,
        )
        scan._read_type = self.read_type()
        return scan

    def new_read(self) -> TableRead:
        self._validate_map_key_filter()
        return TableRead(
            table=self.table,
            predicate=self._predicate,
            read_type=self.read_type(),
            output_projection=self._output_projection,
            limit=self._limit,
        )

    def _nested_name_paths(self):
        """Derive format-adapter paths; the read type remains authoritative."""
        _, paths = reader_adapter(self.read_type(), self._table_read_fields(), self._output_projection)
        return paths if any(len(path) > 1 for path in paths) else None

    def _output_name_paths(self):
        paths = ([path for _, path in self._output_projection.columns]
                 if self._output_projection is not None else [[field.name] for field in self.read_type()])
        return paths if any(len(path) > 1 for path in paths) else None

    def new_predicate_builder(self) -> PredicateBuilder:
        # List projections expose flat leaf names to the Python predicate
        # evaluator. Derive that view without changing the reader request.
        fields = self.read_type()
        paths = {}
        if self._output_projection is not None and not self._output_projection.named:
            fields, name_paths = reader_adapter(fields, self._table_read_fields(), self._output_projection)
            paths = {field.name: path for field, path in zip(fields, name_paths)}
        return _ReadPredicateBuilder(
            fields, self._map_key_output_names(), paths)

    def explain(self, verbose: bool = False) -> ExplainResult:
        """Produce a structured scan plan for this builder.

        Runs one planning pass (manifest list + manifest reads, no data
        files) and returns an :class:`ExplainResult` summarising the
        target snapshot, the pushed-down predicate / projection / limit,
        the partition / bucket / file-stats pruning funnel, and split-
        level execution signals (raw-convertible ratio, deletion-vector
        ratio, level histogram, files-per-split and split-size
        distribution). With ``verbose=True``, every split is listed.

        Cost: ``explain()`` reads manifest list + manifests but never
        opens data files. To produce accurate before/after counters it
        suppresses early bucket and sidecar pruning and counts candidates
        before applying LIMIT, so it can be measurably
        heavier than a regular ``new_scan().plan()`` on tables where the
        early filter usually prunes aggressively (e.g. very wide
        HASH_FIXED tables with a tight predicate).
        """
        from pypaimon.execution.read_explain import explain
        return explain(self, verbose)

    def read_type(self) -> List[DataField]:
        """Return the structured reader request, preserving source field IDs.

        Nested ROWs are pruned in place. Selected MAP keys and VARIANT
        extractions use field-description metadata, as in Java. Result aliases
        and flat list-projection outputs are applied separately by TableRead.
        """
        return self._read_type if self._read_type is not None else self.table.fields

    def _output_fields(self):
        return output_fields(self.read_type(), self._output_projection)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _parse_expression_projection(self, expressions: Dict[str, str]):
        if not expressions:
            raise ValueError("Projection expression mapping must not be empty")
        table_fields = self.table.fields
        if self.table.options.row_tracking_enabled():
            table_fields = SpecialFields.row_type_with_row_tracking(table_fields)
        projection = []
        variants = {}
        outputs = []
        direct_columns = set()
        for alias, expression in expressions.items():
            if not isinstance(alias, str) or not alias:
                raise TypeError("Projection output names must be non-empty strings")
            if alias == ROW_KIND_COLUMN:
                raise ValueError("Projection output name %r is reserved" % alias)
            if not isinstance(expression, str) or not expression:
                raise TypeError("Projection expressions must be non-empty strings")
            paths = self._resolve_projection_paths([expression])
            if paths:
                path = Projection.of(paths).to_name_paths(table_fields)[0]
                source, child = path[0], None
                direct_columns.add(tuple(path))
            else:
                try:
                    call = ast.parse(
                        _normalize_sql_literals(expression), mode='eval').body
                except SyntaxError as error:
                    raise ValueError(
                        "Unsupported projection expression %r" % expression
                    ) from error
                if (not isinstance(call, ast.Call)
                        or not isinstance(call.func, ast.Name)
                        or call.func.id.lower() not in ('variant_get', 'try_variant_get')
                        or len(call.args) != 3 or call.keywords
                        or not (isinstance(call.args[0], ast.Name)
                                or isinstance(call.args[0], ast.Attribute)
                                or _string_literal(call.args[0]) is not None)
                        or any(_string_literal(arg) is None
                               for arg in call.args[1:])):
                    raise ValueError(
                        "Unsupported projection expression %r" % expression)
                source = _source_expression(call.args[0])
                source_indexes = self._resolve_projection_paths([source]) if source else []
                source_paths = Projection.of(source_indexes).to_name_paths(table_fields)
                reader_path = source_paths[0] if source_paths else []
                field = _field_at_path(table_fields, reader_path)
                if (field is None or not isinstance(field.type, AtomicType)
                        or field.type.type.upper() != 'VARIANT'):
                    raise ValueError("Variant extraction requires a VARIANT column: %r" % source)
                path, target_type = (_string_literal(arg) for arg in call.args[1:])
                if ';' in path:
                    raise ValueError(
                        "Variant extraction path must not contain ';': %s"
                        % path)
                if target_type.lower() != 'float':
                    raise ValueError(
                        "Only float32 Variant extractions are supported")
                options = variants.setdefault(tuple(reader_path), {
                    'paths': [], 'target_type': pyarrow.float32(),
                    'fail_on_error': [],
                })
                child = len(options['paths'])
                options['paths'].append(path)
                options['fail_on_error'].append(
                    call.func.id.lower() == 'variant_get')
                path = reader_path + [str(child)]
            if child is None:
                reader_path = path
            if reader_path not in projection:
                projection.append(reader_path)
            outputs.append((alias, path))
        if any(variant[:len(direct)] == direct for direct in direct_columns for variant in variants):
            raise ValueError(
                "A VARIANT column cannot be read both whole and extracted")
        return projection, variants or None, outputs

    def _resolve_projection_paths(self, names: List[str]) -> List[ProjectionPath]:
        """Translate ROW paths and MAP-key selectors into internal paths."""
        table_fields = self.table.fields
        if self.table.options.row_tracking_enabled():
            table_fields = SpecialFields.row_type_with_row_tracking(table_fields)
        top_index = {f.name: i for i, f in enumerate(table_fields)}

        paths: List[ProjectionPath] = []
        for name in names:
            # Dot can be part of a top-level field name, not only a struct path
            # separator. Top-level match takes precedence over struct walk.
            if name in top_index:
                paths.append([top_index[name]])
                continue

            map_selector = _map_key_selector(name, table_fields)
            if map_selector is not None:
                map_path, key = map_selector
                paths.append(map_path + [MapKey(key)])
                continue

            if "." not in name:
                continue

            # Preserve the original ROW-path semantics before considering a
            # dotted top-level field name as the path prefix.
            parts = name.split(".")
            top = parts[0]
            if top in top_index:
                path = _resolve_row_path(table_fields, top_index[top], parts[1:])
                if path is not None:
                    paths.append(path)
                    continue

            candidates = [
                field_name
                for field_name in top_index
                if name.startswith(field_name + ".")
                and is_row_type(table_fields[top_index[field_name]].type)
            ]
            if not candidates:
                continue
            top = max(candidates, key=len)
            prefix_length = len(top) + 1
            parts = name[prefix_length:].split(".")
            path = _resolve_row_path(table_fields, top_index[top], parts)
            if path is not None:
                paths.append(path)
        return paths

    def _validate_map_key_filter(self):
        if self._predicate is None:
            return
        unsupported = predicate_field_names(self._predicate) & self._map_key_output_names()
        if unsupported:
            raise NotImplementedError("Filtering projected MAP keys is not supported: {}".format(sorted(unsupported)))

    def _map_key_output_names(self):
        if self._output_projection is None or self._output_projection.named:
            # Named result aliases do not participate in source predicates.
            return set()
        fields = {field.name: field for field in self._table_read_fields()}
        return {alias for alias, path in self._output_projection.columns
                if any(isinstance(_field_at_path(list(fields.values()), path[:i]).type, MapType)
                       for i in range(1, len(path)))}


def _resolve_row_path(
    table_fields: List[DataField], top_index: int, parts: List[str]
) -> Optional[List[int]]:
    """Walk ROW children from a top-level field; return None for invalid paths."""
    path = [top_index]
    current_field = table_fields[top_index]
    for part in parts:
        if not is_row_type(current_field.type):
            return None
        child_fields = current_field.type.fields
        child_idx = next((i for i, f in enumerate(child_fields) if f.name == part), -1)
        if child_idx < 0:
            return None
        path.append(child_idx)
        current_field = child_fields[child_idx]
    return path


def _field_at_path(fields, path):
    field = None
    for name in path:
        field = next((field for field in fields if field.name == name), None)
        if field is None:
            return None
        fields = field.type.fields if is_row_type(field.type) else []
    return field


def _source_expression(node):
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        parent = _source_expression(node.value)
        return parent + '.' + node.attr if parent else None
    return _string_literal(node)


def _map_key_selector(name, table_fields):
    if not name.endswith("]"):
        return None
    candidates = []

    def visit(fields, prefix, indexes):
        for index, field in enumerate(fields):
            full = prefix + field.name
            path = indexes + [index]
            if _is_string_key_map(field.type) and name.startswith(full + "["):
                candidates.append((full, path))
            elif is_row_type(field.type):
                visit(field.type.fields, full + '.', path)
    visit(table_fields, '', [])
    for full, path in sorted(candidates, key=lambda pair: len(pair[0]), reverse=True):
        try:
            key = ast.literal_eval(name[len(full) + 1:-1])
        except (SyntaxError, ValueError):
            continue
        if isinstance(key, str):
            return path, key
    return None


def _is_string_key_map(data_type) -> bool:
    return (
        isinstance(data_type, MapType)
        and isinstance(data_type.key, AtomicType)
        and data_type.key.type.upper() == 'STRING'
    )
