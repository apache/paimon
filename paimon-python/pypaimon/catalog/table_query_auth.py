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

import json
from typing import Callable, Dict, List, Optional

import pyarrow as pa
import pyarrow.compute as pc

from pypaimon.common.predicate_json_parser import (
    _collect_all_field_refs_from_transform,
    extract_referenced_fields,
    parse_predicate_to_batch_filter,
)
from pypaimon.read.plan import Plan
from pypaimon.read.query_auth_split import QueryAuthSplit
from pypaimon.schema.data_types import DataField
from pypaimon.table.special_fields import SpecialFields

# SchemaValidation forbids a stored column from carrying one of these names, so such a rule
# always means the synthetic field, which binds only where the query already projects it.
_RESERVED_RULE_NAMES = SpecialFields.SYSTEM_FIELD_NAMES

_SYSTEM_FIELD_IDS = {
    field.name: field.id
    for field in (SpecialFields.SEQUENCE_NUMBER,
                  SpecialFields.VALUE_KIND,
                  SpecialFields.ROW_ID)
}


def _check_field_exists(
        rule: str,
        field: str,
        table_field_names: List[str],
):
    if field in _RESERVED_RULE_NAMES:
        return
    if field not in table_field_names:
        raise ValueError(
            "{} references column '{}' which does not exist in table schema {}. "
            "The rule may be stale after a column rename or drop; refusing to read."
            .format(rule, field, table_field_names))


def _check_not_renamed(
        rule: str,
        field: str,
        latest_by_name: Dict[str, DataField],
        read_by_name: Dict[str, DataField],
        read_by_id: Dict[int, DataField],
):
    # absent from latest: already reported by validate_against_schema
    if field not in latest_by_name:
        return
    field_id = latest_by_name[field].id
    read_field = read_by_name.get(field)
    if read_field is not None:
        # a dropped and re-added column keeps the name but gets a fresh id
        if read_field.id != field_id:
            raise ValueError(
                "{} references column '{}' which the snapshot being read exposes as a "
                "different column of the same name (dropped and re-added since); refusing "
                "to read to avoid applying the rule to unrelated data.".format(rule, field))
        return
    renamed = read_by_id.get(field_id)
    if renamed is not None:
        raise ValueError(
            "{} references column '{}' which the snapshot being read exposes as '{}' "
            "(renamed since); refusing to read to avoid applying the rule by a stale name."
            .format(rule, field, renamed.name))


def reject_search_under_query_auth(table) -> None:
    """Refuses a search on a query-auth table. Called from the methods that build a scan or a
    read rather than from the builder constructors, which a deserialized builder skips."""
    from pypaimon.table.file_store_table import FileStoreTable

    if isinstance(table, FileStoreTable) and table.options.query_auth_enabled:
        raise ValueError(
            "Search is not supported on a query-auth table: the index ranks raw values, "
            "which a column mask invalidates.")


class TableQueryAuthResult:

    def __init__(self, filter: Optional[List[str]], column_masking: Optional[Dict[str, str]]):
        self.filter = [f for f in filter if f] if filter else filter
        self.column_masking = (
            {k: v for k, v in column_masking.items() if k and v}
            if column_masking else column_masking
        )

    @property
    def has_restrictions(self):
        return bool(self.filter) or bool(self.column_masking)

    def convert_plan(self, plan):
        if not self.has_restrictions:
            return plan
        auth_splits = [QueryAuthSplit(split, self) for split in plan.splits()]
        return Plan(auth_splits, snapshot_id=plan.snapshot_id)

    def extract_row_filter(self) -> Optional[Callable[[pa.RecordBatch], pa.Array]]:
        if not self.filter:
            return None
        filters = [parse_predicate_to_batch_filter(json_str) for json_str in self.filter]
        if len(filters) == 1:
            return filters[0]

        def combined(batch: pa.RecordBatch) -> pa.Array:
            result = filters[0](batch)
            for f in filters[1:]:
                result = pc.and_(result, f(batch))
            return result
        return combined

    def parsed_column_masking(self) -> Dict[str, dict]:
        if not self.column_masking:
            return {}
        parsed = {}
        for column, transform_json in self.column_masking.items():
            transform = json.loads(transform_json)
            if transform is None:
                raise ValueError(
                    "Column masking on '{}' is JSON null; refusing to read rather than "
                    "returning the column unmasked.".format(column))
            parsed[column] = transform
        return parsed

    def filter_field_names(self) -> set:
        names = set()
        for json_str in self.filter or []:
            names.update(extract_referenced_fields(json_str))
        return names

    def rule_field_names(self, readable: Optional[set] = None) -> set:
        names = self.filter_field_names()
        for target, transform in self.parsed_column_masking().items():
            names.add(target)
            # an unreadable target makes the mask inert, so its inputs bind nothing
            if readable is None or target in readable:
                names.update(_collect_all_field_refs_from_transform(transform))
        return names

    def validate_against_schema(
            self,
            table_fields: List[DataField],
    ):
        """Validate that every column the auth rules reference exists in the table's latest
        schema. A rule keyed by a since-renamed column would silently stop masking; fail closed.
        """
        masking = self.parsed_column_masking()
        table_field_names = [f.name for f in table_fields]
        for target, transform in masking.items():
            _check_field_exists("Column masking", target, table_field_names)
            for source in _collect_all_field_refs_from_transform(transform):
                _check_field_exists("Column masking", source, table_field_names)
                # a transform reads the raw row, so a masked input would be published
                # unmasked through this target
                if source != target and source in masking:
                    raise ValueError(
                        "Column masking on '{}' reads column '{}', which is masked too. "
                        "The mask would be computed from the raw value of '{}' and expose "
                        "it through '{}'.".format(target, source, source, target))
        for operand in self.filter_field_names():
            _check_field_exists("Row filter", operand, table_field_names)

    def validate_readable_without_rename(
            self,
            latest_fields: List[DataField],
            snapshot_fields: List[DataField],
    ):
        """Fail closed when the snapshot being read exposes a rule's column under a different
        name, where enforcing by name would skip it. A column absent from it stays inert.
        """
        latest_by_name = {f.name: f for f in latest_fields}
        read_by_name = {f.name: f for f in snapshot_fields}
        read_by_id = {f.id: f for f in snapshot_fields}
        read_names = set(read_by_name)
        for target, transform in self.parsed_column_masking().items():
            _check_not_renamed("Column masking", target, latest_by_name, read_by_name, read_by_id)
            # a mask whose target is absent from the read schema is inert; skip its inputs
            if target in read_names:
                for source in _collect_all_field_refs_from_transform(transform):
                    _check_not_renamed(
                        "Column masking", source, latest_by_name, read_by_name, read_by_id)
        for operand in self.filter_field_names():
            _check_not_renamed("Row filter", operand, latest_by_name, read_by_name, read_by_id)

    def validate_read_type(
            self,
            table_fields: List[DataField],
            read_fields: List[DataField],
            nested_name_paths: Optional[List[List[str]]] = None,
            snapshot_fields: Optional[List[DataField]] = None,
    ):
        """Validate a physical projection before adding columns required by authorization
        rules: a rule must never be applied to a partially projected column.
        """
        read_names = {f.name for f in read_fields}
        rule_fields = self.rule_field_names(read_names)
        # a system column is synthesised, not stored, so it cannot be added to the projection;
        # a mask on an unprojected one needs nothing and stays inert
        needed_system = {
            name for name in self.filter_field_names() if name in _RESERVED_RULE_NAMES}
        for target, transform in self.parsed_column_masking().items():
            if target not in read_names:
                continue
            needed_system.update(
                name for name in _collect_all_field_refs_from_transform(transform)
                if name in _RESERVED_RULE_NAMES)
        unbound = needed_system - read_names
        if unbound:
            raise RuntimeError(
                "Query auth rules need system column(s) {} which the query does not project; "
                "a system column cannot be added to the read.".format(sorted(unbound)))
        # identity from the schema the rules name, type from the one the split was written
        # under, which may since have been widened
        table_by_name = {f.name: f for f in table_fields}
        type_by_name = {f.name: f for f in (snapshot_fields or table_fields)}
        expected_id = {n: table_by_name[n].id for n in rule_fields if n in table_by_name}
        # a system column is absent from the table schema, so bind it by its canonical id:
        # a projected subfield flattens to a top-level name and could otherwise pass as one
        expected_id.update(
            {n: i for n, i in _SYSTEM_FIELD_IDS.items() if n in rule_fields})
        # a masked column reached through a generated name (email -> email__0) is published
        # raw; a filter operand still binds by its own name
        masked_ids = {expected_id[n] for n in self.parsed_column_masking() if n in expected_id}
        for field in read_fields:
            if field.name not in rule_fields:
                if field.id in masked_ids:
                    raise RuntimeError(
                        "Query auth rules mask field id {}, which the query also projects "
                        "under the generated name '{}'; the second copy would be raw."
                        .format(field.id, field.name))
                continue
            want = expected_id.get(field.name)
            if want is not None and field.id != want:
                raise RuntimeError(
                    "Query auth rules involve column '{}', which the query projects as a "
                    "different column (field id {} instead of {}); cannot bind the rules "
                    "by name.".format(field.name, field.id, want))
            table_field = type_by_name.get(field.name)
            if table_field is None:
                continue
            if field.type != table_field.type:
                raise RuntimeError(
                    "Query auth rules involve column '{}', which the query projects with a "
                    "pruned type {} instead of its table type {}; cannot apply the rules to "
                    "a partial column.".format(field.name, field.type, table_field.type))
        # a projected ROW subfield becomes a new top-level column, so the pruned parent never
        # shows up by name above
        complete = {path[0] for path in (nested_name_paths or []) if len(path) == 1}
        masking = self.parsed_column_masking()
        for path in nested_name_paths or []:
            if len(path) <= 1 or path[0] not in rule_fields:
                continue
            if path[0] not in complete:
                raise RuntimeError(
                    "Query auth rules involve column '{}', which the query projects only as "
                    "nested field '{}'; cannot apply the rules to a partial column."
                    .format(path[0], '.'.join(path)))
            # the whole column is projected too, so the rules still bind to it; a mask is the
            # exception, because the nested copy carries the leaf id and stays raw
            if path[0] in masking:
                raise RuntimeError(
                    "Query auth rules mask column '{}', which the query also projects as "
                    "nested field '{}'; the second copy would be raw."
                    .format(path[0], '.'.join(path)))

    def get_extra_fields_for_filter(
            self,
            read_fields: List[DataField],
            table_fields: List[DataField],
    ) -> List[DataField]:
        if not self.filter:
            return []
        read_field_names = {f.name for f in read_fields}
        extra = []
        for json_str in self.filter:
            referenced = extract_referenced_fields(json_str)
            for name in referenced:
                if name not in read_field_names:
                    field = next((f for f in table_fields if f.name == name), None)
                    if field:
                        extra.append(field)
                        read_field_names.add(name)
        return extra
