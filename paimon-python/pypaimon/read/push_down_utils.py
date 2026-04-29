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

from typing import Dict, List, Optional, Set

from pypaimon.common.predicate import Predicate
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.schema.data_types import DataField


def extract_partition_spec_from_predicate(
    predicate: Predicate, partition_keys: List[str]
) -> Optional[Dict[str, str]]:
    if not predicate or not partition_keys:
        return None
    parts = _split_and(predicate)
    spec: Dict[str, str] = {}
    for p in parts:
        if p.method != "equal" or p.field is None or p.literals is None or len(p.literals) != 1:
            continue
        if p.field in partition_keys:
            spec[p.field] = str(p.literals[0])
    if set(spec.keys()) == set(partition_keys):
        return spec
    return None


def trim_and_transform_predicate(input_predicate: Predicate, all_fields: List[str], trimmed_keys: List[str]):
    new_predicate = trim_predicate_by_fields(input_predicate, trimmed_keys)
    part_to_index = {element: idx for idx, element in enumerate(trimmed_keys)}
    mapping: Dict[int, int] = {
        i: part_to_index.get(all_fields[i], -1)
        for i in range(len(all_fields))
    }
    return _change_index(new_predicate, mapping)


def trim_predicate_by_fields(input_predicate: Predicate, trimmed_keys: List[str]):
    if not input_predicate or not trimmed_keys:
        return None

    predicates: list[Predicate] = _split_and(input_predicate)
    predicates = [element for element in predicates if _get_all_fields(element).issubset(trimmed_keys)]
    return PredicateBuilder.and_predicates(predicates)


def _split_and(input_predicate: Predicate):
    if not input_predicate:
        return list()

    if input_predicate.method == 'and':
        return [p for element in (input_predicate.literals or []) for p in _split_and(element)]

    return [input_predicate]


def rewrite_predicate_indices(
    input_predicate: Optional[Predicate],
    read_fields: List[DataField],
) -> Optional[Predicate]:
    """Rewrite predicate leaf indices to match positions in ``read_fields``.

    Predicate leaves are built against the original table schema (via
    PredicateBuilder), so their ``index`` field encodes that schema's column
    order. When the same predicate is later evaluated row-by-row against a
    projected scan (read_type narrower or reordered), those indices no longer
    match the OffsetRow layout the reader hands to FilterRecordReader, and
    ``OffsetRow.get_field(idx)`` raises IndexError.

    Returns a new predicate where every leaf's ``index`` is rebound to its
    column's position in ``read_fields``. The caller is responsible for
    ensuring that every leaf field is present in ``read_fields``.
    """
    if input_predicate is None:
        return None
    name_to_pos = {f.name: i for i, f in enumerate(read_fields)}
    return _rewrite_by_name(input_predicate, name_to_pos)


def _rewrite_by_name(p: Predicate, name_to_pos: Dict[str, int]) -> Predicate:
    if p.method == 'and' or p.method == 'or':
        return p.new_literals(
            [_rewrite_by_name(c, name_to_pos) for c in (p.literals or [])]
        )
    if p.field is None or p.field not in name_to_pos:
        raise ValueError(
            "Cannot rewrite predicate index for leaf {!r}: field {!r} is not "
            "in read fields {}. The caller must ensure all referenced columns "
            "are projected.".format(p, p.field, list(name_to_pos))
        )
    return p.new_index(name_to_pos[p.field])


def _change_index(input_predicate: Predicate, mapping: Dict[int, int]):
    if not input_predicate:
        return None

    if input_predicate.method == 'and' or input_predicate.method == 'or':
        predicates: list[Predicate] = input_predicate.literals
        new_predicates = [_change_index(element, mapping) for element in predicates]
        return input_predicate.new_literals(new_predicates)

    return input_predicate.new_index(mapping[input_predicate.index])


def _get_all_fields(predicate: Predicate) -> Set[str]:
    if predicate.field is not None:
        return {predicate.field}
    involved_fields = set()
    if predicate.literals:
        for sub_predicate in predicate.literals:
            involved_fields.update(_get_all_fields(sub_predicate))
    return involved_fields


def remove_row_id_filter(predicate: Predicate) -> Optional[Predicate]:
    from pypaimon.table.special_fields import SpecialFields

    if not predicate:
        return None
    if predicate.field == SpecialFields.ROW_ID.name:
        return None
    if predicate.method == "and":
        parts = _split_and(predicate)
        non_row_id = [
            p for p in parts
            if _get_all_fields(p) != {SpecialFields.ROW_ID.name}
        ]
        if not non_row_id:
            return None
        filtered = []
        for p in non_row_id:
            r = remove_row_id_filter(p)
            if r is None:
                return None
            filtered.append(r)
        return PredicateBuilder.and_predicates(filtered)
    if predicate.method == "or":
        new_children = []
        for c in predicate.literals or []:
            r = remove_row_id_filter(c)
            if r is not None:
                new_children.append(r)
        if not new_children:
            return None
        if len(new_children) == 1:
            return new_children[0]
        return PredicateBuilder.or_predicates(new_children)
    return predicate
