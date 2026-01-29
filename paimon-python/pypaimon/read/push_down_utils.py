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
