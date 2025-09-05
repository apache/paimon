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

from typing import Dict, List, Set

from pypaimon.common.predicate import Predicate


def extract_predicate_to_list(result: list, input_predicate: 'Predicate', keys: List[str]):
    if not input_predicate or not keys:
        return

    if input_predicate.method == 'and':
        for sub_predicate in input_predicate.literals:
            extract_predicate_to_list(result, sub_predicate, keys)
        return
    elif input_predicate.method == 'or':
        # condition: involved keys all belong to primary keys
        involved_fields = _get_all_fields(input_predicate)
        if involved_fields and involved_fields.issubset(keys):
            result.append(input_predicate)
        return

    if input_predicate.field in keys:
        result.append(input_predicate)


def _get_all_fields(predicate: 'Predicate') -> Set[str]:
    if predicate.field is not None:
        return {predicate.field}
    involved_fields = set()
    if predicate.literals:
        for sub_predicate in predicate.literals:
            involved_fields.update(_get_all_fields(sub_predicate))
    return involved_fields


def extract_predicate_to_dict(result: Dict, input_predicate: 'Predicate', keys: List[str]):
    if not input_predicate or not keys:
        return

    if input_predicate.method == 'and':
        for sub_predicate in input_predicate.literals:
            extract_predicate_to_dict(result, sub_predicate, keys)
        return
    elif input_predicate.method == 'or':
        # ensure no recursive and/or
        if not input_predicate.literals or any(p.field is None for p in input_predicate.literals):
            return
        # condition: only one key for 'or', and the key belongs to keys
        involved_fields = {p.field for p in input_predicate.literals}
        field = involved_fields.pop() if len(involved_fields) == 1 else None
        if field is not None and field in keys:
            result[field].append(input_predicate)
        return

    if input_predicate.field in keys:
        result[input_predicate.field].append(input_predicate)
