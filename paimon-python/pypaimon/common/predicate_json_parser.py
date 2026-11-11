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

"""Extract field references for shared query-authorization validation."""

import json


# REST DateExtractTransform names are metadata, independent of Arrow execution.
_DATE_EXTRACT_NAMES = frozenset((
    'YEAR',
    'MONTH',
    'DAY',
    'HOUR',
    'MINUTE',
    'SECOND',
    'QUARTER',
    'DAY_OF_YEAR',
    'WEEKDAY',
    'ISO_DAY_OF_WEEK',
    'DAY_OF_WEEK',
    'WEEK',
    'YEAR_OF_WEEK',
))


def extract_referenced_fields(json_str: str) -> set:
    data = json.loads(json_str)
    fields = set()
    _collect_fields(data, fields)
    return fields


def _collect_fields(data: dict, fields: set):
    kind = data.get("kind")
    if kind == "LEAF":
        _collect_all_field_refs_from_transform(data["transform"], fields)
    elif kind == "COMPOUND":
        for child in data["children"]:
            _collect_fields(child, fields)


def _collect_all_field_refs_from_transform(transform: dict, fields: set = None) -> set:
    if fields is None:
        fields = set()
    name = transform.get("name")
    if (name in ("FIELD_REF", "CAST") or name in _DATE_EXTRACT_NAMES) and "fieldRef" in transform:
        fields.add(transform["fieldRef"]["name"])
    else:
        for inp in transform.get("inputs", []):
            if isinstance(inp, dict) and "name" in inp:
                # _resolve_transform_input reads every dict input as a column by name
                fields.add(inp["name"])
                _collect_all_field_refs_from_transform(inp, fields)
    return fields
