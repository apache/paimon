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


def filter_predicate_by_primary_keys(predicate, primary_keys):
    """
    Filter out predicates that are not related to primary key fields.
    """
    from pypaimon.api import Predicate

    if predicate is None or primary_keys is None:
        return predicate

    py_predicate = predicate.py_predicate

    if py_predicate.method in ['and', 'or']:
        filtered_literals = []
        for literal in py_predicate.literals:
            filtered = filter_predicate_by_primary_keys(literal, primary_keys)
            if filtered is not None:
                filtered_literals.append(filtered)

        if not filtered_literals:
            return None

        if len(filtered_literals) == 1:
            return filtered_literals[0]

        return Predicate(Predicate(
            method=py_predicate.method,
            index=py_predicate.index,
            field=py_predicate.field,
            literals=filtered_literals
        ), None)

    if py_predicate.field in primary_keys:
        return predicate
    else:
        return None
