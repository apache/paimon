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

from functools import reduce

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
from pyarrow.dataset import Expression

from pypaimon.api import Predicate
from pypaimon.pynative.common.predicate import PredicateImpl


def convert_predicate(predicate: Predicate) -> Expression | bool:
    """
    # Convert Paimon's Predicate to PyArrow Dataset's filter
    """
    if not isinstance(predicate, PredicateImpl):
        raise RuntimeError("Type of predicate should be PredicateImpl")

    if predicate.method == 'equal':
        return ds.field(predicate.field) == predicate.literals[0]
    elif predicate.method == 'notEqual':
        return ds.field(predicate.field) != predicate.literals[0]
    elif predicate.method == 'lessThan':
        return ds.field(predicate.field) < predicate.literals[0]
    elif predicate.method == 'lessOrEqual':
        return ds.field(predicate.field) <= predicate.literals[0]
    elif predicate.method == 'greaterThan':
        return ds.field(predicate.field) > predicate.literals[0]
    elif predicate.method == 'greaterOrEqual':
        return ds.field(predicate.field) >= predicate.literals[0]
    elif predicate.method == 'isNull':
        return ds.field(predicate.field).is_null()
    elif predicate.method == 'isNotNull':
        return ds.field(predicate.field).is_valid()
    elif predicate.method == 'in':
        return ds.field(predicate.field).isin(predicate.literals)
    elif predicate.method == 'notIn':
        return ~ds.field(predicate.field).isin(predicate.literals)
    elif predicate.method == 'startsWith':
        pattern = predicate.literals[0]
        return pc.starts_with(ds.field(predicate.field).cast(pa.string()), pattern)
    elif predicate.method == 'endsWith':
        pattern = predicate.literals[0]
        return pc.ends_with(ds.field(predicate.field).cast(pa.string()), pattern)
    elif predicate.method == 'contains':
        pattern = predicate.literals[0]
        return pc.match_substring(ds.field(predicate.field).cast(pa.string()), pattern)
    elif predicate.method == 'between':
        return (ds.field(predicate.field) >= predicate.literals[0]) & \
            (ds.field(predicate.field) <= predicate.literals[1])
    elif predicate.method == 'and':
        return reduce(lambda x, y: x & y,
                      [convert_predicate(p) for p in predicate.literals])
    elif predicate.method == 'or':
        return reduce(lambda x, y: x | y,
                      [convert_predicate(p) for p in predicate.literals])
    else:
        raise ValueError(f"Unsupported predicate method: {predicate.method}")
