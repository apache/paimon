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


from typing import Optional

from pypaimon.common.predicate import Predicate
from pypaimon.table.special_fields import SpecialFields


class DataEvolutionBatchScan:
    @staticmethod
    def remove_row_id_filter(predicate: Optional[Predicate]) -> Optional[Predicate]:
        if predicate is None:
            return None
        return DataEvolutionBatchScan._remove(predicate)

    @staticmethod
    def _remove(predicate: Predicate) -> Optional[Predicate]:
        if predicate.method == 'and':
            new_children = []
            for p in predicate.literals:
                sub = DataEvolutionBatchScan._remove(p)
                if sub is not None:
                    new_children.append(sub)
            if not new_children:
                return None
            if len(new_children) == 1:
                return new_children[0]
            return Predicate(
                method='and',
                index=predicate.index,
                field=predicate.field,
                literals=new_children
            )
        if predicate.method == 'or':
            new_children = []
            for p in predicate.literals:
                sub = DataEvolutionBatchScan._remove(p)
                if sub is None:
                    return None
                new_children.append(sub)
            if len(new_children) == 1:
                return new_children[0]
            return Predicate(
                method='or',
                index=predicate.index,
                field=predicate.field,
                literals=new_children
            )
        # Leaf: remove if _ROW_ID
        if predicate.field == SpecialFields.ROW_ID.name:
            return None
        return predicate
