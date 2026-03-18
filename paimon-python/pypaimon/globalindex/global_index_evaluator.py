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

"""Global index evaluator for filtering data using global indexes."""

from typing import Callable, Collection, Dict, List, Optional

from pypaimon.globalindex.global_index_reader import GlobalIndexReader, FieldRef
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.common.predicate import Predicate
from pypaimon.globalindex.vector_search import VectorSearch
from pypaimon.schema.data_types import DataField


class GlobalIndexEvaluator:
    """
    Predicate evaluator for filtering data using global indexes.
    """

    def __init__(
        self,
        fields: List[DataField],
        readers_function: Callable[[DataField], Collection[GlobalIndexReader]]
    ):
        self._fields = fields
        self._field_by_name = {f.name: f for f in fields}
        self._readers_function = readers_function
        self._index_readers_cache: Dict[int, Collection[GlobalIndexReader]] = {}

    def evaluate(
        self,
        predicate: Optional[Predicate],
        vector_search: Optional[VectorSearch]
    ) -> Optional[GlobalIndexResult]:
        compound_result: Optional[GlobalIndexResult] = None
        
        # Evaluate predicate first
        if predicate is not None:
            compound_result = self._visit_predicate(predicate)
        
        # Evaluate vector search
        if vector_search is not None:
            field = self._field_by_name.get(vector_search.field_name)
            if field is None:
                raise ValueError(f"Field not found: {vector_search.field_name}")
            
            field_id = field.id
            readers = self._index_readers_cache.get(field_id)
            if readers is None:
                readers = self._readers_function(field)
                self._index_readers_cache[field_id] = readers
            
            # If we have a compound result from predicates, use it to filter vector search
            if compound_result is not None:
                vector_search = vector_search.with_include_row_ids(compound_result.results())
            
            for reader in readers:
                child_result = vector_search.visit(reader)
                if child_result is None:
                    continue
                
                # AND operation
                if compound_result is not None:
                    compound_result = compound_result.and_(child_result)
                else:
                    compound_result = child_result
                
                if compound_result.is_empty():
                    return compound_result
        
        return compound_result

    def _visit_predicate(self, predicate: Predicate) -> Optional[GlobalIndexResult]:
        """Visit a predicate and return the index result."""
        if predicate.method == 'and':
            compound_result: Optional[GlobalIndexResult] = None
            for child in predicate.literals:
                child_result = self._visit_predicate(child)
                
                if child_result is not None:
                    if compound_result is not None:
                        compound_result = compound_result.and_(child_result)
                    else:
                        compound_result = child_result
                
                if compound_result is not None and compound_result.is_empty():
                    return compound_result
            
            return compound_result
        
        elif predicate.method == 'or':
            compound_result = GlobalIndexResult.create_empty()
            for child in predicate.literals:
                child_result = self._visit_predicate(child)
                
                if child_result is None:
                    return None
                
                compound_result = compound_result.or_(child_result)
            
            return compound_result
        
        else:
            # Leaf predicate
            return self._visit_leaf_predicate(predicate)

    def _visit_leaf_predicate(self, predicate: Predicate) -> Optional[GlobalIndexResult]:
        """Visit a leaf predicate and return the index result."""
        field = self._field_by_name.get(predicate.field)
        if field is None:
            return None
        
        field_id = field.id
        readers = self._index_readers_cache.get(field_id)
        if readers is None:
            readers = self._readers_function(field)
            self._index_readers_cache[field_id] = readers
        
        field_ref = FieldRef(predicate.index, predicate.field, str(field.type))
        
        compound_result: Optional[GlobalIndexResult] = None
        
        for reader in readers:
            child_result = self._visit_function(reader, predicate, field_ref)
            if child_result is None:
                continue
            
            if compound_result is not None:
                compound_result = compound_result.and_(child_result)
            else:
                compound_result = child_result
            
            if compound_result.is_empty():
                return compound_result
        
        return compound_result

    def _visit_function(
        self,
        reader: GlobalIndexReader,
        predicate: Predicate,
        field_ref: FieldRef
    ) -> Optional[GlobalIndexResult]:
        """Visit a predicate function with the given reader."""
        method = predicate.method
        literals = predicate.literals
        
        if method == 'equal':
            return reader.visit_equal(field_ref, literals[0])
        elif method == 'notEqual':
            return reader.visit_not_equal(field_ref, literals[0])
        elif method == 'lessThan':
            return reader.visit_less_than(field_ref, literals[0])
        elif method == 'lessOrEqual':
            return reader.visit_less_or_equal(field_ref, literals[0])
        elif method == 'greaterThan':
            return reader.visit_greater_than(field_ref, literals[0])
        elif method == 'greaterOrEqual':
            return reader.visit_greater_or_equal(field_ref, literals[0])
        elif method == 'isNull':
            return reader.visit_is_null(field_ref)
        elif method == 'isNotNull':
            return reader.visit_is_not_null(field_ref)
        elif method == 'in':
            return reader.visit_in(field_ref, literals)
        elif method == 'notIn':
            return reader.visit_not_in(field_ref, literals)
        elif method == 'startsWith':
            return reader.visit_starts_with(field_ref, literals[0])
        elif method == 'endsWith':
            return reader.visit_ends_with(field_ref, literals[0])
        elif method == 'contains':
            return reader.visit_contains(field_ref, literals[0])
        elif method == 'between':
            return reader.visit_between(field_ref, literals[0], literals[1])
        
        return None

    def close(self) -> None:
        """Close the evaluator and release resources."""
        for readers in self._index_readers_cache.values():
            for reader in readers:
                try:
                    reader.close()
                except Exception:
                    pass
        self._index_readers_cache.clear()

    def __enter__(self) -> 'GlobalIndexEvaluator':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
