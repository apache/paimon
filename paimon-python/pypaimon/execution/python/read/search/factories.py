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

"""Construct Python search executors from the public builder configuration."""


def new_vector_scan(request):
    from pypaimon.execution.python.read.search.vector_search_scan import DataEvolutionVectorScan
    scan_class = DataEvolutionVectorScan
    if request._is_primary_key_vector_search():
        from pypaimon.execution.python.read.search.primary_key_vector_scan import PrimaryKeyVectorScan
        scan_class = PrimaryKeyVectorScan
    return scan_class(
        request._table,
        request._vector_column,
        filter_=request._filter,
        partition_filter=request._partition_filter,
        options=request._options,
    )


def new_vector_read(request):
    from pypaimon.execution.python.read.search.vector_search_read import DataEvolutionVectorRead
    read_class = DataEvolutionVectorRead
    if request._is_primary_key_vector_search():
        from pypaimon.execution.python.read.search.primary_key_vector_read import PrimaryKeyVectorRead
        read_class = PrimaryKeyVectorRead
    return read_class(
        request._table,
        request._limit,
        request._vector_column,
        request._query_vector,
        filter_=request._filter,
        partition_filter=request._partition_filter,
        options=request._options,
    )


def new_batch_vector_read(request):
    from pypaimon.execution.python.read.search.vector_search_read import BatchVectorSearchReadImpl
    return BatchVectorSearchReadImpl(
        request._table,
        request._limit,
        request._vector_column,
        request._query_vectors,
        filter_=request._filter,
        partition_filter=request._partition_filter,
        options=request._options,
    )


def new_full_text_scan(request):
    definition = request._primary_key_full_text_definition()
    if definition is not None:
        from pypaimon.execution.python.read.search.primary_key_full_text_scan import PrimaryKeyFullTextScan
        return PrimaryKeyFullTextScan(
            request._table, definition, partition_filter=request._partition_filter)
    from pypaimon.execution.python.read.search.full_text_scan import DataEvolutionFullTextScan
    return DataEvolutionFullTextScan(
        request._table,
        request._text_columns(),
        partition_filter=request._partition_filter,
        filter_=request._filter,
    )


def new_full_text_read(request):
    definition = request._primary_key_full_text_definition()
    if definition is not None:
        from pypaimon.execution.python.read.search.primary_key_full_text_read import PrimaryKeyFullTextRead
        return PrimaryKeyFullTextRead(
            request._table, request._limit, request._text_columns(), request._query,
            definition=definition, partition_filter=request._partition_filter)
    from pypaimon.execution.python.read.search.full_text_read import DataEvolutionFullTextRead
    return DataEvolutionFullTextRead(
        request._table,
        request._limit,
        request._text_columns(),
        request._query,
        partition_filter=request._partition_filter,
        filter_=request._filter,
    )
