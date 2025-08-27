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

# pypaimon.api implementation based on Java code & py4j lib

from typing import Any, Iterator, List, Optional

import pyarrow as pa

try:
    from pypaimon.tests.py4j_impl import java_utils
    from pypaimon.tests.py4j_impl.gateway_factory import get_gateway
    from pypaimon.tests.py4j_impl.java_utils import (deserialize_java_object,
                                                     serialize_java_object)
    PY4J_IMPL_AVAILABLE = True
except ImportError:
    # py4j implementation not available
    PY4J_IMPL_AVAILABLE = False
    java_utils = get_gateway = deserialize_java_object = serialize_java_object = None


class SchemaPy4j:
    """Schema of a table."""

    def __init__(self,
                 pa_schema: pa.Schema,
                 partition_keys: Optional[List[str]] = None,
                 primary_keys: Optional[List[str]] = None,
                 options: Optional[dict] = None,
                 comment: Optional[str] = None):
        self.pa_schema = pa_schema
        self.partition_keys = partition_keys
        self.primary_keys = primary_keys
        self.options = options
        self.comment = comment


class CatalogPy4j:

    def __init__(self, j_catalog, catalog_options: dict):
        self._j_catalog = j_catalog
        self._catalog_options = catalog_options

    @staticmethod
    def create(catalog_options: dict) -> 'CatalogPy4j':
        j_catalog_context = java_utils.to_j_catalog_context(catalog_options)
        gateway = get_gateway()
        j_catalog = gateway.jvm.CatalogFactory.createCatalog(j_catalog_context)
        return CatalogPy4j(j_catalog, catalog_options)

    def get_table(self, identifier: str) -> 'TablePy4j':
        j_identifier = java_utils.to_j_identifier(identifier)
        j_table = self._j_catalog.getTable(j_identifier)
        return TablePy4j(j_table, self._catalog_options)

    def create_database(self, name: str, ignore_if_exists: bool, properties: Optional[dict] = None):
        if properties is None:
            properties = {}
        self._j_catalog.createDatabase(name, ignore_if_exists, properties)

    def create_table(self, identifier: str, schema: SchemaPy4j, ignore_if_exists: bool):
        j_identifier = java_utils.to_j_identifier(identifier)
        j_schema = java_utils.to_paimon_schema(schema)
        self._j_catalog.createTable(j_identifier, j_schema, ignore_if_exists)


class TablePy4j:

    def __init__(self, j_table, catalog_options: dict):
        self._j_table = j_table
        self._catalog_options = catalog_options

    def new_read_builder(self) -> 'ReadBuilderPy4j':
        j_read_builder = get_gateway().jvm.InvocationUtil.getReadBuilder(self._j_table)
        return ReadBuilderPy4j(j_read_builder, self._j_table.rowType(), self._catalog_options)

    def new_batch_write_builder(self) -> 'BatchWriteBuilderPy4j':
        java_utils.check_batch_write(self._j_table)
        j_batch_write_builder = get_gateway().jvm.InvocationUtil.getBatchWriteBuilder(self._j_table)
        return BatchWriteBuilderPy4j(j_batch_write_builder)


class ReadBuilderPy4j:

    def __init__(self, j_read_builder, j_row_type, catalog_options: dict):
        self._j_read_builder = j_read_builder
        self._j_row_type = j_row_type
        self._catalog_options = catalog_options

    def with_filter(self, predicate: 'PredicatePy4j'):
        self._j_read_builder.withFilter(predicate.to_j_predicate())
        return self

    def with_projection(self, projection: List[str]) -> 'ReadBuilderPy4j':
        field_names = list(map(lambda field: field.name(), self._j_row_type.getFields()))
        int_projection = list(map(lambda p: field_names.index(p), projection))
        gateway = get_gateway()
        int_projection_arr = gateway.new_array(gateway.jvm.int, len(projection))
        for i in range(len(projection)):
            int_projection_arr[i] = int_projection[i]
        self._j_read_builder.withProjection(int_projection_arr)
        return self

    def with_limit(self, limit: int) -> 'ReadBuilderPy4j':
        self._j_read_builder.withLimit(limit)
        return self

    def new_scan(self) -> 'TableScanPy4j':
        j_table_scan = self._j_read_builder.newScan()
        return TableScanPy4j(j_table_scan)

    def new_read(self) -> 'TableReadPy4j':
        j_table_read = self._j_read_builder.newRead().executeFilter()
        return TableReadPy4j(j_table_read, self._j_read_builder.readType(), self._catalog_options)

    def new_predicate_builder(self) -> 'PredicateBuilderPy4j':
        return PredicateBuilderPy4j(self._j_row_type)

    def read_type(self) -> 'RowTypePy4j':
        return RowTypePy4j(self._j_read_builder.readType())


class RowTypePy4j:

    def __init__(self, j_row_type):
        self._j_row_type = j_row_type

    def as_arrow(self) -> "pa.Schema":
        return java_utils.to_arrow_schema(self._j_row_type)


class TableScanPy4j:

    def __init__(self, j_table_scan):
        self._j_table_scan = j_table_scan

    def plan(self) -> 'PlanPy4j':
        j_plan = self._j_table_scan.plan()
        j_splits = j_plan.splits()
        return PlanPy4j(j_splits)


class PlanPy4j:

    def __init__(self, j_splits):
        self._j_splits = j_splits

    def splits(self) -> List['SplitPy4j']:
        return list(map(lambda s: self._build_single_split(s), self._j_splits))

    def _build_single_split(self, j_split) -> 'SplitPy4j':
        j_split_bytes = serialize_java_object(j_split)
        row_count = j_split.rowCount()
        files_optional = j_split.convertToRawFiles()
        if not files_optional.isPresent():
            file_size = 0
            file_paths = []
        else:
            files = files_optional.get()
            file_size = sum(file.length() for file in files)
            file_paths = [file.path() for file in files]
        return SplitPy4j(j_split_bytes, row_count, file_size, file_paths)


class SplitPy4j:

    def __init__(self, j_split_bytes, row_count: int, file_size: int, file_paths: List[str]):
        self._j_split_bytes = j_split_bytes
        self._row_count = row_count
        self._file_size = file_size
        self._file_paths = file_paths

    def to_j_split(self):
        return deserialize_java_object(self._j_split_bytes)

    def row_count(self) -> int:
        return self._row_count

    def file_size(self) -> int:
        return self._file_size

    def file_paths(self) -> List[str]:
        return self._file_paths


class TableReadPy4j:

    def __init__(self, j_table_read, j_read_type, catalog_options):
        self._arrow_schema = java_utils.to_arrow_schema(j_read_type)
        self._j_bytes_reader = get_gateway().jvm.InvocationUtil.createParallelBytesReader(
            j_table_read, j_read_type, 1)

    def to_arrow(self, splits):
        record_batch_reader = self.to_arrow_batch_reader(splits)
        return pa.Table.from_batches(record_batch_reader, schema=self._arrow_schema)

    def to_arrow_batch_reader(self, splits):
        j_splits = list(map(lambda s: s.to_j_split(), splits))
        self._j_bytes_reader.setSplits(j_splits)
        batch_iterator = self._batch_generator()
        return pa.RecordBatchReader.from_batches(self._arrow_schema, batch_iterator)

    def _batch_generator(self) -> Iterator[pa.RecordBatch]:
        while True:
            next_bytes = self._j_bytes_reader.next()
            if next_bytes is None:
                break
            else:
                stream_reader = pa.RecordBatchStreamReader(pa.BufferReader(next_bytes))
                yield from stream_reader


class BatchWriteBuilderPy4j:

    def __init__(self, j_batch_write_builder):
        self._j_batch_write_builder = j_batch_write_builder

    def overwrite(self, static_partition: Optional[dict] = None) -> 'BatchWriteBuilderPy4j':
        if static_partition is None:
            static_partition = {}
        self._j_batch_write_builder.withOverwrite(static_partition)
        return self

    def new_write(self) -> 'BatchTableWritePy4j':
        j_batch_table_write = self._j_batch_write_builder.newWrite()
        return BatchTableWritePy4j(j_batch_table_write, self._j_batch_write_builder.rowType())

    def new_commit(self) -> 'BatchTableCommitPy4j':
        j_batch_table_commit = self._j_batch_write_builder.newCommit()
        return BatchTableCommitPy4j(j_batch_table_commit)


class BatchTableWritePy4j:

    def __init__(self, j_batch_table_write, j_row_type):
        self._j_batch_table_write = j_batch_table_write
        self._j_bytes_writer = get_gateway().jvm.InvocationUtil.createBytesWriter(
            j_batch_table_write, j_row_type)
        self._arrow_schema = java_utils.to_arrow_schema(j_row_type)

    def write_arrow(self, table):
        for record_batch in table.to_reader():
            self._write_arrow_batch(record_batch)

    def write_arrow_batch(self, record_batch):
        self._write_arrow_batch(record_batch)

    def _write_arrow_batch(self, record_batch):
        stream = pa.BufferOutputStream()
        with pa.RecordBatchStreamWriter(stream, record_batch.schema) as writer:
            writer.write(record_batch)
        arrow_bytes = stream.getvalue().to_pybytes()
        self._j_bytes_writer.write(arrow_bytes)

    def prepare_commit(self) -> List['CommitMessagePy4j']:
        j_commit_messages = self._j_batch_table_write.prepareCommit()
        return list(map(lambda cm: CommitMessagePy4j(cm), j_commit_messages))

    def close(self):
        self._j_batch_table_write.close()
        self._j_bytes_writer.close()


class CommitMessagePy4j:

    def __init__(self, j_commit_message):
        self._j_commit_message = j_commit_message

    def to_j_commit_message(self):
        return self._j_commit_message


class BatchTableCommitPy4j:

    def __init__(self, j_batch_table_commit):
        self._j_batch_table_commit = j_batch_table_commit

    def commit(self, commit_messages: List[CommitMessagePy4j]):
        j_commit_messages = list(map(lambda cm: cm.to_j_commit_message(), commit_messages))
        self._j_batch_table_commit.commit(j_commit_messages)

    def close(self):
        self._j_batch_table_commit.close()


class PredicatePy4j:

    def __init__(self, j_predicate_bytes):
        self._j_predicate_bytes = j_predicate_bytes

    def to_j_predicate(self):
        return deserialize_java_object(self._j_predicate_bytes)


class PredicateBuilderPy4j:

    def __init__(self, j_row_type):
        self._field_names = j_row_type.getFieldNames()
        self._j_row_type = j_row_type
        self._j_predicate_builder = get_gateway().jvm.PredicateBuilder(j_row_type)

    def _build(self, method: str, field: str, literals: Optional[List[Any]] = None):
        error = ValueError(f'The field {field} is not in field list {self._field_names}.')
        try:
            index = self._field_names.index(field)
            if index == -1:
                raise error
        except ValueError:
            raise error

        if literals is None:
            literals = []

        j_predicate = get_gateway().jvm.PredicationUtil.build(
            self._j_row_type,
            self._j_predicate_builder,
            method,
            index,
            literals
        )
        return PredicatePy4j(serialize_java_object(j_predicate))

    def equal(self, field: str, literal: Any) -> PredicatePy4j:
        return self._build('equal', field, [literal])

    def not_equal(self, field: str, literal: Any) -> PredicatePy4j:
        return self._build('notEqual', field, [literal])

    def less_than(self, field: str, literal: Any) -> PredicatePy4j:
        return self._build('lessThan', field, [literal])

    def less_or_equal(self, field: str, literal: Any) -> PredicatePy4j:
        return self._build('lessOrEqual', field, [literal])

    def greater_than(self, field: str, literal: Any) -> PredicatePy4j:
        return self._build('greaterThan', field, [literal])

    def greater_or_equal(self, field: str, literal: Any) -> PredicatePy4j:
        return self._build('greaterOrEqual', field, [literal])

    def is_null(self, field: str) -> PredicatePy4j:
        return self._build('isNull', field)

    def is_not_null(self, field: str) -> PredicatePy4j:
        return self._build('isNotNull', field)

    def startswith(self, field: str, pattern_literal: Any) -> PredicatePy4j:
        return self._build('startsWith', field, [pattern_literal])

    def endswith(self, field: str, pattern_literal: Any) -> PredicatePy4j:
        return self._build('endsWith', field, [pattern_literal])

    def contains(self, field: str, pattern_literal: Any) -> PredicatePy4j:
        return self._build('contains', field, [pattern_literal])

    def is_in(self, field: str, literals: List[Any]) -> PredicatePy4j:
        return self._build('in', field, literals)

    def is_not_in(self, field: str, literals: List[Any]) -> PredicatePy4j:
        return self._build('notIn', field, literals)

    def between(self, field: str, included_lower_bound: Any, included_upper_bound: Any) \
            -> PredicatePy4j:
        return self._build('between', field, [included_lower_bound, included_upper_bound])

    def and_predicates(self, predicates: List[PredicatePy4j]) -> PredicatePy4j:
        predicates = list(map(lambda p: p.to_j_predicate(), predicates))
        j_predicate = get_gateway().jvm.PredicationUtil.buildAnd(predicates)
        return PredicatePy4j(serialize_java_object(j_predicate))

    def or_predicates(self, predicates: List[PredicatePy4j]) -> PredicatePy4j:
        predicates = list(map(lambda p: p.to_j_predicate(), predicates))
        j_predicate = get_gateway().jvm.PredicationUtil.buildOr(predicates)
        return PredicatePy4j(serialize_java_object(j_predicate))
