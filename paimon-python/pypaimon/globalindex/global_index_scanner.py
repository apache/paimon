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

"""Scanner for shard-based global indexes."""

from typing import Collection, Optional

from pypaimon.globalindex.global_index_evaluator import GlobalIndexEvaluator
from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta
from pypaimon.globalindex.global_index_reader import GlobalIndexReader
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.common.predicate import Predicate
from pypaimon.read.push_down_utils import _get_all_fields
from pypaimon.schema.data_types import DataField
from pypaimon.utils.range import Range


class GlobalIndexScanner:
    """Scanner for shard-based global indexes."""

    def __init__(
        self,
        fields: list,
        file_io,
        index_path: str,
        index_files: Collection['IndexFileMeta']
    ):
        self._evaluator = self._create_evaluator(fields, file_io, index_path, index_files)

    def _create_evaluator(self, fields, file_io, index_path, index_files):
        index_metas = {}
        for index_file in index_files:
            global_index_meta = index_file.global_index_meta
            if global_index_meta is None:
                continue

            field_id = global_index_meta.index_field_id
            index_type = index_file.index_type

            if field_id not in index_metas:
                index_metas[field_id] = {}
            if index_type not in index_metas[field_id]:
                index_metas[field_id][index_type] = {}

            range_key = Range(global_index_meta.row_range_start, global_index_meta.row_range_end)
            if range_key not in index_metas[field_id][index_type]:
                index_metas[field_id][index_type][range_key] = []

            io_meta = GlobalIndexIOMeta(
                file_name=index_file.file_name,
                file_size=index_file.file_size,
                metadata=global_index_meta.index_meta,
                external_path=index_file.external_path,
            )
            index_metas[field_id][index_type][range_key].append(io_meta)

        def readers_function(field: DataField) -> Collection[GlobalIndexReader]:
            return _create_readers(file_io, index_path, index_metas.get(field.id), field)

        return GlobalIndexEvaluator(fields, readers_function)

    @staticmethod
    def create(table, index_files=None, partition_filter=None, predicate=None) -> Optional['GlobalIndexScanner']:
        """Create a GlobalIndexScanner.

        Can be called in two ways:
        1. create(table, index_files) - with explicit index files
        2. create(table, partition_filter=..., predicate=...) - scan index files from snapshot
        """
        from pypaimon.index.index_file_handler import IndexFileHandler

        if index_files is not None:
            if len(index_files) == 0:
                return None
            return GlobalIndexScanner(
                fields=table.fields,
                file_io=table.file_io,
                index_path=table.path_factory().global_index_path_factory().index_path(),
                index_files=index_files
            )

        # Scan index files from snapshot using partition_filter and predicate
        filter_field_names = _get_all_fields(predicate)
        filter_field_ids = set()
        if predicate is not None:
            for field_item in table.fields:
                if field_item.name in filter_field_names:
                    filter_field_ids.add(field_item.id)

        def index_file_filter(entry):
            if partition_filter is not None:
                if not partition_filter.test(entry.partition):
                    return False
            global_index_meta = entry.index_file.global_index_meta
            if global_index_meta is None:
                return False
            return global_index_meta.index_field_id in filter_field_ids

        from pypaimon.snapshot.snapshot_manager import SnapshotManager
        snapshot = SnapshotManager(table).get_latest_snapshot()
        index_file_handler = IndexFileHandler(table=table)
        entries = index_file_handler.scan(snapshot, index_file_filter)
        scanned_index_files = [entry.index_file for entry in entries]

        if len(scanned_index_files) == 0:
            return None
        return GlobalIndexScanner(
            fields=table.fields,
            file_io=table.file_io,
            index_path=table.path_factory().global_index_path_factory().index_path(),
            index_files=scanned_index_files
        )

    def scan(self, predicate: Optional[Predicate]) -> Optional[GlobalIndexResult]:
        """Scan the global index with the given predicate."""
        return self._evaluator.evaluate(predicate)

    def close(self):
        """Close the scanner and release resources."""
        self._evaluator.close()

    def __enter__(self) -> 'GlobalIndexScanner':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


def _create_readers(file_io, index_path, index_type_metas, field):
    """Create readers for a specific field, dispatched by index_type.

    Unknown indexTypes raise — a silent skip would make
    ``VectorSearchReadImpl._pre_filter`` return ``None`` and the vector search
    would then return unfiltered results that violate the user predicate.
    """
    if index_type_metas is None:
        return []

    from pypaimon.globalindex.offset_global_index_reader import (
        OffsetGlobalIndexReader,
    )
    from pypaimon.globalindex.union_global_index_reader import (
        UnionGlobalIndexReader,
    )

    readers = []
    for index_type, range_metas in index_type_metas.items():
        offset_readers = []
        for range_key, io_metas in range_metas.items():
            inner_readers = _create_inner_readers(
                index_type, file_io, index_path, field, io_metas)
            for inner in inner_readers:
                offset_readers.append(
                    OffsetGlobalIndexReader(
                        inner, range_key.from_, range_key.to))
        if offset_readers:
            readers.append(UnionGlobalIndexReader(offset_readers))
    return readers


def _create_inner_readers(index_type, file_io, index_path, field, io_metas):
    """Build per-file (or per-shard) readers for a single indexType/range."""
    if index_type == 'btree':
        from pypaimon.globalindex.btree import BTreeIndexReader
        from pypaimon.globalindex.btree.key_serializer import create_serializer
        key_serializer = create_serializer(field.type)
        return [
            BTreeIndexReader(
                key_serializer=key_serializer,
                file_io=file_io,
                index_path=index_path,
                io_meta=io_meta,
            )
            for io_meta in io_metas
        ]

    from pypaimon.globalindex.tantivy import (
        TANTIVY_FULLTEXT_IDENTIFIER,
        TantivyFullTextGlobalIndexReader,
    )
    if index_type == TANTIVY_FULLTEXT_IDENTIFIER:
        # Tantivy expects one file per shard; create one reader per io_meta.
        return [
            TantivyFullTextGlobalIndexReader(file_io, index_path, [io_meta])
            for io_meta in io_metas
        ]

    raise ValueError(
        "Unsupported global-index type in scanner: '%s'" % index_type)
