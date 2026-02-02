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

"""Builder for scanning global indexes."""

from abc import ABC, abstractmethod
from typing import List, Optional, Collection
from concurrent.futures import ThreadPoolExecutor, as_completed

from pypaimon.globalindex import GlobalIndexIOMeta, GlobalIndexReader, GlobalIndexEvaluator
from pypaimon.globalindex.range import Range
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.schema.data_types import DataField


class GlobalIndexScanBuilder(ABC):
    """Builder for scanning global indexes."""

    @abstractmethod
    def with_snapshot(self, snapshot_or_id) -> 'GlobalIndexScanBuilder':
        """Set the snapshot to scan."""
        pass

    @abstractmethod
    def with_partition_predicate(self, partition_predicate) -> 'GlobalIndexScanBuilder':
        """Set the partition predicate."""
        pass

    @abstractmethod
    def with_row_range(self, row_range: Range) -> 'GlobalIndexScanBuilder':
        """Set the row range to scan."""
        pass

    @abstractmethod
    def build(self) -> 'RowRangeGlobalIndexScanner':
        """Build the scanner."""
        pass

    @abstractmethod
    def shard_list(self) -> List[Range]:
        """Return sorted and non-overlapping ranges."""
        pass

    @staticmethod
    def parallel_scan(
        ranges: List[Range],
        builder: 'GlobalIndexScanBuilder',
        filter_predicate: Optional['Predicate'],
        vector_search: Optional['VectorSearch'],
        thread_num: Optional[int] = None
    ) -> Optional[GlobalIndexResult]:
        
        if not ranges:
            return None
        
        scanners = []
        try:
            # Build scanners for each range
            for row_range in ranges:
                scanner = builder.with_row_range(row_range).build()
                scanners.append((row_range, scanner))
            
            # Execute scans in parallel
            results: List[Optional[GlobalIndexResult]] = [None] * len(ranges)
            
            def scan_range(idx: int, scanner: 'RowRangeGlobalIndexScanner') -> tuple:
                result = scanner.scan(filter_predicate, vector_search)
                return idx, result
            
            with ThreadPoolExecutor(max_workers=thread_num) as executor:
                futures = {
                    executor.submit(scan_range, idx, scanner): idx
                    for idx, (_, scanner) in enumerate(scanners)
                }
                
                for future in as_completed(futures):
                    idx, result = future.result()
                    results[idx] = result
            
            # Combine results
            if all(r is None for r in results):
                return None

            # Find the first non-None result to start combining
            combined: Optional[GlobalIndexResult] = None
            for i, row_range in enumerate(ranges):
                if results[i] is not None:
                    if combined is None:
                        combined = results[i]
                    else:
                        combined = combined.or_(results[i])
                else:
                    # If no result for this range, include the full range
                    range_result = GlobalIndexResult.from_range(row_range)
                    if combined is None:
                        combined = range_result
                    else:
                        combined = combined.or_(range_result)

            return combined
            
        finally:
            # Close all scanners
            for _, scanner in scanners:
                try:
                    scanner.close()
                except Exception:
                    pass


class RowRangeGlobalIndexScanner:
    """Scanner for shard-based global indexes."""

    def __init__(
        self,
        options: dict,
        fields: list,
        file_io,
        index_path: str,
        row_range: Range,
        index_entries: list
    ):
        self._options = options
        self._row_range = row_range
        self._evaluator = self._create_evaluator(fields, file_io, index_path, index_entries)

    def _create_evaluator(self, fields, file_io, index_path, index_entries):
        index_metas = {}
        for entry in index_entries:
            global_index_meta = entry.get('global_index_meta')
            if global_index_meta is None:
                continue
            
            field_id = global_index_meta.index_field_id
            index_type = entry.get('index_type', 'unknown')
            
            if field_id not in index_metas:
                index_metas[field_id] = {}
            if index_type not in index_metas[field_id]:
                index_metas[field_id][index_type] = []
            
            io_meta = GlobalIndexIOMeta(
                file_name=entry.get('file_name'),
                file_size=entry.get('file_size'),
                metadata=global_index_meta.index_meta
            )
            index_metas[field_id][index_type].append(io_meta)
        
        def readers_function(field: DataField) -> Collection[GlobalIndexReader]:
            readers = []
            if field.id not in index_metas:
                return readers
            
            for index_type, io_metas in index_metas[field.id].items():
                if index_type == 'faiss-vector-ann':
                    # Lazy import to avoid requiring faiss when not used
                    from pypaimon.globalindex.faiss import (
                        FaissVectorIndexOptions,
                        FaissVectorGlobalIndexReader
                    )
                    options = FaissVectorIndexOptions.from_options(self._options)
                    reader = FaissVectorGlobalIndexReader(
                        file_io=file_io,
                        index_path=index_path,
                        io_metas=io_metas,
                        options=options
                    )
                    readers.append(reader)
                if index_type == 'btree':
                    from pypaimon.globalindex.btree import BTreeIndexReader
                    from pypaimon.globalindex.btree.key_serializer import create_serializer
                    for metadata in io_metas:
                        reader = BTreeIndexReader(
                            key_serializer=create_serializer(field.type),
                            file_io=file_io,
                            index_path=index_path,
                            io_meta=metadata
                        )
                        readers.append(reader)
            
            return readers
        
        return GlobalIndexEvaluator(fields, readers_function)

    def scan(
        self,
        predicate: Optional['Predicate'],
        vector_search: Optional['VectorSearch']
    ) -> Optional[GlobalIndexResult]:
        """Scan the global index with the given predicate and vector search."""
        return self._evaluator.evaluate(predicate, vector_search)

    def close(self) -> None:
        """Close the scanner and release resources."""
        self._evaluator.close()

    def __enter__(self) -> 'RowRangeGlobalIndexScanner':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
