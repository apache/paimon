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

"""Search interfaces and transportable plans shared by execution backends."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List

from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.read.split import DataSplit
from pypaimon.table.source.full_text_search_split import FullTextSearchSplit


class VectorSearchScanPlan:
    """Plan of vector search scan."""

    def __init__(self, splits, snapshot=None):
        # type: (list, object) -> None
        self._splits = splits
        self._snapshot = snapshot

    def splits(self):
        # type: () -> list
        return self._splits

    def snapshot(self):
        return self._snapshot


class VectorSearchScan(ABC):
    """Vector search scan to scan index files."""

    @abstractmethod
    def scan(self):
        # type: () -> VectorSearchScanPlan
        pass


class VectorSearchRead(ABC):
    """Vector search read to read index files."""

    def read_plan(self, plan):
        # type: (VectorSearchScanPlan) -> GlobalIndexResult
        return self._read(plan.splits(), plan.snapshot())

    def read(self, splits):
        # type: (List[VectorSearchSplit]) -> GlobalIndexResult
        return self._read(splits, None)

    @abstractmethod
    def _read(self, splits, snapshot):
        pass


class BatchVectorSearchRead(ABC):
    """Batch vector search read to read index files."""

    def read_batch_plan(self, plan):
        # type: (VectorSearchScanPlan) -> List[GlobalIndexResult]
        return self._read_batch(plan.splits(), plan.snapshot())

    def read_batch(self, splits):
        # type: (List[VectorSearchSplit]) -> List[GlobalIndexResult]
        return self._read_batch(splits, None)

    @abstractmethod
    def _read_batch(self, splits, snapshot):
        pass


class FullTextScanPlan:
    """Plan of full-text scan."""

    def __init__(self, splits: List[FullTextSearchSplit], snapshot=None):
        self._splits = splits
        self._snapshot = snapshot

    def snapshot(self):
        return self._snapshot

    def splits(self) -> List[FullTextSearchSplit]:
        return self._splits


class FullTextScan(ABC):
    """Full-text scan to scan index files."""

    @abstractmethod
    def scan(self) -> FullTextScanPlan:
        pass


class FullTextRead(ABC):
    """Full-text read to read index files."""

    def read_plan(self, plan: FullTextScanPlan) -> GlobalIndexResult:
        return self.read(plan.splits())

    @abstractmethod
    def read(self, splits: List[FullTextSearchSplit]) -> GlobalIndexResult:
        pass


@dataclass(frozen=True)
class PrimaryKeyVectorSearchSplit:
    data_split: DataSplit
    payloads: tuple
    uncovered_data_files: tuple
    row_ranges_by_file: dict


class PrimaryKeyVectorScanPlan(VectorSearchScanPlan):
    def __init__(self, snapshot_id, splits):
        super().__init__(splits)
        self.snapshot_id = snapshot_id


@dataclass(frozen=True)
class PrimaryKeyFullTextSearchSplit:
    data_split: DataSplit
    payloads: tuple
    uncovered_data_files: tuple
    row_ranges_by_file: dict


class PrimaryKeyFullTextScanPlan(FullTextScanPlan):
    def __init__(self, snapshot_id, splits):
        super().__init__(splits)
        self.snapshot_id = snapshot_id
