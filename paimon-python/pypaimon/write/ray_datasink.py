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


"""Public Ray datasink configuration and execution entry points."""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional

from ray.data.datasource.datasink import Datasink
from ray.util.annotations import DeveloperAPI
from ray.data.block import Block
from ray.data._internal.execution.interfaces import TaskContext

if TYPE_CHECKING:
    from pypaimon.table.table import Table
    from pypaimon.write.write_builder import WriteBuilder
    from pypaimon.write.commit_message import CommitMessage


@dataclass(frozen=True)
class PaimonWriteResult:
    """Metadata reported by the exact coordinator commit."""

    row_count: int
    snapshot_id: int


# Python 3.8 / Ray 2.10: Datasink is not subscriptable at runtime
try:
    _DatasinkBase = Datasink[List["CommitMessage"]]
except TypeError:
    _DatasinkBase = Datasink


@DeveloperAPI
class PaimonDatasink(_DatasinkBase):
    def __init__(
        self,
        table: "Table",
        overwrite: bool = False,
        static_partition: Optional[Dict[str, Any]] = None,
        postpone_bucket_plan=None,
    ):
        self.table = table
        self.overwrite = overwrite
        self.static_partition = static_partition
        self._postpone_bucket_plan = postpone_bucket_plan
        self._table_name = table.identifier.get_full_name()
        self._writer_builder: Optional["WriteBuilder"] = None
        self.commit_result: Optional[PaimonWriteResult] = None

    def _is_overwrite(self) -> bool:
        return self.overwrite or self.static_partition is not None

    def __getstate__(self) -> dict:
        state = self.__dict__.copy()
        return state

    def __setstate__(self, state: dict) -> None:
        self.__dict__.update(state)
        writer_builder = getattr(self, '_writer_builder', None)
        if writer_builder is not None and not hasattr(writer_builder, 'table'):
            self._writer_builder = None
        if not hasattr(self, '_table_name'):
            self._table_name = self.table.identifier.get_full_name()
        if not hasattr(self, 'static_partition'):
            self.static_partition = None
        if not hasattr(self, '_postpone_bucket_plan'):
            self._postpone_bucket_plan = None

    def on_write_start(self, schema=None) -> None:
        from pypaimon.execution.datasink.ray_datasink import RayDatasinkOperations
        return RayDatasinkOperations.on_write_start(self, schema)

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> List["CommitMessage"]:
        from pypaimon.execution.datasink.ray_datasink import RayDatasinkOperations
        return RayDatasinkOperations.write(self, blocks, ctx)

    @staticmethod
    def _extract_write_returns(write_result: Any):
        from pypaimon.execution.datasink.ray_datasink import RayDatasinkOperations
        return RayDatasinkOperations._extract_write_returns(write_result)

    @staticmethod
    def _extract_row_count(write_result: Any, write_returns) -> int:
        from pypaimon.execution.datasink.ray_datasink import RayDatasinkOperations
        return RayDatasinkOperations._extract_row_count(write_result, write_returns)

    def on_write_complete(
        self, write_result: Any
    ):
        from pypaimon.execution.datasink.ray_datasink import RayDatasinkOperations
        return RayDatasinkOperations.on_write_complete(self, write_result)

    def on_write_failed(self, error: Exception) -> None:
        from pypaimon.execution.datasink.ray_datasink import RayDatasinkOperations
        return RayDatasinkOperations.on_write_failed(self, error)


def write_paimon_dataset(
    dataset,
    table,
    *,
    overwrite: bool = False,
    static_partition: Optional[Dict[str, Any]] = None,
    concurrency: Optional[int] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    hash_fixed_precluster: str = "auto",
    postpone_bucket_planner=None,
) -> Optional[PaimonWriteResult]:
    """Write a Ray Dataset through the safe path for the table's bucket mode."""
    from pypaimon.ray.shuffle import HASH_FIXED_PRECLUSTER_MODES

    if hash_fixed_precluster not in HASH_FIXED_PRECLUSTER_MODES:
        raise ValueError(
            "hash_fixed_precluster must be one of {}, got {!r}".format(
                sorted(HASH_FIXED_PRECLUSTER_MODES),
                hash_fixed_precluster,
            )
        )
    from pypaimon.execution.datasink.ray_datasink import write_paimon_dataset as execute

    return execute(
        dataset, table, overwrite=overwrite, static_partition=static_partition,
        concurrency=concurrency, ray_remote_args=ray_remote_args,
        hash_fixed_precluster=hash_fixed_precluster,
        postpone_bucket_planner=postpone_bucket_planner,
    )
