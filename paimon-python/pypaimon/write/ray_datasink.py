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

"""
Module to write a Paimon table from a Ray Dataset, by using the Ray Datasink API.
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional

from ray.data.datasource.datasink import Datasink

from ray.util.annotations import DeveloperAPI
from ray.data.block import BlockAccessor, Block
from ray.data._internal.execution.interfaces import TaskContext
import pyarrow as pa

if TYPE_CHECKING:
    from pypaimon.table.table import Table
    from pypaimon.write.write_builder import WriteBuilder
    from pypaimon.write.commit_message import CommitMessage

logger = logging.getLogger(__name__)


def _cast_binary_to_table_schema(table: pa.Table, target_schema: pa.Schema) -> pa.Table:
    """Cast binary to large_binary for BLOB fields.

    When map_batches returns Python dicts, PyArrow infers bytes as binary,
    losing the original large_binary (BLOB) type. Cast back before writing.
    """
    cast_indices = []
    for i, field in enumerate(table.schema):
        target_field = target_schema.field(field.name) if field.name in target_schema.names else None
        if target_field and pa.types.is_binary(field.type) and pa.types.is_large_binary(target_field.type):
            cast_indices.append(i)

    if not cast_indices:
        return table

    columns = table.columns
    for i in cast_indices:
        columns[i] = columns[i].cast(pa.large_binary())
    fields = [target_schema.field(f.name) if i in cast_indices else f
              for i, f in enumerate(table.schema)]
    return pa.table(columns, schema=pa.schema(fields))

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
    ):
        self.table = table
        self.overwrite = overwrite
        self.static_partition = static_partition
        self._table_name = table.identifier.get_full_name()
        self._writer_builder: Optional["WriteBuilder"] = None
        self._pending_commit_messages: List["CommitMessage"] = []

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

    def on_write_start(self, schema=None) -> None:
        logger.info(f"Starting write job for table {self._table_name}")

        self._writer_builder = self.table.new_batch_write_builder()
        if self._is_overwrite():
            self._writer_builder = self._writer_builder.overwrite(self.static_partition)

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> List["CommitMessage"]:
        commit_messages_list: List["CommitMessage"] = []
        table_write = None

        try:
            writer_builder = self.table.new_batch_write_builder()
            if self._is_overwrite():
                writer_builder = writer_builder.overwrite(self.static_partition)
            
            table_write = writer_builder.new_write()

            table_schema = self.table.table_schema
            from pypaimon.schema.data_types import PyarrowFieldParser
            target_pa_schema = PyarrowFieldParser.from_paimon_schema(table_schema.fields)

            for block in blocks:
                block_arrow: pa.Table = BlockAccessor.for_block(block).to_arrow()

                if block_arrow.num_rows == 0:
                    continue

                block_arrow = _cast_binary_to_table_schema(block_arrow, target_pa_schema)

                table_write.write_arrow(block_arrow)

            commit_messages = table_write.prepare_commit()
            commit_messages_list.extend(commit_messages)

            table_write.close()
            table_write = None
            return commit_messages_list
        except Exception:
            if table_write is not None:
                try:
                    table_write.abort()
                except Exception as abort_error:
                    logger.warning(
                        f"Error aborting worker-side table_write: {abort_error}",
                        exc_info=abort_error
                    )
            raise

    @staticmethod
    def _extract_write_returns(write_result: Any):
        """Normalize WriteResult.write_returns (Ray 2.44+) vs list of returns
        (older Ray) into a list of per-task commit-message lists."""
        if hasattr(write_result, "write_returns"):
            return write_result.write_returns
        if isinstance(write_result, list):
            return write_result
        raise TypeError(
            f"Unexpected write_result type {type(write_result).__name__}: "
            "expected object with .write_returns or list of commit message "
            "lists. Refusing to proceed to avoid silent data loss."
        )

    def on_write_complete(
        self, write_result: Any
    ):
        table_commit = None
        try:
            write_returns = self._extract_write_returns(write_result)
            all_commit_messages = [
                commit_message
                for commit_messages in write_returns
                for commit_message in commit_messages
            ]

            non_empty_messages = [
                msg for msg in all_commit_messages if not msg.is_empty()
            ]

            self._pending_commit_messages = non_empty_messages

            if not non_empty_messages and not self._is_overwrite():
                logger.info("No data to commit (all commit messages are empty)")
                self._pending_commit_messages = []
                return

            logger.info(
                f"Committing {len(non_empty_messages)} commit messages "
                f"for table {self._table_name}"
            )

            table_commit = self._writer_builder.new_commit()
            table_commit.commit(non_empty_messages)

            self._pending_commit_messages = []

            logger.info(f"Successfully committed write job for table {self._table_name}")
        except Exception as e:
            logger.error(
                f"Error committing write job for table {self._table_name}: {e}",
                exc_info=e
            )
            if table_commit is not None:
                self._pending_commit_messages = []
            raise
        finally:
            if table_commit is not None:
                try:
                    table_commit.close()
                except Exception as e:
                    logger.warning(
                        f"Error closing table_commit: {e}",
                        exc_info=e
                    )

    def on_write_failed(self, error: Exception) -> None:
        logger.error(
            f"Write job failed for table {self._table_name}. Error: {error}",
            exc_info=error
        )
        
        if self._pending_commit_messages:
            try:
                table_commit = self._writer_builder.new_commit()
                try:
                    table_commit.abort(self._pending_commit_messages)
                    logger.info(
                        f"Aborted {len(self._pending_commit_messages)} commit messages "
                        f"for table {self._table_name} in on_write_failed()"
                    )
                finally:
                    table_commit.close()
            except Exception as abort_error:
                logger.error(
                    f"Error aborting commit messages in on_write_failed(): {abort_error}",
                    exc_info=abort_error
                )
            finally:
                self._pending_commit_messages = []


def write_paimon_dataset(
    dataset,
    table,
    *,
    overwrite: bool = False,
    static_partition: Optional[Dict[str, Any]] = None,
    concurrency: Optional[int] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    hash_fixed_precluster: str = "auto",
) -> None:
    """Write a Ray Dataset through the safe path for the table's bucket mode."""
    from pypaimon.ray.shuffle import (
        HASH_FIXED_PRECLUSTER_MAP_GROUPS,
        maybe_apply_repartition,
    )
    from pypaimon.table.bucket_mode import BucketMode

    if (
        hash_fixed_precluster == HASH_FIXED_PRECLUSTER_MAP_GROUPS
        and table.bucket_mode() == BucketMode.HASH_FIXED
        and getattr(table, "is_primary_key_table", False)
    ):
        _write_primary_key_groups(
            dataset,
            table,
            overwrite=overwrite,
            static_partition=static_partition,
            concurrency=concurrency,
            ray_remote_args=ray_remote_args,
        )
        return

    dataset = maybe_apply_repartition(dataset, table, hash_fixed_precluster)
    dataset.write_datasink(
        PaimonDatasink(
            table,
            overwrite=overwrite,
            static_partition=static_partition,
        ),
        concurrency=concurrency,
        ray_remote_args=ray_remote_args,
    )


def _write_primary_key_groups(
    dataset,
    table,
    *,
    overwrite: bool,
    static_partition: Optional[Dict[str, Any]],
    concurrency: Optional[int],
    ray_remote_args: Optional[Dict[str, Any]],
) -> None:
    import inspect
    import pickle

    from pypaimon.ray.shuffle import (
        _coerce_large_string_types,
        _group_by_partition_bucket,
    )

    grouped, bucket_col = _group_by_partition_bucket(dataset, table)
    message_col = "__paimon_commit_messages__"
    captured_table = table

    # Keep the writer inside the group UDF. Ray may split the UDF output
    # into multiple blocks, so only serialized commit messages leave it.
    def _write_group(group: pa.Table) -> pa.Table:
        if group.num_rows == 0:
            return pa.table({message_col: pa.array([], type=pa.binary())})

        rows = _coerce_large_string_types(
            group.drop_columns([bucket_col])
        )
        worker_sink = PaimonDatasink(
            captured_table,
            overwrite=overwrite,
            static_partition=static_partition,
        )
        commit_messages = worker_sink.write([rows], None)
        return pa.table({
            message_col: pa.array(
                [pickle.dumps(commit_messages)], type=pa.binary()
            )
        })

    map_kwargs = {"batch_format": "pyarrow"}
    if concurrency is not None:
        concurrency_param = inspect.signature(
            grouped.map_groups
        ).parameters.get("concurrency")
        if (
            concurrency_param is not None
            and concurrency_param.kind != inspect.Parameter.VAR_KEYWORD
        ):
            map_kwargs["concurrency"] = concurrency
        else:
            from ray.data._internal.compute import TaskPoolStrategy
            map_kwargs["compute"] = TaskPoolStrategy(size=concurrency)
    if ray_remote_args:
        map_kwargs.update(ray_remote_args)

    messages = grouped.map_groups(_write_group, **map_kwargs)
    coordinator = PaimonDatasink(
        table,
        overwrite=overwrite,
        static_partition=static_partition,
    )
    coordinator.on_write_start()
    try:
        write_returns = []
        for batch in messages.iter_batches(batch_format="pyarrow"):
            for blob in batch.column(message_col).to_pylist():
                write_returns.append(pickle.loads(blob))
        coordinator.on_write_complete(write_returns)
    except Exception as error:
        coordinator.on_write_failed(error)
        raise
