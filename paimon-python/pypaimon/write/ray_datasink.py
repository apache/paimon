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
"""
Module to write a Paimon table from a Ray Dataset, by using the Ray Datasink API.
"""

import logging
from typing import TYPE_CHECKING, Any, Iterable, List, Optional

from ray.data.datasource.datasink import Datasink
try:
    from ray.data.datasource.datasink import WriteResult
except ImportError:
    WriteResult = None  # type: ignore[misc, assignment]  # Ray < 2.44 has no WriteResult

from ray.util.annotations import DeveloperAPI
from ray.data.block import BlockAccessor, Block
from ray.data._internal.execution.interfaces import TaskContext
import pyarrow as pa

if TYPE_CHECKING:
    from pypaimon.table.table import Table
    from pypaimon.write.write_builder import WriteBuilder
    from pypaimon.write.commit_message import CommitMessage

logger = logging.getLogger(__name__)

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
    ):
        self.table = table
        self.overwrite = overwrite
        self._table_name = table.identifier.get_full_name()
        self._writer_builder: Optional["WriteBuilder"] = None
        self._pending_commit_messages: List["CommitMessage"] = []

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

    def on_write_start(self, schema=None) -> None:
        logger.info(f"Starting write job for table {self._table_name}")

        self._writer_builder = self.table.new_batch_write_builder()
        if self.overwrite:
            self._writer_builder = self._writer_builder.overwrite()

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> List["CommitMessage"]:
        commit_messages_list: List["CommitMessage"] = []
        table_write = None

        try:
            writer_builder = self.table.new_batch_write_builder()
            if self.overwrite:
                writer_builder = writer_builder.overwrite()
            
            table_write = writer_builder.new_write()

            for block in blocks:
                block_arrow: pa.Table = BlockAccessor.for_block(block).to_arrow()

                if block_arrow.num_rows == 0:
                    continue

                table_write.write_arrow(block_arrow)

            commit_messages = table_write.prepare_commit()
            commit_messages_list.extend(commit_messages)
        finally:
            if table_write is not None:
                table_write.close()

        return commit_messages_list

    def on_write_complete(
        self, write_result: Any
    ):
        table_commit = None
        commit_messages_to_abort = []
        try:
            # WriteResult.write_returns (Ray 2.44+); older Ray may pass compatible object
            if hasattr(write_result, "write_returns"):
                write_returns = write_result.write_returns
            else:
                write_returns = write_result if isinstance(write_result, list) else []
            all_commit_messages = [
                commit_message
                for commit_messages in write_returns
                for commit_message in commit_messages
            ]

            non_empty_messages = [
                msg for msg in all_commit_messages if not msg.is_empty()
            ]

            self._pending_commit_messages = non_empty_messages

            if not non_empty_messages:
                logger.info("No data to commit (all commit messages are empty)")
                self._pending_commit_messages = []
                return

            logger.info(
                f"Committing {len(non_empty_messages)} commit messages "
                f"for table {self._table_name}"
            )

            table_commit = self._writer_builder.new_commit()
            commit_messages_to_abort = non_empty_messages
            table_commit.commit(non_empty_messages)

            commit_messages_to_abort = []
            self._pending_commit_messages = []

            logger.info(f"Successfully committed write job for table {self._table_name}")
        except Exception as e:
            logger.error(
                f"Error committing write job for table {self._table_name}: {e}",
                exc_info=e
            )
            if table_commit is not None and commit_messages_to_abort:
                try:
                    table_commit.abort(commit_messages_to_abort)
                    logger.info(
                        f"Aborted {len(commit_messages_to_abort)} commit messages "
                        f"for table {self._table_name}"
                    )
                except Exception as abort_error:
                    logger.error(
                        f"Error aborting commit messages: {abort_error}",
                        exc_info=abort_error
                    )
                finally:
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
