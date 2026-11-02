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

from typing import Callable, List, Optional

import pyarrow as pa


class WriteBuffer:
    """Accumulates Arrow tables, concatenating them only when asked.

    ``append`` adds a table's row count and size to running totals, so a writer
    can answer its rolling check without touching Arrow. ``materialize`` and
    ``take`` are the operations that concatenate.
    """

    def __init__(self, merge: Callable[[pa.Table, pa.Table], pa.Table]):
        # ``merge(existing, new)`` combines two tables the way the owning writer
        # wants, and runs once per ``materialize`` rather than once per append.
        self._merge = merge
        self._table: Optional[pa.Table] = None
        self._appended: List[pa.Table] = []
        self._schema: Optional[pa.Schema] = None
        # ``concat_tables`` only collects chunks and ``nbytes`` sums the buffers
        # they reference, so this running total is what the fold will report.
        self.nbytes = 0
        self.num_rows = 0

    @property
    def is_empty(self) -> bool:
        """True when nothing has been appended and no table has been set.

        Distinct from ``num_rows == 0``, which a zero-row table also satisfies.
        """
        return self._table is None and not self._appended

    def append(self, data: pa.Table) -> None:
        # ``concat_tables`` rejects any schema difference while ``TableWrite``
        # admits a few (differing nullability, ``binary`` vs
        # ``fixed_size_binary``). Reject here so those keep failing in
        # ``write``, which aborts, instead of in ``prepare_commit``, which does
        # not. ``Schema.equals`` ignores metadata, and so does concat.
        if self._schema is None:
            self._schema = data.schema
        elif not data.schema.equals(self._schema):
            raise ValueError(
                "Cannot buffer a batch whose schema differs from the batches "
                f"already buffered.\nBuffered schema is: {self._schema}\n"
                f"Incoming schema is: {data.schema}")
        self._appended.append(data)
        self.nbytes += data.nbytes
        self.num_rows += data.num_rows

    def materialize(self) -> Optional[pa.Table]:
        """Concatenate everything appended so far into one table and return it.

        Returns None while the buffer is empty, and is a no-op when called again
        with nothing appended since.
        """
        if self._appended:
            folded = (self._appended[0] if len(self._appended) == 1
                      else pa.concat_tables(self._appended))
            self._appended = []
            self._table = (folded if self._table is None
                           else self._merge(self._table, folded))
            self._schema = self._table.schema
            self.nbytes = self._table.nbytes
            self.num_rows = self._table.num_rows
        return self._table

    def take(self) -> Optional[pa.Table]:
        """Return everything buffered as one table and empty the buffer."""
        table = self.materialize()
        self.reset()
        return table

    def reset(self, table: Optional[pa.Table] = None) -> None:
        """Replace the contents with ``table``, or empty the buffer.

        ``table`` is measured from scratch because the usual caller passes a
        slice of the table the running totals were describing.
        """
        self._appended = []
        self._table = table
        self._schema = None if table is None else table.schema
        self.nbytes = 0 if table is None else table.nbytes
        self.num_rows = 0 if table is None else table.num_rows
