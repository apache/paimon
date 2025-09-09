"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
"""
PyArrow compatibility module for Python 3.6 and PyArrow 5.0.0
"""

import pyarrow as pa
import pyarrow.compute as pc


def sort_table_by_column(table: pa.Table, column_name: str, order: str = 'ascending') -> pa.Table:
    """
    Sort a PyArrow Table by a column name.
    This function provides compatibility for PyArrow 5.0.0 which doesn't have Table.sort_by method.
    """
    if hasattr(table, 'sort_by'):
        # PyArrow >= 6.0 has native sort_by method
        return table.sort_by(column_name)
    else:
        # PyArrow 5.0 compatibility using sort_indices and take
        indices = pc.sort_indices(table, sort_keys=[(column_name, order)])
        return table.take(indices)


table_sort_by = sort_table_by_column
