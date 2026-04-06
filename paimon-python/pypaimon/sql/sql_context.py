#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import pyarrow as pa


class SQLContext:
    """SQL query context for Paimon tables.

    Uses pypaimon-rust and DataFusion to execute SQL queries against Paimon tables.
    Supports registering multiple catalogs for cross-catalog queries.

    Example::

        from pypaimon.sql import SQLContext

        ctx = SQLContext()
        ctx.register_catalog("paimon", {"warehouse": "/path/to/warehouse"})
        ctx.set_current_catalog("paimon")
        ctx.set_current_database("default")
        result = ctx.sql("SELECT * FROM my_table")
    """

    def __init__(self):
        try:
            from datafusion import SessionContext
        except ImportError:
            raise ImportError(
                "datafusion is required for SQL query support. "
                "Install it with: pip install pypaimon[sql]"
            )

        self._ctx = SessionContext()

    def register_catalog(self, name, options):
        """Register a Paimon catalog as a DataFusion catalog provider.

        Args:
            name: The catalog name to register under.
            options: A dict of catalog options (e.g. {"warehouse": "/path/to/warehouse"}).
        """
        try:
            from pypaimon_rust.datafusion import PaimonCatalog
        except ImportError:
            raise ImportError(
                "pypaimon-rust is required for SQL query support. "
                "Install it with: pip install pypaimon[sql]"
            )

        paimon_catalog = PaimonCatalog(options)
        self._ctx.register_catalog_provider(name, paimon_catalog)

    def set_current_catalog(self, catalog_name: str):
        """Set the default catalog for SQL queries."""
        self._ctx.sql(f"SET datafusion.catalog.default_catalog = '{catalog_name}'")

    def set_current_database(self, database: str):
        """Set the default database for SQL queries."""
        self._ctx.sql(f"SET datafusion.catalog.default_schema = '{database}'")

    def sql(self, query: str) -> pa.Table:
        """Execute a SQL query and return the result as a PyArrow Table."""
        df = self._ctx.sql(query)
        batches = df.collect()
        if not batches:
            return pa.Table.from_batches([], schema=df.schema())
        return pa.Table.from_batches(batches)
