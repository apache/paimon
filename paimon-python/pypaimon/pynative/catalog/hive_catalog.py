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

from typing import Optional

from pypaimon.api import Schema
from pypaimon.pynative.catalog.abstract_catalog import AbstractCatalog
from pypaimon.pynative.common.identifier import TableIdentifier


class HiveCatalog(AbstractCatalog):
    """Hive Catalog implementation for Paimon."""

    @staticmethod
    def identifier() -> str:
        return "hive"

    def create_database_impl(self, name: str, properties: Optional[dict] = None):
        pass

    def create_table_impl(self, table_identifier: TableIdentifier, schema: 'Schema'):
        pass

    def get_table_schema(self, table_identifier: TableIdentifier):
        pass
