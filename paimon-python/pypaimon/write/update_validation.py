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

from pypaimon.common.predicate import Predicate


def _authorize_update(table):
    """Require the same whole-table grant as plan_all_files for fast paths."""
    from pypaimon.catalog.catalog_exception import TableNoPermissionException
    from pypaimon.read.table_scan import authorize

    query_auth = table.catalog_environment.table_query_auth(table.options, table.identifier)
    if authorize(table, query_auth, None) is not None:
        raise TableNoPermissionException(table.identifier)


def predicate_fields(predicate: Predicate) -> set:
    if predicate.field is not None:
        return {predicate.field}
    fields = set()
    for child in predicate.literals or []:
        fields.update(predicate_fields(child))
    return fields
