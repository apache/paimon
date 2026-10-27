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

"""Authorization helpers shared by scan and read entry points."""

from pypaimon.read.push_down_utils import predicate_field_names
from pypaimon.read.query_auth_split import query_auth_columns, resolve_auth_result
from pypaimon.table.special_fields import SpecialFields


def latest_auth_fields(table):
    """The table's current columns, re-read so an alter under a long-lived handle fails closed.

    The catalog answers when it produced the table: a REST data token need not reach the schema
    directory, which the scanners avoid for the same reason.
    """
    environment = getattr(table, "catalog_environment", None)
    loader = getattr(environment, "catalog_loader", None)
    if loader is not None and environment.identifier is not None:
        return loader.load().get_table(environment.identifier).fields
    latest = table.schema_manager.latest()
    return latest.fields if latest is not None else table.fields


def validate_auth_rules(table, auth_result, latest_fields=None):
    """Refuse the read when the rules cannot be bound to the columns they name.

    The reader calls this too, so a split that never met a validating scan is held to it.
    """
    if not auth_result.has_restrictions:
        return table.fields
    if latest_fields is None:
        latest_fields = latest_auth_fields(table)
    auth_result.validate_against_schema(latest_fields)
    auth_result.validate_readable_without_rename(latest_fields, table.fields)
    return latest_fields


def _authorization_fields(table):
    fields = table.fields
    if table.options.row_tracking_enabled():
        fields = SpecialFields.row_type_with_row_tracking(fields)
    return {field.name for field in fields}


def authorization_columns(table, read_type, predicate=None, partition_predicate=None):
    return query_auth_columns(read_type, predicate, partition_predicate, _authorization_fields(table))


def authorize(table, query_auth_fn, read_type, predicate=None, partition_predicate=None):
    if query_auth_fn is None:
        return None
    auth_result = resolve_auth_result(query_auth_fn, read_type, predicate, partition_predicate,
                                      _authorization_fields(table))
    if auth_result is not None:
        validate_auth_rules(table, auth_result)
        if partition_predicate is not None and auth_result.column_masking:
            if predicate_field_names(partition_predicate) & set(auth_result.column_masking):
                raise ValueError("Explicit partition filters cannot reference masked columns")
    return auth_result
