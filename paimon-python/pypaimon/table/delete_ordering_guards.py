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

"""Reject guarded histories before selecting an unsupported execution backend."""

_UNSET = object()
_ENABLED = 'delete-ordering-guards.enabled'
_PREFIX = 'delete-ordering-guards.'
_ERROR = (
    'PyPaimon does not support delete ordering guards. '
    'Use a guard-aware Java client to read or write this table; '
    'disabling capture does not remove retained guard history.'
)


def validate_delete_ordering_guards(table, snapshot=_UNSET):
    if not table.is_primary_key_table:
        return
    options = table.options.options.to_map()
    if str(options.get(_ENABLED, 'false')).lower() == 'true':
        raise NotImplementedError(_ERROR)
    if snapshot is _UNSET:
        snapshot = table.snapshot_manager().get_latest_snapshot()
    properties = snapshot.properties if snapshot is not None else None
    if properties and any(key.startswith(_PREFIX) for key in properties):
        raise NotImplementedError(_ERROR)
