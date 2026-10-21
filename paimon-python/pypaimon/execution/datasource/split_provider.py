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

"""Shared lazy table resolution and split-plan caching for datasource inputs."""


def _ensure_table(self):
    if self._table_cached is None:
        from pypaimon.catalog.catalog_factory import CatalogFactory
        catalog = CatalogFactory.create(self._catalog_options)
        table = catalog.get_table(self._table_identifier)
        dynamic_options = {}
        if self._snapshot_id is not None:
            dynamic_options["scan.snapshot-id"] = str(self._snapshot_id)
        if self._tag_name is not None:
            dynamic_options["scan.tag-name"] = self._tag_name
        if self._dynamic_options:
            dynamic_options.update(self._dynamic_options)
        if dynamic_options:
            if self._preserve_current_schema:
                table = table.copy_without_time_travel(dynamic_options)
            else:
                table = table.copy(dynamic_options)
        self._table_cached = table
    return self._table_cached


def _ensure_planned(self):
    if self._splits_cached is not None and self._read_type_cached is not None:
        return
    from pypaimon.read.read_builder import ReadBuilder
    rb = ReadBuilder(self._ensure_table())
    if self._predicate is not None:
        rb = rb.with_filter(self._predicate)
    if self._projection is not None:
        rb = rb.with_projection(self._projection)
    if self._limit is not None:
        rb = rb.with_limit(self._limit)
    self._read_type_cached = rb.read_type()
    self._output_projection_cached = rb._output_projection
    self._splits_cached = rb.new_scan().plan().splits()
