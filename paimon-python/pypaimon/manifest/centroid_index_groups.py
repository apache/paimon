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

"""Validate the centroid index groups written to a Python index manifest."""

from collections import defaultdict
from struct import Struct

from pypaimon.index.data_evolution_index_source_meta import DataEvolutionIndexSourceMeta


_HEADER = Struct('>8sBci')


def centroid_routers(entries):
    groups = defaultdict(list)
    for entry in entries:
        file = entry.index_file
        meta = file.global_index_meta
        if entry.kind == 0 and meta is not None and meta.is_centroid_index():
            key = (tuple(entry.partition.values), entry.bucket, file.index_type,
                   meta.index_field_id, meta.row_range_start, meta.row_range_end)
            groups[key].append(entry)
    return [_validate_group(group) for group in groups.values()]


def _validate_group(entries):
    invalid = RuntimeError("Incomplete or conflicting centroid generation for '%s'" % entries[0].index_file.file_name)
    routers = []
    shards, bitmaps = {}, {}
    for entry in entries:
        file = entry.index_file
        descriptor = file.global_index_meta.index_meta
        if len(descriptor) < _HEADER.size:
            raise invalid
        _, version, role, number = _HEADER.unpack_from(descriptor)
        if version != 1 or number < 0 or file.file_size < 0 or file.row_count < 0:
            raise invalid
        if role == b'R':
            routers.append((entry, number))
        elif role in (b'S', b'B'):
            parts = shards if role == b'S' else bitmaps
            if number in parts or file.file_size == 0:
                raise invalid
            parts[number] = file
        else:
            raise invalid
    if len(routers) != 1:
        raise invalid
    router, nlist = routers[0]
    file = router.index_file
    meta = file.global_index_meta
    metric = meta.index_meta[_HEADER.size:]
    extra = meta.extra_field_ids or []
    range_length = meta.row_range_end - meta.row_range_start + 1
    vector_count = sum(shard.row_count for shard in shards.values())
    if (file.row_count != 0 or (file.file_size == 0) != (nlist == 0)
            or metric not in (b'l2', b'inner_product', b'cosine')
            or meta.index_field_id in extra or len(set(extra)) != len(extra)
            or meta.row_range_start < 0 or range_length <= 0 or len(shards) != nlist
            or vector_count > range_length or (nlist > 0 and vector_count == 0)
            or any(shard_id >= nlist for shard_id in shards)):
        raise invalid
    for entry in entries:
        part = entry.index_file.global_index_meta
        if part.extra_field_ids != meta.extra_field_ids or part.source_meta != meta.source_meta:
            raise invalid
    for shard in shards.values():
        payload = shard.global_index_meta.index_meta[_HEADER.size:]
        if metric == b'l2' and file.index_type in ('ivf-flat', 'ivf-pq') and shard.row_count:
            if len(payload) != 4 or int.from_bytes(payload, 'big', signed=True) <= 0:
                raise invalid
        elif payload:
            raise invalid
    expected = {i: shard.row_count for i, shard in shards.items() if extra and shard.row_count > 0}
    if {i: bitmap.row_count for i, bitmap in bitmaps.items()} != expected:
        raise invalid
    if not DataEvolutionIndexSourceMeta.is_data_evolution_meta(meta.source_meta):
        raise invalid
    source = DataEvolutionIndexSourceMeta.deserialize(meta.source_meta)
    if bitmaps and (source.source_schema_id is None or source.source_schema_id < 0):
        raise invalid
    return router
