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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from dataclasses import replace
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon.catalog.filesystem_catalog import FileSystemCatalog
from pypaimon.common.options import Options
from pypaimon.filesystem.caching_file_io import CachingFileIO, LocalMemoryCacheManager
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.manifest.manifest_sidecar import Block, Selection, read_selected_bytes
from pypaimon.schema.schema import Schema


@pytest.mark.parametrize('disk', [False, True])
def test_scan_uses_manifest_sizes_without_file_status(tmp_path, disk):
    options = {'warehouse': str(tmp_path), 'local-cache.enabled': 'true',
               'local-cache.block-size': '64 b'}
    if disk:
        options['local-cache.dir'] = str(tmp_path / 'cache')

    catalog = FileSystemCatalog(Options(options))
    catalog.create_database('default', False)
    schema = Schema.from_pyarrow_schema(pa.schema([('v', pa.int32())]))
    catalog.create_table('default.t', schema, False)
    table = catalog.get_table('default.t')
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.table({'v': pa.array([1, 2, 3], type=pa.int32())}))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()

    snapshot = table.snapshot_manager().get_latest_snapshot()
    for name, size in ((snapshot.base_manifest_list, snapshot.base_manifest_list_size),
                       (snapshot.delta_manifest_list, snapshot.delta_manifest_list_size)):
        assert size > 0
        assert size == table.file_io.get_file_size(
            '{}/manifest/{}'.format(table.table_path, name))

    # A fresh catalog has no in-process file-size cache, even if disk blocks persist.
    fresh_catalog = FileSystemCatalog(Options(options))
    fresh_table = fresh_catalog.get_table('default.t')
    delegate = fresh_table.file_io._delegate
    with patch.object(delegate, 'get_file_size', wraps=delegate.get_file_size) as get_size:
        splits = fresh_table.new_read_builder().new_scan().plan().splits()
    assert splits
    assert fresh_table.new_read_builder().new_read().to_arrow(splits).column('v').to_pylist() == [1, 2, 3]
    assert not [call for call in get_size.call_args_list
                if '/manifest/' in call[0][0]]

    # Old snapshots without list sizes retain the existing status lookup.
    legacy = replace(snapshot, base_manifest_list_size=None,
                     delta_manifest_list_size=None)
    legacy_catalog = FileSystemCatalog(Options(options))
    legacy_table = legacy_catalog.get_table('default.t')
    delegate = legacy_table.file_io._delegate
    with patch.object(delegate, 'get_file_size', wraps=delegate.get_file_size) as get_size:
        assert ManifestListManager(legacy_table).read_all(legacy)
    assert len([call for call in get_size.call_args_list
                if '/manifest/' in call[0][0]]) == 2

    wrong_size_catalog = FileSystemCatalog(Options(options))
    wrong_size_table = wrong_size_catalog.get_table('default.t')
    with pytest.raises(EOFError, match='Truncated manifest list'):
        ManifestListManager(wrong_size_table).read(
            snapshot.base_manifest_list, snapshot.base_manifest_list_size + 1)


def test_selected_manifest_blocks_use_known_size():
    data = b'headfoo'
    delegate = SimpleNamespace(
        new_input_stream=Mock(side_effect=lambda path: BytesIO(data)),
        get_file_size=Mock(side_effect=AssertionError('source stat')))
    file_io = CachingFileIO(delegate, LocalMemoryCacheManager(1024, block_size=4))
    selection = Selection(b'head', (Block(4, 3, 0, 1),))

    assert read_selected_bytes(file_io, '/manifest/manifest-1', selection, len(data)) == data
    delegate.get_file_size.assert_not_called()
