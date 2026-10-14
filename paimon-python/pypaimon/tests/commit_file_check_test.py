"""Checks of prepared commit files use real metadata and local storage."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import call, patch

import pytest

from pypaimon.common.identifier import Identifier
from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.index.index_file_meta import IndexFileMeta
from pypaimon.manifest.index_manifest_entry import IndexManifestEntry
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.utils.file_store_path_factory import FileStorePathFactory, canonical_data_file_path
from pypaimon.write.commit_file_check import check_commit_files


def _table(tmp_path, **path_options):
    factory = FileStorePathFactory(
        root=(tmp_path / 'table').as_uri(),
        partition_keys=['part'],
        default_part_value='__DEFAULT_PARTITION__',
        format_identifier='parquet',
        data_file_prefix='data-',
        changelog_file_prefix='changelog-',
        legacy_partition_name=True,
        file_suffix_include_compression=False,
        file_compression='zstd',
        **path_options
    )
    return SimpleNamespace(
        identifier=Identifier('db', 'commit_file_check'),
        file_io=LocalFileIO(),
        path_factory=lambda: factory,
    )


def _partition(value):
    return GenericRow([value], [DataField(0, 'part', AtomicType('STRING'))])


def _entry(file_name='data.parquet', extra_files=(), kind=0, partition='value',
           external_path=None, file_path=None):
    file_meta = DataFileMeta(
        file_name=file_name,
        file_size=8,
        row_count=1,
        min_key=GenericRow([], []),
        max_key=GenericRow([], []),
        key_stats=SimpleStats.empty_stats(),
        value_stats=SimpleStats.empty_stats(),
        min_sequence_number=0,
        max_sequence_number=0,
        schema_id=0,
        level=0,
        extra_files=list(extra_files),
        external_path=external_path,
        file_path=file_path,
    )
    return ManifestEntry(kind, _partition(partition), 2, 3, file_meta)


def _index_entry(index_type='DELETION_VECTORS', file_name='index.bin', kind=0,
                 partition='value', external_path=None):
    return IndexManifestEntry(
        kind, _partition(partition), 2,
        IndexFileMeta(index_type, file_name, 8, 1, external_path=external_path),
    )


def _data_path(table, entry):
    return canonical_data_file_path(table, entry.partition.values, entry.bucket, entry.file.file_name)


def _write_files(table, paths):
    for path in paths:
        with table.file_io.new_output_stream(path) as output:
            output.write(b'prepared')


def _assert_missing(error, table, path):
    message = str(error.value)
    assert str(table.identifier) in message
    assert path in message
    assert 'cannot recover' in message.lower()
    assert 'missing' in message.lower()


def test_empty_commit_does_not_check_files(tmp_path):
    table = _table(tmp_path)
    with patch.object(table.file_io, 'exists', wraps=table.file_io.exists) as exists:
        assert check_commit_files(table, []) is None
    exists.assert_not_called()


def test_checks_existing_data_sidecars_changelog_and_indexes(tmp_path):
    table = _table(tmp_path)
    data = _entry(extra_files=['data.row'])
    changelog = _entry('changelog.parquet', extra_files=['changelog.row'])
    indexes = [_index_entry(index_type, index_type + '.index')
               for index_type in ('DELETION_VECTORS', 'HASH', 'BTREE')]
    bucket_path = table.path_factory().bucket_path(('value',), 2)
    paths = [bucket_path + '/' + name
             for name in ('data.parquet', 'data.row', 'changelog.parquet', 'changelog.row')]
    paths.extend(table.path_factory().index_path() + '/' + entry.index_file.file_name
                 for entry in indexes)
    _write_files(table, paths)
    original = deepcopy((data, changelog, indexes))

    with patch.object(table.file_io, 'exists', wraps=table.file_io.exists) as exists:
        assert check_commit_files(table, iter([data]), iter([changelog]), iter(indexes)) is None

    assert exists.call_count == len(paths)
    exists.assert_has_calls([call(path) for path in paths], any_order=True)
    assert (data, changelog, indexes) == original
    assert all(table.file_io.exists(path) for path in paths)


@pytest.mark.parametrize('missing_name', [
    'data.parquet', 'data.row', 'changelog.parquet', 'changelog.row',
])
def test_missing_data_sidecar_or_changelog_preserves_files(tmp_path, missing_name):
    table = _table(tmp_path)
    data = _entry(extra_files=['data.row'])
    changelog = _entry('changelog.parquet', extra_files=['changelog.row'])
    bucket_path = table.path_factory().bucket_path(('value',), 2)
    missing_path = bucket_path + '/' + missing_name
    paths = [bucket_path + '/' + name
             for name in ('data.parquet', 'data.row', 'changelog.parquet', 'changelog.row')
             if name != missing_name]
    _write_files(table, paths)
    original = deepcopy((data, changelog))

    with patch.object(table.file_io, 'delete') as delete, \
            patch.object(table.file_io, 'delete_quietly') as delete_quietly:
        with pytest.raises(RuntimeError) as error:
            check_commit_files(table, [data], [changelog])

    _assert_missing(error, table, missing_path)
    delete.assert_not_called()
    delete_quietly.assert_not_called()
    assert (data, changelog) == original
    assert all(table.file_io.exists(path) for path in paths)


@pytest.mark.parametrize('index_type', ['DELETION_VECTORS', 'HASH', 'BTREE'])
@pytest.mark.parametrize('location', ['table', 'bucket', 'external', 'global-external'])
@pytest.mark.parametrize('present', [True, False])
def test_checks_added_index_locations(tmp_path, index_type, location, present):
    path_options = {'index_file_in_data_file_dir': location == 'bucket'}
    global_root = tmp_path / 'global%2F#?'
    if location == 'global-external':
        path_options['global_index_external_path'] = 'file:' + str(global_root)
    table = _table(tmp_path, **path_options)
    external = tmp_path / 'external%2F#?' / 'index.bin'
    entry = _index_entry(
        index_type, partition='a/b',
        external_path='file:' + str(external) if location == 'external' else None,
    )
    if location == 'external':
        expected_path = str(external)
    elif location == 'bucket' and index_type in ('DELETION_VECTORS', 'HASH'):
        expected_path = str(tmp_path / 'table' / 'part=a%2Fb' / 'bucket-2' / 'index.bin')
    elif location == 'global-external' and index_type == 'BTREE':
        expected_path = str(global_root / 'index.bin')
    else:
        expected_path = table.path_factory().index_path() + '/index.bin'
    if present:
        _write_files(table, [expected_path])
    original = deepcopy(vars(entry.index_file))

    with patch.object(table.file_io, 'exists', wraps=table.file_io.exists) as exists:
        if present:
            assert check_commit_files(table, [], index_entries=[entry]) is None
        else:
            with pytest.raises(RuntimeError) as error:
                check_commit_files(table, [], index_entries=[entry])
            _assert_missing(error, table, expected_path)

    exists.assert_called_once_with(expected_path)
    assert vars(entry.index_file) == original


def test_ignores_delete_entries_in_all_manifests(tmp_path):
    table = _table(tmp_path)
    data = _entry(kind=1, extra_files=['missing.row'])
    changelog = _entry('missing-changelog.parquet', kind=1)
    indexes = [_index_entry(index_type, kind=1)
               for index_type in ('DELETION_VECTORS', 'HASH', 'BTREE')]
    with patch.object(table.file_io, 'exists', wraps=table.file_io.exists) as exists:
        assert check_commit_files(table, [data], [changelog], indexes) is None
    exists.assert_not_called()


def test_deduplicates_paths_across_data_sidecars_changelog_and_indexes(tmp_path):
    table = _table(tmp_path)
    data = _entry(extra_files=['data.row', 'data.row'])
    data_path = _data_path(table, data)
    sidecar_path = data_path.rsplit('/', 1)[0] + '/data.row'
    changelog = _entry('data.row')
    indexes = [_index_entry(index_type, external_path=sidecar_path)
               for index_type in ('DELETION_VECTORS', 'HASH', 'BTREE')]
    _write_files(table, [data_path, sidecar_path])

    with patch.object(table.file_io, 'exists', wraps=table.file_io.exists) as exists:
        check_commit_files(table, [data, data], [data, changelog], indexes + indexes)

    assert exists.call_count == 2
    exists.assert_has_calls([call(data_path), call(sidecar_path)], any_order=True)


@pytest.mark.parametrize('changelog', [False, True])
@pytest.mark.parametrize('missing_name', [None, 'data.parquet', 'data.row'])
def test_external_paths_override_transient_paths_without_mutation(tmp_path, changelog, missing_name):
    table = _table(tmp_path)
    external_root = tmp_path / 'external%2F#? space'
    data_path = str(external_root / 'data.parquet')
    sidecar_path = str(external_root / 'data.row')
    transient_path = str(tmp_path / 'staged' / 'data.parquet')
    entry = _entry(extra_files=['data.row'], external_path='file:' + data_path, file_path=transient_path)
    original = deepcopy(entry)
    existing_paths = [str(external_root / name) for name in ('data.parquet', 'data.row')
                      if name != missing_name]
    existing_paths.extend([transient_path, str(tmp_path / 'staged' / 'data.row')])
    _write_files(table, existing_paths)
    entries, changelog_entries = ([], [entry]) if changelog else ([entry], [])

    with patch.object(table.file_io, 'exists', wraps=table.file_io.exists) as exists:
        if missing_name is None:
            check_commit_files(table, entries, changelog_entries)
            assert exists.call_args_list == [call(data_path), call(sidecar_path)]
        else:
            with pytest.raises(RuntimeError) as error:
                check_commit_files(table, entries, changelog_entries)
            _assert_missing(error, table, str(external_root / missing_name))

    assert entry == original
    assert all(table.file_io.exists(path) for path in existing_paths)


@pytest.mark.parametrize('data_directory', [None, 'data/nested'])
def test_canonical_paths_preserve_escaped_partitions(tmp_path, data_directory):
    table = _table(tmp_path, data_file_path_directory=data_directory)
    entry = _entry(partition='a/b%#?=中 空', extra_files=['data.row'])
    root = tmp_path / 'table'
    if data_directory:
        root = root / data_directory
    bucket_path = root / 'part=a%2Fb%25%23%3F%3D中 空' / 'bucket-2'
    paths = [str(bucket_path / name) for name in ('data.parquet', 'data.row')]
    _write_files(table, paths)
    original = deepcopy(entry)

    with patch.object(table.file_io, 'exists', wraps=table.file_io.exists) as exists:
        check_commit_files(table, [entry])

    assert exists.call_args_list == [call(path) for path in paths]
    assert entry == original


def test_honors_physical_file_path_and_aligned_sidecars(tmp_path):
    table = _table(tmp_path)
    data_path = str(tmp_path / 'prepared' / 'data.parquet')
    sidecar_path = str(tmp_path / 'sidecars' / 'data.row')
    entry = _entry(file_path=data_path, extra_files=['../sidecars/data.row'])
    _write_files(table, [data_path, sidecar_path])
    original = deepcopy(entry)

    with patch.object(table.file_io, 'exists', wraps=table.file_io.exists) as exists:
        check_commit_files(table, [entry])

    assert exists.call_args_list == [call(data_path), call(sidecar_path)]
    assert entry == original


@pytest.mark.parametrize('error_type', [PermissionError, OSError])
@pytest.mark.parametrize('file_kind', ['data', 'sidecar', 'changelog', 'index'])
def test_propagates_file_io_errors_unchanged(tmp_path, error_type, file_kind):
    table = _table(tmp_path)
    data = _entry(extra_files=['data.row'])
    changelog = _entry('changelog.parquet')
    index = _index_entry()
    data_path = _data_path(table, data)
    paths = {
        'data': data_path,
        'sidecar': data_path.rsplit('/', 1)[0] + '/data.row',
        'changelog': _data_path(table, changelog),
        'index': table.path_factory().index_path() + '/index.bin',
    }
    _write_files(table, paths.values())
    failure = error_type('storage access failed')
    local_exists = table.file_io.exists
    original = deepcopy((data, changelog))

    def exists(path):
        if path == paths[file_kind]:
            raise failure
        return local_exists(path)

    with patch.object(table.file_io, 'exists', side_effect=exists):
        with pytest.raises(error_type) as error:
            check_commit_files(table, [data], [changelog], [index])

    assert error.value is failure
    assert (data, changelog) == original
    assert all(local_exists(path) for path in paths.values())
