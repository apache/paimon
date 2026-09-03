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

"""LeRobot component tables and version publication."""

from array import array
import json
import numbers
import uuid
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from pypaimon import Schema as PaimonSchema
from pypaimon.catalog.catalog_exception import (
    DatabaseNotExistException,
    TableAlreadyExistException,
    TableNotExistException,
)
from pypaimon.common.identifier import Identifier
from pypaimon.multimodal.hdf5 import _SnapshotRecorder
from pypaimon.multimodal.table import _target_schema


_VERSION_ID = "version_id"
_OWNER_ID_OPTION = "pypaimon.lerobot.owner-id"
_PANDAS_METADATA_OPTION = "pypaimon.lerobot.pandas-metadata"
_TABLE_SUFFIXES = {
    "versions": "__versions",
    "episodes": "__episodes",
    "tasks": "__tasks",
    "subtasks": "__subtasks",
}
_COMPANION_OPTION_KEYS = {
    name: "pypaimon.lerobot.%s-table" % name
    for name in _TABLE_SUFFIXES
}

_VERSIONS_SCHEMA = pa.schema([
    pa.field(_VERSION_ID, pa.int64(), nullable=False),
    pa.field("status", pa.string(), nullable=False),
    pa.field("info_json", pa.string(), nullable=False),
    pa.field("stats_json", pa.string()),
    pa.field("has_subtasks", pa.bool_(), nullable=False),
])
_EMPTY_TASKS_SCHEMA = pa.schema([
    pa.field("task_index", pa.int64(), nullable=False),
    pa.field("task", pa.string(), nullable=False),
])
_EMPTY_EPISODES_SCHEMA = pa.schema([
    pa.field("episode_index", pa.int64(), nullable=False),
    pa.field("dataset_from_index", pa.int64(), nullable=False),
    pa.field("dataset_to_index", pa.int64(), nullable=False),
    pa.field("tasks", pa.list_(pa.string()), nullable=False),
    pa.field("length", pa.int64(), nullable=False),
])
_EPISODE_CONTROL_COLUMNS = [
    "episode_index",
    "dataset_from_index",
    "dataset_to_index",
    "tasks",
    "length",
]


class _EpisodeIndex:

    def __init__(self):
        self._ranges = array("q")
        self._task_offsets = array("q", [0])
        self._task_indices = array("q")

    def append(self, begin, end, task_indices):
        self._ranges.extend((begin, end))
        self._task_indices.extend(task_indices)
        self._task_offsets.append(len(self._task_indices))

    def __len__(self):
        return len(self._ranges) // 2

    def __getitem__(self, index):
        if index < 0:
            index += len(self)
        if index < 0 or index >= len(self):
            raise IndexError(index)
        task_begin = self._task_offsets[index]
        task_end = self._task_offsets[index + 1]
        begin = self._ranges[index * 2]
        end = self._ranges[index * 2 + 1]
        return {
            "episode_index": index,
            "dataset_from_index": begin,
            "dataset_to_index": end,
            "length": end - begin,
            "task_indices": self._task_indices[task_begin:task_end],
        }


def _load_dataset_metadata(dataset, info, source):
    fps = _positive_integer(info.get("fps"), "fps")
    stats = _source_stats(dataset, source)
    tasks_table = _source_tasks(
        dataset, source, int(info["total_tasks"]))
    task_indices = _task_indices(
        tasks_table.to_pylist(), int(info["total_tasks"]))
    subtasks_table = _source_subtasks(dataset, source)
    subtask_indices = _subtask_indices(subtasks_table, info)
    total_episodes = int(info["total_episodes"])
    episode_source = (
        _source_episodes(dataset, source)
        if total_episodes > 0
        else {"paths": [], "schema": _EMPTY_EPISODES_SCHEMA}
    )
    return {
        "fps": fps,
        "info_json": _canonical_json(info),
        "stats_json": (
            None if stats is None else _canonical_json(
                stats, allow_nan=True)),
        "episodes": None,
        "episodes_schema": episode_source["schema"],
        "episode_paths": episode_source["paths"],
        "tasks_table": tasks_table,
        "subtasks_table": subtasks_table,
        "source": source,
        "task_indices": task_indices,
        "total_frames": int(info["total_frames"]),
        "total_episodes": total_episodes,
        "subtask_indices": subtask_indices,
    }


def _new_owner_id():
    return uuid.uuid4().hex


def _companion_identifier(frames_identifier, suffix):
    identifier = (
        frames_identifier
        if isinstance(frames_identifier, Identifier)
        else Identifier.from_string(str(frames_identifier))
    )
    if identifier.is_system_table():
        raise ValueError(
            "LeRobot target cannot be a Paimon system table: %s"
            % frames_identifier)
    companion = Identifier(
        identifier.get_database_name(),
        identifier.get_table_name() + suffix,
        branch=identifier.get_branch_name(),
    )
    return "%s.%s" % (
        _quote_identifier_part(companion.get_database_name()),
        _quote_identifier_part(companion.get_object_name()),
    )


def _quote_identifier_part(value):
    return "`%s`" % value if "." in value else value


def _managed_table_options(frames_identifier, owner_id):
    identifier = Identifier.from_string(str(frames_identifier))
    if identifier.get_branch_name() is not None:
        raise ValueError(
            "LeRobot import does not support table branches.")
    result = {_OWNER_ID_OPTION: owner_id}
    for name, suffix in _TABLE_SUFFIXES.items():
        result[_COMPANION_OPTION_KEYS[name]] = _companion_identifier(
            frames_identifier, suffix)
    return result


def _is_managed_root(options):
    return _OWNER_ID_OPTION in options and all(
        key in options for key in _COMPANION_OPTION_KEYS.values())


def _companion_table_identifiers(frames_table):
    options = frames_table.table_schema.options
    identifiers = {}
    for name, key in _COMPANION_OPTION_KEYS.items():
        value = options.get(key)
        if not value:
            raise ValueError(
                "LeRobot table %s is missing managed option %s."
                % (frames_table.identifier, key))
        identifiers[name] = value
    return identifiers


def _prepare_metadata_tables(connection, frames_table, owner_id, metadata):
    schemas = {
        "versions": _VERSIONS_SCHEMA,
        "episodes": metadata["episodes_schema"],
        "tasks": metadata["tasks_table"].schema,
    }
    if metadata["subtasks_table"] is not None:
        schemas["subtasks"] = metadata["subtasks_table"].schema
    identifiers = _companion_table_identifiers(frames_table)
    if metadata["subtasks_table"] is None:
        try:
            connection.catalog.get_table(identifiers["subtasks"])
        except (DatabaseNotExistException, TableNotExistException):
            pass
        else:
            raise ValueError(
                "LeRobot metadata table %s already exists."
                % identifiers["subtasks"])
    tables = {}
    for name, schema in schemas.items():
        identifier = identifiers[name]
        try:
            table = connection.catalog.get_table(identifier)
        except (DatabaseNotExistException, TableNotExistException):
            options = {
                "bucket": "-1",
                _OWNER_ID_OPTION: owner_id,
            }
            pandas_metadata = (schema.metadata or {}).get(b"pandas")
            if pandas_metadata is not None:
                options[_PANDAS_METADATA_OPTION] = pandas_metadata.decode(
                    "utf-8")
            paimon_schema = PaimonSchema.from_pyarrow_schema(
                schema,
                options=options,
            )
            try:
                connection.catalog.create_table(
                    identifier, paimon_schema, False)
            except TableAlreadyExistException:
                pass
            table = connection.catalog.get_table(identifier)
        if table.table_schema.primary_keys:
            raise ValueError(
                "LeRobot metadata table %s must be append-only." % identifier)
        actual = _target_schema(table)
        if not actual.equals(schema, check_metadata=False):
            raise ValueError(
                "LeRobot metadata table %s has schema %s; expected %s."
                % (identifier, actual, schema))
        actual_owner_id = table.table_schema.options.get(_OWNER_ID_OPTION)
        if actual_owner_id != owner_id:
            raise ValueError(
                "LeRobot metadata table %s belongs to a different target "
                "table. Drop the stale companion tables before importing."
                % identifier)
        tables[name] = table
    return tables


def _restore_pandas_metadata(table, data):
    pandas_metadata = table.table_schema.options.get(
        _PANDAS_METADATA_OPTION)
    if pandas_metadata is None:
        return data
    metadata = dict(data.schema.metadata or {})
    metadata[b"pandas"] = pandas_metadata.encode("utf-8")
    return data.replace_schema_metadata(metadata)


def _reserve_dataset_version(
        versions_table,
        version_id,
        metadata):
    pending = _manifest_row(version_id, "PENDING", metadata)
    snapshot_id = _append_arrow(
        versions_table,
        pa.Table.from_pylist([pending], schema=_VERSIONS_SCHEMA),
    )
    _require_initial_snapshot("versions", snapshot_id)


def _publish_dataset(
        connection,
        tables,
        version_id,
        metadata,
        frames_identifier,
        frames_snapshot_id,
        episodes_snapshot_id):
    _require_initial_snapshot("frames", frames_snapshot_id)
    _require_initial_snapshot("episodes", episodes_snapshot_id)
    tasks_snapshot_id = _append_arrow(
        tables["tasks"], metadata["tasks_table"])
    _require_initial_snapshot("tasks", tasks_snapshot_id)
    component_snapshots = [
        (frames_identifier, frames_snapshot_id),
        (tables["episodes"].identifier, episodes_snapshot_id),
        (tables["tasks"].identifier, tasks_snapshot_id),
    ]
    if metadata["subtasks_table"] is not None:
        subtasks_snapshot_id = _append_arrow(
            tables["subtasks"], metadata["subtasks_table"])
        _require_initial_snapshot("subtasks", subtasks_snapshot_id)
        component_snapshots.append(
            (tables["subtasks"].identifier, subtasks_snapshot_id))
    tag = str(version_id)
    for identifier, snapshot_id in component_snapshots:
        _create_tag(connection.catalog, identifier, tag, snapshot_id)

    manifest = _manifest_row(version_id, "READY", metadata)
    _append_arrow(tables["versions"], pa.Table.from_pylist(
        [manifest], schema=_VERSIONS_SCHEMA))


def _require_initial_snapshot(component, snapshot_id):
    if snapshot_id is None:
        raise ValueError(
            "LeRobot tag-backed import requires a non-empty %s component."
            % component)
    if snapshot_id != 1:
        raise RuntimeError(
            "LeRobot initial import detected concurrent writes to %s; "
            "expected snapshot 1, found %d." % (component, snapshot_id))


def _manifest_row(
        version_id,
        status,
        metadata):
    return {
        _VERSION_ID: version_id,
        "status": status,
        "info_json": metadata["info_json"],
        "stats_json": metadata["stats_json"],
        "has_subtasks": metadata["subtasks_table"] is not None,
    }


def _drop_import_tables(
        catalog, frames_table, owner_id, owned_only=False):
    identifiers = list(
        _companion_table_identifiers(frames_table).values())
    identifiers.append(frames_table.identifier)
    quarantined = []
    try:
        for identifier in identifiers:
            source = (
                identifier
                if isinstance(identifier, Identifier)
                else Identifier.from_string(str(identifier))
            )
            if owned_only:
                try:
                    table = catalog.get_table(source)
                except (DatabaseNotExistException, TableNotExistException):
                    continue
                if table.table_schema.options.get(
                        _OWNER_ID_OPTION) != owner_id:
                    continue
            quarantine = Identifier(
                source.get_database_name(),
                "__pypaimon_drop_%s" % uuid.uuid4().hex,
            )
            try:
                catalog.rename_table(source, quarantine)
            except (DatabaseNotExistException, TableNotExistException):
                continue
            quarantined.append((source, quarantine))

        owned = []
        foreign = []
        for source, quarantine in quarantined:
            table = catalog.get_table(quarantine)
            actual_owner = table.table_schema.options.get(_OWNER_ID_OPTION)
            target = owned if actual_owner == owner_id else foreign
            target.append((source, quarantine))
    except BaseException as error:
        failures = _restore_quarantined(catalog, quarantined)
        if failures:
            raise RuntimeError(
                "Failed to restore quarantined LeRobot tables: %s"
                % ", ".join(failures)) from error
        raise

    if foreign and not owned_only:
        error = ValueError(
            "Refusing to drop %s because it belongs to a different table."
            % foreign[0][0])
        failures = _restore_quarantined(catalog, quarantined)
        if failures:
            raise RuntimeError(
                "Failed to restore quarantined LeRobot tables: %s"
                % ", ".join(failures)) from error
        raise error

    restore_failures = _restore_quarantined(catalog, foreign)
    drop_failures = _drop_quarantined(catalog, owned, owner_id)
    if drop_failures:
        drop_failures = _drop_quarantined(
            catalog, drop_failures, owner_id)
    if restore_failures or drop_failures:
        raise RuntimeError(
            "LeRobot cleanup left quarantined tables: %s"
            % ", ".join(restore_failures + [
                str(quarantine) for _, quarantine in drop_failures
            ]))


def _restore_quarantined(catalog, tables):
    failures = []
    for source, quarantine in reversed(tables):
        for attempt in range(2):
            try:
                if not _table_exists(catalog, quarantine):
                    break
                if _table_exists(catalog, source):
                    failures.append(str(quarantine))
                    break
                catalog.rename_table(quarantine, source)
                break
            except BaseException:
                if attempt == 1:
                    failures.append(str(quarantine))
    return failures


def _drop_quarantined(catalog, tables, owner_id):
    failures = []
    for source, quarantine in tables:
        try:
            table = catalog.get_table(quarantine)
            if table.table_schema.options.get(_OWNER_ID_OPTION) != owner_id:
                failures.append((source, quarantine))
                continue
            catalog.drop_table(quarantine)
        except (DatabaseNotExistException, TableNotExistException):
            pass
        except BaseException:
            failures.append((source, quarantine))
    return failures


def _table_exists(catalog, identifier):
    try:
        catalog.get_table(identifier)
        return True
    except (DatabaseNotExistException, TableNotExistException):
        return False


def _append_arrow(table, data):
    return _append_arrow_tables(table, [data])


def _append_arrow_tables(table, tables):
    builder = table.new_batch_write_builder()
    table_write = None
    table_commit = None
    commit_started = False
    recorder = _SnapshotRecorder()
    try:
        table_write = builder.new_write()
        table_commit = builder.new_commit()
        table_commit.add_commit_callback(recorder)
        row_count = 0
        target_schema = _target_schema(table)
        for data in tables:
            if data.num_rows == 0:
                continue
            if not data.schema.equals(target_schema, check_metadata=False):
                raise ValueError(
                    "LeRobot component schema %s does not match target %s."
                    % (data.schema, target_schema))
            table_write.write_arrow(data)
            row_count += data.num_rows
            del data
        if row_count == 0:
            table_write.abort()
            return None
        messages = table_write.prepare_commit()
        commit_started = True
        table_commit.commit(messages)
        if recorder.snapshot_id is None:
            raise RuntimeError("LeRobot metadata commit has no snapshot id.")
        return recorder.snapshot_id
    except BaseException:
        if table_write is not None and not commit_started:
            table_write.abort()
        raise
    finally:
        try:
            if table_write is not None:
                table_write.close()
        finally:
            if table_commit is not None:
                table_commit.close()


def _create_tag(catalog, identifier, tag_name, snapshot_id):
    try:
        catalog.create_tag(
            identifier, tag_name, snapshot_id=snapshot_id)
    except NotImplementedError:
        catalog.get_table(identifier).create_tag(
            tag_name, snapshot_id=snapshot_id)


def _source_stats(dataset, source):
    if source.file_io is not None:
        from pypaimon.multimodal.lerobot.source import (
            _read_remote_json,
            _remote_path,
        )
        path = _remote_path(source.path, "meta/stats.json")
        try:
            source.file_io.get_file_status(path)
        except FileNotFoundError:
            return None
        return _read_remote_json(source.file_io, path)
    root = _metadata_root(dataset, source)
    path = root / "meta" / "stats.json"
    if not path.is_file():
        return None
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def _source_tasks(dataset, source, total_tasks):
    if total_tasks == 0:
        return pa.Table.from_pylist([], schema=_EMPTY_TASKS_SCHEMA)
    if source.file_io is not None:
        from pypaimon.multimodal.lerobot.source import (
            _read_remote_parquet,
            _remote_path,
        )
        path = _remote_path(source.path, "meta/tasks.parquet")
        return _read_remote_parquet(source.file_io, path)
    path = _metadata_root(dataset, source) / "meta" / "tasks.parquet"
    try:
        return pq.read_table(path)
    except (OSError, ValueError, pa.ArrowException) as error:
        raise ValueError(
            "Cannot read LeRobot task metadata %s: %s" % (path, error)
        ) from error


def _source_subtasks(dataset, source):
    if source.file_io is not None:
        from pypaimon.multimodal.lerobot.source import (
            _read_remote_parquet,
            _remote_path,
        )
        path = _remote_path(source.path, "meta/subtasks.parquet")
        try:
            source.file_io.get_file_status(path)
        except FileNotFoundError:
            return None
        return _read_remote_parquet(source.file_io, path)
    path = _metadata_root(dataset, source) / "meta" / "subtasks.parquet"
    if not path.is_file():
        return None
    try:
        return pq.read_table(path)
    except (OSError, ValueError, pa.ArrowException) as error:
        raise ValueError(
            "Cannot read LeRobot subtask metadata %s: %s" % (path, error)
        ) from error


def _source_episodes(dataset, source):
    if source.file_io is not None:
        from pypaimon.multimodal.lerobot.source import (
            _read_remote_parquet_schema,
            _remote_parquet_files,
            _remote_path,
        )
        directory = _remote_path(source.path, "meta/episodes")
        paths = _remote_parquet_files(source.file_io, directory)

        def read_schema(path):
            return _read_remote_parquet_schema(source.file_io, path)

    else:
        directory = _metadata_root(dataset, source) / "meta" / "episodes"
        paths = sorted(directory.rglob("*.parquet"))
        read_schema = pq.read_schema

    if not paths:
        return {
            "paths": [],
            "schema": _EMPTY_EPISODES_SCHEMA,
        }
    try:
        schemas = [read_schema(path) for path in paths]
        schema = schemas[0]
        if any(not item.equals(schema, check_metadata=False)
               for item in schemas[1:]):
            raise ValueError("Episode Parquet schemas are inconsistent.")
    except (OSError, ValueError, pa.ArrowException) as error:
        raise ValueError(
            "Cannot read LeRobot Episode metadata %s: %s"
            % (directory, error)) from error
    return {"paths": paths, "schema": schema}


def _validated_episode_tables(metadata):
    episodes = _EpisodeIndex()
    expected_begin = 0
    for table in _source_episode_tables(metadata):
        controls = table.select(_EPISODE_CONTROL_COLUMNS)
        columns = {
            name: controls.column(name)
            for name in _EPISODE_CONTROL_COLUMNS
        }
        for offset in range(controls.num_rows):
            index = _integer(
                columns["episode_index"][offset].as_py(),
                "episode_index",
            )
            begin = _integer(
                columns["dataset_from_index"][offset].as_py(),
                "dataset_from_index",
            )
            end = _integer(
                columns["dataset_to_index"][offset].as_py(),
                "dataset_to_index",
            )
            length = _integer(
                columns["length"][offset].as_py(), "length")
            if index != len(episodes) or begin != expected_begin \
                    or end <= begin or length != end - begin:
                raise ValueError(
                    "LeRobot Episode %d has inconsistent index, range, "
                    "or length." % len(episodes))
            names = columns["tasks"][offset].as_py() or []
            if isinstance(names, str):
                names = [names]
            if metadata["task_indices"] and not names:
                raise ValueError(
                    "LeRobot Episode %d does not declare any task." % index)
            try:
                task_indices = [
                    metadata["task_indices"][str(name)] for name in names
                ]
            except (KeyError, TypeError) as error:
                raise ValueError(
                    "LeRobot Episode %d refers to an unknown task." % index
                ) from error
            if len(set(task_indices)) != len(task_indices):
                raise ValueError(
                    "LeRobot Episode %d repeats a task." % index)
            episodes.append(begin, end, task_indices)
            expected_begin = end
        yield table
        del table
    if len(episodes) != metadata["total_episodes"]:
        raise ValueError(
            "LeRobot metadata reports %d Episodes but %d were found."
            % (metadata["total_episodes"], len(episodes)))
    if expected_begin != metadata["total_frames"]:
        raise ValueError(
            "LeRobot Episode ranges cover %d frames but metadata reports %d."
            % (expected_begin, metadata["total_frames"]))
    metadata["episodes"] = episodes


def _source_episode_tables(metadata):
    source = metadata["source"]
    if source.file_io is not None:
        from pypaimon.multimodal.lerobot.source import _read_remote_parquet
        for path in metadata["episode_paths"]:
            yield _read_remote_parquet(source.file_io, path)
    else:
        for path in metadata["episode_paths"]:
            try:
                yield pq.read_table(path)
            except (OSError, ValueError, pa.ArrowException) as error:
                raise ValueError(
                    "Cannot read LeRobot Episode metadata %s: %s"
                    % (path, error)) from error


def _metadata_root(dataset, source):
    if source.root is not None:
        return Path(source.root)
    root = getattr(dataset, "root", None)
    if root is None:
        raise ValueError(
            "Cannot resolve cached LeRobot metadata for %s." % source.path)
    return Path(root)


def _task_indices(records, total_tasks):
    seen = [False] * total_tasks
    by_name = {}
    for record in records:
        index = _integer(record.get("task_index"), "task_index")
        task = record.get("task", record.get("name"))
        if task is None:
            task = record.get("__index_level_0__")
        if index < 0 or index >= total_tasks or task is None \
                or seen[index]:
            raise ValueError("LeRobot task metadata is invalid: %s" % record)
        task = str(task)
        if task in by_name:
            raise ValueError("LeRobot task metadata repeats task %r." % task)
        by_name[task] = index
        seen[index] = True
    if not all(seen):
        raise ValueError(
            "LeRobot task metadata does not cover [0, %d)." % total_tasks)
    return by_name


def _subtask_indices(subtasks_table, info):
    has_feature = "subtask_index" in info["features"]
    if subtasks_table is None:
        if has_feature:
            raise ValueError(
                "LeRobot frames declare subtask_index but "
                "meta/subtasks.parquet is missing.")
        return None
    if not has_feature:
        raise ValueError(
            "LeRobot meta/subtasks.parquet requires a subtask_index feature.")
    if "subtask_index" not in subtasks_table.column_names:
        raise ValueError(
            "LeRobot subtask metadata is missing subtask_index.")
    records = subtasks_table.to_pylist()
    for expected, record in enumerate(records):
        label = record.get("subtask", record.get("name"))
        if label is None:
            label = record.get("__index_level_0__")
        if _integer(record.get("subtask_index"), "subtask_index") \
                != expected or not isinstance(label, str) or not label:
            raise ValueError(
                "LeRobot subtask metadata must provide ordered numeric and "
                "text mappings for [0, %d)."
                % subtasks_table.num_rows)
    return range(subtasks_table.num_rows)


def _canonical_json(value, allow_nan=False):
    return json.dumps(
        _json_value(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=allow_nan,
    )


def _json_value(value):
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    as_py = getattr(value, "as_py", None)
    if callable(as_py):
        return _json_value(as_py())
    tolist = getattr(value, "tolist", None)
    if callable(tolist):
        return _json_value(tolist())
    item = getattr(value, "item", None)
    if callable(item):
        return _json_value(item())
    raise TypeError("LeRobot metadata contains a non-JSON value: %r" % value)


def _positive_integer(value, name):
    result = _integer(value, name)
    if result <= 0:
        raise ValueError("LeRobot metadata %s must be positive." % name)
    return result


def _integer(value, name):
    item = getattr(value, "item", None)
    if callable(item):
        value = item()
    if isinstance(value, bool) or not isinstance(value, numbers.Integral):
        raise ValueError("LeRobot metadata %s must be an integer." % name)
    return int(value)
