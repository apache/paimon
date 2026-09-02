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
_TABLE_SUFFIXES = {
    "versions": "__versions",
    "episodes": "__episodes",
    "tasks": "__tasks",
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


def _load_dataset_metadata(dataset, info, source):
    fps = _positive_integer(info.get("fps"), "fps")
    stats = _source_stats(dataset, source)
    tasks_table = _source_tasks(
        dataset, source, int(info["total_tasks"]))
    tasks, task_indices = _task_rows(
        tasks_table.to_pylist(), int(info["total_tasks"]))
    total_episodes = int(info["total_episodes"])
    episode_source = (
        _source_episodes(dataset, source)
        if total_episodes > 0
        else {"paths": [], "rows": [], "schema": _EMPTY_EPISODES_SCHEMA}
    )
    episodes = _episode_rows(
        episode_source["rows"],
        task_indices,
        info,
        int(info["total_frames"]),
        total_episodes,
    )
    return {
        "fps": fps,
        "info_json": _canonical_json(info),
        "stats_json": (
            None if stats is None else _canonical_json(stats)),
        "episodes": episodes,
        "tasks": tasks,
        "episodes_schema": episode_source["schema"],
        "episode_paths": episode_source["paths"],
        "tasks_table": tasks_table,
        "source": source,
    }


def _new_owner_id():
    return uuid.uuid4().hex


def _companion_identifier(frames_identifier, suffix):
    identifier = Identifier.from_string(str(frames_identifier))
    if identifier.is_system_table():
        raise ValueError(
            "LeRobot target cannot be a Paimon system table: %s"
            % frames_identifier)
    return Identifier(
        identifier.get_database_name(),
        identifier.get_table_name() + suffix,
        branch=identifier.get_branch_name(),
    ).get_full_name()


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
    identifiers = _companion_table_identifiers(frames_table)
    tables = {}
    for name, schema in schemas.items():
        identifier = identifiers[name]
        try:
            table = connection.catalog.get_table(identifier)
        except (DatabaseNotExistException, TableNotExistException):
            paimon_schema = PaimonSchema.from_pyarrow_schema(
                schema,
                options={
                    "bucket": "-1",
                    _OWNER_ID_OPTION: owner_id,
                },
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
        frames_snapshot_id):
    _require_initial_snapshot("frames", frames_snapshot_id)
    episodes_snapshot_id = _append_arrow_tables(
        tables["episodes"],
        _source_episode_tables(metadata),
    )
    _require_initial_snapshot("episodes", episodes_snapshot_id)
    tasks_snapshot_id = _append_arrow(
        tables["tasks"], metadata["tasks_table"])
    _require_initial_snapshot("tasks", tasks_snapshot_id)
    tag = str(version_id)
    for identifier, snapshot_id in (
            (frames_identifier, frames_snapshot_id),
            (tables["episodes"].identifier, episodes_snapshot_id),
            (tables["tasks"].identifier, tasks_snapshot_id)):
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
    }


def _drop_import_tables(catalog, frames_table, owner_id):
    identifiers = list(
        _companion_table_identifiers(frames_table).values())
    identifiers.append(frames_table.identifier.get_full_name())
    for identifier in identifiers:
        try:
            table = catalog.get_table(identifier)
        except (DatabaseNotExistException, TableNotExistException):
            continue
        if table.table_schema.options.get(_OWNER_ID_OPTION) == owner_id:
            catalog.drop_table(identifier, ignore_if_not_exists=True)


def _append_arrow(table, data):
    return _append_arrow_tables(table, [data])


def _append_arrow_tables(table, tables):
    builder = table.new_batch_write_builder()
    table_write = builder.new_write()
    table_commit = builder.new_commit()
    commit_started = False
    recorder = _SnapshotRecorder()
    table_commit.add_commit_callback(recorder)
    try:
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
        if not commit_started:
            table_write.abort()
        raise
    finally:
        try:
            table_write.close()
        finally:
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


def _source_episodes(dataset, source):
    if source.file_io is not None:
        from pypaimon.multimodal.lerobot.source import (
            _read_remote_parquet,
            _read_remote_parquet_schema,
            _remote_parquet_files,
            _remote_path,
        )
        directory = _remote_path(source.path, "meta/episodes")
        paths = _remote_parquet_files(source.file_io, directory)

        def read(path, columns=None):
            return _read_remote_parquet(
                source.file_io, path, columns=columns)

        def read_schema(path):
            return _read_remote_parquet_schema(source.file_io, path)
    else:
        directory = _metadata_root(dataset, source) / "meta" / "episodes"
        paths = sorted(directory.rglob("*.parquet"))

        def read(path, columns=None):
            return pq.read_table(path, columns=columns)

        read_schema = pq.read_schema
    if not paths:
        return {
            "paths": [],
            "rows": [],
            "schema": _EMPTY_EPISODES_SCHEMA,
        }
    try:
        schemas = [read_schema(path) for path in paths]
        schema = schemas[0]
        if any(not item.equals(schema, check_metadata=False)
               for item in schemas[1:]):
            raise ValueError("Episode Parquet schemas are inconsistent.")
        tables = [read(path, columns=_EPISODE_CONTROL_COLUMNS)
                  for path in paths]
    except (OSError, ValueError, pa.ArrowException) as error:
        raise ValueError(
            "Cannot read LeRobot Episode metadata %s: %s"
            % (directory, error)) from error
    rows = []
    for table in tables:
        rows.extend(table.to_pylist())
    rows.sort(key=lambda row: _integer(
        row.get("episode_index"), "episode_index"))
    return {"paths": paths, "rows": rows, "schema": schema}


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


def _reject_subtasks(dataset, source):
    if source.file_io is not None:
        from pypaimon.multimodal.lerobot.source import _remote_path
        path = _remote_path(source.path, "meta/subtasks.parquet")
        try:
            source.file_io.get_file_status(path)
        except FileNotFoundError:
            return
    else:
        path = _metadata_root(dataset, source) / "meta" / "subtasks.parquet"
        if not path.is_file():
            return
    raise ValueError(
        "LeRobot subtask metadata is not supported yet: %s" % path)


def _metadata_root(dataset, source):
    if source.root is not None:
        return Path(source.root)
    root = getattr(dataset, "root", None)
    if root is None:
        raise ValueError(
            "Cannot resolve cached LeRobot metadata for %s." % source.path)
    return Path(root)


def _task_rows(records, total_tasks):
    rows = [None] * total_tasks
    by_name = {}
    for record in records:
        index = _integer(record.get("task_index"), "task_index")
        task = record.get("task", record.get("name"))
        if task is None:
            task = record.get("__index_level_0__")
        if index < 0 or index >= total_tasks or task is None \
                or rows[index] is not None:
            raise ValueError("LeRobot task metadata is invalid: %s" % record)
        task = str(task)
        if task in by_name:
            raise ValueError("LeRobot task metadata repeats task %r." % task)
        by_name[task] = index
        extra = dict(record)
        for key in ("task_index", "task", "name", "__index_level_0__"):
            extra.pop(key, None)
        rows[index] = {
            "task_index": index,
            "task": task,
            "task_metadata_json": (
                _canonical_json(extra) if extra else None),
        }
    if any(row is None for row in rows):
        raise ValueError(
            "LeRobot task metadata does not cover [0, %d)." % total_tasks)
    return rows, by_name


def _episode_rows(records, task_indices, info, total_frames, total_episodes):
    if len(records) != total_episodes:
        raise ValueError(
            "LeRobot metadata reports %d Episodes but %d were found."
            % (total_episodes, len(records)))
    splits = _episode_splits(info.get("splits"), total_episodes)
    rows = []
    expected_begin = 0
    for ordinal, record in enumerate(records):
        index = _integer(record.get("episode_index"), "episode_index")
        begin = _integer(
            record.get("dataset_from_index"), "dataset_from_index")
        end = _integer(record.get("dataset_to_index"), "dataset_to_index")
        length = _integer(record.get("length"), "length")
        if index != ordinal or begin != expected_begin or end <= begin \
                or length != end - begin:
            raise ValueError(
                "LeRobot Episode %d has inconsistent index, range, or length."
                % ordinal)
        names = record.get("tasks", [])
        if isinstance(names, str):
            names = [names]
        if task_indices and not names:
            raise ValueError(
                "LeRobot Episode %d does not declare any task." % ordinal)
        try:
            episode_task_indices = [task_indices[str(name)] for name in names]
        except (KeyError, TypeError) as error:
            raise ValueError(
                "LeRobot Episode %d refers to an unknown task." % ordinal
            ) from error
        if len(set(episode_task_indices)) != len(episode_task_indices):
            raise ValueError(
                "LeRobot Episode %d repeats a task." % ordinal)

        stats = {
            key[len("stats/"):]: value
            for key, value in record.items() if key.startswith("stats/")
        }
        extra = {
            key: value for key, value in record.items()
            if key not in {
                "episode_index", "dataset_from_index", "dataset_to_index",
                "length", "tasks"
            } and not key.startswith("stats/")
        }
        rows.append({
            "episode_index": index,
            "dataset_from_index": begin,
            "dataset_to_index": end,
            "length": length,
            "task_indices": episode_task_indices,
            "split": splits[index],
            "episode_stats_json": (
                _canonical_json(stats) if stats else None),
            "episode_metadata_json": _canonical_json(extra),
        })
        expected_begin = end
    if expected_begin != total_frames:
        raise ValueError(
            "LeRobot Episode ranges cover %d frames but metadata reports %d."
            % (expected_begin, total_frames))
    return rows


def _episode_splits(value, total_episodes):
    result = [None] * total_episodes
    if not isinstance(value, dict):
        return result
    for name, bounds in value.items():
        if not isinstance(bounds, str) or bounds.count(":") != 1:
            continue
        begin_text, end_text = bounds.split(":")
        try:
            begin, end = int(begin_text), int(end_text)
        except ValueError:
            continue
        if begin < 0 or end < begin or end > total_episodes:
            continue
        for index in range(begin, end):
            if result[index] is None:
                result[index] = str(name)
            elif result[index] != str(name):
                result[index] = None
    return result


def _canonical_json(value):
    return json.dumps(
        _json_value(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
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
