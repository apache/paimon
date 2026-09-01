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

"""Metadata tables for imported LeRobot datasets."""

import hashlib
import json
import numbers
import uuid
from datetime import datetime, timezone
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


_DATASET_ID = "dataset_id"
_VERSION_ID = "version_id"
_OWNER_ID_OPTION = "pypaimon.lerobot.owner-id"
_DEFAULT_DATASET_ID_OPTION = "pypaimon.lerobot.dataset-id"
_TABLE_SUFFIXES = {
    "datasets": "__datasets",
    "episodes": "__episodes",
    "tasks": "__tasks",
}
_COMPANION_OPTION_KEYS = {
    name: "pypaimon.lerobot.%s-table" % name
    for name in _TABLE_SUFFIXES
}

_FRAME_ID_FIELDS = [
    pa.field(_DATASET_ID, pa.string(), nullable=False),
]
_DATASETS_SCHEMA = pa.schema([
    pa.field(_DATASET_ID, pa.string(), nullable=False),
    pa.field(_VERSION_ID, pa.string(), nullable=False),
    pa.field("parent_version_id", pa.string()),
    pa.field("status", pa.string(), nullable=False),
    pa.field("published_at", pa.timestamp("us", tz="UTC")),
    pa.field("format", pa.string(), nullable=False),
    pa.field("format_version", pa.string(), nullable=False),
    pa.field("fps", pa.int64(), nullable=False),
    pa.field("features_json", pa.string(), nullable=False),
    pa.field("info_json", pa.string(), nullable=False),
    pa.field("global_stats_json", pa.string()),
    pa.field("total_frames", pa.int64(), nullable=False),
    pa.field("total_episodes", pa.int64(), nullable=False),
    pa.field("total_tasks", pa.int64(), nullable=False),
    pa.field("frames_snapshot_id", pa.int64()),
    pa.field("episodes_snapshot_id", pa.int64()),
    pa.field("tasks_snapshot_id", pa.int64()),
    pa.field("source_uri", pa.string(), nullable=False),
    pa.field("metadata_checksum", pa.string(), nullable=False),
])
_EPISODES_SCHEMA = pa.schema([
    pa.field(_DATASET_ID, pa.string(), nullable=False),
    pa.field("episode_index", pa.int64(), nullable=False),
    pa.field("dataset_from_index", pa.int64(), nullable=False),
    pa.field("dataset_to_index", pa.int64(), nullable=False),
    pa.field("length", pa.int64(), nullable=False),
    pa.field("task_indices", pa.list_(pa.int64()), nullable=False),
    pa.field("split", pa.string()),
    pa.field("episode_stats_json", pa.string()),
    pa.field("episode_metadata_json", pa.string(), nullable=False),
])
_TASKS_SCHEMA = pa.schema([
    pa.field(_DATASET_ID, pa.string(), nullable=False),
    pa.field("task_index", pa.int64(), nullable=False),
    pa.field("task", pa.string(), nullable=False),
    pa.field("task_metadata_json", pa.string()),
])


def _frame_schema(source_schema):
    reserved = [
        field.name for field in _FRAME_ID_FIELDS
        if field.name in source_schema.names
    ]
    if reserved:
        raise ValueError(
            "LeRobot features use reserved Paimon fields: %s" % reserved)
    return pa.schema(list(source_schema) + _FRAME_ID_FIELDS)


def _with_frame_identity(table, dataset_id):
    size = table.num_rows
    return pa.Table.from_arrays(
        list(table.columns) + [
            pa.array([dataset_id] * size, type=pa.string()),
        ],
        schema=pa.schema(list(table.schema) + _FRAME_ID_FIELDS),
    )


def _load_dataset_metadata(dataset, info, source):
    fps = _positive_integer(info.get("fps"), "fps")
    stats = _source_stats(dataset, source)
    task_records = _source_tasks(dataset, source, int(info["total_tasks"]))
    tasks, task_indices = _task_rows(task_records, int(info["total_tasks"]))
    total_episodes = int(info["total_episodes"])
    episode_records = [] if total_episodes == 0 \
        else _source_episodes(dataset, source)
    episodes = _episode_rows(
        episode_records,
        task_indices,
        info,
        int(info["total_frames"]),
        total_episodes,
    )
    canonical = {
        "info": info,
        "stats": stats,
        "episodes": episodes,
        "tasks": tasks,
    }
    return {
        "fps": fps,
        "features_json": _canonical_json(info["features"]),
        "info_json": _canonical_json(info),
        "global_stats_json": (
            None if stats is None else _canonical_json(stats)),
        "episodes": episodes,
        "tasks": tasks,
        "metadata_checksum": "sha256:" + hashlib.sha256(
            _canonical_json(canonical).encode("utf-8")
        ).hexdigest(),
    }


def _new_id():
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
    result = {
        _OWNER_ID_OPTION: owner_id,
        _DEFAULT_DATASET_ID_OPTION: str(frames_identifier),
    }
    for name, suffix in _TABLE_SUFFIXES.items():
        result[_COMPANION_OPTION_KEYS[name]] = _companion_identifier(
            frames_identifier, suffix)
    return result


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


def _prepare_metadata_tables(connection, frames_table, owner_id):
    schemas = {
        "datasets": _DATASETS_SCHEMA,
        "episodes": _EPISODES_SCHEMA,
        "tasks": _TASKS_SCHEMA,
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
        datasets_table,
        dataset_id,
        version_id,
        info,
        source,
        metadata):
    pending = _manifest_row(
        dataset_id,
        version_id,
        None,
        "PENDING",
        None,
        info,
        source,
        metadata,
        None,
        None,
        None,
    )
    snapshot_id = _append_arrow(
        datasets_table,
        pa.Table.from_pylist([pending], schema=_DATASETS_SCHEMA),
    )
    if snapshot_id is None:
        raise RuntimeError("LeRobot version reservation created no snapshot.")


def _publish_dataset(
        connection,
        tables,
        dataset_id,
        version_id,
        info,
        source,
        metadata,
        frames_identifier,
        frames_snapshot_id):
    episodes = _dataset_table(
        metadata["episodes"],
        _EPISODES_SCHEMA,
        dataset_id,
    )
    tasks = _dataset_table(
        metadata["tasks"],
        _TASKS_SCHEMA,
        dataset_id,
    )
    episodes_snapshot_id = _append_arrow(tables["episodes"], episodes)
    tasks_snapshot_id = _append_arrow(tables["tasks"], tasks)

    tag = "pypaimon-lerobot-%s" % version_id
    for identifier, snapshot_id in (
            (frames_identifier, frames_snapshot_id),
            (tables["episodes"].identifier, episodes_snapshot_id),
            (tables["tasks"].identifier, tasks_snapshot_id)):
        if snapshot_id is not None:
            _create_tag(connection.catalog, identifier, tag, snapshot_id)

    manifest = _manifest_row(
        dataset_id,
        version_id,
        None,
        "READY",
        datetime.now(timezone.utc),
        info,
        source,
        metadata,
        frames_snapshot_id,
        episodes_snapshot_id,
        tasks_snapshot_id,
    )
    _append_arrow(tables["datasets"], pa.Table.from_pylist(
        [manifest], schema=_DATASETS_SCHEMA))
    return episodes_snapshot_id, tasks_snapshot_id


def _manifest_row(
        dataset_id,
        version_id,
        parent_version_id,
        status,
        published_at,
        info,
        source,
        metadata,
        frames_snapshot_id,
        episodes_snapshot_id,
        tasks_snapshot_id):
    return {
        _DATASET_ID: dataset_id,
        _VERSION_ID: version_id,
        "parent_version_id": parent_version_id,
        "status": status,
        "published_at": published_at,
        "format": "lerobot",
        "format_version": str(info["codebase_version"]),
        "fps": metadata["fps"],
        "features_json": metadata["features_json"],
        "info_json": metadata["info_json"],
        "global_stats_json": metadata["global_stats_json"],
        "total_frames": int(info["total_frames"]),
        "total_episodes": int(info["total_episodes"]),
        "total_tasks": int(info["total_tasks"]),
        "frames_snapshot_id": frames_snapshot_id,
        "episodes_snapshot_id": episodes_snapshot_id,
        "tasks_snapshot_id": tasks_snapshot_id,
        "source_uri": str(source.path),
        "metadata_checksum": metadata["metadata_checksum"],
    }


def _dataset_table(rows, schema, dataset_id):
    values = []
    for row in rows:
        value = dict(row)
        value[_DATASET_ID] = dataset_id
        values.append(value)
    return pa.Table.from_pylist(values, schema=schema)


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
    if data.num_rows == 0:
        return None
    builder = table.new_batch_write_builder()
    table_write = builder.new_write()
    table_commit = builder.new_commit()
    commit_started = False
    recorder = _SnapshotRecorder()
    table_commit.add_commit_callback(recorder)
    try:
        table_write.write_arrow(data)
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
        return []
    if source.file_io is not None:
        from pypaimon.multimodal.lerobot.source import (
            _read_remote_parquet,
            _remote_path,
        )
        path = _remote_path(source.path, "meta/tasks.parquet")
        return _read_remote_parquet(source.file_io, path).to_pylist()
    path = _metadata_root(dataset, source) / "meta" / "tasks.parquet"
    try:
        return pq.read_table(path).to_pylist()
    except (OSError, ValueError, pa.ArrowException) as error:
        raise ValueError(
            "Cannot read LeRobot task metadata %s: %s" % (path, error)
        ) from error


def _source_episodes(dataset, source):
    if source.file_io is not None:
        from pypaimon.multimodal.lerobot.source import (
            _read_remote_parquet,
            _remote_parquet_files,
            _remote_path,
        )
        directory = _remote_path(source.path, "meta/episodes")
        paths = _remote_parquet_files(source.file_io, directory)
        tables = [
            _read_remote_parquet(source.file_io, path) for path in paths
        ]
    else:
        directory = _metadata_root(dataset, source) / "meta" / "episodes"
        paths = sorted(directory.rglob("*.parquet"))
        try:
            tables = [pq.read_table(path) for path in paths]
        except (OSError, ValueError, pa.ArrowException) as error:
            raise ValueError(
                "Cannot read LeRobot Episode metadata %s: %s"
                % (directory, error)) from error
    rows = []
    for table in tables:
        rows.extend(table.to_pylist())
    rows.sort(key=lambda row: _integer(
        row.get("episode_index"), "episode_index"))
    return rows


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
