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

"""LeRobot component tables and training tags."""

from array import array
import json
import numbers
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from pypaimon import Schema as PaimonSchema
from pypaimon.catalog.catalog_exception import (
    TableAlreadyExistException,
    TagNotExistException,
)
from pypaimon.common.identifier import Identifier
from pypaimon.multimodal.hdf5 import _SnapshotRecorder
from pypaimon.multimodal.table import _target_schema


_PANDAS_METADATA_OPTION = "pypaimon.lerobot.pandas-metadata"
_TABLE_SUFFIXES = {
    "info": "__info",
    "stats": "__stats",
    "episodes": "__episodes",
    "tasks": "__tasks",
    "subtasks": "__subtasks",
}
_COMPANION_OPTION_KEYS = {
    name: "pypaimon.lerobot.%s-table" % name
    for name in _TABLE_SUFFIXES
}

_EMPTY_TASKS_SCHEMA = pa.schema([
    pa.field("task_index", pa.int64(), nullable=False),
    pa.field("task", pa.string(), nullable=False),
])
_EMPTY_SUBTASKS_SCHEMA = pa.schema([
    pa.field("subtask_index", pa.int64(), nullable=False),
    pa.field("subtask", pa.string(), nullable=False),
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

    def __init__(self, video_fields=()):
        self._ranges = array("q")
        self._task_offsets = array("q", [0])
        self._task_indices = array("q")
        self._video_fields = tuple(video_fields)
        self._video_indices = {
            field: (array("q"), array("q"))
            for field in self._video_fields
        }
        self._video_timestamps = {
            field: (array("d"), array("d"))
            for field in self._video_fields
        }

    def append(self, begin, end, task_indices, video_values=None):
        self._ranges.extend((begin, end))
        self._task_indices.extend(task_indices)
        self._task_offsets.append(len(self._task_indices))
        video_values = video_values or {}
        for field in self._video_fields:
            values = video_values[field]
            chunk_indices, file_indices = self._video_indices[field]
            from_timestamps, to_timestamps = self._video_timestamps[field]
            chunk_indices.append(values[0])
            file_indices.append(values[1])
            from_timestamps.append(values[2])
            to_timestamps.append(values[3])

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
        result = {
            "episode_index": index,
            "dataset_from_index": begin,
            "dataset_to_index": end,
            "length": end - begin,
            "task_indices": self._task_indices[task_begin:task_end],
        }
        for field in self._video_fields:
            chunk_indices, file_indices = self._video_indices[field]
            from_timestamps, to_timestamps = self._video_timestamps[field]
            prefix = "videos/%s/" % field
            result[prefix + "chunk_index"] = chunk_indices[index]
            result[prefix + "file_index"] = file_indices[index]
            result[prefix + "from_timestamp"] = from_timestamps[index]
            result[prefix + "to_timestamp"] = to_timestamps[index]
        return result


def _load_dataset_metadata(dataset, info, source):
    fps = _positive_integer(info.get("fps"), "fps")
    stats = _source_stats(dataset, source)
    stats_table = None if stats is None else _metadata_table(stats)
    tasks_table = _source_tasks(
        dataset, source, int(info["total_tasks"]))
    task_indices = _task_indices(
        tasks_table, int(info["total_tasks"]))
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
        "info_table": _metadata_table(info),
        "stats_table": (stats_table if stats_table is not None
                        and stats_table.num_rows > 0 else None),
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


def _managed_table_options(frames_identifier, metadata=None):
    identifier = Identifier.from_string(str(frames_identifier))
    if identifier.get_branch_name() is not None:
        raise ValueError(
            "LeRobot import does not support table branches.")
    result = {}
    for name, suffix in _TABLE_SUFFIXES.items():
        if metadata is not None and name in ("stats", "subtasks") \
                and metadata[name + "_table"] is None:
            continue
        result[_COMPANION_OPTION_KEYS[name]] = _companion_identifier(
            frames_identifier, suffix)
    return result


def _companion_table_identifiers(frames_table):
    options = frames_table.table_schema.options
    identifiers = {}
    for name, key in _COMPANION_OPTION_KEYS.items():
        value = options.get(key)
        if not value:
            if name in ("stats", "subtasks"):
                continue
            raise ValueError(
                "LeRobot table %s is missing managed option %s."
                % (frames_table.identifier, key))
        identifiers[name] = value
    return identifiers


def _prepare_metadata_tables(connection, frames_table, metadata):
    schemas = {
        "info": metadata["info_table"].schema,
        "episodes": metadata["episodes_schema"],
        "tasks": metadata["tasks_table"].schema,
    }
    for name in ("stats", "subtasks"):
        if metadata[name + "_table"] is not None:
            schemas[name] = metadata[name + "_table"].schema
    identifiers = _companion_table_identifiers(frames_table)
    tables = {}
    for name, schema in schemas.items():
        identifier = identifiers[name]
        options = {"bucket": "-1"}
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
        except TableAlreadyExistException as error:
            raise ValueError(
                "LeRobot metadata table %s already exists." % identifier
            ) from error
        table = connection.catalog.get_table(identifier)
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


def _commit_metadata(
        connection,
        tables,
        tag_name,
        metadata,
        frames_identifier,
        frames_snapshot_id,
        episodes_snapshot_id):
    _require_initial_snapshot("frames", frames_snapshot_id)
    _require_initial_snapshot("episodes", episodes_snapshot_id)
    component_snapshots = [
        (tables["episodes"].identifier, episodes_snapshot_id),
    ]
    for name in ("tasks", "subtasks", "stats", "info"):
        if name not in tables:
            continue
        snapshot_id = _append_arrow(tables[name], metadata[name + "_table"])
        _require_initial_snapshot(name, snapshot_id)
        component_snapshots.append((tables[name].identifier, snapshot_id))
    frames_table = connection.catalog.get_table(frames_identifier)
    frames_snapshot_id = _build_initial_btree(
        frames_table, "index", frames_snapshot_id)
    # Tag the root last so a failed component tag does not expose a root tag.
    component_snapshots.append((frames_identifier, frames_snapshot_id))
    if tag_name is not None:
        for identifier, snapshot_id in component_snapshots:
            _create_tag(connection.catalog, identifier, tag_name, snapshot_id)


def _build_initial_btree(table, column, data_snapshot_id):
    latest = table.snapshot_manager().get_latest_snapshot()
    if latest is None or latest.id != data_snapshot_id:
        raise RuntimeError(
            "LeRobot initial import detected concurrent writes to %s before "
            "building its %s BTree." % (table.identifier, column))
    added = table.create_global_index(column, index_type="btree")
    latest = table.snapshot_manager().get_latest_snapshot()
    if added <= 0 or latest is None or latest.id != data_snapshot_id + 1:
        raise RuntimeError(
            "LeRobot initial import could not publish an isolated "
            "%s BTree for %s." % (column, table.identifier))
    return latest.id


def create_lerobot_tag(connection, table_name, tag_name):
    """Tag the current snapshots of a LeRobot table group for training.

    Pause group writes until this call returns. Tags across tables are not an
    atomic transaction: use the name only after success, and read every
    component with that tag (never fall back to latest). Failed calls may leave
    partial tags. Retrying is safe while the component snapshots are unchanged.
    Returns a mapping from component name to tagged snapshot ID.
    """
    _validate_tag_name(tag_name)
    frames = connection.catalog.get_table(connection._identifier(table_name))
    identifiers = _companion_table_identifiers(frames)
    identifiers["frames"] = frames.identifier
    snapshots = {}
    for name, identifier in identifiers.items():
        table = connection.catalog.get_table(identifier)
        snapshot = table.snapshot_manager().get_latest_snapshot()
        if snapshot is None:
            raise ValueError("LeRobot component %s has no snapshot." % name)
        snapshots[name] = snapshot.id
        existing = _tag_snapshot_id(connection.catalog, identifier, tag_name)
        if existing is not None and existing != snapshot.id:
            raise ValueError(
                "LeRobot tag %s on %s already points to snapshot %s; "
                "use a new tag name." % (tag_name, identifier, existing))
    for name, identifier in identifiers.items():
        _create_tag(connection.catalog, identifier, tag_name, snapshots[name])
    return snapshots


def _validate_tag_name(tag_name):
    if not isinstance(tag_name, str) or not tag_name.strip() \
            or any(character in tag_name for character in ("/", "\\", "\x00")):
        raise ValueError("tag_name must be a non-blank name without path separators.")


def _require_initial_snapshot(component, snapshot_id):
    if snapshot_id is None:
        raise ValueError(
            "LeRobot import requires a non-empty %s component."
            % component)
    if snapshot_id != 1:
        raise RuntimeError(
            "LeRobot initial import detected concurrent writes to %s; "
            "expected snapshot 1, found %d." % (component, snapshot_id))


def _append_arrow(table, data):
    return _append_arrow_tables(table, [data])


def _overwrite_arrow(table, data):
    target_schema = _target_schema(table)
    if not data.schema.equals(target_schema, check_metadata=False):
        raise ValueError(
            "LeRobot component schema %s does not match target %s."
            % (data.schema, target_schema))
    builder = table.new_batch_write_builder().overwrite()
    table_write = builder.new_write()
    table_commit = builder.new_commit()
    commit_started = False
    try:
        table_write.write_arrow(data)
        messages = table_write.prepare_commit()
        commit_started = True
        table_commit.commit(messages)
    except BaseException:
        if not commit_started:
            table_write.abort()
        raise
    finally:
        try:
            table_write.close()
        finally:
            table_commit.close()


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
        try:
            catalog.create_tag(
                identifier, tag_name, snapshot_id=snapshot_id)
        except NotImplementedError:
            catalog.get_table(identifier).create_tag(
                tag_name, snapshot_id=snapshot_id)
    except Exception as error:
        try:
            actual_snapshot_id = _tag_snapshot_id(
                catalog, identifier, tag_name)
        except Exception:
            raise error
        if actual_snapshot_id == snapshot_id:
            return
        if actual_snapshot_id is not None:
            raise RuntimeError(
                "LeRobot tag %s on %s points to snapshot %s; expected %s."
                % (tag_name, identifier, actual_snapshot_id, snapshot_id)
            ) from error
        raise error


def _tag_snapshot_id(catalog, identifier, tag_name):
    try:
        response = catalog.get_tag(identifier, tag_name)
        snapshot = response.snapshot
    except TagNotExistException:
        return None
    except NotImplementedError:
        snapshot = catalog.get_table(identifier).tag_manager().get(tag_name)
    return None if snapshot is None else snapshot.id


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


def _validated_episode_tables(metadata, video_fields=()):
    episodes = _EpisodeIndex(video_fields)
    expected_begin = 0
    video_columns = [
        "videos/%s/%s" % (field, suffix)
        for field in video_fields
        for suffix in (
            "chunk_index", "file_index", "from_timestamp", "to_timestamp")
    ]
    required_columns = _EPISODE_CONTROL_COLUMNS + video_columns
    for table in _source_episode_tables(metadata):
        missing = [
            name for name in required_columns
            if name not in table.column_names
        ]
        if missing:
            raise ValueError(
                "LeRobot Episode metadata is missing columns: %s."
                % ", ".join(missing))
        controls = table.select(required_columns)
        columns = {
            name: controls.column(name)
            for name in required_columns
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
            video_values = {}
            for field in video_fields:
                prefix = "videos/%s/" % field
                chunk_index = _integer(
                    columns[prefix + "chunk_index"][offset].as_py(),
                    prefix + "chunk_index",
                )
                file_index = _integer(
                    columns[prefix + "file_index"][offset].as_py(),
                    prefix + "file_index",
                )
                if chunk_index < 0 or file_index < 0:
                    raise ValueError(
                        "LeRobot Episode video indices must be non-negative.")
                from_timestamp = columns[
                    prefix + "from_timestamp"][offset].as_py()
                to_timestamp = columns[
                    prefix + "to_timestamp"][offset].as_py()
                if isinstance(from_timestamp, bool) \
                        or not isinstance(from_timestamp, numbers.Real) \
                        or isinstance(to_timestamp, bool) \
                        or not isinstance(to_timestamp, numbers.Real):
                    raise ValueError(
                        "LeRobot Episode video timestamps must be numeric.")
                video_values[field] = (
                    chunk_index,
                    file_index,
                    float(from_timestamp),
                    float(to_timestamp),
                )
            episodes.append(begin, end, task_indices, video_values)
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


def _task_indices(tasks_table, total_tasks):
    if total_tasks == 0:
        return {}
    from pypaimon.multimodal.lerobot.source import _pandas_index_column
    label_column = _pandas_index_column(tasks_table.schema, "task")
    records = tasks_table.select([
        "task_index", label_column
    ]).to_pylist()
    seen = [False] * total_tasks
    by_name = {}
    for record in records:
        index = _integer(record.get("task_index"), "task_index")
        task = record[label_column]
        if index < 0 or index >= total_tasks \
                or not isinstance(task, str) or not task \
                or seen[index]:
            raise ValueError("LeRobot task metadata is invalid: %s" % record)
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
    from pypaimon.multimodal.lerobot.source import _pandas_index_column
    label_column = _pandas_index_column(subtasks_table.schema, "subtask")
    records = subtasks_table.select([
        "subtask_index", label_column
    ]).to_pylist()
    for expected, record in enumerate(records):
        label = record[label_column]
        if _integer(record.get("subtask_index"), "subtask_index") \
                != expected or not isinstance(label, str) or not label:
            raise ValueError(
                "LeRobot subtask metadata must provide ordered numeric and "
                "text mappings for [0, %d)."
                % subtasks_table.num_rows)
    return range(subtasks_table.num_rows)


def _metadata_table(value):
    if not isinstance(value, dict):
        raise ValueError("LeRobot info and stats metadata must be objects.")
    return pa.table({
        "key": pa.array(list(value), type=pa.string()),
        "value": pa.array([
            json.dumps(_json_value(item), ensure_ascii=False, separators=(",", ":"))
            for item in value.values()
        ], type=pa.string()),
    })


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
