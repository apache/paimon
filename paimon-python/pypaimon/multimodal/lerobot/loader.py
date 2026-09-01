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

"""LeRobot frame conversion and batch writing."""

import io
import math
import numbers
from pathlib import Path

import pyarrow as pa

from pypaimon.multimodal.arrow_utils import strict_arrow_table
from pypaimon.multimodal.hdf5 import _SnapshotRecorder
from pypaimon.multimodal.lerobot.metadata import _with_frame_identity
from pypaimon.multimodal.lerobot.schema import _feature_shape
from pypaimon.multimodal.table import _target_schema


_DECLARED_NUMERIC_RANGES = {
    "uint8": (0, 255),
    "uint16": (0, 65535),
    "uint32": (0, 4294967295),
    "float16": (-65504.0, 65504.0),
    "float32": (-3.4028234663852886e38, 3.4028234663852886e38),
}
_NUMERIC_DTYPES = {
    "int8",
    "int16",
    "int32",
    "int64",
    "uint8",
    "uint16",
    "uint32",
    "float16",
    "float32",
    "float64",
}
_BOOLEAN_DTYPES = {"bool", "boolean"}


def _strict_lerobot_table(data, target_schema, source, batch_index):
    return strict_arrow_table(
        data,
        target_schema,
        source.path,
        batch_index,
        "LeRobot",
    )


def _write_dataset(
        table,
        dataset,
        info,
        source,
        source_schema,
        batch_size,
        dataset_id,
        metadata):
    target_schema = _target_schema(table.raw_table)
    write_builder = table.raw_table.new_batch_write_builder()
    table_write = None
    table_commit = None
    commit_started = False
    batch_count = 0
    row_count = 0
    episodes = metadata["episodes"]
    task_names = {
        row["task_index"]: row["task"] for row in metadata["tasks"]
    }
    observed_tasks = {}
    snapshot_recorder = _SnapshotRecorder()

    try:
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        table_commit.add_commit_callback(snapshot_recorder)
        for episode_index, episode_begin, task_indices, begin, end in \
                _episode_batches(dataset, info, batch_size, episodes):
            batch = _read_batch(
                dataset, info, begin, end, source_schema, task_names)
            seen_tasks = _validate_frame_controls(
                batch,
                int(info["fps"]),
                episode_index,
                episode_begin,
                begin,
                task_indices,
            )
            observed_tasks.setdefault(episode_index, set()).update(
                seen_tasks)
            batch = _with_frame_identity(batch, dataset_id)
            batch = _strict_lerobot_table(
                batch,
                target_schema,
                source,
                batch_count,
            )
            table_write.write_arrow(batch)
            batch_count += 1
            row_count += batch.num_rows

        _validate_episode_tasks(episodes, observed_tasks)

        expected_rows = int(info.get("total_frames", len(dataset)))
        if row_count != expected_rows:
            raise ValueError(
                "LeRobot metadata reports %d frames but import produced %d."
                % (expected_rows, row_count))
        messages = table_write.prepare_commit()
        commit_started = True
        table_commit.commit(messages)
        if snapshot_recorder.snapshot_id is None:
            raise RuntimeError(
                "LeRobot append committed without reporting a snapshot id.")
        return snapshot_recorder.snapshot_id
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


def _episode_batches(dataset, info, batch_size, episodes):
    episode_count = int(info.get("total_episodes", 0))
    total_frames = int(info.get("total_frames", len(dataset)))
    expected_begin = 0
    for ordinal in range(episode_count):
        episode = episodes.iloc[ordinal] if hasattr(episodes, "iloc") \
            else episodes[ordinal]
        episode_index = int(_python_scalar(episode["episode_index"]))
        begin = int(_python_scalar(episode["dataset_from_index"]))
        end = int(_python_scalar(episode["dataset_to_index"]))
        length = int(_python_scalar(episode["length"]))
        if episode_index != ordinal or begin != expected_begin \
                or end <= begin or length != end - begin:
            raise ValueError(
                "LeRobot episode %d has an invalid index, range, or length."
                % ordinal)
        episode_begin = begin
        task_indices = episode.get("task_indices", ())
        while begin < end:
            batch_end = min(begin + batch_size, end)
            yield (
                episode_index,
                episode_begin,
                task_indices,
                begin,
                batch_end,
            )
            begin = batch_end
        expected_begin = end
    if expected_begin != total_frames:
        raise ValueError(
            "LeRobot episode ranges cover %d frames but metadata reports %d."
            % (expected_begin, total_frames))


def _validate_frame_controls(
        batch,
        fps,
        episode_index,
        episode_begin,
        begin,
        task_indices):
    required = [
        "index", "episode_index", "frame_index", "timestamp", "task_index"
    ]
    missing = [name for name in required if name not in batch.column_names]
    if missing:
        raise ValueError(
            "LeRobot frame data is missing control columns: %s."
            % ", ".join(missing))

    allowed_tasks = set(task_indices)
    values = {
        name: batch.column(name).to_pylist()
        for name in required
    }
    seen_tasks = set()
    for offset in range(batch.num_rows):
        index = begin + offset
        frame_index = index - episode_begin
        _require_control_integer(
            values["index"][offset], "index", index, index)
        _require_control_integer(
            values["episode_index"][offset],
            "episode_index",
            episode_index,
            index,
        )
        _require_control_integer(
            values["frame_index"][offset],
            "frame_index",
            frame_index,
            index,
        )
        timestamp = values["timestamp"][offset]
        if (isinstance(timestamp, bool)
                or not isinstance(timestamp, numbers.Real)
                or not math.isclose(
                    float(timestamp), frame_index / fps,
                    rel_tol=0.0, abs_tol=1e-4)):
            raise ValueError(
                "LeRobot frame %d has timestamp %r; expected %r."
                % (index, timestamp, frame_index / fps))
        task_index = _control_integer(
            values["task_index"][offset], "task_index", index)
        if task_index not in allowed_tasks:
            raise ValueError(
                "LeRobot frame %d has task_index %d outside Episode %d "
                "tasks %s."
                % (index, task_index, episode_index,
                   sorted(allowed_tasks)))
        seen_tasks.add(task_index)
    return seen_tasks


def _validate_episode_tasks(episodes, observed_tasks):
    for episode in episodes:
        episode_index = episode["episode_index"]
        expected = set(episode["task_indices"])
        actual = observed_tasks.get(episode_index, set())
        if actual != expected:
            raise ValueError(
                "LeRobot Episode %d declares task indices %s but its "
                "frames use %s."
                % (episode_index, sorted(expected), sorted(actual)))


def _require_control_integer(value, name, expected, frame_index):
    actual = _control_integer(value, name, frame_index)
    if actual != expected:
        raise ValueError(
            "LeRobot frame %d has %s %d; expected %d."
            % (frame_index, name, actual, expected))


def _control_integer(value, name, frame_index):
    value = _python_scalar(value)
    if isinstance(value, bool) or not isinstance(value, numbers.Integral):
        raise ValueError(
            "LeRobot frame %d has non-integer %s %r."
            % (frame_index, name, value))
    return int(value)


def _read_batch(dataset, info, begin, end, schema, task_names=None):
    read_batch = getattr(dataset, "read_batch", None)
    if callable(read_batch):
        raw = read_batch(begin, end)
    else:
        raw = dataset.hf_dataset.with_format("arrow")[begin:end]
    if isinstance(raw, pa.RecordBatch):
        raw = pa.Table.from_batches([raw])
    elif not isinstance(raw, pa.Table):
        raw = pa.Table.from_pydict(raw)
    features = info["features"]

    arrays = []
    fields = []
    for name, feature in features.items():
        field = schema.field(name)
        dtype = feature["dtype"]
        if name not in raw.column_names:
            raise ValueError(
                "LeRobot data is missing metadata feature %s." % name)
        values = raw.column(name).to_pylist()
        if dtype == "image":
            image_reader = getattr(dataset, "image_bytes", None)
            if callable(image_reader):
                values = [image_reader(value) for value in values]
            else:
                values = [_image_bytes(value, dataset.root)
                          for value in values]
        else:
            values = [_normalize_value(value, feature, name)
                      for value in values]
        arrays.append(_safe_array(values, field, name, dtype))
        fields.append(field)

    if "task" in schema.names:
        task_indices = raw.column("task_index").to_pylist()
        tasks = dataset.meta.tasks if task_names is None else task_names
        arrays.append(pa.array(
            [_task_name(tasks, value) for value in task_indices],
            type=pa.string(),
        ))
        fields.append(schema.field("task"))
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def _safe_array(values, field, name, dtype):
    _validate_declared_range(values, field.type, name, dtype)
    try:
        return pa.array(values).cast(field.type, safe=True)
    except (pa.ArrowException, TypeError, ValueError, OverflowError) as error:
        raise ValueError(
            "LeRobot feature %s cannot be safely converted to %s: %s"
            % (name, field.type, error)) from error


def _validate_declared_range(values, target_type, name, dtype):
    if (pa.types.is_list(target_type)
            or pa.types.is_large_list(target_type)
            or pa.types.is_fixed_size_list(target_type)):
        for value in values:
            if value is not None:
                _validate_declared_range(
                    value, target_type.value_type, name, dtype)
        return

    value_range = _DECLARED_NUMERIC_RANGES.get(dtype)
    for value in values:
        if value is None:
            continue
        _validate_value_domain(value, name, dtype)
        if value_range is None:
            continue
        minimum, maximum = value_range
        if dtype.startswith("float") and not isinstance(
                value, numbers.Integral):
            try:
                if not math.isfinite(value):
                    continue
            except (TypeError, ValueError, OverflowError) as error:
                raise ValueError(
                    "LeRobot feature %s contains a value incompatible with "
                    "dtype %s: %r" % (name, dtype, value)) from error
        try:
            out_of_range = value < minimum or value > maximum
        except (TypeError, ValueError, OverflowError) as error:
            raise ValueError(
                "LeRobot feature %s contains a value incompatible with "
                "dtype %s: %r" % (name, dtype, value)) from error
        if out_of_range:
            raise ValueError(
                "LeRobot feature %s contains a value outside the %s "
                "range [%s, %s]: %r"
                % (name, dtype, minimum, maximum, value))


def _validate_value_domain(value, name, dtype):
    if dtype in _NUMERIC_DTYPES:
        valid = isinstance(value, numbers.Real) \
            and not isinstance(value, bool)
        expected = "numeric"
    elif dtype in _BOOLEAN_DTYPES:
        valid = isinstance(value, bool)
        expected = "boolean"
    elif dtype == "string":
        valid = isinstance(value, str)
        expected = "string"
    else:
        return
    if not valid:
        raise ValueError(
            "LeRobot feature %s declares dtype %s but contains a non-%s "
            "value: %r" % (name, dtype, expected, value))


def _normalize_value(value, feature, name):
    shape = _feature_shape(feature, name)
    if shape in ((), (1,)):
        if isinstance(value, (list, tuple)):
            if len(value) != 1:
                raise ValueError(
                    "LeRobot feature %s expected shape %s, got %s."
                    % (name, shape, _value_shape(value)))
            return value[0]
        return _python_scalar(value)
    actual_shape = _value_shape(value)
    if actual_shape != shape:
        raise ValueError(
            "LeRobot feature %s expected shape %s, got %s."
            % (name, shape, actual_shape))
    return value


def _value_shape(value):
    if hasattr(value, "shape"):
        return tuple(int(size) for size in value.shape)
    if isinstance(value, (list, tuple)):
        if not value:
            return (0,)
        child = _value_shape(value[0])
        if any(_value_shape(item) != child for item in value[1:]):
            return (len(value), -1)
        return (len(value),) + child
    return ()


def _python_scalar(value):
    item = getattr(value, "item", None)
    if callable(item):
        return item()
    return value


def _image_bytes(value, root):
    if value is None:
        raise ValueError("LeRobot image feature contains a null frame.")
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    if isinstance(value, dict):
        body = value.get("bytes")
        if body is not None:
            return bytes(body)
        image_path = value.get("path")
        if image_path:
            root = Path(root).resolve()
            path = Path(image_path)
            if not path.is_absolute():
                path = root / path
            path = path.resolve()
            try:
                path.relative_to(root)
            except ValueError as error:
                raise ValueError(
                    "LeRobot image path must stay within the source "
                    "directory: %s" % image_path) from error
            return path.read_bytes()
    return _encode_media_frame(value)


def _encode_media_frame(value):
    try:
        import numpy as np
        from PIL import Image
    except ImportError as error:
        raise ImportError(
            "LeRobot media import requires numpy and Pillow from the "
            "'pypaimon[lerobot]' extra.") from error

    if isinstance(value, Image.Image):
        image = value
    else:
        detach = getattr(value, "detach", None)
        if callable(detach):
            value = detach().cpu().numpy()
        array = np.asarray(value)
        if array.ndim == 3 and array.shape[0] in (1, 3, 4):
            array = np.transpose(array, (1, 2, 0))
        if np.issubdtype(array.dtype, np.floating):
            array = np.rint(np.clip(array, 0.0, 1.0) * 255.0).astype(np.uint8)
        if array.ndim == 3 and array.shape[2] == 1:
            array = array[:, :, 0]
        try:
            image = Image.fromarray(array)
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(
                "Unsupported LeRobot media frame shape or dtype: %s, %s."
                % (array.shape, array.dtype)) from error
    output = io.BytesIO()
    image.save(output, format="PNG")
    return output.getvalue()


def _task_name(tasks, task_index):
    index = int(_python_scalar(task_index))
    if index < 0 or index >= len(tasks):
        raise ValueError(
            "LeRobot task_index %d is outside [0, %d)."
            % (index, len(tasks)))
    if hasattr(tasks, "iloc"):
        return str(tasks.iloc[index].name)
    task = tasks[index]
    if isinstance(task, dict):
        return str(task.get("task", task.get("name")))
    return str(task)
