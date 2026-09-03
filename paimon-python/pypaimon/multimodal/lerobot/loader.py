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
from pypaimon.multimodal.lerobot.schema import _feature_shape
from pypaimon.multimodal.table import _target_schema
from pypaimon.table.row.blob import VideoFrameDescriptor


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
_VIDEO_TIMESTAMP_TOLERANCE = 1e-4


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
        video_fields=()):
    target_schema = _target_schema(table.raw_table)
    write_builder = table.raw_table.new_batch_write_builder()
    table_write = None
    table_commit = None
    commit_started = False
    batch_count = 0
    row_count = 0
    snapshot_recorder = _SnapshotRecorder()
    video_sources = {}

    try:
        table_write = write_builder.new_write()
        reader_factory = getattr(
            dataset, "video_uri_reader_factory", None)
        if video_fields and reader_factory is not None:
            table_write.with_blob_uri_reader_factory(reader_factory)
        table_commit = write_builder.new_commit()
        table_commit.add_commit_callback(snapshot_recorder)
        for episode, episode_begin, episode_end in _episodes(dataset, info):
            if video_fields:
                table_write.begin_video_episode(episode_end - episode_begin)
            for begin in range(episode_begin, episode_end, batch_size):
                end = min(begin + batch_size, episode_end)
                batch = _read_batch(
                    dataset,
                    info,
                    begin,
                    end,
                    source_schema,
                    episode=episode,
                    video_sources=video_sources,
                )
                batch = _strict_lerobot_table(
                    batch,
                    target_schema,
                    source,
                    batch_count,
                )
                table_write.write_arrow(batch)
                batch_count += 1
                row_count += batch.num_rows

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


def _episodes(dataset, info):
    episodes = getattr(dataset.meta, "episodes", None)
    episode_count = int(info.get("total_episodes", 0))
    total_frames = int(info.get("total_frames", len(dataset)))
    if episodes is None:
        raise ValueError("LeRobot v3 metadata is missing episode boundaries.")
    expected_begin = 0
    for ordinal in range(episode_count):
        episode = episodes.iloc[ordinal] if hasattr(episodes, "iloc") \
            else episodes[ordinal]
        try:
            episode_index = _nonnegative_integer(
                episode["episode_index"], "episode_index")
            length = _nonnegative_integer(episode["length"], "length")
            begin = _nonnegative_integer(
                episode["dataset_from_index"], "dataset_from_index")
            end = _nonnegative_integer(
                episode["dataset_to_index"], "dataset_to_index")
        except (KeyError, TypeError) as error:
            raise ValueError(
                "LeRobot episode %d is missing required boundary metadata."
                % ordinal
            ) from error
        if episode_index != ordinal or begin != expected_begin \
                or end <= begin or length != end - begin:
            raise ValueError(
                "LeRobot episode %d has invalid index, length, or frame "
                "range [%d, %d); expected it to start at %d."
                % (ordinal, begin, end, expected_begin))
        yield episode, begin, end
        expected_begin = end
    if expected_begin != total_frames:
        raise ValueError(
            "LeRobot episode ranges cover %d frames but metadata reports %d."
            % (expected_begin, total_frames))


def _read_batch(
        dataset, info, begin, end, schema, episode=None,
        video_sources=None):
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
    video_rows = None
    if any(feature.get("dtype") == "video"
           for feature in features.values()):
        video_rows = _validate_video_rows(
            raw, info, episode, begin, end)

    arrays = []
    fields = []
    for name, feature in features.items():
        field = schema.field(name)
        dtype = feature["dtype"]
        if dtype == "video":
            if episode is None:
                raise ValueError(
                    "LeRobot video import requires Episode metadata.")
            values = _video_frame_descriptors(
                dataset,
                info,
                episode,
                video_rows,
                name,
                feature,
                begin,
                end,
                video_sources if video_sources is not None else {},
            )
        elif name not in raw.column_names:
            raise ValueError(
                "LeRobot data is missing metadata feature %s." % name)
        else:
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
        arrays.append(pa.array(
            [_task_name(dataset.meta.tasks, value) for value in task_indices],
            type=pa.string(),
        ))
        fields.append(schema.field("task"))
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def _video_frame_descriptors(
        dataset, info, episode, video_rows, name, feature, begin, end, cache):
    episode_begin = _nonnegative_integer(
        episode["dataset_from_index"], "dataset_from_index")
    episode_end = _nonnegative_integer(
        episode["dataset_to_index"], "dataset_to_index")
    if begin < episode_begin or end > episode_end:
        raise ValueError(
            "LeRobot video batch [%d, %d) crosses Episode range [%d, %d)."
            % (begin, end, episode_begin, episode_end)
        )

    fps = _video_fps(info, feature, name)
    prefix = "videos/%s/" % name
    try:
        chunk_index = _nonnegative_integer(
            episode[prefix + "chunk_index"], prefix + "chunk_index")
        file_index = _nonnegative_integer(
            episode[prefix + "file_index"], prefix + "file_index")
        from_timestamp = float(_python_scalar(
            episode[prefix + "from_timestamp"]))
        to_timestamp = float(_python_scalar(
            episode[prefix + "to_timestamp"]))
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError(
            "LeRobot Episode metadata is missing video mapping for %s."
            % name
        ) from error

    first_frame = _aligned_frame_ordinal(
        from_timestamp, fps, "video feature %s from_timestamp" % name)
    to_frame = _aligned_frame_ordinal(
        to_timestamp, fps, "video feature %s to_timestamp" % name)
    if first_frame < 0 or to_frame - first_frame != episode_end - episode_begin:
        raise ValueError(
            "LeRobot video feature %s has frame range [%d, %d), but "
            "Episode [%d, %d) contains %d frames."
            % (
                name,
                first_frame,
                to_frame,
                episode_begin,
                episode_end,
                episode_end - episode_begin,
            )
        )

    source_key = (name, chunk_index, file_index)
    source = cache.get(source_key)
    if source is None:
        source = _video_source(
            dataset, info, episode, name, chunk_index, file_index)
        cache[source_key] = source
    uri, length = source
    descriptors = []
    for episode_frame_index, timestamp in video_rows:
        frame_index = first_frame + episode_frame_index
        shifted_timestamp = from_timestamp + timestamp
        if not math.isclose(
                shifted_timestamp,
                frame_index / fps,
                rel_tol=0.0,
                abs_tol=_VIDEO_TIMESTAMP_TOLERANCE):
            raise ValueError(
                "LeRobot video feature %s frame %d has shifted timestamp "
                "%s, expected %s."
                % (name, episode_frame_index, shifted_timestamp,
                   frame_index / fps)
            )
        descriptors.append(VideoFrameDescriptor(
            uri, 0, length, frame_index).serialize())
    return descriptors


def _validate_video_rows(raw, info, episode, begin, end):
    required = ("episode_index", "frame_index", "timestamp")
    missing = [name for name in required if name not in raw.column_names]
    if missing:
        raise ValueError(
            "LeRobot video import requires frame columns %s."
            % ", ".join(missing)
        )
    episode_index = _nonnegative_integer(
        episode["episode_index"], "episode_index")
    episode_begin = _nonnegative_integer(
        episode["dataset_from_index"], "dataset_from_index")
    expected_frames = list(range(
        begin - episode_begin, end - episode_begin))
    actual_episodes = [
        _nonnegative_integer(value, "episode_index")
        for value in raw.column("episode_index").to_pylist()
    ]
    actual_frames = [
        _nonnegative_integer(value, "frame_index")
        for value in raw.column("frame_index").to_pylist()
    ]
    if actual_episodes != [episode_index] * (end - begin) \
            or actual_frames != expected_frames:
        raise ValueError(
            "LeRobot frame rows do not match Episode %d range [%d, %d)."
            % (episode_index, begin, end)
        )
    timestamps = raw.column("timestamp").to_pylist()
    global_fps = _positive_fps(info.get("fps"), "dataset")
    timestamp_values = []
    for frame_index, timestamp in zip(actual_frames, timestamps):
        try:
            value = float(_python_scalar(timestamp))
        except (TypeError, ValueError) as error:
            raise ValueError("LeRobot frame timestamp is invalid.") from error
        if not math.isfinite(value) or not math.isclose(
                value,
                frame_index / global_fps,
                rel_tol=0.0,
                abs_tol=_VIDEO_TIMESTAMP_TOLERANCE):
            raise ValueError(
                "LeRobot frame %d has timestamp %r, expected %s."
                % (frame_index, timestamp, frame_index / global_fps)
            )
        timestamp_values.append(value)
    return list(zip(actual_frames, timestamp_values))


def _aligned_frame_ordinal(timestamp, fps, owner):
    if not math.isfinite(timestamp):
        raise ValueError("LeRobot %s is invalid." % owner)
    frame = int(round(timestamp * fps))
    if not math.isclose(
            timestamp,
            frame / fps,
            rel_tol=0.0,
            abs_tol=_VIDEO_TIMESTAMP_TOLERANCE):
        raise ValueError(
            "LeRobot %s %s is not aligned to FPS %s."
            % (owner, timestamp, fps)
        )
    return frame


def _video_fps(info, feature, name):
    global_fps = _positive_fps(info.get("fps"), "dataset")
    values = []
    for key in ("info", "video_info"):
        details = feature.get(key)
        if isinstance(details, dict) and "video.fps" in details:
            values.append(details["video.fps"])
    if "fps" in feature:
        values.append(feature["fps"])
    value = values[0] if values else global_fps
    fps = _positive_fps(value, "video feature %s" % name)
    if any(not math.isclose(
            fps, _positive_fps(other, "video feature %s" % name),
            rel_tol=1e-6, abs_tol=1e-6) for other in values[1:]) \
            or not math.isclose(
                fps, global_fps, rel_tol=1e-6, abs_tol=1e-6):
        raise ValueError(
            "LeRobot video feature %s FPS does not match dataset FPS."
            % name
        )
    return fps


def _positive_fps(value, owner):
    try:
        fps = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(
            "LeRobot %s is missing a valid FPS." % owner
        ) from error
    if not math.isfinite(fps) or fps <= 0:
        raise ValueError(
            "LeRobot %s is missing a valid FPS." % owner)
    return fps


def _video_source(
        dataset, info, episode, name, chunk_index, file_index):
    resolver = getattr(dataset, "video_source", None)
    if callable(resolver):
        return resolver(name, episode)

    template = info.get("video_path")
    if not isinstance(template, str) or not template:
        raise ValueError("LeRobot v3 metadata is missing info.video_path.")
    relative = template.format(
        video_key=name,
        chunk_index=chunk_index,
        file_index=file_index,
    )
    root = Path(dataset.root).resolve()
    path = (root / relative).resolve()
    try:
        path.relative_to(root)
    except ValueError as error:
        raise ValueError(
            "LeRobot video path must stay within the source directory: %s"
            % relative
        ) from error
    if not path.is_file():
        raise FileNotFoundError("LeRobot video file does not exist: %s" % path)
    length = path.stat().st_size
    if length <= 0:
        raise ValueError("LeRobot video file is empty: %s" % path)
    return path.as_uri(), length


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


def _nonnegative_integer(value, name):
    value = _python_scalar(value)
    if isinstance(value, bool) or not isinstance(value, numbers.Integral) \
            or value < 0:
        raise ValueError(
            "LeRobot metadata field %s must be a non-negative integer; "
            "found %r." % (name, value)
        )
    return int(value)


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
