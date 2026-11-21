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

"""Export a source Dataset from Paimon to LeRobot v2.0, v2.1, or v3.0."""

import copy
import hashlib
import json
import re
import shutil
import tempfile
from fractions import Fraction
from pathlib import Path
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.parquet as pq

from pypaimon.catalog.catalog_exception import TableNotExistException
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.data.generic_variant import GenericVariant
from pypaimon.multimodal.lerobot.dataset import _component_table, _read_arrow
from pypaimon.multimodal.lerobot.metadata import (
    _companion_identifier, _companion_table_identifiers, _validate_tag_name,
)
from pypaimon.multimodal.lerobot.reader import _PaimonTableFrameReader
from pypaimon.multimodal.table import _target_schema
from pypaimon.table.row.blob import Blob, VideoFrameDescriptor
from pypaimon.table.object.object_table import ObjectTable


_V2_DATA_PATH = "data/chunk-{episode_chunk:03d}/episode_{episode_index:06d}.parquet"
_V2_VIDEO_PATH = "videos/chunk-{episode_chunk:03d}/{video_key}/episode_{episode_index:06d}.mp4"
_V3_DATA_PATH = "data/chunk-{chunk_index:03d}/file-{file_index:03d}.parquet"
_V3_VIDEO_PATH = "videos/{video_key}/chunk-{chunk_index:03d}/file-{file_index:03d}.mp4"


def export_to_lerobot(connection, table_name, *, output_table, sub_folder,
                      version="v3.0", dataset_id=None, tag_name=None,
                      batch_size=1024):
    """Export one original Dataset; the destination must not already exist.

    The v3 exporter copies each encoded video payload once. The v2 exporter
    writes one exact-frame MP4 per Episode and camera; this requires PyAV.
    Both formats preserve source-local frame, Episode, and task indices.
    """
    if version not in ("v2.0", "v2.1", "v3.0"):
        raise ValueError("LeRobot export version must be v2.0, v2.1, or v3.0.")
    if isinstance(batch_size, bool) or not isinstance(batch_size, int) \
            or batch_size <= 0:
        raise ValueError("batch_size must be a positive integer.")
    if dataset_id is not None and (not isinstance(dataset_id, str)
                                   or not dataset_id):
        raise ValueError("dataset_id must be a non-empty string.")
    if tag_name is not None:
        _validate_tag_name(tag_name)
    if not isinstance(output_table, str) or not output_table:
        raise ValueError("output_table must name an Object Table.")
    if (not isinstance(sub_folder, str)
            or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", sub_folder) is None):
        raise ValueError("sub_folder must be one safe directory name.")
    table = connection.catalog.get_table(connection._identifier(output_table))
    if not isinstance(table, ObjectTable):
        raise ValueError("output_table must name an Object Table.")
    object_location = table.location()
    parsed = urlparse(object_location)
    if parsed.scheme != "oss" or not parsed.netloc:
        raise ValueError("Object Table output must use OSS storage.")
    target_uri = object_location.rstrip("/") + "/" + sub_folder

    try:
        frames = connection.get_table(table_name)
    except TableNotExistException:
        frames_name = _companion_identifier(
            connection._identifier(table_name), "_frames")
        frames = connection.get_table(frames_name)
    raw_frames = frames.raw_table
    identifiers = _companion_table_identifiers(raw_frames)
    catalog = connection.catalog
    read_frames = _component_table(catalog, raw_frames, tag_name)
    datasets = _component_table(
        catalog, catalog.get_table(identifiers["datasets"]), tag_name)
    episodes = _component_table(
        catalog, catalog.get_table(identifiers["episodes"]), tag_name)
    sources = _read_arrow(datasets, _target_schema(datasets).names)
    if dataset_id is None:
        if sources.num_rows != 1:
            raise ValueError(
                "dataset_id is required when the LeRobot group has %d Datasets."
                % sources.num_rows)
        source = sources.to_pylist()[0]
    else:
        selected = [row for row in sources.to_pylist()
                    if row["id"] == dataset_id]
        if len(selected) != 1:
            raise ValueError("LeRobot dataset_id %r was not found." % dataset_id)
        source = selected[0]
    info = json.loads(source["info"])
    source_episodes = _read_source_episodes(episodes, source["id"])
    if len(source_episodes) != info["total_episodes"]:
        raise ValueError("LeRobot Episode count disagrees with Dataset metadata.")
    expected = 0
    for number, episode in enumerate(source_episodes):
        if (episode["episode_index"] != number
                or episode["dataset_from_index"] != expected
                or episode["dataset_to_index"] - expected != episode["length"]):
            raise ValueError("LeRobot source Episode ranges are not contiguous.")
        expected = episode["dataset_to_index"]
    if expected != info["total_frames"]:
        raise ValueError("LeRobot Frame count disagrees with Dataset metadata.")

    stage = None
    reader = None
    try:
        if _prefix_has_files(table.file_io, target_uri):
            raise FileExistsError(target_uri)
        stage = Path(tempfile.mkdtemp(prefix=".lerobot-export-"))
        reader = _PaimonTableFrameReader(
            read_frames, columns=_target_schema(read_frames).names)
        _write_dataset(stage, reader, source, info, source_episodes,
                       version, batch_size)
        reader.close()
        reader = None
        _upload_to_object_table(stage, target_uri, table.file_io)
        return target_uri
    finally:
        if reader is not None:
            reader.close()
        if stage is not None and stage.exists():
            shutil.rmtree(stage)


def _upload_to_object_table(stage, prefix, file_io):
    _reserve_object_prefix(prefix, file_io)
    info = stage / "meta/info.json"
    for path in sorted(stage.rglob("*")):
        if not path.is_file() or path == info:
            continue
        _upload_file(path, prefix + "/" + path.relative_to(stage).as_posix(),
                     file_io)
    _upload_file(info, prefix + "/meta/info.json", file_io)


def _prefix_has_files(file_io, prefix):
    return file_io.exists(prefix) or bool(file_io.list_status(prefix))


def _reserve_object_prefix(prefix, file_io):
    if _prefix_has_files(file_io, prefix):
        raise FileExistsError(prefix)
    if not file_io.try_to_write_atomic(
            prefix + "/meta/.pypaimon-export-claim", "claimed"):
        raise FileExistsError(prefix)


def _upload_file(path, uri, file_io):
    if file_io.exists(uri):
        raise FileExistsError(uri)
    with path.open("rb") as source, file_io.new_output_stream(uri) as target:
        shutil.copyfileobj(source, target, length=1024 * 1024)


def _read_source_episodes(table, dataset_id):
    builder = table.new_read_builder()
    predicate = builder.new_predicate_builder().equal("dataset_id", dataset_id)
    builder.with_filter(predicate)
    plan = builder.new_scan().plan()
    rows = builder.new_read().to_arrow(plan.splits()).to_pylist()
    schema = _target_schema(table)
    variant_groups = {
        field.name for field in schema
        if (field.metadata or {}).get(b"description") ==
        b"pypaimon.lerobot.episode-variant-group"
    }
    result = []
    for row in rows:
        if row["dataset_id"] != dataset_id:
            continue
        original = {key: value for key, value in row.items()
                    if key not in ("dataset_id", "meta", "data", "videos", "stats")
                    and key not in variant_groups}
        for group in ("meta", "data", "videos", "stats"):
            if group not in schema.names:
                continue
            value = GenericVariant.from_arrow_struct(row[group]).to_python()
            if group == "stats":
                for feature, statistics in value.items():
                    for name, item in statistics.items():
                        original["stats/%s/%s" % (feature, name)] = item
            else:
                original.update({group + "/" + key: item
                                 for key, item in value.items()})
        for group in variant_groups:
            if row[group] is not None:
                original.update({group + "/" + key: item
                                 for key, item in GenericVariant.from_arrow_struct(
                                     row[group]).to_python().items()})
        result.append(original)
    return sorted(result, key=lambda row: row["episode_index"])


def _write_dataset(stage, reader, source, info, episodes, version, batch_size):
    video_keys = [name for name, feature in info["features"].items()
                  if feature.get("dtype") == "video"]
    image_keys = [name for name, feature in info["features"].items()
                  if feature.get("dtype") == "image"]
    frame_columns = list(info["features"])
    columns = ["dataset_id"] + frame_columns
    physical_schema = _target_schema(reader._table)
    frame_schema = pa.schema([
        pa.field(name, pa.struct([
            pa.field("bytes", pa.binary()), pa.field("path", pa.string())]))
        if name in image_keys else physical_schema.field(name)
        for name in frame_columns if name not in video_keys
    ])
    (stage / "meta").mkdir()
    source_identity = {key: value for key, value in source.items()
                       if key not in ("info", "tasks", "subtasks", "stats")}
    (stage / "meta/paimon_source.json").write_text(
        json.dumps(source_identity, ensure_ascii=False), encoding="utf-8")
    tasks = sorted(source["tasks"], key=lambda row: row["task_index"])
    subtasks = source["subtasks"]
    if version == "v3.0":
        _write_v3_vocabulary(stage, "task", tasks)
        if subtasks is not None:
            _write_v3_vocabulary(stage, "subtask", subtasks)
    else:
        _write_jsonl(stage / "meta/tasks.jsonl", tasks)
        if subtasks is not None:
            _write_jsonl(stage / "meta/subtasks.jsonl", subtasks)
    if source["stats"] is not None:
        (stage / "meta/stats.json").write_text(source["stats"], encoding="utf-8")

    output_info = copy.deepcopy(info)
    if version.startswith("v2."):
        if version == "v2.0" and source["stats"] is None:
            raise ValueError("LeRobot v2.0 export requires source-level statistics.")
        for key in video_keys:
            feature = info["features"][key]
            for details in (feature, feature.get("info"),
                            feature.get("video_info")):
                if isinstance(details, dict) and any(details.get(marker) for marker in (
                        "is_depth_map", "video.is_depth_map", "has_audio",
                        "video.has_audio")):
                    raise ValueError(
                        "LeRobot v2 export supports RGB video without audio; "
                        "%s has depth or audio." % key)
        output_info.update(
            codebase_version=version, chunks_size=1000,
            total_chunks=(len(episodes) + 999) // 1000,
            total_videos=len(episodes) * len(video_keys),
            data_path=_V2_DATA_PATH,
            video_path=_V2_VIDEO_PATH if video_keys else None)
        for key in video_keys:
            feature = output_info["features"][key]
            for name in ("info", "video_info"):
                details = feature.get(name)
                if not isinstance(details, dict):
                    continue
                details.update({"video.codec": "h264",
                                "video.fps": info["fps"],
                                "video.pix_fmt": "yuv420p",
                                "has_audio": False})
                for stale in ("video.gop_size", "video.bit_rate",
                              "video.extra_options", "video.video_backend"):
                    details.pop(stale, None)
                if "video.has_audio" in details:
                    details["video.has_audio"] = False
                if "video.encoder" in details:
                    details["video.encoder"] = "libx264"
                if "video.crf" in details:
                    details["video.crf"] = 18
    else:
        output_info.setdefault("robot_type", None)
        output_info.setdefault("chunks_size", 1000)
        output_info.setdefault("data_files_size_in_mb", 100)
        output_info.setdefault("video_files_size_in_mb", 200)
        output_info.setdefault("data_path", _V3_DATA_PATH)
        output_info.setdefault("video_path", _V3_VIDEO_PATH if video_keys else None)
        for episode in episodes:
            episode.setdefault("data/chunk_index", episode["episode_index"] // 1000)
            episode.setdefault("data/file_index", 0)
            episode.setdefault("meta/episodes/chunk_index", episode["episode_index"] // 1000)
            episode.setdefault("meta/episodes/file_index", 0)
    parquet_writer = None
    current_data_file = None
    written_data_files = set()
    v3_video_files = {}
    episode_stats = []
    source_video_cache = stage / ".source-videos"
    try:
        for episode in episodes:
            episode_index = episode["episode_index"]
            begin, end = episode["dataset_from_index"], episode["dataset_to_index"]
            if version.startswith("v2."):
                data_path = stage / _V2_DATA_PATH.format(
                    episode_chunk=episode_index // 1000,
                    episode_index=episode_index)
            else:
                data_path = _safe_path(stage, output_info["data_path"].format(
                    chunk_index=episode["data/chunk_index"],
                    file_index=episode["data/file_index"]))
            data_path.parent.mkdir(parents=True, exist_ok=True)
            if data_path != current_data_file:
                if parquet_writer is not None:
                    parquet_writer.close()
                if data_path in written_data_files:
                    raise ValueError("LeRobot source data files are not Episode ordered.")
                written_data_files.add(data_path)
                current_data_file = data_path
                parquet_writer = None
            video_frames = {key: [] for key in video_keys}
            for offset in range(begin, end, batch_size):
                indices = list(range(offset, min(offset + batch_size, end)))
                table = reader.read_keys(
                    [(source["id"], index) for index in indices], columns)
                if table.num_rows != len(indices):
                    raise ValueError("LeRobot source Frame range is incomplete.")
                table = table.sort_by("index")
                rows = table.to_pylist()
                if [row["index"] for row in rows] != indices \
                        or any(row["dataset_id"] != source["id"]
                               or row["episode_index"] != episode_index
                               for row in rows):
                    raise ValueError("LeRobot source Frame keys are inconsistent.")
                for row in rows:
                    del row["dataset_id"]
                    for key in video_keys:
                        video_frames[key].append(
                            VideoFrameDescriptor.deserialize(row.pop(key)))
                    for key in image_keys:
                        row[key] = {"bytes": _blob_bytes(reader.file_io, row[key]),
                                    "path": None}
                batch = pa.Table.from_pylist(rows, schema=frame_schema)
                if parquet_writer is None:
                    parquet_writer = pq.ParquetWriter(data_path, batch.schema)
                parquet_writer.write_table(batch)

            for key in video_keys:
                descriptors = video_frames[key]
                if not descriptors or any(
                        item.payload_descriptor != descriptors[0].payload_descriptor
                        for item in descriptors):
                    raise ValueError("LeRobot Episode video payload changes within an Episode.")
                if version.startswith("v2."):
                    video_path = _safe_path(stage, _V2_VIDEO_PATH.format(
                        episode_chunk=episode_index // 1000,
                        episode_index=episode_index, video_key=key))
                    payload = descriptors[0].payload_descriptor
                    cached = source_video_cache / hashlib.sha256(
                        (payload.uri + "\0" + str(payload.offset) + "\0" +
                         str(payload.length)).encode("utf-8")).hexdigest()
                    if not cached.exists():
                        _copy_video(reader.file_io, payload, cached)
                    _encode_episode_video(cached, video_path, descriptors,
                                          info["fps"])
                else:
                    payload = descriptors[0].payload_descriptor
                    identity = (payload.uri, payload.offset, payload.length)
                    key_files = v3_video_files.setdefault(key, {})
                    if identity not in key_files:
                        chunk_index, file_index = divmod(
                            len(key_files), output_info["chunks_size"])
                        video_path = _safe_path(stage, output_info["video_path"].format(
                            video_key=key, chunk_index=chunk_index,
                            file_index=file_index))
                        _copy_video(reader.file_io, payload, video_path)
                        key_files[identity] = (chunk_index, file_index, video_path)
                    chunk_index, file_index, video_path = key_files[identity]
                    prefix = "videos/%s/" % key
                    episode[prefix + "chunk_index"] = chunk_index
                    episode[prefix + "file_index"] = file_index
                    if reader._table.options.options.get(
                            CoreOptions.WRITE_LEROBOT_REMUX_EPISODES):
                        start = _video_frame_time(video_path, descriptors[0].frame_index)
                        episode[prefix + "from_timestamp"] = start
                        episode[prefix + "to_timestamp"] = start + len(descriptors) / info["fps"]
            if version.startswith("v2."):
                episode_stats.append({"episode_index": episode_index,
                                      "stats": _episode_stats(episode)})
        if parquet_writer is not None:
            parquet_writer.close()
            parquet_writer = None
        if version == "v3.0":
            _write_v3_episodes(stage, episodes)
            if "total_videos" in output_info:
                output_info["total_videos"] = sum(
                    len(files) for files in v3_video_files.values())
        else:
            _write_jsonl(stage / "meta/episodes.jsonl", [
                {key: episode[key] for key in ("episode_index", "tasks", "length")}
                for episode in episodes])
            if version == "v2.1":
                _write_jsonl(stage / "meta/episodes_stats.jsonl", episode_stats)
        (stage / "meta/info.json").write_text(
            json.dumps(output_info, ensure_ascii=False), encoding="utf-8")
    finally:
        if parquet_writer is not None:
            parquet_writer.close()
        if source_video_cache.exists():
            shutil.rmtree(source_video_cache)


def _write_v3_vocabulary(stage, singular, rows):
    import pandas as pd

    index = singular + "_index"
    frame = pd.DataFrame(rows, columns=[index, singular]).set_index(singular)
    pq.write_table(pa.Table.from_pandas(frame),
                   stage / ("meta/%ss.parquet" % singular))


def _write_v3_episodes(stage, episodes):
    if not episodes:
        return
    names = list(dict.fromkeys(key for episode in episodes for key in episode))

    def chunk_rows(begin):
        rows = []
        for episode in episodes[begin:begin + 1000]:
            row = {key: episode.get(key) for key in names}
            if "meta/episodes/chunk_index" in row:
                row["meta/episodes/chunk_index"] = begin // 1000
            if "meta/episodes/file_index" in row:
                row["meta/episodes/file_index"] = 0
            rows.append(row)
        return rows

    schema = pa.unify_schemas([
        pa.Table.from_pylist(chunk_rows(begin)).schema
        for begin in range(0, len(episodes), 1000)
    ])
    for begin in range(0, len(episodes), 1000):
        path = stage / ("meta/episodes/chunk-%03d/file-000.parquet" %
                        (begin // 1000))
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(chunk_rows(begin), schema=schema),
                       path)


def _safe_path(root, relative):
    root = root.resolve()
    path = (root / relative).resolve()
    try:
        path.relative_to(root)
    except ValueError:
        raise ValueError(
            "LeRobot metadata path escapes the output directory.") from None
    return path


def _episode_stats(episode):
    stats = {}
    for key, value in episode.items():
        if key.startswith("stats/"):
            feature, name = key[6:].rsplit("/", 1)
            stats.setdefault(feature, {})[name] = value
    return stats


def _write_jsonl(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as stream:
        for row in rows:
            stream.write(json.dumps(row, ensure_ascii=False) + "\n")


def _copy_video(file_io, descriptor, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    reader = file_io.uri_reader_factory.create(descriptor.uri)
    with Blob.from_descriptor(reader, descriptor).new_input_stream() as source, \
            path.open("wb") as target:
        shutil.copyfileobj(source, target, length=1024 * 1024)


def _blob_bytes(file_io, value):
    with Blob.from_bytes(value, file_io=file_io).new_input_stream() as source:
        return source.read()


def _video_frame_time(path, index):
    import av

    with av.open(str(path)) as container:
        for ordinal, frame in enumerate(container.decode(video=0)):
            if ordinal == index:
                return float(frame.pts * frame.time_base)
    raise ValueError("LeRobot video descriptor points beyond the MP4 frames.")


def _encode_episode_video(source_path, output_path, descriptors, fps):
    try:
        import av
    except ImportError as error:
        raise ImportError("LeRobot v2 video export requires PyAV.") from error

    wanted = [item.frame_index for item in descriptors]
    if wanted != sorted(wanted):
        raise ValueError("LeRobot Episode video frames are not ordered.")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    position = 0
    with av.open(str(source_path)) as source, \
            av.open(str(output_path), mode="w", format="mp4") as target:
        video = source.streams.video[0]
        stream = target.add_stream(
            "libx264", rate=Fraction(str(fps)).limit_denominator(1000000))
        stream.width, stream.height = video.width, video.height
        stream.pix_fmt = "yuv420p"
        stream.options = {"crf": "18", "threads": "4"}
        for ordinal, frame in enumerate(source.decode(video=0)):
            while position < len(wanted) and wanted[position] == ordinal:
                encoded = av.VideoFrame.from_ndarray(
                    frame.to_ndarray(format="rgb24"), format="rgb24")
                encoded.pts = position
                for packet in stream.encode(encoded):
                    target.mux(packet)
                position += 1
            if position == len(wanted):
                break
        if position != len(wanted):
            raise ValueError("LeRobot video descriptor points beyond the MP4 frames.")
        for packet in stream.encode():
            target.mux(packet)
