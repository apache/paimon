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

"""Helpers for descriptor-backed video frame rows."""

import io
import os
from collections import OrderedDict, deque
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor, wait
from contextlib import contextmanager
from fractions import Fraction
from itertools import chain

from pypaimon.table.row.blob import Blob, VideoFrameDescriptor
from pypaimon.table.row.video_keyframe_index import VideoKeyframeIndex


def _require_video_av():
    """Check the optional decoder before opening a video source."""
    try:
        import av
    except ImportError as error:
        raise ImportError(
            "Automatic video import requires PyAV; install pypaimon[video]."
        ) from error
    return av


def _raw_video_time_base(fps):
    from numbers import Real

    if isinstance(fps, bool) or not isinstance(fps, Real):
        raise ValueError("source_fps must be a positive finite number.")
    try:
        rate = Fraction(str(fps))
        if rate <= 0 or max(rate.numerator, rate.denominator) > 2147483647:
            raise ValueError()
    except (ValueError, OverflowError):
        raise ValueError("source_fps must be a positive finite representable frame rate.")
    return 1 / rate


def _remux_raw_hevc(source, output_path, time_base):
    """Preserve HEVC packets and assign constant-rate presentation timestamps."""
    from array import array
    import shutil
    import tempfile

    av = _require_video_av()
    # Snapshot to disk for both passes.
    with tempfile.TemporaryFile() as raw:
        shutil.copyfileobj(source, raw, length=1024 * 1024)
        raw.seek(0)
        ordinals = array("q")
        frame_count = 0
        delay = 0
        with av.open(raw) as container:
            if container.format.name != "hevc" or len(container.streams) != 1:
                raise ValueError("source_fps is only supported for a raw H.265/HEVC stream.")
            for packet in container.demux(video=0):
                if packet.size:
                    # Track packet IDs through B-frame reordering.
                    packet.pts = len(ordinals)
                    packet.dts = None
                    packet.time_base = time_base
                    ordinals.append(-1)
                for frame in packet.decode():
                    identity = frame.pts
                    if frame.is_corrupt or identity is None or not 0 <= identity < len(ordinals) \
                            or ordinals[identity] != -1:
                        raise ValueError("Cannot determine raw HEVC frame order.")
                    ordinals[identity] = frame_count
                    delay = max(delay, identity - frame_count)
                    frame_count += 1
        if not frame_count or frame_count != len(ordinals):
            raise ValueError("Raw HEVC must contain one decodable frame per packet.")
        raw.seek(0)
        with av.open(raw) as container, av.open(output_path, "w", format="mp4") as output:
            if hasattr(output, "add_stream_from_template"):
                stream = output.add_stream_from_template(container.streams.video[0], opaque=True)
            else:
                stream = output.add_stream(template=container.streams.video[0])
            stream.time_base = time_base
            count = 0
            for packet in container.demux(video=0):
                if not packet.size:
                    continue
                packet.pts = ordinals[count]
                packet.dts = count - delay
                packet.duration = 1
                packet.time_base = time_base
                packet.stream = stream
                output.mux(packet)
                count += 1
            if count != frame_count:
                raise ValueError("Raw HEVC packet count changed during remux.")
        return frame_count


def _inspect_video_frames(source):
    """Read presentation timestamps without retaining decoded images."""
    av = _require_video_av()
    from array import array

    timestamps = array("d")
    first_time = None
    previous_time = None
    with av.open(source) as container:
        if len(container.streams.video) != 1:
            raise ValueError("Automatic video import requires one video stream.")
        for frame in container.decode(container.streams.video[0]):
            if frame.is_corrupt:
                raise ValueError("Video contains a corrupt frame.")
            if frame.pts is None or frame.time_base is None:
                raise ValueError("Video frame is missing a presentation timestamp.")
            current_time = frame.pts * frame.time_base
            if previous_time is not None and current_time <= previous_time:
                raise ValueError("Video presentation timestamps must be strictly increasing.")
            if first_time is None:
                first_time = current_time
            timestamps.append(float(current_time - first_time))
            previous_time = current_time
    if not timestamps:
        raise ValueError("Video contains no decodable frames.")
    return timestamps


class _VideoDecodePool:
    """One lazily created decode pool per dataset and worker process."""

    def __init__(self):
        self._executor = None
        self._parallelism = 0
        self._pid = os.getpid()

    def get(self, parallelism):
        if self._pid != os.getpid():
            # Never join threads inherited from the parent process.
            self._executor = None
            self._pid = os.getpid()
        if self._executor is not None and self._parallelism != parallelism:
            self.close()
        if parallelism > 1 and self._executor is None:
            self._executor = ThreadPoolExecutor(max_workers=parallelism)
        self._parallelism = parallelism
        return self._executor

    def close(self):
        executor, self._executor = self._executor, None
        if executor is not None and self._pid == os.getpid():
            executor.shutdown(wait=True)

    def __getstate__(self):
        return {"_executor": None, "_parallelism": 0, "_pid": None}


def _encode_images_to_video(
        images,
        output_path,
        *,
        fps,
        codec,
        pixel_format,
        gop_size,
        codec_options=None):
    if isinstance(fps, bool) or not isinstance(fps, int) or fps <= 0:
        raise ValueError("fps must be a positive int.")
    if not isinstance(codec, str) or not codec:
        raise ValueError("codec must be a non-empty string.")
    if not isinstance(pixel_format, str) or not pixel_format:
        raise ValueError("pixel_format must be a non-empty string.")
    if isinstance(gop_size, bool) \
            or not isinstance(gop_size, int) or gop_size <= 0:
        raise ValueError("gop_size must be a positive int.")
    if codec_options is not None and not isinstance(codec_options, Mapping):
        raise ValueError("codec_options must be a mapping or None.")

    try:
        import av
        from PIL import Image
    except ImportError as error:
        raise ImportError(
            "Image-to-video encoding requires PyAV and Pillow; install "
            "pypaimon[video]."
        ) from error

    iterator = iter(images)
    try:
        first = next(iterator)
    except StopIteration as error:
        raise ValueError("images must contain at least one frame.") from error

    def open_image(value):
        if isinstance(value, (bytes, bytearray, memoryview)):
            image = Image.open(io.BytesIO(bytes(value)))
        elif isinstance(value, Image.Image):
            image = value
        else:
            raise TypeError(
                "images must contain encoded image bytes or Pillow images."
            )
        image.load()
        return image.convert("RGB")

    first = open_image(first)
    width, height = first.size
    time_base = Fraction(1, fps)
    count = 0
    try:
        with av.open(output_path, mode="w", format="mp4") as container:
            stream = container.add_stream(
                codec,
                rate=fps,
                options={
                    str(key): str(value)
                    for key, value in (codec_options or {}).items()
                },
            )
            stream.width = width
            stream.height = height
            stream.pix_fmt = pixel_format
            stream.gop_size = gop_size
            stream.time_base = time_base
            decoded = chain(
                (first,), (open_image(value) for value in iterator)
            )
            for count, image in enumerate(decoded, start=1):
                if image.size != (width, height):
                    raise ValueError(
                        "All images must have the same dimensions."
                    )
                frame = av.VideoFrame.from_image(image)
                frame.pts = count - 1
                frame.time_base = time_base
                for packet in stream.encode(frame):
                    container.mux(packet)
            for packet in stream.encode():
                container.mux(packet)
    except BaseException:
        try:
            os.remove(output_path)
        except FileNotFoundError:
            pass
        raise
    return count


class VideoFrameCollator:
    """Decode frame rows in a DataLoader worker while reusing video sessions.

    ``decoder_factory`` receives a seekable stream containing exactly one
    descriptor-backed video. ``decode_fn`` receives the cached decoder, the
    frame ordinal embedded in the descriptor, and one row dictionary. This
    keeps Paimon independent of a particular video codec library while allowing
    PyAV, TorchCodec, or an application decoder to be plugged in.

    Batch inputs are grouped by physical video and decoded in ascending frame
    order to avoid unnecessary decoder seeks. Rows are restored to their input
    order before ``collate_fn`` is called.

    Optional ``decode_batch_fn`` replaces ``decode_fn`` for each payload group.
    It receives the cached decoder, sorted frame indices, and corresponding row
    dictionaries, and returns a sequence with one frame per row in that order.
    At least one of ``decode_fn`` or ``decode_batch_fn`` must be provided.

    The cache is process-local and keyed by physical video payload identity.
    ``collate_fn`` defaults to PyTorch's ``default_collate`` and may be replaced
    for decoders that already return batched objects.
    """

    def __init__(
            self,
            table,
            *,
            video_column,
            decoder_factory,
            decode_fn=None,
            output_column="frame",
            max_open_videos=16,
            collate_fn=None,
            decode_batch_fn=None,
            range_parallelism=4):
        if not video_column:
            raise ValueError("video_column is required.")
        if not callable(decoder_factory):
            raise ValueError("decoder_factory must be callable.")
        if decode_fn is not None and not callable(decode_fn):
            raise ValueError("decode_fn must be callable or None.")
        if decode_batch_fn is not None and not callable(decode_batch_fn):
            raise ValueError("decode_batch_fn must be callable or None.")
        if decode_fn is None and decode_batch_fn is None:
            raise ValueError(
                "At least one of decode_fn or decode_batch_fn is required.")
        if (
            isinstance(max_open_videos, bool)
            or not isinstance(max_open_videos, int)
            or max_open_videos <= 0
        ):
            raise ValueError("max_open_videos must be a positive int.")
        if collate_fn is not None and not callable(collate_fn):
            raise ValueError("collate_fn must be callable or None.")
        if (isinstance(range_parallelism, bool)
                or not isinstance(range_parallelism, int)
                or range_parallelism <= 0):
            raise ValueError("range_parallelism must be a positive int.")

        raw_table = getattr(table, "raw_table", table)
        file_io = getattr(raw_table, "file_io", None)
        if file_io is None:
            raise ValueError("table must provide raw_table.file_io or file_io.")

        self.file_io = file_io
        self.video_column = video_column
        self.decoder_factory = decoder_factory
        self.decode_fn = decode_fn
        self.decode_batch_fn = decode_batch_fn
        self.output_column = output_column
        self.max_open_videos = max_open_videos
        self.collate_fn = collate_fn
        self.range_parallelism = range_parallelism
        self._decoders = OrderedDict()
        self._owner_pid = os.getpid()

    def __call__(self, rows):
        self._ensure_process_local_cache()
        single_row = isinstance(rows, Mapping)
        input_rows = [rows] if single_row else list(rows)
        decoded_rows = self._decode_rows(input_rows)
        if single_row:
            return decoded_rows[0]
        return self._collate(decoded_rows)

    def close(self):
        resources = list(self._decoders.values())
        self._decoders.clear()
        first_error = None
        for resource in resources:
            try:
                self._close_resource(resource)
            except Exception as error:
                if first_error is None:
                    first_error = error
        if first_error is not None:
            raise first_error

    def __getstate__(self):
        # DataLoader spawn workers must never inherit non-picklable decoder or
        # stream state opened in the parent process.
        state = self.__dict__.copy()
        state["_decoders"] = OrderedDict()
        state["_owner_pid"] = None
        return state

    @contextmanager
    def _map_decoders(self, requests, decode, parallelism, executor=None):
        self._ensure_process_local_cache()
        limit = min(parallelism, self.max_open_videos, len(requests))
        if executor is None:
            yield (decode(self._decoder(*key), value)
                   for key, value in requests.items())
            return
        items = iter(requests.items())
        pending = deque()

        def submit():
            try:
                key, value = next(items)
            except StopIteration:
                return
            # Keep cache mutations ordered; pending decoders remain most recent.
            decoder = self._decoder(*key)
            pending.append(executor.submit(decode, decoder, value))

        def results():
            while pending:
                future = pending[0]
                value = future.result()
                pending.popleft()
                yield value
                del value, future
                submit()

        try:
            for _ in range(limit):
                submit()
            yield results()
        finally:
            for future in pending:
                future.cancel()
            # A failed or abandoned batch must finish before cache reuse/close.
            wait(pending)

    def _decode_rows(self, rows):
        decoded = [None] * len(rows)
        grouped = OrderedDict()
        for position, row in enumerate(rows):
            output, descriptor = self._prepare_row(row)
            if descriptor is None:
                decoded[position] = output
                continue
            payload = descriptor.payload_descriptor
            index = descriptor.keyframe_index_descriptor
            group = grouped.setdefault(payload, [index, []])
            if group[0] != index:
                raise ValueError(
                    "One video payload references different keyframe indexes."
                )
            group[1].append((descriptor.frame_index, position, output))

        for payload, (index, frames) in grouped.items():
            decoder = self._decoder(payload, index)
            frames.sort(key=lambda frame: frame[0])
            if self.decode_batch_fn is not None:
                values = self.decode_batch_fn(
                    decoder,
                    [index for index, _, _ in frames],
                    [output for _, _, output in frames],
                )
                if len(values) != len(frames):
                    raise ValueError(
                        "decode_batch_fn must return one frame per row.")
            else:
                values = (
                    self.decode_fn(decoder, index, output)
                    for index, _, output in frames
                )
            for (_, position, output), value in zip(frames, values):
                output[self.output_column] = value
                decoded[position] = output
        return decoded

    def _prepare_row(self, row):
        if not isinstance(row, Mapping):
            raise ValueError("VideoFrameCollator expects row dictionaries.")
        if self.video_column not in row:
            raise ValueError(
                "Video column %r is missing from the row." % self.video_column
            )

        raw = row[self.video_column]
        output = dict(row)
        if raw is None:
            output[self.output_column] = None
            return output, None
        if hasattr(raw, "as_py"):
            raw = raw.as_py()
        if not VideoFrameDescriptor.is_video_frame_descriptor(raw):
            raise ValueError(
                "Video column %r must contain serialized "
                "VideoFrameDescriptor bytes from a .video file."
                % self.video_column
            )

        serialized = bytes(raw)
        descriptor = VideoFrameDescriptor.deserialize(serialized)
        if descriptor.serialize() != serialized:
            raise ValueError(
                "Video column %r must contain one exact serialized "
                "VideoFrameDescriptor without trailing bytes."
                % self.video_column
            )
        return output, descriptor

    def _decoder(self, descriptor, keyframe_index_descriptor=None, replica=0):
        cache_key = (descriptor, keyframe_index_descriptor, replica)
        resource = self._decoders.pop(cache_key, None)
        if resource is not None:
            self._decoders[cache_key] = resource
            return resource[0]

        # Reuse the table's resolved FileIO. Rebuilding a reader from raw URI
        # options can drop merged REST/DLF storage credentials in workers.
        stream = Blob.from_file(
            self.file_io,
            descriptor.uri,
            descriptor.offset,
            descriptor.length,
        ).new_input_stream()
        try:
            if keyframe_index_descriptor is not None:
                mapping = self.file_io.read_file_range(
                    keyframe_index_descriptor.uri,
                    keyframe_index_descriptor.offset,
                    keyframe_index_descriptor.length,
                )
                stream.video_keyframe_index = (
                    VideoKeyframeIndex.deserialize(mapping, descriptor.length))
                stream.video_length = descriptor.length
                stream.video_read_ranges = lambda ranges: (
                    self.file_io.read_ranges_coalesced(
                        [(
                            descriptor.uri,
                            descriptor.offset + offset,
                            length,
                        ) for offset, length in ranges],
                        self.range_parallelism,
                    )
                )
            decoder = self.decoder_factory(stream)
        except Exception:
            stream.close()
            raise
        resource = (decoder, stream)
        self._decoders[cache_key] = resource
        if len(self._decoders) > self.max_open_videos:
            _, evicted = self._decoders.popitem(last=False)
            self._close_resource(evicted)
        return decoder

    def _collate(self, rows):
        if self.collate_fn is not None:
            return self.collate_fn(rows)
        try:
            from torch.utils.data import default_collate
        except ImportError as error:
            raise ImportError(
                "VideoFrameCollator requires PyTorch for its default collate "
                "function; install pypaimon[torch] or pass collate_fn=."
            ) from error
        return default_collate(rows)

    def _ensure_process_local_cache(self):
        pid = os.getpid()
        if self._owner_pid == pid:
            return
        # A forked worker owns duplicate descriptors for any inherited file
        # handles. Closing them here affects only the worker's copies.
        try:
            self.close()
        finally:
            self._owner_pid = pid

    @staticmethod
    def _close_resource(resource):
        decoder, stream = resource
        close = getattr(decoder, "close", None)
        try:
            if close is not None:
                close()
        finally:
            stream.close()
