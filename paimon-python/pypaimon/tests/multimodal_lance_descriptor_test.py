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

"""Lance materialization respects known descriptor storage provenance."""

import shutil

import pyarrow as pa
import pytest

from pypaimon.table.row.blob import BlobDescriptor, VideoFrameDescriptor
from pypaimon.tests import multimodal_lance_test as lance_tests

conn = lance_tests.conn
lance = lance_tests.lance


def _descriptor(path, version, offset, length, padding=b""):
    encoded = BlobDescriptor(str(path), offset, length).serialize()
    if version < 2:
        encoded = bytes([version]) + encoded[9:]
    return encoded + padding


@pytest.mark.parametrize("version", [0, 1, 2])
@pytest.mark.parametrize("padding", [b"", b"padding"])
@pytest.mark.parametrize("option", ["blob-descriptor-field", "blob.stored-descriptor-fields"])
def test_export_lance_materializes_known_descriptor_wire_versions(conn, tmp_path, version, padding, option):
    external = tmp_path / "external.bin"
    external.write_bytes(b"0123456789")
    table = lance_tests._table(conn, "descriptors", {"id": pa.int32(), "payload": pa.large_binary()}, [
        {"id": 1, "payload": _descriptor(external, version, 2, 5, padding)},
        {"id": 2, "payload": None},
        {"id": 3, "payload": _descriptor(external, version, 7, 0, padding)},
    ], options={option: "payload"})
    expected = [{"payload": b"23456", "id": 1}, {"payload": None, "id": 2}, {"payload": b"", "id": 3}]
    assert table.copy({"blob-as-descriptor": "false"}).scan().select(["payload", "id"]).to_list() == expected
    path = tmp_path / "descriptors.lance"

    table.scan().where("id <= 3").select(["payload", "id"]).export_lance(str(path))
    external.unlink()
    shutil.rmtree(str(tmp_path / "warehouse"))

    assert lance.dataset(str(path)).schema.names == ["payload", "id"]
    assert lance_tests._rows(path) == expected


def test_export_lance_materializes_descriptor_field_with_colliding_projection_alias(conn, tmp_path):
    external = tmp_path / "external.bin"
    external.write_bytes(b"prefix-body-suffix")
    table = lance_tests._table(conn, "descriptor_alias", {
        "id": pa.int32(), "media": pa.struct([("payload", pa.string())]),
        "media_payload": pa.large_binary(),
    }, [{"id": 1, "media": {"payload": "ordinary"}, "media_payload": _descriptor(external, 1, 7, 4)}],
        options={"blob-descriptor-field": "media_payload"})
    columns = ["media.payload", "media_payload", "id"]
    expected = [{"media_payload": "ordinary", "media_payload__0": b"body", "id": 1}]
    assert table.copy({"blob-as-descriptor": "false"}).scan().select(columns).to_list() == expected
    path = tmp_path / "descriptor_alias.lance"

    table.scan().select(columns).export_lance(str(path))
    external.unlink()

    assert lance_tests._rows(path) == expected


@pytest.mark.parametrize("version", [0, 1])
@pytest.mark.parametrize("padding", [b"", b"padding"])
def test_export_lance_keeps_legacy_descriptor_shaped_managed_payloads_raw(conn, tmp_path, version, padding):
    raw = _descriptor(tmp_path / "must-not-be-read.bin", version, 2, 5, padding)
    table = lance_tests._table(conn, "raw_payloads", {"id": pa.int32(), "payload": pa.large_binary()}, [
        {"id": 1, "payload": raw}, {"id": 2, "payload": None}, {"id": 3, "payload": b""},
    ])
    path = tmp_path / "raw_payloads.lance"

    table.scan().export_lance(str(path))
    shutil.rmtree(str(tmp_path / "warehouse"))

    assert lance_tests._rows(path) == [
        {"id": 1, "payload": raw}, {"id": 2, "payload": None}, {"id": 3, "payload": b""},
    ]


@pytest.mark.parametrize("mode", ["create", "overwrite"])
def test_export_lance_unresolvable_legacy_descriptor_does_not_publish(conn, tmp_path, mode):
    table = lance_tests._table(conn, "missing_reference", {"id": pa.int32(), "payload": pa.large_binary()}, [
        {"id": 1, "payload": _descriptor(tmp_path / "missing.bin", 1, 2, 5)},
    ], options={"blob-descriptor-field": "payload"})
    path = tmp_path / "missing_reference.lance"
    if mode == "overwrite":
        lance.write_dataset(pa.table({"id": [99]}), str(path))

    with pytest.raises((OSError, RuntimeError)):
        table.scan().export_lance(str(path), mode=mode)

    if mode == "overwrite":
        assert lance.dataset(str(path)).to_table().to_pylist() == [{"id": 99}]
    else:
        with pytest.raises(ValueError):
            lance.dataset(str(path))


@pytest.mark.parametrize("mode", ["create", "overwrite"])
def test_export_lance_rejects_video_frame_in_ordinary_descriptor_field(conn, tmp_path, mode):
    video = tmp_path / "video.bin"
    video.write_bytes(b"encoded-video")
    descriptor = VideoFrameDescriptor(str(video), 0, video.stat().st_size, 2, -1, 0)
    # Seed the invalid legacy value through Python; Native rejects it before export.
    table = lance_tests._table(conn, "frame_reference", {"id": pa.int32(), "payload": pa.large_binary()}, [
        {"id": 1, "payload": descriptor},
    ], options={"blob-descriptor-field": "payload", "write.native.enabled": "false"})
    path = tmp_path / "frame_reference.lance"
    if mode == "overwrite":
        lance.write_dataset(pa.table({"id": [99]}), str(path))

    with pytest.raises((ValueError, OSError), match="(?i)video"):
        table.scan().export_lance(str(path), mode=mode)

    if mode == "overwrite":
        assert lance.dataset(str(path)).to_table().to_pylist() == [{"id": 99}]
    else:
        with pytest.raises(ValueError):
            lance.dataset(str(path))
