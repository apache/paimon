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

"""Lance exports preserve authorized values after masking changes Arrow types."""

import json
import os
import subprocess
import sys
import textwrap
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon.catalog.table_query_auth import TableQueryAuthResult
from pypaimon.table.row.blob import BlobDescriptor, VideoFrameDescriptor
from pypaimon.tests import multimodal_lance_test as lance_tests

pytest.importorskip("lance", minversion="13.0.0", reason="pylance >= 13.0.0 is required for Lance exports")
conn = lance_tests.conn

_AUTH_EXPORT_PROBE = textwrap.dedent("""
    import json
    import pathlib
    import sys

    import lance
    import pyarrow as pa

    import pypaimon.multimodal as pmm
    from pypaimon.catalog.table_query_auth import TableQueryAuthResult
    from pypaimon.table.row.blob import BlobDescriptor

    root, kind, mode = pathlib.Path(sys.argv[1]), sys.argv[2], sys.argv[3]
    conn = pmm.connect(options={"warehouse": str(root / "warehouse")})
    source = root / "external.bin"
    source.write_bytes(b"secret")
    blob = kind in ("blob", "copy_blob", "copy_legacy_blob", "copy_binary")
    source_is_blob = kind in ("copy_blob", "copy_legacy_blob")
    value_type = pa.large_binary() if blob else pa.string()
    source_type = (pa.large_binary() if source_is_blob else
                   (pa.binary() if kind == "copy_binary" else pa.string()))
    initial = (b"original" if kind == "copy_legacy_blob" else
               (BlobDescriptor(str(source), 0, 6).serialize() if blob else "original"))
    options = {"file.format": "parquet", "read.batch-size": "1", "query-auth.enabled": "true"}
    if blob:
        options["blob-descriptor-field"] = "value"
    if kind == "copy_legacy_blob":
        hidden = root / "hidden.bin"
        hidden.write_bytes(b"masked")
        wire = BlobDescriptor(str(hidden), 0, 6).serialize()
        source_value = bytes([1]) + wire[9:]
        options["blob-descriptor-field"] = "source"
    elif kind == "copy_binary":
        source_value = BlobDescriptor(str(root / "must-not-read.bin"), 0, 5).serialize()
    elif source_is_blob:
        source_value = b"masked"
    else:
        source_value = "not-int" if kind == "failure" else "masked"
    table = conn.create_table(
        "masked", schema=pa.schema([("id", pa.int32()), ("value", value_type), ("source", source_type)]),
        data=[{"id": 1, "value": initial, "source": source_value},
              {"id": 2, "value": initial, "source": source_value}],
        options=options)
    field = {"index": 2, "name": "source", "type": "BLOB" if source_is_blob else
             ("BYTES" if kind == "copy_binary" else "STRING")}
    if blob:
        mask = ({"name": "FIELD_REF", "fieldRef": field} if kind.startswith("copy_") else
                {"name": "CONCAT", "inputs": [field]})
        expected = [{"value": source_value if kind == "copy_binary" else b"masked"}]
    elif kind == "string":
        mask = {"name": "LENGTH", "inputs": [field]}
        expected = [{"value": "6"}]
    else:
        mask = {"name": "CAST", "fieldRef": field, "type": "INT"}
    row_filter = json.dumps({
        "kind": "LEAF", "function": "EQUAL", "literals": [1],
        "transform": {"name": "FIELD_REF", "fieldRef": {"index": 0, "name": "id", "type": "INT"}},
    })
    auth = TableQueryAuthResult([row_filter], {"value": json.dumps(mask)})
    table.raw_table.catalog_environment.table_query_auth = lambda options, identifier: lambda select: auth
    query = table.scan().select("value")
    path = root / "masked.lance"
    if mode == "overwrite":
        lance.write_dataset(pa.table({"id": [99]}), str(path))
    if kind == "failure":
        for read in (query.to_arrow, lambda: query.export_lance(str(path), mode=mode)):
            try:
                read()
            except Exception as error:
                assert "not-int" in str(error), str(error)
            else:
                raise AssertionError("The authorization mask must fail")
        if mode == "overwrite":
            assert lance.dataset(str(path)).to_table().to_pylist() == [{"id": 99}]
        else:
            try:
                lance.dataset(str(path))
            except ValueError:
                pass
            else:
                raise AssertionError("The failed export published a dataset")
    else:
        logical = table.copy({"blob-as-descriptor": "false"}).scan().select("value") if blob else query
        assert logical.to_list() == expected
        exported = query.export_lance(str(path), mode=mode)
        if blob:
            with exported.take_blobs("value", indices=[0])[0] as handle:
                actual = [{"value": handle.read()}]
        else:
            actual = exported.to_table().to_pylist()
        assert actual == expected, actual
    print("authorized export verified")
""")


def _run_auth_export_probe(tmp_path, kind, mode="create"):
    # Keep Arrow schema/array mismatches in a subprocess: affected versions
    # can abort while consuming the stream instead of raising a Python error.
    environment = dict(os.environ)
    environment["PYTHONPATH"] = os.pathsep.join(path for path in sys.path if path)
    result = subprocess.run(
        [sys.executable, "-u", "-c", _AUTH_EXPORT_PROBE, str(tmp_path), kind, mode],
        env=environment, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "authorized export verified" in result.stdout


@pytest.mark.parametrize("kind", ["string", "blob", "copy_blob", "copy_legacy_blob", "copy_binary"])
def test_export_lance_preserves_authorized_mask_values(tmp_path, kind):
    _run_auth_export_probe(tmp_path, kind)


@pytest.mark.parametrize("mode", ["create", "overwrite"])
def test_export_lance_auth_mask_failure_does_not_publish(tmp_path, mode):
    _run_auth_export_probe(tmp_path, "failure", mode)


@pytest.mark.parametrize("mode", ["create", "overwrite"])
def test_export_lance_rejects_computed_blob_masks_before_writing(conn, tmp_path, mode):
    external = tmp_path / "external.bin"
    external.write_bytes(b"body")
    descriptor = BlobDescriptor(str(external), 0, 4).serialize()
    table = lance_tests._table(conn, "computed_mask", {"id": pa.int32(), "payload": pa.large_binary()}, [
        {"id": 1, "payload": descriptor},
    ], options={"blob-descriptor-field": "payload"})
    auth = TableQueryAuthResult(None, {"payload": json.dumps({
        "name": "CAST", "fieldRef": {"index": 1, "name": "payload", "type": "BYTES"}, "type": "BYTES",
    })})
    table.raw_table.catalog_environment.table_query_auth = lambda options, identifier: lambda select: auth
    query = table.scan()
    assert query.to_list() == [{"id": 1, "payload": descriptor}]
    path = tmp_path / "computed_mask.lance"
    if mode == "overwrite":
        lance_tests.lance.write_dataset(pa.table({"id": [99]}), str(path))

    with patch.object(lance_tests.lance, "write_dataset", side_effect=AssertionError("must reject before writing")):
        with pytest.raises(ValueError, match="(?i)mask"):
            query.export_lance(str(path), mode=mode)

    if mode == "overwrite":
        assert lance_tests._rows(path) == [{"id": 99}]
    else:
        with pytest.raises(ValueError):
            lance_tests.lance.dataset(str(path))


@pytest.mark.parametrize("descriptor_mode", ["true", "false"])
@pytest.mark.parametrize("mode", ["create", "overwrite"])
def test_export_lance_rejects_hidden_video_mask_source_before_writing(conn, tmp_path, descriptor_mode, mode):
    video = tmp_path / "video.bin"
    video.write_bytes(b"encoded-video")
    frame = VideoFrameDescriptor(str(video), 0, video.stat().st_size, 3, -1, 0)
    table = lance_tests._table(conn, "hidden_video", {
        "id": pa.int32(), "out": pa.large_binary(), "frame": pa.large_binary(),
    }, [{"id": 1, "out": b"ordinary", "frame": frame}], options={"video-frame-field": "frame"})
    auth = TableQueryAuthResult(None, {"out": json.dumps({
        "name": "FIELD_REF", "fieldRef": {"index": 2, "name": "frame", "type": "BLOB"},
    })})
    table.raw_table.catalog_environment.table_query_auth = lambda options, identifier: lambda select: auth
    query = table.copy({"blob-as-descriptor": descriptor_mode}).scan().select(["out", "id"])
    path = tmp_path / "hidden_video.lance"
    if mode == "overwrite":
        lance_tests.lance.write_dataset(pa.table({"id": [99]}), str(path))

    with patch.object(lance_tests.lance, "write_dataset", side_effect=AssertionError("must reject before writing")):
        with pytest.raises(ValueError, match="(?i)video"):
            query.export_lance(str(path), mode=mode)

    if mode == "overwrite":
        assert lance_tests._rows(path) == [{"id": 99}]
    else:
        with pytest.raises(ValueError):
            lance_tests.lance.dataset(str(path))


@pytest.mark.parametrize("descriptor_mode", ["true", "false"])
def test_export_lance_allows_masks_from_row_tracking_fields(conn, tmp_path, descriptor_mode):
    table = lance_tests._table(conn, "row_id_mask", {"id": pa.int32(), "payload": pa.large_binary()}, [
        {"id": 1, "payload": b"original"},
    ])
    auth = TableQueryAuthResult(None, {"payload": json.dumps({
        "name": "CAST", "fieldRef": {"index": 2, "name": "_ROW_ID", "type": "BIGINT"}, "type": "STRING",
    })})
    table.raw_table.catalog_environment.table_query_auth = lambda options, identifier: lambda select: auth
    query = table.copy({"blob-as-descriptor": descriptor_mode}).scan().select("payload").with_row_id()
    expected = [{"payload": b"0", "_ROW_ID": 0}]
    assert query.to_list() == expected
    path = tmp_path / "row_id_mask.lance"

    query.export_lance(str(path))

    assert lance_tests._rows(path) == expected
