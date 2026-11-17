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

import io
import sys
from types import SimpleNamespace
from unittest.mock import Mock, patch
from urllib.parse import unquote, urlparse

import pyarrow.fs as pafs
import pytest

from pypaimon.multimodal.lerobot.source import _LeRobotSource
from pypaimon.multimodal.lerobot.text_fields import _read_text_fields


pytestmark = pytest.mark.skipif(sys.version_info < (3, 10), reason="LeRobot import requires Python 3.10+")


class MemoryFileIO:
    def __init__(self, root):
        self.root = root

    def path(self, uri):
        return self.root / unquote(urlparse(uri).path).removeprefix("/dataset/")

    def list_status(self, uri):
        path = self.path(uri + "/" if uri.endswith("/dataset") else uri)
        return [pafs.FileInfo("memory://bucket/dataset/" + child.relative_to(self.root).as_posix()
                              + ("/" if child.is_dir() else ""),
                              pafs.FileType.Directory if child.is_dir() else pafs.FileType.File)
                for child in path.iterdir()]

    def new_input_stream(self, uri):
        return io.BytesIO(self.path(uri).read_bytes())


@pytest.mark.parametrize("backend", ["local", "fileio", "hub"])
def test_text_file_globs_keep_directory_boundaries(tmp_path, backend):
    files = {"config/model.json": '{ "value": 1 }\r\n',
             "config/nested/model.json": "nested\n", "config/model a.json": "spaces\n",
             "other/config/model.json": "other\n", "notes.md": "# Notes\n"}
    for name, text in files.items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(text.encode("utf-8"))
    source = _LeRobotSource(str(tmp_path), tmp_path, "")
    if backend == "fileio":
        source = _LeRobotSource("memory://bucket/dataset", None, "", MemoryFileIO(tmp_path))
    elif backend == "hub":
        source = _LeRobotSource("example/dataset", None, "example/dataset")
    listing = Mock(return_value=list(files))
    download = Mock(side_effect=lambda repo, name, **kw: str(tmp_path / name))
    with patch.dict(sys.modules, {
        "huggingface_hub": SimpleNamespace(list_repo_files=listing, hf_hub_download=download),
        "huggingface_hub.errors": SimpleNamespace(EntryNotFoundError=FileNotFoundError),
    }):
        actual = _read_text_fields(source, {"config": "config/*.json", "notes": "notes.md"})
        assert actual == {"config": {"model": files["config/model.json"],
                                     "model a": files["config/model a.json"]},
                          "notes": files["notes.md"]}
        recursive = _read_text_fields(source, {"config": "config/**/*.json"})
        assert recursive["config"]["nested/model"] == "nested\n"
        assert len(recursive["config"]) == 3
        single = _read_text_fields(source, {"config": "config/model.json*"})
        assert single == {"config": {"model": files["config/model.json"]}}
        assert _read_text_fields(source, {"notes": "missing.md", "config": "missing/*.json"}) == {
            "notes": None, "config": None}
        if backend == "hub":
            assert listing.call_args.kwargs == {"repo_type": "dataset"}
            assert download.call_count == 8
        else:
            listing.assert_not_called()
            download.assert_not_called()


def test_extra_text_symlink_must_stay_within_source(tmp_path):
    root = tmp_path / "source"
    root.mkdir()
    outside = tmp_path / "outside.txt"
    outside.write_text("outside")
    (root / "notes.txt").symlink_to(outside)
    source = _LeRobotSource(str(root), root, "")
    with pytest.raises(ValueError, match="within the source directory"):
        _read_text_fields(source, {"notes": "notes.txt"})
