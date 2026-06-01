################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import unittest
from dataclasses import dataclass
from typing import Optional

import pytest

pypaimon = pytest.importorskip("pypaimon")
daft = pytest.importorskip("daft")

from pypaimon.daft.daft_datasource import PaimonDataSource


@dataclass
class _DataFile:
    file_path: str
    external_path: Optional[str] = None


def _build_uri(warehouse_scheme: str, file_path: str) -> str:
    class _Stub:
        pass
    stub = _Stub()
    stub._warehouse_scheme = warehouse_scheme
    return PaimonDataSource._build_file_uri(stub, file_path)


class BuildFileUriTest(unittest.TestCase):

    def test_passes_through_when_path_already_has_scheme(self):
        cases = [
            ("",     "oss://bucket/db.db/tbl/data.parquet"),
            ("",     "s3://bucket/key.parquet"),
            ("",     "s3a://bucket/key.parquet"),
            ("",     "s3n://bucket/key.parquet"),
            ("",     "hdfs://nameservice/path/data.parquet"),
            ("file", "file:///abs/path/data.parquet"),
            ("oss",  "oss://bucket/db.db/tbl/data.parquet"),
            ("",     "oss://clg-paimon-fe4767/db.db/tbl/bucket-0/data-0.parquet"),
        ]
        for warehouse_scheme, file_path in cases:
            with self.subTest(warehouse_scheme=warehouse_scheme, file_path=file_path):
                self.assertEqual(_build_uri(warehouse_scheme, file_path), file_path)

    def test_adds_warehouse_scheme_when_path_unschemed(self):
        self.assertEqual(
            _build_uri("oss", "bucket/db.db/tbl/data.parquet"),
            "oss://bucket/db.db/tbl/data.parquet",
        )

    def test_defaults_to_file_scheme_when_both_unschemed(self):
        self.assertEqual(
            _build_uri("", "/tmp/pytest-xxx/db.db/tbl/data.parquet"),
            "file:///tmp/pytest-xxx/db.db/tbl/data.parquet",
        )


class DataFilePathTest(unittest.TestCase):

    def test_prefers_external_path(self):
        data_file = _DataFile(
            file_path="file:///warehouse/db.db/tbl/bucket-0/data.parquet",
            external_path="s3://external-bucket/data/db.db/tbl/bucket-0/data.parquet",
        )

        self.assertEqual(
            PaimonDataSource._data_file_path(data_file),
            "s3://external-bucket/data/db.db/tbl/bucket-0/data.parquet",
        )

    def test_falls_back_to_file_path(self):
        data_file = _DataFile(
            file_path="file:///warehouse/db.db/tbl/bucket-0/data.parquet",
            external_path=None,
        )

        self.assertEqual(
            PaimonDataSource._data_file_path(data_file),
            "file:///warehouse/db.db/tbl/bucket-0/data.parquet",
        )


if __name__ == "__main__":
    unittest.main()
