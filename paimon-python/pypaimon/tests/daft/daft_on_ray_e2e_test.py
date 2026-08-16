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

"""End-to-end tests for pypaimon.daft with Daft's Ray runner.

Daft runners are process-global and cannot be switched after initialization.
Run the Ray-runner scenario in a fresh Python subprocess.
"""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
import textwrap

import pytest

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("daft") is None
    or importlib.util.find_spec("ray") is None,
    reason="Daft-on-Ray e2e requires both daft and ray",
)


def test_daft_on_ray_read_write_e2e():
    python_root = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..")
    )
    script = textwrap.dedent(
        r'''
        import os
        import shutil
        import tempfile

        os.environ.setdefault("RAY_TMPDIR", "/tmp")

        import daft
        import pyarrow as pa
        import ray
        from daft import runners
        from pypaimon import CatalogFactory, Schema
        from pypaimon.daft import PaimonCatalog, read_paimon, write_paimon

        root = tempfile.mkdtemp(prefix="paimon-daft-on-ray-")
        try:
            warehouse = os.path.join(root, "warehouse")
            catalog_options = {"warehouse": warehouse}
            catalog = CatalogFactory.create(catalog_options)
            catalog.create_database("test_db", False)

            ray.init(
                num_cpus=2,
                include_dashboard=False,
                ignore_reinit_error=True,
            )
            runners.set_runner_ray()
            assert runners.get_or_create_runner().name == "ray"

            schema = Schema.from_pyarrow_schema(
                pa.schema([
                    pa.field("id", pa.int64()),
                    pa.field("name", pa.string()),
                    pa.field("dt", pa.string()),
                ]),
                partition_keys=["dt"],
                options={"file.format": "parquet"},
            )
            catalog.create_table("test_db.t", schema, False)

            df = daft.from_pydict({
                "id": [3, 1, 2, 4],
                "name": ["c", "a", "b", "d"],
                "dt": [
                    "2026-06-07",
                    "2026-06-07",
                    "2026-06-07",
                    "2026-06-08",
                ],
            }).into_partitions(2)

            summary = write_paimon(
                df,
                "test_db.t",
                catalog_options=catalog_options,
            ).to_arrow()
            assert sum(summary.column("rows").to_pylist()) == 4

            result = (
                read_paimon("test_db.t", catalog_options=catalog_options)
                .where(daft.col("dt") == "2026-06-07")
                .select("id", "name")
                .sort("id")
                .to_arrow()
            )
            assert result.to_pydict() == {
                "id": [1, 2, 3],
                "name": ["a", "b", "c"],
            }

            write_paimon(
                daft.from_pydict({
                    "id": [10],
                    "name": ["z"],
                    "dt": ["2026-06-07"],
                }),
                "test_db.t",
                catalog_options=catalog_options,
                mode="overwrite",
            ).to_arrow()
            same_partition = (
                read_paimon("test_db.t", catalog_options=catalog_options)
                .sort("id")
                .to_arrow()
            )
            assert same_partition.to_pydict() == {
                "id": [4, 10],
                "name": ["d", "z"],
                "dt": ["2026-06-08", "2026-06-07"],
            }

            write_paimon(
                daft.from_pydict({
                    "id": [20],
                    "name": ["new"],
                    "dt": ["2026-06-09"],
                }),
                "test_db.t",
                catalog_options=catalog_options,
                mode="overwrite",
            ).to_arrow()
            new_partition = (
                read_paimon("test_db.t", catalog_options=catalog_options)
                .sort("id")
                .to_arrow()
            )
            assert new_partition.to_pydict() == {
                "id": [4, 10, 20],
                "name": ["d", "z", "new"],
                "dt": ["2026-06-08", "2026-06-07", "2026-06-09"],
            }

            unpartitioned_schema = Schema.from_pyarrow_schema(
                pa.schema([
                    pa.field("id", pa.int64()),
                    pa.field("name", pa.string()),
                ]),
                options={"file.format": "parquet"},
            )
            catalog.create_table(
                "test_db.unpartitioned",
                unpartitioned_schema,
                False,
            )
            write_paimon(
                daft.from_pydict({"id": [1, 2], "name": ["a", "b"]}),
                "test_db.unpartitioned",
                catalog_options=catalog_options,
            ).to_arrow()
            write_paimon(
                daft.from_pydict({"id": [30], "name": ["only"]}),
                "test_db.unpartitioned",
                catalog_options=catalog_options,
                mode="overwrite",
            ).to_arrow()
            unpartitioned = read_paimon(
                "test_db.unpartitioned",
                catalog_options=catalog_options,
            ).to_arrow()
            assert unpartitioned.to_pydict() == {
                "id": [30],
                "name": ["only"],
            }

            daft_catalog = PaimonCatalog(
                CatalogFactory.create(catalog_options),
                name="paimon",
            )
            wrapper_result = (
                daft_catalog.get_table("test_db.unpartitioned")
                .read()
                .to_arrow()
            )
            assert wrapper_result.to_pydict() == {
                "id": [30],
                "name": ["only"],
            }
        finally:
            if ray.is_initialized():
                ray.shutdown()
            shutil.rmtree(root, ignore_errors=True)
        '''
    )

    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        [python_root, env.get("PYTHONPATH", "")]
    ).rstrip(os.pathsep)
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        env=env,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, (
        f"stdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )
