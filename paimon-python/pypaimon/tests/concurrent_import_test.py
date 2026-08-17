# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import base64
import os
import pickle
import subprocess
import sys
import unittest

from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.read.query_auth_split import QueryAuthSplit
from pypaimon.read.split import DataSplit
from pypaimon.table.row.generic_row import GenericRow


class ConcurrentImportTest(unittest.TestCase):

    @unittest.skipIf(
        sys.version_info[:2] < (3, 7),
        "module-level lazy attributes require Python 3.7+",
    )
    def test_concurrent_query_auth_split_deserialization(self):
        empty = GenericRow([], [])
        file_meta = DataFileMeta.create(
            file_name="data.parquet",
            file_size=1,
            row_count=1,
            min_key=empty,
            max_key=empty,
            key_stats=SimpleStats.empty_stats(),
            value_stats=SimpleStats.empty_stats(),
            min_sequence_number=0,
            max_sequence_number=0,
            schema_id=0,
            level=0,
            extra_files=[],
            first_row_id=0,
        )
        payload = base64.b64encode(pickle.dumps(QueryAuthSplit(
            DataSplit([file_meta], empty, 0, snapshot_id=1), None,
        ))).decode("ascii")
        script = r"""
import base64
import importlib
import pickle
import sys
import threading

payload = base64.b64decode(sys.argv[1])
barrier = threading.Barrier(24)
errors = []
modules = [
    "pypaimon.manifest.schema.data_file_meta",
    "pypaimon.manifest.schema.simple_stats",
    "pypaimon.read.query_auth_split",
]


def deserialize(index):
    try:
        barrier.wait()
        if index < len(modules):
            importlib.import_module(modules[index])
        for _ in range(20):
            pickle.loads(payload)
    except BaseException as error:
        errors.append(repr(error))


threads = [
    threading.Thread(target=deserialize, args=(index,))
    for index in range(24)
]
for thread in threads:
    thread.start()
for thread in threads:
    thread.join()
if errors:
    print("\n".join(errors))
    raise SystemExit(1)
"""
        result = subprocess.run(
            [sys.executable, "-c", script, payload],
            env=os.environ.copy(),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            timeout=30,
        )
        self.assertEqual(0, result.returncode, result.stdout)

    @unittest.skipIf(
        sys.version_info[:2] < (3, 7),
        "module-level lazy attributes require Python 3.7+",
    )
    def test_concurrent_top_level_lazy_exports(self):
        script = r"""
import sys
import threading

sys.setswitchinterval(1e-6)
thread_count = 16
barrier = threading.Barrier(thread_count)
errors = []
statements = ["from pypaimon import Tag", "from pypaimon import TagManager"]


def resolve(statement):
    try:
        barrier.wait()
        exec(statement)
    except BaseException as error:
        errors.append(repr(error))


threads = [
    threading.Thread(target=resolve, args=(statements[index % 2],))
    for index in range(thread_count)
]
for thread in threads:
    thread.start()
for thread in threads:
    thread.join(15)
if errors:
    print("\n".join(sorted(set(errors))))
    raise SystemExit(1)
"""
        # Lazy names resolve once per process, so retries need fresh ones.
        for attempt in range(8):
            result = subprocess.run(
                [sys.executable, "-c", script],
                env=os.environ.copy(),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                timeout=30,
            )
            self.assertEqual(
                0, result.returncode,
                "attempt {}: {}".format(attempt, result.stdout),
            )

    def test_fresh_import_of_cycle_prone_leaf_modules(self):
        # Each module must import cleanly as a process's first pypaimon
        # import, without a package init pulling a circular chain.
        for module in [
            "pypaimon.index.index_file_meta",
            "pypaimon.manifest.index_manifest_entry",
            "pypaimon.read.scanner.bucket_select_converter",
            "pypaimon.table.data_evolution_merge_into",
            "pypaimon.write.table_delete",
            "pypaimon.tag.tag_manager",
        ]:
            result = subprocess.run(
                [sys.executable, "-c", "import " + module],
                env=os.environ.copy(),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                timeout=60,
            )
            self.assertEqual(
                0, result.returncode,
                "{}: {}".format(module, result.stdout),
            )


if __name__ == "__main__":
    unittest.main()
