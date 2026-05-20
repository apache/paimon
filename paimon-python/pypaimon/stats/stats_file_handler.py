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

import uuid
from typing import Optional

from pypaimon.common.file_io import FileIO
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.stats.statistics import Statistics


class StatsFileHandler:
    """Read/write statistics JSON files under <table>/statistics/."""

    def __init__(self, file_io: FileIO, table_path: str):
        self._file_io = file_io
        self._statistics_path = f"{table_path.rstrip('/')}/statistics"

    def read_stats(self, snapshot: Snapshot) -> Optional[Statistics]:
        file_name = snapshot.statistics
        if file_name is None:
            return None
        return self._read_file(file_name)

    def write_stats(self, stats: Statistics) -> str:
        file_name = f"statistics-{uuid.uuid4().hex}"
        path = f"{self._statistics_path}/{file_name}"
        self._file_io.write_file(path, stats.to_json(), overwrite=False)
        return file_name

    def _read_file(self, file_name: str) -> Statistics:
        path = f"{self._statistics_path}/{file_name}"
        json_str = self._file_io.read_file_utf8(path)
        return Statistics.from_json(json_str)
