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

import unittest
from unittest.mock import Mock

from pypaimon.index.index_file_handler import IndexFileHandler


class IndexFileHandlerTest(unittest.TestCase):

    def test_scan_resolves_unspecified_snapshot(self):
        snapshot_manager = Mock(spec=['get_latest_snapshot'])
        snapshot_manager.get_latest_snapshot.return_value = None
        table = Mock()
        table.snapshot_manager.return_value = snapshot_manager

        self.assertEqual([], IndexFileHandler(table).scan(None))
        snapshot_manager.get_latest_snapshot.assert_called_once_with()


if __name__ == '__main__':
    unittest.main()
