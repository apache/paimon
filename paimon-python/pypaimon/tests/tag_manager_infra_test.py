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
from pypaimon.tag.tag import Tag


class TagManagerInfraTest(unittest.TestCase):
    def _build_manager(self, file_io, table_path="/tmp/test_table"):
        from pypaimon.tag.tag_manager import TagManager
        return TagManager(file_io, table_path)

    def test_tagged_snapshots_returns_sorted(self):
        file_io = Mock()
        manager = self._build_manager(file_io)
        tag_a, tag_b, tag_c = Mock(spec=Tag), Mock(spec=Tag), Mock(spec=Tag)
        tag_a.id, tag_b.id, tag_c.id = 5, 2, 8
        manager.list_tags = Mock(return_value=["tag_a", "tag_c", "tag_b"])
        manager.get = Mock(side_effect=lambda name: {"tag_a": tag_a, "tag_b": tag_b, "tag_c": tag_c}[name])
        result = manager.tagged_snapshots()
        self.assertEqual([t.id for t in result], [2, 5, 8])

    def test_tagged_snapshots_skips_unreadable(self):
        file_io = Mock()
        manager = self._build_manager(file_io)
        tag_a = Mock(spec=Tag)
        tag_a.id = 1
        manager.list_tags = Mock(return_value=["tag_a", "tag_broken"])
        manager.get = Mock(side_effect=lambda name: tag_a if name == "tag_a" else None)
        result = manager.tagged_snapshots()
        self.assertEqual(len(result), 1)

    def test_tagged_snapshots_empty(self):
        file_io = Mock()
        manager = self._build_manager(file_io)
        manager.list_tags = Mock(return_value=[])
        self.assertEqual(manager.tagged_snapshots(), [])


if __name__ == '__main__':
    unittest.main()
