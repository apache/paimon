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


class ChangelogManagerInfraTest(unittest.TestCase):
    def _build_manager(self, file_io, table_path="/tmp/test_table"):
        from pypaimon.changelog.changelog_manager import ChangelogManager
        return ChangelogManager(file_io, table_path)

    def test_delete_changelog(self):
        file_io = Mock()
        manager = self._build_manager(file_io)
        manager.delete_changelog(5)
        file_io.delete_quietly.assert_called_once_with("/tmp/test_table/changelog/changelog-5")


if __name__ == '__main__':
    unittest.main()
