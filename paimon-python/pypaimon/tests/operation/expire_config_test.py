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

import sys
import unittest


class ExpireConfigTest(unittest.TestCase):

    def test_defaults(self):
        from pypaimon.options.expire_config import ExpireConfig
        config = ExpireConfig()
        self.assertEqual(config.snapshot_retain_max, sys.maxsize)
        self.assertEqual(config.snapshot_retain_min, 1)
        self.assertEqual(config.snapshot_time_retain_millis, sys.maxsize)
        self.assertEqual(config.snapshot_max_deletes, sys.maxsize)

    def test_changelog_fallback_to_snapshot(self):
        from pypaimon.options.expire_config import ExpireConfig
        config = ExpireConfig(snapshot_retain_max=10, snapshot_retain_min=2)
        self.assertEqual(config.effective_changelog_retain_max(), 10)
        self.assertEqual(config.effective_changelog_retain_min(), 2)

    def test_changelog_override(self):
        from pypaimon.options.expire_config import ExpireConfig
        config = ExpireConfig(snapshot_retain_max=10, changelog_retain_max=20)
        self.assertEqual(config.effective_changelog_retain_max(), 20)

    def test_changelog_decoupled_auto_derived(self):
        from pypaimon.options.expire_config import ExpireConfig
        config = ExpireConfig(snapshot_retain_max=10, changelog_retain_max=20)
        self.assertTrue(config.is_changelog_decoupled())

    def test_changelog_not_decoupled_when_equal(self):
        from pypaimon.options.expire_config import ExpireConfig
        config = ExpireConfig(snapshot_retain_max=10)
        self.assertFalse(config.is_changelog_decoupled())

    def test_retain_max_must_gte_retain_min(self):
        from pypaimon.options.expire_config import ExpireConfig
        config = ExpireConfig(snapshot_retain_max=1, snapshot_retain_min=5)
        with self.assertRaises(ValueError):
            config.validate()

if __name__ == '__main__':
    unittest.main()
