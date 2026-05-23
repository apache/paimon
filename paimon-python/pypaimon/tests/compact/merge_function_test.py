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
from unittest.mock import MagicMock

from pypaimon.common.options import Options
from pypaimon.common.options.core_options import CoreOptions, MergeEngine
from pypaimon.read.reader.merge_function import (
    DeduplicateMergeFunction, DeduplicateMergeFunctionFactory,
    create_merge_function_factory)
from pypaimon.table.row.key_value import KeyValue


def _kv(key: int, seq: int, value: str = "v", value_kind: int = 0) -> KeyValue:
    kv = KeyValue(key_arity=1, value_arity=1)
    kv.replace((key, seq, value_kind, value))
    return kv


class DeduplicateMergeFunctionTest(unittest.TestCase):

    def test_keeps_last_added(self):
        mf = DeduplicateMergeFunction()
        mf.reset()
        mf.add(_kv(1, 10, "old"))
        mf.add(_kv(1, 20, "new"))
        result = mf.get_result()
        self.assertIsNotNone(result)
        self.assertEqual(20, result.sequence_number)

    def test_reset_clears_state(self):
        mf = DeduplicateMergeFunction()
        mf.add(_kv(1, 1))
        mf.reset()
        self.assertIsNone(mf.get_result())


class CreateMergeFunctionFactoryTest(unittest.TestCase):

    def _options_for(self, engine: MergeEngine) -> CoreOptions:
        opts = Options({CoreOptions.MERGE_ENGINE.key(): engine.value})
        return CoreOptions(opts)

    def test_deduplicate_returns_factory(self):
        factory = create_merge_function_factory(self._options_for(MergeEngine.DEDUPLICATE))
        self.assertIsInstance(factory, DeduplicateMergeFunctionFactory)
        self.assertIsInstance(factory.create(), DeduplicateMergeFunction)

    def test_partial_update_factory_creates_stub_that_raises(self):
        factory = create_merge_function_factory(self._options_for(MergeEngine.PARTIAL_UPDATE))
        mf = factory.create()
        with self.assertRaises(NotImplementedError):
            mf.add(_kv(1, 1))

    def test_aggregate_factory_creates_stub_that_raises(self):
        factory = create_merge_function_factory(self._options_for(MergeEngine.AGGREGATE))
        mf = factory.create()
        with self.assertRaises(NotImplementedError):
            mf.add(_kv(1, 1))

    def test_first_row_factory_creates_stub_that_raises(self):
        factory = create_merge_function_factory(self._options_for(MergeEngine.FIRST_ROW))
        mf = factory.create()
        with self.assertRaises(NotImplementedError):
            mf.add(_kv(1, 1))


if __name__ == "__main__":
    unittest.main()
