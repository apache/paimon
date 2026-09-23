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

"""Unit tests for the FieldAggregator registry contract.

Drives :func:`register_aggregator` / :func:`create_field_aggregator`
without touching the read pipeline so the wiring is pinned down before
any concrete aggregators land in :mod:`aggregators`.
"""

import unittest

from pypaimon.read.reader.aggregate import (
    create_field_aggregator,
    register_aggregator,
)
from pypaimon.read.reader.aggregate.field_aggregator import FieldAggregator
from pypaimon.schema.data_types import AtomicType


class _DummyAgg(FieldAggregator):
    """Minimal concrete subclass used only by these tests."""

    def agg(self, accumulator, input_field):
        return input_field


class FieldAggregatorRegistryTest(unittest.TestCase):

    def test_register_and_create_returns_instance(self):
        register_aggregator(
            "_dummy_for_registry_test",
            lambda field_type, field_name, options: _DummyAgg(
                "_dummy_for_registry_test", field_type
            ),
        )
        agg = create_field_aggregator(
            AtomicType("INT"),
            "field0",
            "_dummy_for_registry_test",
            options=None,
        )
        self.assertIsInstance(agg, _DummyAgg)
        self.assertEqual(agg.name, "_dummy_for_registry_test")
        self.assertEqual(agg.field_type, AtomicType("INT"))

    def test_re_register_replaces_existing_factory(self):
        register_aggregator(
            "_dummy_replaceable",
            lambda ft, fn, opts: _DummyAgg("first", ft),
        )
        register_aggregator(
            "_dummy_replaceable",
            lambda ft, fn, opts: _DummyAgg("second", ft),
        )
        agg = create_field_aggregator(
            AtomicType("BIGINT"), "f", "_dummy_replaceable", options=None
        )
        self.assertEqual(agg.name, "second")

    def test_unknown_identifier_raises_value_error(self):
        with self.assertRaises(ValueError) as ctx:
            create_field_aggregator(
                AtomicType("INT"),
                "field0",
                "this_aggregator_does_not_exist",
                options=None,
            )
        msg = str(ctx.exception)
        self.assertIn("unsupported aggregation", msg)
        self.assertIn("this_aggregator_does_not_exist", msg)

    def test_default_retract_raises_not_implemented(self):
        agg = _DummyAgg("dummy", AtomicType("INT"))
        with self.assertRaises(NotImplementedError) as ctx:
            agg.retract(1, 2)
        self.assertIn("does not support retract", str(ctx.exception))
        self.assertIn("dummy", str(ctx.exception))

    def test_default_reset_is_noop(self):
        # Base-class reset() must not raise so subclasses without
        # per-group state can skip overriding it.
        agg = _DummyAgg("dummy", AtomicType("INT"))
        agg.reset()  # no exception expected


if __name__ == '__main__':
    unittest.main()
