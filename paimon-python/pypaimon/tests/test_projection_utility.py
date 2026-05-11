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

from pypaimon.schema.data_types import AtomicType, DataField, RowType
from pypaimon.utils.projection import (NestedProjection, Projection,
                                       TopLevelProjection)


def _atomic(idx: int, name: str, type_name: str = 'BIGINT') -> DataField:
    return DataField(idx, name, AtomicType(type_name))


def _struct(idx: int, name: str, sub_fields) -> DataField:
    return DataField(idx, name, RowType(False, list(sub_fields)))


def _three_top_fields():
    """schema: [pk: BIGINT, mv: ROW<latest_version BIGINT, latest_value STRING>, val: STRING]"""
    return [
        _atomic(1, 'pk'),
        _struct(2, 'mv', [
            _atomic(10, 'latest_version'),
            _atomic(11, 'latest_value', 'STRING'),
        ]),
        _atomic(3, 'val', 'STRING'),
    ]


class TopLevelProjectionTest(unittest.TestCase):

    def test_factory_produces_top_level(self):
        p = Projection.of([2, 0])
        self.assertIsInstance(p, TopLevelProjection)
        self.assertFalse(p.is_nested())

    def test_indexes_round_trip(self):
        p = Projection.of([2, 0, 1])
        self.assertEqual(p.to_top_level_indexes(), [2, 0, 1])
        # nested form lifts each top-level index into a singleton path
        self.assertEqual(p.to_nested_indexes(), [[2], [0], [1]])

    def test_project_picks_fields_in_order(self):
        fields = _three_top_fields()
        res = Projection.of([2, 0]).project(fields)
        self.assertEqual([f.name for f in res], ['val', 'pk'])
        self.assertEqual([f.id for f in res], [3, 1])

    def test_to_name_paths(self):
        fields = _three_top_fields()
        names = Projection.of([2, 0]).to_name_paths(fields)
        self.assertEqual(names, [['val'], ['pk']])

    def test_range_factory(self):
        p = Projection.range(1, 4)
        self.assertEqual(p.to_top_level_indexes(), [1, 2, 3])

    def test_range_zero_or_negative_returns_empty(self):
        self.assertFalse(Projection.range(2, 2).is_nested())
        self.assertEqual(Projection.range(2, 2).to_top_level_indexes(), [])
        self.assertEqual(Projection.range(5, 1).to_top_level_indexes(), [])


class NestedProjectionTest(unittest.TestCase):

    def test_factory_produces_nested(self):
        p = Projection.of([[1, 0], [1, 1]])
        self.assertIsInstance(p, NestedProjection)
        self.assertTrue(p.is_nested())

    def test_singleton_paths_reported_not_nested(self):
        # paths of length 1 only — observable behaviour matches top level.
        p = Projection.of([[2], [0]])
        self.assertIsInstance(p, NestedProjection)
        self.assertFalse(p.is_nested())

    def test_top_level_indexes_dedup_in_path_order(self):
        p = Projection.of([[1, 0], [1, 1], [0]])
        self.assertEqual(p.to_top_level_indexes(), [1, 0])

    def test_nested_indexes_round_trip(self):
        p = Projection.of([[1, 0], [1, 1]])
        self.assertEqual(p.to_nested_indexes(), [[1, 0], [1, 1]])

    def test_to_name_paths_walks_into_struct(self):
        fields = _three_top_fields()
        names = Projection.of([[1, 0], [1, 1], [0]]).to_name_paths(fields)
        self.assertEqual(
            names,
            [['mv', 'latest_version'], ['mv', 'latest_value'], ['pk']])

    def test_project_flattens_with_underscore_join(self):
        fields = _three_top_fields()
        res = Projection.of([[1, 0], [1, 1], [0]]).project(fields)
        self.assertEqual(
            [f.name for f in res], ['mv_latest_version', 'mv_latest_value', 'pk'])

    def test_project_preserves_leaf_field_id(self):
        # Schema-evolution remapping is by field ID, so flattened nested
        # fields must inherit the leaf's ID — not the parent struct's.
        fields = _three_top_fields()
        res = Projection.of([[1, 0], [1, 1]]).project(fields)
        self.assertEqual([f.id for f in res], [10, 11])

    def test_collision_dedup_via_dollar_suffix(self):
        # Two leaves under different parents with the same final name.
        sub_a = _atomic(20, 'x')
        sub_b = _atomic(21, 'x')
        fields = [
            _struct(1, 'a', [sub_a]),
            _struct(2, 'b', [sub_b]),
        ]
        # path [0,0] -> 'a_x', path [1,0] -> 'b_x' (no collision yet).
        res = Projection.of([[0, 0], [1, 0]]).project(fields)
        self.assertEqual([f.name for f in res], ['a_x', 'b_x'])

        # When two collapse to the SAME name, the second gets `__N`.
        # Build two parents whose leaves have the same compound name.
        sub_x_only = _atomic(30, 'x')
        fields2 = [
            _struct(1, 'a', [sub_x_only]),
            _atomic(2, 'a_x'),  # plain top-level already named 'a_x'.
        ]
        res2 = Projection.of([[0, 0], [1]]).project(fields2)
        # First path produces 'a_x'; second is already-existing 'a_x'.
        # Collision → suffix on the second.
        self.assertEqual(res2[0].name, 'a_x')
        self.assertTrue(res2[1].name.startswith('a_x__'))

    def test_project_rejects_non_row_step(self):
        # Trying to walk into an atomic field must fail loudly.
        fields = _three_top_fields()
        with self.assertRaises(ValueError):
            Projection.of([[0, 0]]).project(fields)

    def test_constructor_rejects_empty_paths_list(self):
        with self.assertRaises(ValueError):
            NestedProjection([])

    def test_constructor_rejects_zero_length_path(self):
        with self.assertRaises(ValueError):
            NestedProjection([[]])

    def test_dup_count_is_monotonic_across_distinct_collisions(self):
        # ``__N`` is a per-call monotonic counter — distinct collisions
        # share the suffix space, they don't each restart at 0.
        sub_x_1 = _atomic(20, 'x')
        sub_x_2 = _atomic(21, 'x')
        sub_y_1 = _atomic(30, 'y')
        sub_y_2 = _atomic(31, 'y')
        fields = [
            _atomic(1, 'a_x'),
            _struct(2, 'a', [sub_x_1]),
            _atomic(3, 'a_y'),
            _struct(4, 'a', [sub_y_1]),  # collides via path [3, 0] → a_y
            _struct(5, 'a', [sub_x_2, sub_y_2]),  # second a.x collision
        ]
        # Order: a_x (top) → keeps; [1, 0] flatten → a_x (collision) → a_x__0
        # a_y (top) → keeps; [3, 0] flatten → a_y (collision) → a_y__1
        res = Projection.of(
            [[0], [1, 0], [2], [3, 0]]).project(fields)
        self.assertEqual(
            [f.name for f in res], ['a_x', 'a_x__0', 'a_y', 'a_y__1'])

    def test_of_rejects_mixed_int_and_path(self):
        # Mixing top-level indexes and nested paths is a programming error;
        # ``of`` should fail loudly at the call site instead of producing a
        # broken projection that explodes downstream.
        with self.assertRaises(TypeError):
            Projection.of([1, [2, 3]])
        with self.assertRaises(TypeError):
            Projection.of([[1, 2], 3])


class EmptyProjectionTest(unittest.TestCase):

    def test_of_empty(self):
        self.assertEqual(Projection.of([]).to_top_level_indexes(), [])
        self.assertEqual(Projection.of([]).to_nested_indexes(), [])

    def test_empty_factory(self):
        p = Projection.empty()
        self.assertEqual(p.project(_three_top_fields()), [])
        self.assertFalse(p.is_nested())
        self.assertEqual(p.to_name_paths(_three_top_fields()), [])


if __name__ == '__main__':
    unittest.main()
