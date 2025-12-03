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

"""Tests for Lance vector and scalar indexing support."""

import unittest
import logging

# Try to import indexing modules
try:
    from pypaimon.read.reader.lance.vector_index import VectorIndexBuilder
    from pypaimon.read.reader.lance.scalar_index import ScalarIndexBuilder, BitmapIndexHandler, BTreeIndexHandler
    from pypaimon.read.reader.lance.predicate_pushdown import (
        PredicateOptimizer, PredicateExpression, PredicateOperator
    )
    HAS_LANCE_INDEXING = True
except ImportError:
    HAS_LANCE_INDEXING = False

logger = logging.getLogger(__name__)


class VectorIndexBuilderTest(unittest.TestCase):
    """Test VectorIndexBuilder functionality."""

    @unittest.skipUnless(HAS_LANCE_INDEXING, "Lance indexing modules not available")
    def test_ivf_pq_index_creation(self):
        """Test IVF_PQ index builder initialization."""
        builder = VectorIndexBuilder('vector', 'ivf_pq', 'l2')

        self.assertEqual(builder.vector_column, 'vector')
        self.assertEqual(builder.index_type, 'ivf_pq')
        self.assertEqual(builder.metric, 'l2')

    @unittest.skipUnless(HAS_LANCE_INDEXING, "Lance indexing modules not available")
    def test_hnsw_index_creation(self):
        """Test HNSW index builder initialization."""
        builder = VectorIndexBuilder('vector', 'hnsw', 'cosine')

        self.assertEqual(builder.vector_column, 'vector')
        self.assertEqual(builder.index_type, 'hnsw')
        self.assertEqual(builder.metric, 'cosine')

    @unittest.skipUnless(HAS_LANCE_INDEXING, "Lance indexing modules not available")
    def test_invalid_index_type(self):
        """Test error on invalid index type."""
        with self.assertRaises(ValueError):
            VectorIndexBuilder('vector', 'invalid', 'l2')

    @unittest.skipUnless(HAS_LANCE_INDEXING, "Lance indexing modules not available")
    def test_invalid_metric(self):
        """Test error on invalid metric."""
        with self.assertRaises(ValueError):
            VectorIndexBuilder('vector', 'ivf_pq', 'invalid_metric')

    @unittest.skipUnless(HAS_LANCE_INDEXING, "Lance indexing modules not available")
    def test_compression_ratio_calculation(self):
        """Test PQ compression ratio calculation."""
        # 768-dim vector, float32 = 3072 bytes
        # 8 sub-vectors, 8 bits each = 8 bytes
        # Compression ratio = 1 - (8 / 3072) ≈ 0.997
        ratio = VectorIndexBuilder._calculate_compression_ratio(8, 8)
        self.assertGreater(ratio, 0.99)
        self.assertLess(ratio, 1.0)

    @unittest.skipUnless(HAS_LANCE_INDEXING, "Lance indexing modules not available")
    def test_hnsw_memory_estimation(self):
        """Test HNSW memory usage estimation."""
        memory = VectorIndexBuilder._estimate_hnsw_memory(20, 7, 1_000_000)

        # 1M vectors * 3.5 layers * 10 edges * 8 bytes
        # ≈ 280MB
        self.assertGreater(memory, 0)
        self.assertLess(memory, 1_000_000_000)  # Less than 1GB


class ScalarIndexTest(unittest.TestCase):
    """Test scalar indexing functionality."""

    @unittest.skipUnless(HAS_LANCE_INDEXING, "Lance indexing modules not available")
    def test_btree_index_initialization(self):
        """Test BTree index builder initialization."""
        builder = ScalarIndexBuilder('price', 'btree')

        self.assertEqual(builder.column, 'price')
        self.assertEqual(builder.index_type, 'btree')

    @unittest.skipUnless(HAS_LANCE_INDEXING, "Lance indexing modules not available")
    def test_bitmap_index_initialization(self):
        """Test Bitmap index builder initialization."""
        builder = ScalarIndexBuilder('category', 'bitmap')

        self.assertEqual(builder.column, 'category')
        self.assertEqual(builder.index_type, 'bitmap')

    @unittest.skipUnless(HAS_LANCE_INDEXING, "Lance indexing modules not available")
    def test_invalid_scalar_index_type(self):
        """Test error on invalid scalar index type."""
        with self.assertRaises(ValueError):
            ScalarIndexBuilder('column', 'invalid_type')

    @unittest.skipUnless(HAS_LANCE_INDEXING, "Lance indexing modules not available")
    def test_recommend_index_type_low_cardinality(self):
        """Test index type recommendation for low cardinality."""
        data = ['A'] * 950 + ['B'] * 50  # 2% unique
        index_type = ScalarIndexBuilder.recommend_index_type(data)

        self.assertEqual(index_type, 'bitmap')

    @unittest.skipUnless(HAS_LANCE_INDEXING, "Lance indexing modules not available")
    def test_recommend_index_type_high_cardinality(self):
        """Test index type recommendation for high cardinality."""
        data = list(range(1000))  # 100% unique
        index_type = ScalarIndexBuilder.recommend_index_type(data)

        self.assertEqual(index_type, 'btree')


class BitmapIndexHandlerTest(unittest.TestCase):
    """Test Bitmap index handler."""

    @unittest.skipUnless(HAS_LANCE_INDEXING, "Lance indexing modules not available")
    def test_build_bitmaps(self):
        """Test bitmap building from column data."""
        data = ['A', 'B', 'A', 'C', 'B', 'A']
        bitmaps = BitmapIndexHandler.build_bitmaps(data)

        self.assertEqual(set(bitmaps['A']), {0, 2, 5})
        self.assertEqual(set(bitmaps['B']), {1, 4})
        self.assertEqual(set(bitmaps['C']), {3})

    @unittest.skipUnless(HAS_LANCE_INDEXING, "Lance indexing modules not available")
    def test_bitmap_and(self):
        """Test bitmap AND operation."""
        b1 = {0, 1, 2, 3}
        b2 = {1, 2, 4, 5}
        result = BitmapIndexHandler.bitmap_and(b1, b2)

        self.assertEqual(result, {1, 2})

    @unittest.skipUnless(HAS_LANCE_INDEXING, "Lance indexing modules not available")
    def test_bitmap_or(self):
        """Test bitmap OR operation."""
        b1 = {0, 1, 2}
        b2 = {2, 3, 4}
        result = BitmapIndexHandler.bitmap_or(b1, b2)

        self.assertEqual(result, {0, 1, 2, 3, 4})

    @unittest.skipUnless(HAS_LANCE_INDEXING, "Lance indexing modules not available")
    def test_bitmap_not(self):
        """Test bitmap NOT operation."""
        bitmap = {0, 2, 4}
        result = BitmapIndexHandler.bitmap_not(bitmap, 5)

        self.assertEqual(result, {1, 3})


class BTreeIndexHandlerTest(unittest.TestCase):
    """Test BTree index handler."""

    @unittest.skipUnless(HAS_LANCE_INDEXING, "Lance indexing modules not available")
    def test_range_search_inclusive(self):
        """Test range search with inclusive bounds."""
        data = [10, 20, 30, 40, 50, 60, 70, 80, 90]
        result = BTreeIndexHandler.range_search(data, 30, 70, inclusive=True)

        # Should include rows with values 30, 40, 50, 60, 70
        expected = {2, 3, 4, 5, 6}
        self.assertEqual(set(result), expected)

    @unittest.skipUnless(HAS_LANCE_INDEXING, "Lance indexing modules not available")
    def test_range_search_exclusive(self):
        """Test range search with exclusive bounds."""
        data = [10, 20, 30, 40, 50, 60, 70, 80, 90]
        result = BTreeIndexHandler.range_search(data, 30, 70, inclusive=False)

        # Should exclude boundaries
        expected = {3, 4, 5}
        self.assertEqual(set(result), expected)

    @unittest.skipUnless(HAS_LANCE_INDEXING, "Lance indexing modules not available")
    def test_range_search_lower_bound_only(self):
        """Test range search with only lower bound."""
        data = [10, 20, 30, 40, 50]
        result = BTreeIndexHandler.range_search(data, min_val=30, inclusive=True)

        expected = {2, 3, 4}
        self.assertEqual(set(result), expected)

    @unittest.skipUnless(HAS_LANCE_INDEXING, "Lance indexing modules not available")
    def test_range_search_upper_bound_only(self):
        """Test range search with only upper bound."""
        data = [10, 20, 30, 40, 50]
        result = BTreeIndexHandler.range_search(data, max_val=30, inclusive=True)

        expected = {0, 1, 2}
        self.assertEqual(set(result), expected)


class PredicateOptimizerTest(unittest.TestCase):
    """Test predicate optimization."""

    @unittest.skipUnless(HAS_LANCE_INDEXING, "Lance indexing modules not available")
    def test_parse_simple_predicate(self):
        """Test parsing simple equality predicate."""
        optimizer = PredicateOptimizer()
        expressions = optimizer.parse_predicate("status = 'active'")

        self.assertIsNotNone(expressions)
        self.assertEqual(len(expressions), 1)
        self.assertEqual(expressions[0].column, 'status')
        self.assertEqual(expressions[0].operator, PredicateOperator.EQ)

    @unittest.skipUnless(HAS_LANCE_INDEXING, "Lance indexing modules not available")
    def test_parse_range_predicate(self):
        """Test parsing range predicates."""
        optimizer = PredicateOptimizer()
        expressions = optimizer.parse_predicate("price > 100")

        self.assertIsNotNone(expressions)
        self.assertEqual(len(expressions), 1)
        self.assertEqual(expressions[0].operator, PredicateOperator.GT)
        self.assertEqual(expressions[0].value, 100)

    @unittest.skipUnless(HAS_LANCE_INDEXING, "Lance indexing modules not available")
    def test_parse_and_predicate(self):
        """Test parsing AND combined predicates."""
        optimizer = PredicateOptimizer()
        expressions = optimizer.parse_predicate("category = 'A' AND price > 100")

        self.assertIsNotNone(expressions)
        self.assertEqual(len(expressions), 2)

    @unittest.skipUnless(HAS_LANCE_INDEXING, "Lance indexing modules not available")
    def test_parse_in_predicate(self):
        """Test parsing IN predicates."""
        optimizer = PredicateOptimizer()
        expressions = optimizer.parse_predicate("status IN ('active', 'pending')")

        self.assertIsNotNone(expressions)
        self.assertEqual(len(expressions), 1)
        self.assertEqual(expressions[0].operator, PredicateOperator.IN)

    @unittest.skipUnless(HAS_LANCE_INDEXING, "Lance indexing modules not available")
    def test_parse_null_predicate(self):
        """Test parsing NULL predicates."""
        optimizer = PredicateOptimizer()
        expressions = optimizer.parse_predicate("deleted_at IS NULL")

        self.assertIsNotNone(expressions)
        self.assertEqual(expressions[0].operator, PredicateOperator.IS_NULL)

    @unittest.skipUnless(HAS_LANCE_INDEXING, "Lance indexing modules not available")
    def test_register_index(self):
        """Test registering available indexes."""
        optimizer = PredicateOptimizer()
        optimizer.register_index('price', 'btree')
        optimizer.register_index('category', 'bitmap')

        self.assertEqual(optimizer.indexes['price'], 'btree')
        self.assertEqual(optimizer.indexes['category'], 'bitmap')

    @unittest.skipUnless(HAS_LANCE_INDEXING, "Lance indexing modules not available")
    def test_can_use_index(self):
        """Test checking if index can be used for predicate."""
        optimizer = PredicateOptimizer()
        optimizer.register_index('price', 'btree')
        optimizer.register_index('category', 'bitmap')

        # BTree can be used for range queries
        expr_range = PredicateExpression('price', PredicateOperator.GT, 100)
        self.assertTrue(optimizer.can_use_index(expr_range))

        # Bitmap can be used for equality
        expr_eq = PredicateExpression('category', PredicateOperator.EQ, 'A')
        self.assertTrue(optimizer.can_use_index(expr_eq))

        # Bitmap cannot be used for range
        expr_bitmap_range = PredicateExpression('category', PredicateOperator.GT, 'A')
        self.assertFalse(optimizer.can_use_index(expr_bitmap_range))

    @unittest.skipUnless(HAS_LANCE_INDEXING, "Lance indexing modules not available")
    def test_get_filter_hint(self):
        """Test getting optimization hints."""
        optimizer = PredicateOptimizer()
        optimizer.register_index('price', 'btree')
        optimizer.register_index('category', 'bitmap')

        expr1 = PredicateExpression('price', PredicateOperator.GT, 100)
        hint1 = optimizer.get_filter_hint(expr1)
        self.assertIn('BTREE', hint1)

        expr2 = PredicateExpression('category', PredicateOperator.EQ, 'A')
        hint2 = optimizer.get_filter_hint(expr2)
        self.assertIn('BITMAP', hint2)

    @unittest.skipUnless(HAS_LANCE_INDEXING, "Lance indexing modules not available")
    def test_selectivity_estimation(self):
        """Test selectivity estimation."""
        optimizer = PredicateOptimizer()
        optimizer.register_statistics('id', {'cardinality': 1000})

        expr_eq = PredicateExpression('id', PredicateOperator.EQ, 1)
        selectivity_eq = optimizer._estimate_selectivity(expr_eq)
        self.assertAlmostEqual(selectivity_eq, 0.001, places=3)

        expr_range = PredicateExpression('id', PredicateOperator.GT, 500)
        selectivity_range = optimizer._estimate_selectivity(expr_range)
        self.assertAlmostEqual(selectivity_range, 0.25, places=2)


if __name__ == '__main__':
    unittest.main()
