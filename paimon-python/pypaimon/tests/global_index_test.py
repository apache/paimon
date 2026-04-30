import unittest

from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.utils.range import Range


class GlobalIndexTest(unittest.TestCase):

    def test_chained_or(self):
        result = GlobalIndexResult.create_empty()
        for i in range(600):
            other = GlobalIndexResult.from_range(Range(i * 10, i * 10 + 5))
            result = result.or_(other)

        self.assertEqual(result.results().cardinality(), 3600)

    def test_chained_and(self):
        result = GlobalIndexResult.from_range(Range(0, 10000))
        for i in range(600):
            other = GlobalIndexResult.from_range(Range(0, 10000))
            result = result.and_(other)

        self.assertEqual(result.results().cardinality(), 10001)
