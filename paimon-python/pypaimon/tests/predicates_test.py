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
import os
import random
import tempfile
import unittest

import pandas as pd
import pyarrow as pa

from pypaimon import CatalogFactory, Schema


def _check_filtered_result(read_builder, expected_df):
    scan = read_builder.new_scan()
    read = read_builder.new_read()
    actual_df = read.to_pandas(scan.plan().splits())
    pd.testing.assert_frame_equal(
        actual_df.reset_index(drop=True), expected_df.reset_index(drop=True))


def _random_format():
    return random.choice(['parquet', 'avro', 'orc'])


class PredicateTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', False)
        pa_schema = pa.schema([
            ('f0', pa.int64()),
            ('f1', pa.string()),
        ])
        cls.catalog.create_table('default.test_append', Schema.from_pyarrow_schema(
            pa_schema, options={'file.format': _random_format()}), False)
        cls.catalog.create_table('default.test_pk', Schema.from_pyarrow_schema(
            pa_schema, primary_keys=['f0'], options={'bucket': '1', 'file.format': _random_format()}), False)

        df = pd.DataFrame({
            'f0': [1, 2, 3, 4, 5],
            'f1': ['abc', 'abbc', 'bc', 'd', None],
        })

        append_table = cls.catalog.get_table('default.test_append')
        write_builder = append_table.new_batch_write_builder()
        write = write_builder.new_write()
        commit = write_builder.new_commit()
        write.write_pandas(df)
        commit.commit(write.prepare_commit())
        write.close()
        commit.close()

        pk_table = cls.catalog.get_table('default.test_pk')
        write_builder = pk_table.new_batch_write_builder()
        write = write_builder.new_write()
        commit = write_builder.new_commit()
        write.write_pandas(df)
        commit.commit(write.prepare_commit())
        write.close()
        commit.close()

        cls.df = df

    def test_wrong_field_name(self):
        table = self.catalog.get_table('default.test_append')
        predicate_builder = table.new_read_builder().new_predicate_builder()
        with self.assertRaises(ValueError) as e:
            predicate_builder.equal('f2', 'a')
        self.assertEqual(str(e.exception), "The field f2 is not in field list ['f0', 'f1'].")

    def test_append_with_duplicate(self):
        pa_schema = pa.schema([
            ('f0', pa.int64()),
            ('f1', pa.string()),
        ])
        self.catalog.create_table('default.test_append_with_duplicate', Schema.from_pyarrow_schema(pa_schema), False)

        df = pd.DataFrame({
            'f0': [1, 1, 2, 2],
            'f1': ['a', 'b', 'c', 'd'],
        })

        table = self.catalog.get_table('default.test_append_with_duplicate')
        write_builder = table.new_batch_write_builder()
        write = write_builder.new_write()
        commit = write_builder.new_commit()
        write.write_pandas(df)
        commit.commit(write.prepare_commit())
        write.close()
        commit.close()

        predicate_builder = table.new_read_builder().new_predicate_builder()

        predicate = predicate_builder.equal('f0', 1)
        _check_filtered_result(table.new_read_builder().with_filter(predicate), df.loc[0:1])

        predicate = predicate_builder.equal('f0', 0)
        read_builder = table.new_read_builder().with_filter(predicate)
        scan = read_builder.new_scan()
        read = read_builder.new_read()
        actual_df = read.to_pandas(scan.plan().splits())
        self.assertEqual(len(actual_df), 0)

    def test_all_field_types_with_equal(self):
        pa_schema = pa.schema([
            # int
            ('_tinyint', pa.int8()),
            ('_smallint', pa.int16()),
            ('_int', pa.int32()),
            ('_bigint', pa.int64()),
            # float
            ('_float16', pa.float32()),  # NOTE: cannot write pa.float16() data into Paimon
            ('_float32', pa.float32()),
            ('_double', pa.float64()),
            # string
            ('_string', pa.string()),
            # bool
            ('_boolean', pa.bool_())
        ])
        self.catalog.create_table('default.test_all_field_types',
                                  Schema.from_pyarrow_schema(pa_schema, options={'file.format': _random_format()}),
                                  False)
        table = self.catalog.get_table('default.test_all_field_types')
        write_builder = table.new_batch_write_builder()
        write = write_builder.new_write()
        commit = write_builder.new_commit()

        df = pd.DataFrame({
            '_tinyint': pd.Series([1, 2], dtype='int8'),
            '_smallint': pd.Series([10, 20], dtype='int16'),
            '_int': pd.Series([100, 200], dtype='int32'),
            '_bigint': pd.Series([1000, 2000], dtype='int64'),
            '_float16': pd.Series([1.0, 2.0], dtype='float16'),
            '_float32': pd.Series([1.00, 2.00], dtype='float32'),
            '_double': pd.Series([1.000, 2.000], dtype='double'),
            '_string': pd.Series(['A', 'B'], dtype='object'),
            '_boolean': [True, False]
        })
        record_batch = pa.RecordBatch.from_pandas(df, schema=pa_schema)
        # prepare for assertion
        df['_float16'] = df['_float16'].astype('float32')

        write.write_arrow_batch(record_batch)
        commit.commit(write.prepare_commit())
        write.close()
        commit.close()

        predicate_builder = table.new_read_builder().new_predicate_builder()

        predicate = predicate_builder.equal('_tinyint', 1)
        _check_filtered_result(table.new_read_builder().with_filter(predicate), df.loc[[0]])

        predicate = predicate_builder.equal('_smallint', 20)
        _check_filtered_result(table.new_read_builder().with_filter(predicate), df.loc[[1]])

        predicate = predicate_builder.equal('_int', 100)
        _check_filtered_result(table.new_read_builder().with_filter(predicate), df.loc[[0]])

        predicate = predicate_builder.equal('_bigint', 2000)
        _check_filtered_result(table.new_read_builder().with_filter(predicate), df.loc[[1]])

        predicate = predicate_builder.equal('_float16', 1.0)
        _check_filtered_result(table.new_read_builder().with_filter(predicate), df.loc[[0]])

        predicate = predicate_builder.equal('_float32', 2.00)
        _check_filtered_result(table.new_read_builder().with_filter(predicate), df.loc[[1]])

        predicate = predicate_builder.equal('_double', 1.000)
        _check_filtered_result(table.new_read_builder().with_filter(predicate), df.loc[[0]])

        predicate = predicate_builder.equal('_string', 'B')
        _check_filtered_result(table.new_read_builder().with_filter(predicate), df.loc[[1]])

        predicate = predicate_builder.equal('_boolean', True)
        _check_filtered_result(table.new_read_builder().with_filter(predicate), df.loc[[0]])

    def test_equal_pk(self):
        table = self.catalog.get_table('default.test_pk')
        predicate_builder = table.new_read_builder().new_predicate_builder()
        predicate = predicate_builder.equal('f0', 1)
        _check_filtered_result(table.new_read_builder().with_filter(predicate), self.df.loc[[0]])

    def test_not_equal_append(self):
        table = self.catalog.get_table('default.test_append')
        predicate_builder = table.new_read_builder().new_predicate_builder()
        predicate = predicate_builder.not_equal('f0', 1)
        _check_filtered_result(table.new_read_builder().with_filter(predicate), self.df.loc[1:4])

    def test_not_equal_pk(self):
        table = self.catalog.get_table('default.test_pk')
        predicate_builder = table.new_read_builder().new_predicate_builder()
        predicate = predicate_builder.not_equal('f0', 1)
        _check_filtered_result(table.new_read_builder().with_filter(predicate), self.df.loc[1:4])

    def test_less_than_append(self):
        table = self.catalog.get_table('default.test_append')
        predicate_builder = table.new_read_builder().new_predicate_builder()
        predicate = predicate_builder.less_than('f0', 3)
        _check_filtered_result(table.new_read_builder().with_filter(predicate), self.df.loc[0:1])

    def test_less_than_pk(self):
        table = self.catalog.get_table('default.test_pk')
        predicate_builder = table.new_read_builder().new_predicate_builder()
        predicate = predicate_builder.less_than('f0', 3)
        _check_filtered_result(table.new_read_builder().with_filter(predicate), self.df.loc[0:1])

    def test_less_or_equal_append(self):
        table = self.catalog.get_table('default.test_append')
        predicate_builder = table.new_read_builder().new_predicate_builder()
        predicate = predicate_builder.less_or_equal('f0', 3)
        _check_filtered_result(table.new_read_builder().with_filter(predicate), self.df.loc[0:2])

    def test_less_or_equal_pk(self):
        table = self.catalog.get_table('default.test_pk')
        predicate_builder = table.new_read_builder().new_predicate_builder()
        predicate = predicate_builder.less_or_equal('f0', 3)
        _check_filtered_result(table.new_read_builder().with_filter(predicate), self.df.loc[0:2])

    def test_greater_than_append(self):
        table = self.catalog.get_table('default.test_append')
        predicate_builder = table.new_read_builder().new_predicate_builder()
        predicate = predicate_builder.greater_than('f0', 3)
        _check_filtered_result(table.new_read_builder().with_filter(predicate), self.df.loc[3:4])

    def test_greater_than_pk(self):
        table = self.catalog.get_table('default.test_pk')
        predicate_builder = table.new_read_builder().new_predicate_builder()
        predicate = predicate_builder.greater_than('f0', 3)
        _check_filtered_result(table.new_read_builder().with_filter(predicate), self.df.loc[3:4])

    def test_greater_or_equal_append(self):
        table = self.catalog.get_table('default.test_append')
        predicate_builder = table.new_read_builder().new_predicate_builder()
        predicate = predicate_builder.greater_or_equal('f0', 3)
        _check_filtered_result(table.new_read_builder().with_filter(predicate), self.df.loc[2:4])

    def test_greater_or_equal_pk(self):
        table = self.catalog.get_table('default.test_pk')
        predicate_builder = table.new_read_builder().new_predicate_builder()
        predicate = predicate_builder.greater_or_equal('f0', 3)
        _check_filtered_result(table.new_read_builder().with_filter(predicate), self.df.loc[2:4])

    def test_is_null_append(self):
        table = self.catalog.get_table('default.test_append')
        predicate_builder = table.new_read_builder().new_predicate_builder()
        predicate = predicate_builder.is_null('f1')
        _check_filtered_result(table.new_read_builder().with_filter(predicate), self.df.loc[[4]])

    def test_is_null_pk(self):
        table = self.catalog.get_table('default.test_pk')
        predicate_builder = table.new_read_builder().new_predicate_builder()
        predicate = predicate_builder.is_null('f1')
        _check_filtered_result(table.new_read_builder().with_filter(predicate), self.df.loc[[4]])

    def test_is_not_null_append(self):
        table = self.catalog.get_table('default.test_append')
        predicate_builder = table.new_read_builder().new_predicate_builder()
        predicate = predicate_builder.is_not_null('f1')
        _check_filtered_result(table.new_read_builder().with_filter(predicate), self.df.loc[0:3])

    def test_is_not_null_pk(self):
        table = self.catalog.get_table('default.test_pk')
        predicate_builder = table.new_read_builder().new_predicate_builder()
        predicate = predicate_builder.is_not_null('f1')
        _check_filtered_result(table.new_read_builder().with_filter(predicate), self.df.loc[0:3])

    def test_startswith_append(self):
        table = self.catalog.get_table('default.test_append')
        predicate_builder = table.new_read_builder().new_predicate_builder()
        predicate = predicate_builder.startswith('f1', 'ab')
        _check_filtered_result(table.new_read_builder().with_filter(predicate), self.df.loc[0:1])

    def test_startswith_pk(self):
        table = self.catalog.get_table('default.test_pk')
        predicate_builder = table.new_read_builder().new_predicate_builder()
        predicate = predicate_builder.startswith('f1', 'ab')
        _check_filtered_result(table.new_read_builder().with_filter(predicate), self.df.loc[0:1])

    def test_endswith_append(self):
        table = self.catalog.get_table('default.test_append')
        predicate_builder = table.new_read_builder().new_predicate_builder()
        predicate = predicate_builder.endswith('f1', 'bc')
        _check_filtered_result(table.new_read_builder().with_filter(predicate), self.df.loc[0:2])

    def test_endswith_pk(self):
        table = self.catalog.get_table('default.test_pk')
        predicate_builder = table.new_read_builder().new_predicate_builder()
        predicate = predicate_builder.endswith('f1', 'bc')
        _check_filtered_result(table.new_read_builder().with_filter(predicate), self.df.loc[0:2])

    def test_contains_append(self):
        table = self.catalog.get_table('default.test_append')
        predicate_builder = table.new_read_builder().new_predicate_builder()
        predicate = predicate_builder.contains('f1', 'bb')
        _check_filtered_result(table.new_read_builder().with_filter(predicate), self.df.loc[[1]])

    def test_contains_pk(self):
        table = self.catalog.get_table('default.test_pk')
        predicate_builder = table.new_read_builder().new_predicate_builder()
        predicate = predicate_builder.contains('f1', 'bb')
        _check_filtered_result(table.new_read_builder().with_filter(predicate), self.df.loc[[1]])

    def test_is_in_append(self):
        table = self.catalog.get_table('default.test_append')
        predicate_builder = table.new_read_builder().new_predicate_builder()
        predicate = predicate_builder.is_in('f0', [1, 2])
        _check_filtered_result(table.new_read_builder().with_filter(predicate), self.df.loc[0:1])

    def test_is_in_pk(self):
        table = self.catalog.get_table('default.test_pk')
        predicate_builder = table.new_read_builder().new_predicate_builder()
        predicate = predicate_builder.is_in('f1', ['abc', 'd'])
        _check_filtered_result(table.new_read_builder().with_filter(predicate), self.df.loc[[0, 3]])

    def test_is_not_in_append(self):
        table = self.catalog.get_table('default.test_append')
        predicate_builder = table.new_read_builder().new_predicate_builder()
        predicate = predicate_builder.is_not_in('f0', [1, 2])
        _check_filtered_result(table.new_read_builder().with_filter(predicate), self.df.loc[2:4])

    def test_is_not_in_pk(self):
        table = self.catalog.get_table('default.test_pk')
        predicate_builder = table.new_read_builder().new_predicate_builder()
        predicate = predicate_builder.is_not_in('f1', ['abc', 'abbc'])
        _check_filtered_result(table.new_read_builder().with_filter(predicate), self.df.loc[2:4])

    def test_between_append(self):
        table = self.catalog.get_table('default.test_append')
        predicate_builder = table.new_read_builder().new_predicate_builder()
        predicate = predicate_builder.between('f0', 1, 3)
        _check_filtered_result(table.new_read_builder().with_filter(predicate), self.df.loc[0:2])

    def test_between_pk(self):
        table = self.catalog.get_table('default.test_pk')
        predicate_builder = table.new_read_builder().new_predicate_builder()
        predicate = predicate_builder.between('f0', 1, 3)
        _check_filtered_result(table.new_read_builder().with_filter(predicate), self.df.loc[0:2])

    def test_and_predicates(self):
        table = self.catalog.get_table('default.test_append')
        predicate_builder = table.new_read_builder().new_predicate_builder()
        predicate1 = predicate_builder.greater_than('f0', 1)
        predicate2 = predicate_builder.startswith('f1', 'ab')
        predicate = predicate_builder.and_predicates([predicate1, predicate2])
        _check_filtered_result(table.new_read_builder().with_filter(predicate), self.df.loc[[1]])

    def test_or_predicates(self):
        table = self.catalog.get_table('default.test_append')
        predicate_builder = table.new_read_builder().new_predicate_builder()
        predicate1 = predicate_builder.greater_than('f0', 3)
        predicate2 = predicate_builder.less_than('f0', 2)
        predicate = predicate_builder.or_predicates([predicate1, predicate2])
        _check_filtered_result(table.new_read_builder().with_filter(predicate),
                               self.df.loc[[0, 3, 4]])

    def test_pk_reader_with_filter(self):
        pa_schema = pa.schema([
            pa.field('key1', pa.int32(), nullable=False),
            pa.field('key2', pa.string(), nullable=False),
            ('behavior', pa.string()),
            pa.field('dt1', pa.string(), nullable=False),
            pa.field('dt2', pa.int32(), nullable=False)
        ])
        schema = Schema.from_pyarrow_schema(pa_schema,
                                            partition_keys=['dt1', 'dt2'],
                                            primary_keys=['key1', 'key2'],
                                            options={'bucket': '1'})
        self.catalog.create_table('default.test_pk_filter', schema, False)
        table = self.catalog.get_table('default.test_pk_filter')

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data1 = {
            'key1': [1, 2, 3, 4],
            'key2': ['h', 'g', 'f', 'e'],
            'behavior': ['a', 'b', 'c', None],
            'dt1': ['p1', 'p1', 'p2', 'p1'],
            'dt2': [2, 2, 1, 2],
        }
        pa_table = pa.Table.from_pydict(data1, schema=pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data1 = {
            'key1': [5, 2, 7, 8],
            'key2': ['d', 'g', 'b', 'a'],
            'behavior': ['e', 'b-new', 'g', 'h'],
            'dt1': ['p2', 'p1', 'p1', 'p2'],
            'dt2': [2, 2, 1, 2]
        }
        pa_table = pa.Table.from_pydict(data1, schema=pa_schema)
        table_write.write_arrow(pa_table)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # test filter by partition
        predicate_builder = table.new_read_builder().new_predicate_builder()
        p1 = predicate_builder.startswith('dt1', "p1")
        p2 = predicate_builder.is_in('dt1', ["p2"])
        p3 = predicate_builder.or_predicates([p1, p2])
        p4 = predicate_builder.equal('dt2', 2)
        g1 = predicate_builder.and_predicates([p3, p4])
        # (dt1 startswith 'p1' or dt1 is_in ["p2"]) and dt2 == 2
        read_builder = table.new_read_builder().with_filter(g1)
        splits = read_builder.new_scan().plan().splits()
        self.assertEqual(len(splits), 2)
        self.assertEqual(splits[0].partition.to_dict()["dt2"], 2)
        self.assertEqual(splits[1].partition.to_dict()["dt2"], 2)

        # test filter by stats
        predicate_builder = table.new_read_builder().new_predicate_builder()
        p1 = predicate_builder.equal('key1', 7)
        p2 = predicate_builder.is_in('key2', ["e", "f"])
        p3 = predicate_builder.or_predicates([p1, p2])
        p4 = predicate_builder.greater_than('key1', 3)
        g1 = predicate_builder.and_predicates([p3, p4])
        # (key1 == 7 or key2 is_in ["e", "f"]) and key1 > 3
        read_builder = table.new_read_builder().with_filter(g1)
        splits = read_builder.new_scan().plan().splits()
        # initial splits meta:
        # p1, 2 -> 2g, 2g; 1e, 4h
        # p2, 1 -> 3f, 3f
        # p2, 2 -> 5a, 8d
        # p1, 1 -> 7b, 7b
        self.assertEqual(len(splits), 3)
        # expect to filter out `p1, 2 -> 2g, 2g` and `p2, 1 -> 3f, 3f`
        count = 0
        for split in splits:
            if split.partition.values == ["p1", 2]:
                count += 1
                self.assertEqual(len(split.files), 1)
                min_values = split.files[0].value_stats.min_values.to_dict()
                max_values = split.files[0].value_stats.max_values.to_dict()
                self.assertTrue(min_values["key1"] == 1 and min_values["key2"] == "e"
                                and max_values["key1"] == 4 and max_values["key2"] == "h")
            elif split.partition.values == ["p2", 2]:
                count += 1
                min_values = split.files[0].value_stats.min_values.to_dict()
                max_values = split.files[0].value_stats.max_values.to_dict()
                self.assertTrue(min_values["key1"] == 5 and min_values["key2"] == "a"
                                and max_values["key1"] == 8 and max_values["key2"] == "d")
            elif split.partition.values == ["p1", 1]:
                count += 1
                min_values = split.files[0].value_stats.min_values.to_dict()
                max_values = split.files[0].value_stats.max_values.to_dict()
                self.assertTrue(min_values["key1"] == max_values["key1"] == 7
                                and max_values["key2"] == max_values["key2"] == "b")
        self.assertEqual(count, 3)
