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

"""Regression tests for exact decimal global index keys."""

from decimal import Decimal, Inexact, Rounded, localcontext
import os
import struct
import tempfile
import unittest

from parameterized import parameterized

from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.globalindex.bitmap.bitmap_index_reader import BitmapIndexReader
from pypaimon.globalindex.bitmap.bitmap_index_writer import BitmapIndexWriter
from pypaimon.globalindex.btree.btree_index_reader import BTreeIndexReader
from pypaimon.globalindex.btree.btree_index_writer import BTreeIndexWriter
from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta
from pypaimon.globalindex.key_serializer import create_serializer
from pypaimon.schema.data_types import AtomicType


class DecimalKeySerializerTest(unittest.TestCase):

    @parameterized.expand([
        (18, 2, '1234567890123456.78', 123456789012345678),
        (38, 2, '123456789012345678901234567890123456.78',
         12345678901234567890123456789012345678),
        (38, 2, '-123456789012345678901234567890123456.78',
         -12345678901234567890123456789012345678),
        (38, 38, '0.12345678901234567890123456789012345678',
         12345678901234567890123456789012345678),
        (38, 2, '0.00', 0),
    ])
    def test_exact_bytes_and_decode_with_low_precision(self, precision, scale, value, unscaled):
        serializer = create_serializer(AtomicType('DECIMAL(%s,%s)' % (precision, scale)))
        # Java encodes compact decimals as little-endian longs and larger
        # decimals as signed, big-endian unscaled integers.
        if precision <= 18:
            expected = struct.pack('<q', unscaled)
        else:
            length = max(1, (unscaled.bit_length() + 8) // 8)
            expected = unscaled.to_bytes(length, 'big', signed=True)
        with localcontext() as context:
            context.prec = 3
            context.traps[Inexact] = True
            context.traps[Rounded] = True
            context.clear_flags()
            self.assertEqual(expected, serializer.serialize(Decimal(value)))
            self.assertEqual(Decimal(value), serializer.deserialize(expected))
            self.assertEqual(3, context.prec)
            self.assertFalse(any(context.flags.values()))

    def test_decode_does_not_round_at_default_precision(self):
        serializer = create_serializer(AtomicType('DECIMAL(38,2)'))
        unscaled = 12345678901234567890123456789012345678
        with localcontext() as context:
            context.prec = 28
            self.assertEqual(
                Decimal('123456789012345678901234567890123456.78'),
                serializer.deserialize(unscaled.to_bytes(16, 'big', signed=True)))

    @parameterized.expand([
        ('1.235', '1.24'), ('-1.235', '-1.24'), ('9.999', '10.00'),
    ])
    def test_rounding_is_independent_of_caller_traps(self, value, expected):
        serializer = create_serializer(AtomicType('DECIMAL(18,2)'))
        with localcontext() as context:
            context.prec = 2
            context.traps[Inexact] = True
            context.traps[Rounded] = True
            self.assertEqual(Decimal(expected), serializer.deserialize(serializer.serialize(value)))

    @parameterized.expand([
        ('btree', BTreeIndexWriter, BTreeIndexReader),
        ('bitmap', BitmapIndexWriter, BitmapIndexReader),
    ])
    def test_persisted_high_precision_keys(self, name, writer_class, reader_class):
        serializer = create_serializer(AtomicType('DECIMAL(38,2)'))
        values = [Decimal(value) for value in (
            '123456789012345678901234567890123456.77',
            '123456789012345678901234567890123456.78',
            '123456789012345678901234567890123456.79',
        )]
        with tempfile.TemporaryDirectory() as directory:
            file_io = LocalFileIO()
            with localcontext() as context:
                context.prec = 50
                writer = writer_class(file_io, directory, serializer)
                for row_id, value in enumerate(values):
                    writer.write(value, row_id)
                entry = writer.finish()[0]
            with localcontext() as context:
                context.prec = 6
                reader = reader_class(serializer, file_io, directory, GlobalIndexIOMeta(
                    file_name=entry.file_name,
                    file_size=os.path.getsize(os.path.join(directory, entry.file_name)),
                    metadata=entry.meta,
                ))
                try:
                    self.assertEqual([1], reader.visit_equal(values[1]).results().to_list())
                    self.assertEqual([0], reader.visit_less_than(values[1]).results().to_list())
                    self.assertEqual([2], reader.visit_greater_than(values[1]).results().to_list())
                finally:
                    reader.close()
