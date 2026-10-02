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

"""Failed normal-file cleanup for composite data writers."""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.write.writer.data_vector_writer import DataVectorWriter
from pypaimon.write.writer.dedicated_format_writer import DedicatedFormatWriter


class WriterFileCleanupTest(unittest.TestCase):
    def test_metadata_failure_deletes_written_file(self):
        for writer_type in (DataVectorWriter, DedicatedFormatWriter):
            with self.subTest(writer=writer_type.__name__), tempfile.TemporaryDirectory() as tmp:
                catalog = CatalogFactory.create({'warehouse': tmp})
                catalog.create_database('db', False)
                data = pa.table({'id': [1]})
                schema = data.schema
                if writer_type is DedicatedFormatWriter:
                    schema = schema.append(pa.field('payload', pa.large_binary()))
                catalog.create_table('db.t', Schema.from_pyarrow_schema(schema, options={
                    'file.format': 'parquet',
                    'data-evolution.enabled': 'true',
                    'row-tracking.enabled': 'true',
                }), False)
                table = catalog.get_table('db.t')
                writer = writer_type(table, (), 0, 0, table.options, write_cols=['id'])
                writer.write(data.to_batches()[0])

                def fail_after_output(path):
                    self.assertTrue(Path(path).is_file())
                    raise OSError('cannot read file size')

                with patch.object(writer.file_io, 'get_file_size', side_effect=fail_after_output):
                    with self.assertRaisesRegex(OSError, 'cannot read file size'):
                        writer.prepare_commit()
                self.assertEqual(list(Path(tmp).rglob('*.parquet')), [])
