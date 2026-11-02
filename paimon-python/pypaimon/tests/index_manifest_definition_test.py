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

import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.globalindex.global_index_meta import GlobalIndexMeta
from pypaimon.index.data_evolution_index_source_meta import DataEvolutionIndexSourceMeta
from pypaimon.index.index_file_meta import IndexFileMeta
from pypaimon.index.pk.primary_key_index_source_file import PrimaryKeyIndexSourceFile
from pypaimon.index.pk.primary_key_index_source_meta import PrimaryKeyIndexSourceMeta
from pypaimon.manifest.index_manifest_entry import IndexManifestEntry
from pypaimon.manifest.index_manifest_file import IndexManifestFile
from pypaimon.table.row.generic_row import GenericRow


class IndexManifestDefinitionTest(unittest.TestCase):

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tempdir, ignore_errors=True)
        catalog = CatalogFactory.create({'warehouse': os.path.join(self.tempdir, 'warehouse')})
        catalog.create_database('default', True)
        schema = Schema.from_pyarrow_schema(pa.schema([
            ('a', pa.int32()), ('b', pa.int32()), ('c', pa.int32()),
        ]))
        catalog.create_table('default.index_definitions', schema, False)
        self.table = catalog.get_table('default.index_definitions')
        self.manifest = IndexManifestFile(self.table)
        self.a, self.b, self.c = [self.table.field_dict[name].id for name in ('a', 'b', 'c')]

    def _entry(self, name, fields, start=0, end=99, index_type='btree', empty_extra=False):
        return IndexManifestEntry(
            kind=0,
            partition=GenericRow([], []),
            bucket=0,
            index_file=IndexFileMeta(
                index_type=index_type,
                file_name=name,
                file_size=1,
                row_count=end - start + 1,
                global_index_meta=GlobalIndexMeta(
                    row_range_start=start,
                    row_range_end=end,
                    index_field_id=fields[0],
                    extra_field_ids=list(fields[1:]) if len(fields) > 1 or empty_extra else None,
                ),
            ),
        )

    def _assert_entries(self, name, expected):
        entries = self.manifest.read(name)
        self.assertEqual(len(expected), len(entries))
        actual = {entry.index_file.file_name: entry for entry in entries}
        self.assertEqual({entry.index_file.file_name for entry in expected}, set(actual))
        for entry in expected:
            restored = actual[entry.index_file.file_name]
            self.assertEqual(entry, restored)
            self.assertEqual(entry.index_file.global_index_meta,
                             restored.index_file.global_index_meta)

    def test_different_ordered_definitions_coexist(self):
        definitions = [
            ((self.a,), (self.a, self.b)),
            ((self.a, self.b), (self.a, self.c)),
            ((self.a, self.b), (self.a, self.b, self.c)),
            ((self.a, self.b, self.c), (self.a, self.c, self.b)),
            ((self.a, self.b), (self.b, self.a)),
        ]
        for index_type in ('btree', 'test-vector-ann', 'test-fulltext'):
            for old_fields, new_fields in definitions:
                with self.subTest(index_type=index_type, old=old_fields, new=new_fields):
                    old = self._entry('old', old_fields, index_type=index_type)
                    new = self._entry('new', new_fields, index_type=index_type)
                    previous = self.manifest.write([old])
                    current = self.manifest.combine_changes(previous, [new], [])
                    self._assert_entries(current, [old, new])

    def test_different_definitions_with_disjoint_ranges_coexist(self):
        old = self._entry('scalar', (self.a,))
        new = self._entry('pair', (self.a, self.b), start=100, end=199)
        previous = self.manifest.write([old])
        current = self.manifest.combine_changes(previous, [new], [])
        self._assert_entries(current, [old, new])

    def test_scalar_extension_preserves_existing_composite_definition(self):
        scalar = self._entry('scalar', (self.a,))
        composite = self._entry('composite', (self.a, self.b))
        extension = self._entry('extension', (self.a,), start=100, end=199)
        # Seed the valid coexistence state produced by the Java manifest combiner.
        previous = self.manifest.write([scalar, composite])
        self._assert_entries(previous, [scalar, composite])
        current = self.manifest.combine_changes(previous, [extension], [])
        self._assert_entries(current, [scalar, composite, extension])

    def test_same_definition_overlap_is_rejected(self):
        old = self._entry('old', (self.a, self.b))
        previous = self.manifest.write([old])
        for start in (50, 99):
            with self.subTest(start=start):
                new = self._entry('new', (self.a, self.b), start=start, end=149)
                with self.assertRaisesRegex(RuntimeError, 'Remove the previous file first'):
                    self.manifest.combine_changes(previous, [new], [])
                self._assert_entries(previous, [old])

    def test_same_definition_disjoint_ranges_coexist(self):
        old = self._entry('old', (self.a, self.b))
        new = self._entry('new', (self.a, self.b), start=100, end=199)
        previous = self.manifest.write([old])
        current = self.manifest.combine_changes(previous, [new], [])
        self._assert_entries(current, [old, new])

    def test_delete_before_replacement(self):
        old = self._entry('old', (self.a, self.b))
        new = self._entry('new', (self.a, self.b), start=50, end=149)
        previous = self.manifest.write([old])
        deleted = IndexManifestEntry(1, old.partition, old.bucket, old.index_file)
        current = self.manifest.combine_changes(previous, [new], [deleted])
        self._assert_entries(current, [new])
        self._assert_entries(previous, [old])

    def test_replacing_one_definition_preserves_other_definitions(self):
        for index_type in ('btree', 'test-vector-ann', 'test-fulltext'):
            with self.subTest(index_type=index_type):
                definitions = [
                    self._entry('single', (self.a,), index_type=index_type),
                    self._entry('pair', (self.a, self.b), index_type=index_type),
                    self._entry('other-pair', (self.a, self.c), index_type=index_type),
                    self._entry('triple', (self.a, self.b, self.c), index_type=index_type),
                    self._entry('reordered', (self.a, self.c, self.b), index_type=index_type),
                ]
                previous = None
                for definition in definitions:
                    previous = self.manifest.combine_changes(previous, [definition], [])
                self._assert_entries(previous, definitions)

                replacement = self._entry('replacement', (self.a, self.b), start=50, end=149,
                                          index_type=index_type)
                with self.assertRaisesRegex(RuntimeError, 'previous file pair still exists'):
                    self.manifest.combine_changes(previous, [replacement], [])
                old = definitions[1]
                deleted = IndexManifestEntry(1, old.partition, old.bucket, old.index_file)
                current = self.manifest.combine_changes(previous, [replacement], [deleted])
                self._assert_entries(current, [definitions[0], replacement] + definitions[2:])
                self._assert_entries(previous, definitions)

    def test_source_metadata_is_preserved_without_disabling_overlap_validation(self):
        sources = [
            (DataEvolutionIndexSourceMeta(1).serialize(),
             DataEvolutionIndexSourceMeta(2).serialize()),
            (PrimaryKeyIndexSourceMeta(1, [PrimaryKeyIndexSourceFile('old.parquet', 100)]).serialize(),
             PrimaryKeyIndexSourceMeta(1, [PrimaryKeyIndexSourceFile('new.parquet', 100)]).serialize()),
        ]
        for old_source, new_source in sources:
            with self.subTest(old_source=old_source):
                old = self._entry('old', (self.a,))
                old.index_file.global_index_meta.source_meta = old_source
                old.index_file.global_index_meta.index_meta = b'old-index-meta'
                previous = self.manifest.write([old])
                replacement = self._entry('replacement', (self.a,))
                replacement.index_file.global_index_meta.source_meta = new_source
                replacement.index_file.global_index_meta.index_meta = b'new-index-meta'
                with self.assertRaisesRegex(RuntimeError, 'Remove the previous file first'):
                    self.manifest.combine_changes(previous, [replacement], [])

                other = self._entry('other', (self.a, self.b))
                other.index_file.global_index_meta.source_meta = new_source
                other.index_file.global_index_meta.index_meta = b'other-index-meta'
                current = self.manifest.combine_changes(previous, [other], [])
                self._assert_entries(current, [old, other])
                self._assert_entries(previous, [old])

    def test_null_and_empty_extra_fields_are_the_same_definition(self):
        for empty_extra in (False, True):
            with self.subTest(empty_extra=empty_extra):
                old = self._entry('old', (self.a,), empty_extra=empty_extra)
                previous = self.manifest.write([old])
                overlap = self._entry('overlap', (self.a,), empty_extra=not empty_extra)
                with self.assertRaisesRegex(RuntimeError, 'Remove the previous file first'):
                    self.manifest.combine_changes(previous, [overlap], [])
                tail = self._entry('tail', (self.a,), start=100, end=199,
                                   empty_extra=not empty_extra)
                current = self.manifest.combine_changes(previous, [tail], [])
                self._assert_entries(current, [old, tail])

    def test_different_types_coexist(self):
        old = self._entry('old', (self.a,))
        new = self._entry('new', (self.a,), index_type='bitmap')
        previous = self.manifest.write([old])
        current = self.manifest.combine_changes(previous, [new], [])
        self._assert_entries(current, [old, new])

    def test_delete_only_and_no_changes_preserve_other_definitions(self):
        scalar = self._entry('scalar', (self.a,))
        composite = self._entry('composite', (self.a, self.b))
        previous = self.manifest.write([scalar, composite])
        self.assertEqual(previous, self.manifest.combine_changes(previous, [], []))
        deleted = IndexManifestEntry(1, scalar.partition, scalar.bucket, scalar.index_file)
        current = self.manifest.combine_changes(previous, [], [deleted])
        self._assert_entries(current, [composite])
        self._assert_entries(previous, [scalar, composite])


if __name__ == '__main__':
    unittest.main()
