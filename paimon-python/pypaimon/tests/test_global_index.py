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

"""Tests for global index functionality."""

import os
import unittest

import faiss
import numpy as np

from pypaimon import Schema
from pypaimon.globalindex.faiss import FaissIndex
from pypaimon.globalindex.vector_search import VectorSearch
from pypaimon.globalindex.faiss.faiss_options import FaissVectorMetric


class TestFaissVectorGlobalIndexE2E(unittest.TestCase):
    """
    End-to-end test for FAISS vector global index.
    """

    @classmethod
    def setUpClass(cls):
        """Set up test fixtures."""
        import tempfile
        from pypaimon import CatalogFactory

        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({
            'warehouse': cls.warehouse
        })
        cls.catalog.create_database('default', True)

        cls.dimension = 32
        cls.n_vectors = 100

        # Generate random vectors with a fixed seed for reproducibility
        np.random.seed(42)
        cls.vectors = np.random.random(
            (cls.n_vectors, cls.dimension)
        ).astype(np.float32)

    @classmethod
    def tearDownClass(cls):
        """Clean up temp files."""
        import shutil
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def test_faiss_vector_global_index_write_scan_read(self):
        """
        Test FAISS vector global index end-to-end.
        """
        import pyarrow as pa
        import json

        from pypaimon.index.index_file_meta import IndexFileMeta
        from pypaimon.globalindex.global_index_meta import GlobalIndexMeta
        from pypaimon.globalindex.faiss.faiss_index_meta import FaissIndexMeta

        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('vec', pa.list_(pa.float32())),
        ])

        table_options = {
            'bucket': '1',
            'vector.dim': str(self.dimension),
            'vector.metric': 'L2',
            'vector.index-type': 'FLAT',
            'data-evolution.enabled': 'true',
            'row-tracking.enabled': 'true',
            'global-index.enabled': 'true',
        }

        schema = Schema.from_pyarrow_schema(pa_schema, options=table_options)
        self.catalog.create_table(
            'default.test_faiss_vector_e2e', schema, False
        )
        table = self.catalog.get_table('default.test_faiss_vector_e2e')

        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        vector_field_name = 'vec'
        data = pa.Table.from_pydict({
            'id': list(range(self.n_vectors)),
            vector_field_name: [self.vectors[i].tolist() for i in range(self.n_vectors)],
        }, schema=pa_schema)

        table_write.write_arrow(data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # ========== Step 3: Build FAISS index ==========
        index_dir = table.path_factory().index_path()
        os.makedirs(index_dir, exist_ok=True)

        index = FaissIndex.create_flat_index(
            self.dimension,
            FaissVectorMetric.L2
        )

        # Add vectors with IDs - same as Java's FaissIndex.addWithIds()
        ids = np.arange(self.n_vectors, dtype=np.int64)
        vectors = np.ascontiguousarray(self.vectors, dtype=np.float32)
        ids = np.ascontiguousarray(ids, dtype=np.int64)
        index._index.add_with_ids(vectors, ids)
        self.assertEqual(index.size(), self.n_vectors)

        # Both use the same underlying FAISS serialization format
        index_filename = 'faiss-vec-0.index'
        index_path = os.path.join(index_dir, index_filename)
        faiss.write_index(index._index, index_path)
        index.close()

        loaded_index = FaissIndex.from_file(index_path)
        self.assertEqual(loaded_index.size(), self.n_vectors)

        # Verify search works on loaded index
        query = self.vectors[42:43]
        distances, labels = loaded_index.search(query, 1)
        self.assertEqual(labels[0][0], 42)  # Should find the exact vector
        loaded_index.close()

        # Create FaissIndexMeta
        faiss_meta = FaissIndexMeta(
            dim=self.dimension,
            metric_value=0,  # L2
            index_type_ordinal=0,  # FLAT
            num_vectors=self.n_vectors,
            min_id=0,
            max_id=self.n_vectors - 1
        )

        # Create GlobalIndexMeta
        global_index_meta = GlobalIndexMeta(
            row_range_start=0,
            row_range_end=self.n_vectors - 1,
            index_field_id=1,
            index_meta=faiss_meta.serialize()
        )

        # Create IndexFileMeta
        index_file_meta = IndexFileMeta(
            index_type='faiss-vector-ann',
            file_name=index_filename,
            file_size=os.path.getsize(index_path),
            row_count=self.n_vectors,
            global_index_meta=global_index_meta
        )

        # Write index manifest file
        manifest_dir = table.path_factory().manifest_path()
        os.makedirs(manifest_dir, exist_ok=True)
        index_manifest_filename = 'index-manifest-0'
        index_manifest_path = os.path.join(manifest_dir, index_manifest_filename)

        # Simple JSON format for index manifest entries
        manifest_entries = [{
            'kind': 0,  # ADD
            'partition': [],
            'bucket': 1,
            'index_file': {
                'file_name': index_file_meta.file_name,
                'file_size': index_file_meta.file_size,
                'row_count': index_file_meta.row_count,
                'index_type': index_file_meta.index_type,
                'global_index_meta': {
                    'row_range_start': global_index_meta.row_range_start,
                    'row_range_end': global_index_meta.row_range_end,
                    'index_field_id': global_index_meta.index_field_id,
                }
            }
        }]

        with open(index_manifest_path, 'w') as f:
            json.dump(manifest_entries, f)

        # Update snapshot to include index manifest
        from pypaimon.snapshot.snapshot import Snapshot
        from pypaimon.common.json_util import JSON
        snapshot_manager = table.snapshot_manager()
        current_snapshot = snapshot_manager.get_latest_snapshot()

        # Create new snapshot with index manifest
        # Set next_row_id to n_vectors since we've indexed all rows (0 to n_vectors-1)
        new_snapshot = Snapshot(
            id=current_snapshot.id + 1,
            schema_id=current_snapshot.schema_id,
            base_manifest_list=current_snapshot.base_manifest_list,
            delta_manifest_list=current_snapshot.delta_manifest_list,
            changelog_manifest_list=current_snapshot.changelog_manifest_list,
            index_manifest=index_manifest_filename,
            commit_kind=current_snapshot.commit_kind,
            commit_user=current_snapshot.commit_user,
            commit_identifier=current_snapshot.commit_identifier,
            version=current_snapshot.version,
            time_millis=current_snapshot.time_millis,
            total_record_count=current_snapshot.total_record_count,
            delta_record_count=current_snapshot.delta_record_count,
            next_row_id=self.n_vectors
        )

        # Write new snapshot
        snapshot_path = os.path.join(
            snapshot_manager.snapshot_dir, 'snapshot-{}'.format(new_snapshot.id)
        )
        with open(snapshot_path, 'w') as f:
            f.write(JSON.to_json(new_snapshot))

        # Update LATEST hint
        with open(snapshot_manager.latest_file, 'w') as f:
            f.write(str(new_snapshot.id))

        query_idx = 42
        query_vector = self.vectors[query_idx]
        vector_search = VectorSearch(
            vector=query_vector,
            limit=10,
            field_name=vector_field_name
        )
        read_builder = table.new_read_builder().with_vector_search(vector_search)

        # Get scan and plan
        scan = read_builder.new_scan()
        plan = scan.plan()
        plan_splits = plan.splits()

        # Read data through ReadBuilder API
        table_read = read_builder.new_read()
        result = table_read.to_arrow(plan_splits)

        result_ids = result.column('id').to_pylist()

        # Verify result count matches vector search limit
        self.assertEqual(
            len(result_ids), 10,
            "Vector search with limit=10 should return exactly 10 results. "
            "Got {} results: {}".format(len(result_ids), result_ids)
        )

        # Query vector (id=42) should be in results
        self.assertIn(
            query_idx, result_ids,
            "Query vector (id={}) should be in results: {}".format(
                query_idx, result_ids
            )
        )


class JavaPyFaissE2ETest(unittest.TestCase):
    """
    E2E test for FAISS vector index: Java writes, Python reads.
    Following the JavaPyReadWriteTest pattern.

    Run this test with:
      1. Run Java test first: mvn test -Dtest=JavaPyFaissE2ETest#testJavaWriteFaissVectorIndex \
         -pl paimon-faiss/paimon-faiss-index -Drun.e2e.tests=true
      2. Then run Python test: python -m pytest test_global_index.py::JavaPyFaissE2ETest -v
    """

    @classmethod
    def setUpClass(cls):
        """Set up using the shared e2e warehouse."""
        from pypaimon import CatalogFactory

        # Use the shared e2e warehouse (same as JavaPyE2ETest)
        # The test runs from pypaimon/tests directory, warehouse is in e2e/warehouse
        cls.tempdir = os.path.abspath(os.path.dirname(__file__))
        cls.warehouse = os.path.join(cls.tempdir, 'e2e', 'warehouse')

        # Create database if it doesn't exist (needed for catalog to find tables)
        cls.catalog = CatalogFactory.create({
            'warehouse': cls.warehouse
        })
        cls.catalog.create_database('default', True)

    def test_read_faiss_vector_table(self):
        """
        Read FAISS vector table written by Java (JavaPyFaissE2ETest.testJavaWriteFaissVectorIndex).
        """
        table = self.catalog.get_table('default.faiss_vector_table_j')

        vector_field_name = 'vec'

        # Query with [0.85, 0.15], limit=2
        query_vector = np.array([0.85, 0.15], dtype=np.float32)
        vector_search = VectorSearch(
            vector=query_vector,
            limit=2,
            field_name=vector_field_name
        )
        read_builder = table.new_read_builder().with_vector_search(vector_search)

        scan = read_builder.new_scan()
        plan = scan.plan()
        plan_splits = plan.splits()

        table_read = read_builder.new_read()
        result = table_read.to_arrow(plan_splits)
        result_ids = result.column('id').to_pylist()

        print(f"Vector search results: {result_ids}")

        # With L2 distance, closest to [0.85, 0.15] should be:
        # - [0.95, 0.1] (id=1)
        # - [0.98, 0.05] (id=3)
        self.assertEqual(
            len(result_ids), 2,
            f"Vector search with limit=2 should return exactly 2 results. Got: {result_ids}"
        )
        self.assertIn(1, result_ids, "ID 1 ([0.95, 0.1]) should be in results")
        self.assertIn(3, result_ids, "ID 3 ([0.98, 0.05]) should be in results")


if __name__ == '__main__':
    unittest.main()
