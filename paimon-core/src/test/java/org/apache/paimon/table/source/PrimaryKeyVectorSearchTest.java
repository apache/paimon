/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.table.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryVector;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.IndexedSplit;
import org.apache.paimon.globalindex.testvector.TestVectorGlobalIndexerFactory;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end tests for bucket-local primary-key vector search. */
class PrimaryKeyVectorSearchTest extends TableTestBase {

    @Override
    protected Schema schemaDefault() {
        return vectorSchema("deduplicate", true);
    }

    private Schema vectorSchema(String mergeEngine, boolean deletionVectorsEnabled) {
        return Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("embedding", DataTypes.VECTOR(2, DataTypes.FLOAT()))
                .primaryKey("id")
                .option(CoreOptions.BUCKET.key(), "1")
                .option(CoreOptions.MERGE_ENGINE.key(), mergeEngine)
                .option(
                        CoreOptions.DELETION_VECTORS_ENABLED.key(),
                        Boolean.toString(deletionVectorsEnabled))
                .option(CoreOptions.PK_VECTOR_INDEX_COLUMNS.key(), "embedding")
                .option(
                        "fields.embedding.pk-vector.index.type",
                        TestVectorGlobalIndexerFactory.IDENTIFIER)
                .option("fields.embedding.pk-vector.distance.metric", "l2")
                .option("test.vector.dimension", "2")
                .option("test.vector.metric", "l2")
                .build();
    }

    @Test
    void testVectorSearchMaterializesPhysicalRows() throws Exception {
        createTableDefault();
        FileStoreTable table = getTableDefault();
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            write.withIOManager(ioManager);
            write.write(GenericRow.of(1, BinaryVector.fromPrimitiveArray(new float[] {3, 0})));
            write.write(GenericRow.of(2, BinaryVector.fromPrimitiveArray(new float[] {1, 0})));
            write.write(GenericRow.of(3, BinaryVector.fromPrimitiveArray(new float[] {2, 0})));
            commit.commit(write.prepareCommit());
        }

        GlobalIndexResult result =
                table.newVectorSearchBuilder()
                        .withVectorColumn("embedding")
                        .withVector(new float[] {0, 0})
                        .withLimit(2)
                        .executeLocal();
        assertThat(result).isInstanceOf(GlobalIndexSplitResult.class);

        ReadBuilder readBuilder = table.newReadBuilder();
        TableScan.Plan plan = readBuilder.newScan().withGlobalIndexResult(result).plan();
        assertThat(plan.splits()).allMatch(IndexedSplit.class::isInstance);
        List<Integer> ids = new ArrayList<>();
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
            reader.forEachRemaining(row -> ids.add(row.getInt(0)));
        }

        assertThat(ids).containsExactly(2, 3);
    }

    @Test
    void testFirstRowVectorSearch() throws Exception {
        catalog.createTable(identifier(), vectorSchema("first-row", false), false);
        FileStoreTable table = getTableDefault();

        write(
                table,
                ioManager,
                GenericRow.of(1, BinaryVector.fromPrimitiveArray(new float[] {3, 0})),
                GenericRow.of(2, BinaryVector.fromPrimitiveArray(new float[] {1, 0})));
        write(
                table,
                ioManager,
                GenericRow.of(1, BinaryVector.fromPrimitiveArray(new float[] {0.5f, 0})));

        GlobalIndexResult result =
                table.newVectorSearchBuilder()
                        .withVectorColumn("embedding")
                        .withVector(new float[] {0, 0})
                        .withLimit(1)
                        .executeLocal();
        ReadBuilder readBuilder = table.newReadBuilder();
        TableScan.Plan plan = readBuilder.newScan().withGlobalIndexResult(result).plan();
        List<Integer> ids = new ArrayList<>();
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
            reader.forEachRemaining(row -> ids.add(row.getInt(0)));
        }

        assertThat(ids).containsExactly(2);
    }

    @Test
    void testAggregationVectorSearch() throws Exception {
        catalog.createTable(identifier(), vectorSchema("aggregation", true), false);
        FileStoreTable table = getTableDefault();

        write(
                table,
                ioManager,
                GenericRow.of(1, BinaryVector.fromPrimitiveArray(new float[] {3, 0})),
                GenericRow.of(2, BinaryVector.fromPrimitiveArray(new float[] {1, 0})));
        write(
                table,
                ioManager,
                GenericRow.of(1, BinaryVector.fromPrimitiveArray(new float[] {0.5f, 0})));

        GlobalIndexResult result =
                table.newVectorSearchBuilder()
                        .withVectorColumn("embedding")
                        .withVector(new float[] {0, 0})
                        .withLimit(1)
                        .executeLocal();
        ReadBuilder readBuilder = table.newReadBuilder();
        TableScan.Plan plan = readBuilder.newScan().withGlobalIndexResult(result).plan();
        List<Integer> ids = new ArrayList<>();
        try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
            reader.forEachRemaining(row -> ids.add(row.getInt(0)));
        }

        assertThat(ids).containsExactly(1);
    }
}
