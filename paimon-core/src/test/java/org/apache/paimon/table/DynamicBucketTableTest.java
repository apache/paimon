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

package org.apache.paimon.table;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.index.DynamicBucketIndexMaintainer;
import org.apache.paimon.index.HashBucketAssigner;
import org.apache.paimon.operation.AbstractFileStoreWrite;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.BatchWriteBuilderImpl;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.DynamicBucketRowKeyExtractor;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Pair;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for Dynamic Bucket Table. */
public class DynamicBucketTableTest extends TableTestBase {

    @ParameterizedTest
    @ValueSource(strings = {"scan", "snapshot"})
    public void testSequenceNumberInitPreservesHashIndex(String mode) throws Exception {
        FileStoreTable table = createSequenceNumberTable(mode);
        DynamicBucketRowKeyExtractor extractor = new DynamicBucketRowKeyExtractor(table.schema());
        for (int id = 1; id <= 3; id++) {
            InternalRow row = GenericRow.of(1, id, (long) id);
            write(table, Pair.of(row, 0));
            extractor.setRecord(row);
            assertThat(
                            table.store()
                                    .newIndexFileHandler()
                                    .scanHashIndex(
                                            table.snapshotManager().latestSnapshot(),
                                            extractor.partition(),
                                            0)
                                    .get()
                                    .rowCount())
                    .isEqualTo(id);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"scan", "snapshot"})
    public void testSequenceNumberInitPreservesBucketAssignment(String mode) throws Exception {
        FileStoreTable table = createSequenceNumberTable(mode);
        writeWithNewBucketAssigner(
                table,
                GenericRow.of(1, 1, 1L),
                GenericRow.of(1, 2, 2L),
                GenericRow.of(1, 3, 3L),
                GenericRow.of(1, 4, 4L));
        writeWithNewBucketAssigner(table, GenericRow.of(1, 2, 20L), GenericRow.of(1, 4, 40L));
        writeWithNewBucketAssigner(table, GenericRow.of(1, 3, 300L));

        assertThat(
                        read(table).stream()
                                .map(
                                        row ->
                                                row.getInt(0)
                                                        + ","
                                                        + row.getInt(1)
                                                        + ","
                                                        + row.getLong(2))
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder("1,1,1", "1,2,20", "1,3,300", "1,4,40");
    }

    @ParameterizedTest
    @ValueSource(strings = {"scan", "snapshot"})
    public void testSnapshotSequenceNumberInitOverwrite(String initialMode) throws Exception {
        FileStoreTable table = createSequenceNumberTable(initialMode);
        write(table, Pair.of(GenericRow.of(1, 1, 1L), 0));
        table = table.copy(Collections.singletonMap("write.sequence-number-init-mode", "snapshot"));
        BatchWriteBuilder builder = table.newBatchWriteBuilder().withOverwrite();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            InternalRow row = GenericRow.of(1, 2, 2L);
            DynamicBucketRowKeyExtractor extractor =
                    new DynamicBucketRowKeyExtractor(table.schema());
            extractor.setRecord(row);
            AbstractFileStoreWrite<?> storeWrite =
                    (AbstractFileStoreWrite<?>) ((TableWriteImpl<?>) write).getWrite();
            assertThat(
                            storeWrite
                                    .createWriterContainer(extractor.partition(), 0)
                                    .dynamicBucketMaintainer
                                    .isEmpty())
                    .isTrue();
            write.write(row, 0);
            commit.commit(write.prepareCommit());
        }
        assertThat(read(table)).hasSize(1);
        DynamicBucketRowKeyExtractor extractor = new DynamicBucketRowKeyExtractor(table.schema());
        extractor.setRecord(GenericRow.of(1, 2, 2L));
        assertThat(
                        table.store()
                                .newIndexFileHandler()
                                .scanHashIndex(
                                        table.snapshotManager().latestSnapshot(),
                                        extractor.partition(),
                                        0)
                                .get()
                                .rowCount())
                .isEqualTo(1);
    }

    private FileStoreTable createSequenceNumberTable(String mode) throws Exception {
        catalog.createTable(
                identifier(),
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("id", DataTypes.INT())
                        .column("v", DataTypes.BIGINT())
                        .partitionKeys("pt")
                        .primaryKey("pt", "id")
                        .option("bucket", "-1")
                        .option("write-only", "true")
                        .option("write.sequence-number-init-mode", mode)
                        .option("dynamic-bucket.target-row-num", "2")
                        .build(),
                false);
        return getTableDefault();
    }

    private void writeWithNewBucketAssigner(FileStoreTable table, InternalRow... rows)
            throws Exception {
        HashBucketAssigner assigner =
                new HashBucketAssigner(
                        table.snapshotManager(),
                        commitUser,
                        table.store().newIndexFileHandler(),
                        1,
                        1,
                        0,
                        2,
                        -1);
        DynamicBucketRowKeyExtractor extractor = new DynamicBucketRowKeyExtractor(table.schema());
        writeWithBucketAssigner(
                table,
                row -> {
                    extractor.setRecord(row);
                    return assigner.assign(
                            extractor.partition(), extractor.trimmedPrimaryKey().hashCode());
                },
                rows);
    }

    @Test
    public void testOverwriteDynamicBucketTable() throws Exception {
        createTableDefault();

        commitDefault(writeDataDefault(100, 100));

        Table table = getTableDefault();
        BatchWriteBuilderImpl builder = (BatchWriteBuilderImpl) table.newBatchWriteBuilder();
        TableWriteImpl batchTableWrite = (TableWriteImpl) builder.withOverwrite().newWrite();
        AbstractFileStoreWrite<?> write = (AbstractFileStoreWrite<?>) (batchTableWrite.getWrite());
        write.withIgnorePreviousFiles(true);
        DynamicBucketIndexMaintainer indexMaintainer =
                write.createWriterContainer(BinaryRow.EMPTY_ROW, 0).dynamicBucketMaintainer;

        assertThat(indexMaintainer.isEmpty()).isTrue();
        Pair<InternalRow, Integer> rowWithBucket = data(0);
        batchTableWrite.write(rowWithBucket.getKey(), rowWithBucket.getValue());
        assertThat(
                        ((CommitMessageImpl) batchTableWrite.prepareCommit().get(0))
                                .newFilesIncrement()
                                .newIndexFiles()
                                .get(0)
                                .rowCount())
                .isEqualTo(1);
    }

    protected List<CommitMessage> writeDataDefault(int size, int times) throws Exception {
        List<CommitMessage> messages;
        Table table = getTableDefault();
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite batchTableWrite = builder.newWrite()) {
            for (int i = 0; i < times; i++) {
                for (int j = 0; j < size; j++) {
                    Pair<InternalRow, Integer> rowWithBucket = data(i);
                    batchTableWrite.write(rowWithBucket.getKey(), rowWithBucket.getValue());
                }
            }
            messages = batchTableWrite.prepareCommit();
        }

        return messages;
    }

    protected Schema schemaDefault() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.BIGINT());
        schemaBuilder.column("f1", DataTypes.BIGINT());
        schemaBuilder.column("f2", DataTypes.BIGINT());
        schemaBuilder.column("f3", DataTypes.BIGINT());
        schemaBuilder.option("bucket", "-1");
        schemaBuilder.option("scan.parallelism", "6");
        schemaBuilder.option("sink.parallelism", "3");
        schemaBuilder.option("dynamic-bucket.target-row-num", "100");
        schemaBuilder.primaryKey("f0");
        return schemaBuilder.build();
    }

    private static Pair<InternalRow, Integer> data(int bucket) {
        GenericRow row =
                GenericRow.of(
                        RANDOM.nextLong(),
                        (long) RANDOM.nextInt(10000),
                        (long) RANDOM.nextInt(10000),
                        (long) RANDOM.nextInt(10000));
        return Pair.of(row, bucket);
    }
}
