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
import org.apache.paimon.index.HashIndexMaintainer;
import org.apache.paimon.operation.AbstractFileStoreWrite;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.BatchWriteBuilderImpl;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.TableWriteApi;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.Pair;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/** Tests for Dynamic Bucket Table. */
public class DynamicBucketTableTest extends TableTestBase {

    @Test
    public void testOverwriteDynamicBucketTable() throws Exception {
        createTableDefault();

        commitDefault(writeDataDefault(100, 100));

        Table table = getTableDefault();
        BatchWriteBuilderImpl builder = (BatchWriteBuilderImpl) table.newBatchWriteBuilder();
        TableWriteApi batchTableWrite = (TableWriteApi) builder.withOverwrite().newWrite();
        HashIndexMaintainer indexMaintainer =
                (HashIndexMaintainer)
                        ((AbstractFileStoreWrite<?>) (batchTableWrite.getWrite()))
                                .createWriterContainer(BinaryRow.EMPTY_ROW, 0, true)
                                .indexMaintainer;

        Assertions.assertThat(indexMaintainer.isEmpty()).isTrue();
        Pair<InternalRow, Integer> rowWithBucket = data(0);
        batchTableWrite.write(rowWithBucket.getKey(), rowWithBucket.getValue());
        Assertions.assertThat(
                        ((CommitMessageImpl) batchTableWrite.prepareCommit().get(0))
                                .indexIncrement()
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
