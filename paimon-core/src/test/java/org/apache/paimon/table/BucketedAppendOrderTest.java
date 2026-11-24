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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for preserving the row order of bucketed append tables. */
class BucketedAppendOrderTest extends TableTestBase {

    @Test
    void testFullCompactionAfterAutomaticCompactionPreservesOrder() throws Exception {
        catalog.createTable(
                identifier(),
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.BUCKET_KEY.key(), "id")
                        .option(CoreOptions.BUCKET_APPEND_ORDERED.key(), "true")
                        .option(CoreOptions.TARGET_FILE_ROW_NUM.key(), "1")
                        .option(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "3")
                        .option(CoreOptions.ASYNC_FILE_WRITE.key(), "false")
                        .build(),
                false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        StreamWriteBuilder builder = table.newStreamWriteBuilder();
        List<Integer> expected = Arrays.asList(0, 1, 2, 3, 4);
        try (StreamTableWrite write = builder.newWrite();
                StreamTableCommit commit = builder.newCommit()) {
            for (int id : expected) {
                write.write(GenericRow.of(id));
            }
            // Compact the first three files automatically, leaving two later files in the queue.
            commit.commit(0, write.prepareCommit(true, 0));
            assertThat(table.store().newScan().plan().files()).hasSize(3);
            assertThat(readInitialStream(table)).containsExactlyElementsOf(expected);

            write.compact(BinaryRow.EMPTY_ROW, 0, true);
            commit.commit(1, write.prepareCommit(true, 1));

            assertThat(table.store().newScan().plan().files()).hasSize(1);
            assertThat(readInitialStream(table)).containsExactlyElementsOf(expected);
        }
    }

    private List<Integer> readInitialStream(FileStoreTable table) throws Exception {
        ReadBuilder builder = table.newReadBuilder();
        List<Integer> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                builder.newRead().createReader(builder.newStreamScan().plan())) {
            reader.forEachRemaining(row -> rows.add(row.getInt(0)));
        }
        return rows;
    }
}
