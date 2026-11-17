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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for routing partial-column writes to Data Evolution tables. */
class DataEvolutionPartialWriteTest extends TableTestBase {

    @ParameterizedTest
    @ValueSource(strings = {"id,pt", "pt,id", "pt,id,value", "id,value,pt"})
    void testPartitionUsesWriteType(String columns) throws Exception {
        catalog.createTable(
                identifier(),
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.INT())
                        .column("pt", DataTypes.STRING())
                        .partitionKeys("pt")
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .build(),
                false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        List<String> fields = Arrays.asList(columns.split(","));
        RowType inputType = table.rowType().project(fields);
        GenericRow input = new GenericRow(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            input.setField(
                    i,
                    "pt".equals(fields.get(i))
                            ? BinaryString.fromString("p1")
                            : "id".equals(fields.get(i)) ? 1 : 10);
        }
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite().withWriteType(inputType);
                BatchTableCommit commit = builder.newCommit()) {
            write.write(input);
            assertThat(write.getPartition(input).getString(0).toString()).isEqualTo("p1");
            commit.commit(write.prepareCommit());
        }

        List<Split> splits = table.newScan().plan().splits();
        assertThat(splits).hasSize(1);
        assertThat(((DataSplit) splits.get(0)).partition().getString(0).toString()).isEqualTo("p1");
        try (RecordReaderIterator<InternalRow> reader =
                new RecordReaderIterator<>(table.newRead().createReader(splits))) {
            assertThat(reader.hasNext()).isTrue();
            InternalRow row = reader.next();
            assertThat(row.getInt(0)).isEqualTo(1);
            if (fields.contains("value")) {
                assertThat(row.getInt(1)).isEqualTo(10);
            } else {
                assertThat(row.isNullAt(1)).isTrue();
            }
            assertThat(row.getString(2).toString()).isEqualTo("p1");
            assertThat(reader.hasNext()).isFalse();
        }
    }
}
