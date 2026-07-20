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
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.DataFormatTestUtil;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.InnerTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.table.SimpleTableTestBase.getResult;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/** Unit tests for overwrite table. */
public class OverwriteTableTest extends TableTestBase {

    @ParameterizedTest(name = "dynamic = {0}, partition={2}")
    @MethodSource("overwriteTestData")
    public void testOverwriteAppend(
            boolean dynamicPartitionOverwrite,
            List<InternalRow> overwriteData,
            Map<String, String> overwritePartition,
            List<String> expected)
            throws Exception {
        innerTestOverwrite(
                false,
                dynamicPartitionOverwrite,
                overwriteData,
                Collections.singletonList(overwritePartition),
                expected);
    }

    @ParameterizedTest(name = "dynamic = {0}, partition={2}")
    @MethodSource("overwriteTestData")
    public void testOverwritePrimaryKey(
            boolean dynamicPartitionOverwrite,
            List<InternalRow> overwriteData,
            Map<String, String> overwritePartition,
            List<String> expected)
            throws Exception {
        innerTestOverwrite(
                true,
                dynamicPartitionOverwrite,
                overwriteData,
                Collections.singletonList(overwritePartition),
                expected);
    }

    @Test
    public void testOverwriteMultiplePartitions() throws Exception {
        innerTestOverwrite(
                false,
                false,
                Arrays.asList(overwriteRow(9, 1, "A", "New"), overwriteRow(10, 2, "B", "Data")),
                Arrays.asList(
                        Collections.singletonMap("pt1", "A"), Collections.singletonMap("pt0", "2")),
                Arrays.asList(
                        "4, 1, B, To",
                        "5, 1, B, Apache",
                        "6, 1, B, Paimon",
                        "9, 1, A, New",
                        "10, 2, B, Data"));
    }

    private void innerTestOverwrite(
            boolean withPrimaryKey,
            boolean dynamicPartitionOverwrite,
            List<InternalRow> overwriteData,
            List<Map<String, String>> overwritePartitions,
            List<String> expected)
            throws Exception {
        Identifier identifier = identifier("T");
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("pt0", DataTypes.INT())
                        .column("pt1", DataTypes.STRING())
                        .column("v", DataTypes.STRING())
                        .partitionKeys("pt0", "pt1");
        if (withPrimaryKey) {
            builder = builder.primaryKey("pk", "pt0", "pt1");
            builder.option(BUCKET.key(), "1");
        }
        catalog.createTable(identifier, builder.build(), true);
        Table originTable = catalog.getTable(identifier("T"));
        FileStoreTable table = (FileStoreTable) originTable;
        if (!dynamicPartitionOverwrite) {
            table =
                    table.copy(
                            Collections.singletonMap(
                                    CoreOptions.DYNAMIC_PARTITION_OVERWRITE.key(), "false"));
        }

        // prepare data
        // (1, 1, 'A', 'Hi'), (2, 1, 'A', 'Hello'), (3, 1, 'A', 'World'),
        // (4, 1, 'B', 'To'), (5, 1, 'B', 'Apache'), (6, 1, 'B', 'Paimon')
        // (7, 2, 'A', 'Test')
        // (8, 2, 'B', 'Case')
        write(
                table,
                overwriteRow(1, 1, "A", "Hi"),
                overwriteRow(2, 1, "A", "Hello"),
                overwriteRow(3, 1, "A", "World"),
                overwriteRow(4, 1, "B", "To"),
                overwriteRow(5, 1, "B", "Apache"),
                overwriteRow(6, 1, "B", "Paimon"),
                overwriteRow(7, 2, "A", "Test"),
                overwriteRow(8, 2, "B", "Case"));

        // overwrite data
        try (StreamTableWrite write = table.newWrite(commitUser).withIgnorePreviousFiles(true);
                InnerTableCommit commit = table.newCommit(commitUser)) {
            for (InternalRow row : overwriteData) {
                write.write(row);
            }
            commit.withOverwrite(overwritePartitions).commit(1, write.prepareCommit(true, 1));
        }

        // validate
        List<Split> splits = new ArrayList<>(table.newSnapshotReader().read().dataSplits());
        TableRead read = table.newRead();
        assertThat(
                        getResult(
                                read,
                                splits,
                                row ->
                                        DataFormatTestUtil.toStringNoRowKind(
                                                row, originTable.rowType())))
                .hasSameElementsAs(expected);
    }

    private static List<Arguments> overwriteTestData() {
        // dynamic, overwrite data, overwrite partition, expected
        return Arrays.asList(
                // nothing happen
                arguments(
                        true,
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        Arrays.asList(
                                "1, 1, A, Hi",
                                "2, 1, A, Hello",
                                "3, 1, A, World",
                                "4, 1, B, To",
                                "5, 1, B, Apache",
                                "6, 1, B, Paimon",
                                "7, 2, A, Test",
                                "8, 2, B, Case")),
                // delete all data
                arguments(
                        false,
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        Collections.emptyList()),
                // specify one partition key
                arguments(
                        true,
                        Arrays.asList(
                                overwriteRow(1, 1, "A", "Where"), overwriteRow(2, 1, "A", "When")),
                        Collections.singletonMap("pt0", "1"),
                        Arrays.asList(
                                "1, 1, A, Where",
                                "2, 1, A, When",
                                "4, 1, B, To",
                                "5, 1, B, Apache",
                                "6, 1, B, Paimon",
                                "7, 2, A, Test",
                                "8, 2, B, Case")),
                arguments(
                        false,
                        Arrays.asList(
                                overwriteRow(1, 1, "A", "Where"), overwriteRow(2, 1, "A", "When")),
                        Collections.singletonMap("pt0", "1"),
                        Arrays.asList(
                                "1, 1, A, Where",
                                "2, 1, A, When",
                                "7, 2, A, Test",
                                "8, 2, B, Case")),
                // all dynamic
                arguments(
                        true,
                        Arrays.asList(
                                overwriteRow(4, 1, "B", "Where"),
                                overwriteRow(5, 1, "B", "When"),
                                overwriteRow(10, 2, "A", "Static"),
                                overwriteRow(11, 2, "A", "Dynamic")),
                        Collections.emptyMap(),
                        Arrays.asList(
                                "1, 1, A, Hi",
                                "2, 1, A, Hello",
                                "3, 1, A, World",
                                "4, 1, B, Where",
                                "5, 1, B, When",
                                "10, 2, A, Static",
                                "11, 2, A, Dynamic",
                                "8, 2, B, Case")),
                arguments(
                        false,
                        Arrays.asList(
                                overwriteRow(4, 1, "B", "Where"),
                                overwriteRow(5, 1, "B", "When"),
                                overwriteRow(10, 2, "A", "Static"),
                                overwriteRow(11, 2, "A", "Dynamic")),
                        Collections.emptyMap(),
                        Arrays.asList(
                                "4, 1, B, Where",
                                "5, 1, B, When",
                                "10, 2, A, Static",
                                "11, 2, A, Dynamic")));
    }

    private static InternalRow overwriteRow(Object... values) {
        return GenericRow.of(
                values[0],
                values[1],
                BinaryString.fromString((String) values[2]),
                BinaryString.fromString((String) values[3]));
    }
}
