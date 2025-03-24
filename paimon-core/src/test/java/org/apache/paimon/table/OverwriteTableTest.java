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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.table.SimpleTableTestBase.getResult;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/** Unit tests for overwrite table. */
public class OverwriteTableTest extends TableTestBase {

    private Table bucketsTable;

    @BeforeEach
    public void before() throws Exception {
        Identifier identifier = identifier("T");
        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("pt0", DataTypes.INT())
                        .column("pt1", DataTypes.STRING())
                        .column("v", DataTypes.STRING())
                        .partitionKeys("pt0", "pt1")
                        .build();
        catalog.createTable(identifier, schema, true);
    }

    @ParameterizedTest(name = "dynamic = {0}, partition={2}")
    @MethodSource("overwriteTestData")
    public void testOverwriteNothing(
            boolean dynamicPartitionOverwrite,
            List<InternalRow> overwriteData,
            Map<String, String> overwritePartition,
            List<String> expected)
            throws Exception {
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
        try (StreamTableWrite write = table.newWrite(commitUser);
                InnerTableCommit commit = table.newCommit(commitUser)) {
            write.write(overwriteRow(1, 1, "A", "Hi"));
            write.write(overwriteRow(2, 1, "A", "Hello"));
            write.write(overwriteRow(3, 1, "A", "World"));
            write.write(overwriteRow(4, 1, "B", "To"));
            write.write(overwriteRow(5, 1, "B", "Apache"));
            write.write(overwriteRow(6, 1, "B", "Paimon"));
            write.write(overwriteRow(7, 2, "A", "Test"));
            write.write(overwriteRow(8, 2, "B", "Case"));
            commit.commit(0, write.prepareCommit(true, 0));
        }

        // overwrite data
        try (StreamTableWrite write = table.newWrite(commitUser).withIgnorePreviousFiles(true);
                InnerTableCommit commit = table.newCommit(commitUser)) {
            for (InternalRow row : overwriteData) {
                write.write(row);
            }
            commit.withOverwrite(overwritePartition).commit(1, write.prepareCommit(true, 1));
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
