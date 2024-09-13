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

package org.apache.paimon.flink.action;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.fs.Path;
import org.apache.paimon.partition.file.SuccessFile;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link MarkPartitionDoneAction}. */
public class MarkPartitionDoneActionITCase extends ActionITCaseBase {

    private static final DataType[] FIELD_TYPES =
            new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.STRING(), DataTypes.INT()};

    private static final RowType ROW_TYPE =
            RowType.of(FIELD_TYPES, new String[] {"partKey0", "partKey1", "dt", "value"});

    private static Stream<Arguments> testArguments() {
        return Stream.of(
                Arguments.of(true, "action"),
                Arguments.of(false, "action"),
                Arguments.of(true, "procedure_indexed"),
                Arguments.of(false, "procedure_indexed"),
                Arguments.of(true, "procedure_named"),
                Arguments.of(false, "procedure_named"));
    }

    @ParameterizedTest
    @MethodSource("testArguments")
    public void testPartitionMarkDoneWithSinglePartitionKey(boolean hasPk, String invoker)
            throws Exception {
        FileStoreTable table = prepareTable(hasPk);

        switch (invoker) {
            case "action":
                createAction(
                                MarkPartitionDoneAction.class,
                                "mark_partition_done",
                                "--warehouse",
                                warehouse,
                                "--database",
                                database,
                                "--table",
                                tableName,
                                "--partition",
                                "partKey0=0")
                        .run();
                break;
            case "procedure_indexed":
                executeSQL(
                        String.format(
                                "CALL sys.mark_partition_done('%s.%s', 'partKey0 = 0')",
                                database, tableName));
                break;
            case "procedure_named":
                executeSQL(
                        String.format(
                                "CALL sys.mark_partition_done(`table` => '%s.%s', partitions => 'partKey0 = 0')",
                                database, tableName));
                break;
            default:
                throw new UnsupportedOperationException(invoker);
        }

        Path successPath = new Path(table.location(), "partKey0=0/_SUCCESS");
        SuccessFile successFile = SuccessFile.safelyFromPath(table.fileIO(), successPath);
        assertThat(successFile).isNotNull();
    }

    @ParameterizedTest
    @MethodSource("testArguments")
    public void testDropPartitionWithMultiplePartitionKey(boolean hasPk, String invoker)
            throws Exception {
        FileStoreTable table = prepareTable(hasPk);

        switch (invoker) {
            case "action":
                createAction(
                                MarkPartitionDoneAction.class,
                                "mark_partition_done",
                                "--warehouse",
                                warehouse,
                                "--database",
                                database,
                                "--table",
                                tableName,
                                "--partition",
                                "partKey0=0,partKey1=1",
                                "--partition",
                                "partKey0=1,partKey1=0")
                        .run();
                break;
            case "procedure_indexed":
                executeSQL(
                        String.format(
                                "CALL sys.mark_partition_done('%s.%s', 'partKey0=0,partKey1=1;partKey0=1,partKey1=0')",
                                database, tableName));
                break;
            case "procedure_named":
                executeSQL(
                        String.format(
                                "CALL sys.mark_partition_done(`table` => '%s.%s', partitions => 'partKey0=0,partKey1=1;partKey0=1,partKey1=0')",
                                database, tableName));
                break;
            default:
                throw new UnsupportedOperationException(invoker);
        }

        Path successPath1 = new Path(table.location(), "partKey0=0/partKey1=1/_SUCCESS");
        SuccessFile successFile1 = SuccessFile.safelyFromPath(table.fileIO(), successPath1);
        assertThat(successFile1).isNotNull();

        Path successPath2 = new Path(table.location(), "partKey0=1/partKey1=0/_SUCCESS");
        SuccessFile successFile2 = SuccessFile.safelyFromPath(table.fileIO(), successPath2);
        assertThat(successFile2).isNotNull();
    }

    private FileStoreTable prepareTable(boolean hasPk) throws Exception {
        FileStoreTable table =
                createFileStoreTable(
                        ROW_TYPE,
                        Arrays.asList("partKey0", "partKey1"),
                        hasPk
                                ? Arrays.asList("partKey0", "partKey1", "dt")
                                : Collections.emptyList(),
                        hasPk ? Collections.emptyList() : Collections.singletonList("dt"),
                        new HashMap<>());
        SnapshotManager snapshotManager = table.snapshotManager();
        StreamWriteBuilder streamWriteBuilder =
                table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = streamWriteBuilder.newWrite();
        commit = streamWriteBuilder.newCommit();

        // prepare data
        writeData(
                rowData(0, 0, BinaryString.fromString("2023-01-12"), 101),
                rowData(0, 0, BinaryString.fromString("2023-01-12"), 102),
                rowData(0, 0, BinaryString.fromString("2023-01-13"), 103));

        writeData(
                rowData(0, 1, BinaryString.fromString("2023-01-14"), 110),
                rowData(0, 1, BinaryString.fromString("2023-01-15"), 120),
                rowData(0, 1, BinaryString.fromString("2023-01-16"), 130));

        writeData(
                rowData(1, 0, BinaryString.fromString("2023-01-17"), 2),
                rowData(1, 0, BinaryString.fromString("2023-01-17"), 3),
                rowData(1, 0, BinaryString.fromString("2023-01-17"), 5));

        writeData(
                rowData(1, 1, BinaryString.fromString("2023-01-18"), 82),
                rowData(1, 1, BinaryString.fromString("2023-01-19"), 90),
                rowData(1, 1, BinaryString.fromString("2023-01-20"), 97));

        Snapshot snapshot = snapshotManager.snapshot(snapshotManager.latestSnapshotId());

        assertThat(snapshot.id()).isEqualTo(4);
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);

        return table;
    }
}
