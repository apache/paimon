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

package org.apache.paimon.table.source.snapshot;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.table.system.ModifiedPartitionBucketTable;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ContinuousAppendAndCompactFollowUpScanner}. */
public class ContinuousAppendAndCompactFollowUpScannerTest extends ScannerTestBase {

    private static final RowType ROW_TYPE =
            RowType.of(
                    new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.BIGINT()},
                    new String[] {"a", "b", "c"});

    private final DataFileMetaSerializer dataFileMetaSerializer = new DataFileMetaSerializer();

    @Test
    public void testScan() throws Exception {
        SnapshotManager snapshotManager = table.snapshotManager();
        StreamTableWrite write = table.newWrite(commitUser);
        StreamTableCommit commit = table.newCommit(commitUser);

        write.write(rowData(1, 10, 100L));
        write.write(rowData(1, 20, 200L));
        write.write(rowData(2, 40, 400L));
        commit.commit(0, write.prepareCommit(true, 0));

        List<CommitMessage> messageList = new ArrayList<>();
        write.write(rowData(1, 10, 100L));
        write.write(rowData(1, 20, 200L));
        write.write(rowData(2, 40, 400L));
        messageList.addAll(write.prepareCommit(true, 1));
        write.write(rowData(1, 10, 100L));
        write.write(rowData(1, 20, 200L));
        messageList.addAll(write.prepareCommit(true, 1));
        commit.commit(1, messageList);

        write.compact(binaryRow(1), 0, true);
        commit.commit(2, write.prepareCommit(true, 2));

        write.close();
        commit.close();

        ModifiedPartitionBucketTable bucketsTable = new ModifiedPartitionBucketTable(table, true);
        TableRead read = bucketsTable.newRead();
        ContinuousAppendAndCompactFollowUpScanner scanner =
                new ContinuousAppendAndCompactFollowUpScanner();

        Snapshot snapshot = snapshotManager.snapshot(1);
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);
        assertThat(scanner.shouldScanSnapshot(snapshot)).isTrue();
        TableScan.Plan plan = scanner.scan(snapshot, snapshotReader);
        assertThat(getResult(read, plan.splits()))
                .hasSameElementsAs(Arrays.asList("+I 1|1|0|1", "+I 1|2|0|1"));

        snapshot = snapshotManager.snapshot(2);
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);
        assertThat(scanner.shouldScanSnapshot(snapshot)).isTrue();
        plan = scanner.scan(snapshot, snapshotReader);
        assertThat(getResult(read, plan.splits()))
                .hasSameElementsAs(Arrays.asList("+I 2|1|0|2", "+I 2|2|0|1"));

        snapshot = snapshotManager.snapshot(3);
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);
        assertThat(scanner.shouldScanSnapshot(snapshot)).isTrue();
        plan = scanner.scan(snapshot, snapshotReader);
        assertThat(getResult(read, plan.splits())).hasSameElementsAs(Arrays.asList("+I 3|1|0|1"));
    }

    @Override
    protected String rowDataToString(InternalRow rowData) {
        int numFiles;
        try {
            numFiles = dataFileMetaSerializer.deserializeList(rowData.getBinary(3)).size();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return String.format(
                "%s %d|%d|%d|%d",
                rowData.getRowKind().shortString(),
                rowData.getLong(0),
                deserializeBinaryRow(rowData.getBinary(1)).getInt(0),
                rowData.getInt(2),
                numFiles);
    }

    @Override
    protected FileStoreTable createFileStoreTable(Options conf) throws Exception {
        SchemaManager schemaManager = new SchemaManager(fileIO, tablePath);
        TableSchema tableSchema =
                schemaManager.createTable(
                        new Schema(
                                ROW_TYPE.getFields(),
                                Collections.singletonList("a"),
                                Collections.emptyList(),
                                conf.toMap(),
                                ""));
        return FileStoreTableFactory.create(
                fileIO, tablePath, tableSchema, conf, CatalogEnvironment.empty());
    }
}
