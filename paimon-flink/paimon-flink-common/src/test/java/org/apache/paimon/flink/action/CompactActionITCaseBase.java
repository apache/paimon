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
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

/** Base IT cases for {@link CompactAction} and {@link CompactDatabaseAction} . */
public class CompactActionITCaseBase extends ActionITCaseBase {

    protected static final DataType[] FIELD_TYPES =
            new DataType[] {DataTypes.INT(), DataTypes.INT(), DataTypes.INT(), DataTypes.STRING()};

    protected static final RowType ROW_TYPE =
            RowType.of(FIELD_TYPES, new String[] {"k", "v", "hh", "dt"});

    protected void validateResult(
            FileStoreTable table,
            RowType rowType,
            StreamTableScan scan,
            List<String> expected,
            long timeout)
            throws Exception {
        List<String> actual = new ArrayList<>();
        long start = System.currentTimeMillis();
        while (actual.size() != expected.size()) {
            TableScan.Plan plan = scan.plan();
            actual.addAll(getResult(table.newReadBuilder().newRead(), plan.splits(), rowType));

            if (System.currentTimeMillis() - start > timeout) {
                break;
            }
        }
        if (actual.size() != expected.size()) {
            throw new TimeoutException(
                    String.format(
                            "Cannot collect %s records in %s milliseconds.",
                            expected.size(), timeout));
        }
        actual.sort(String::compareTo);
        assertThat(actual).isEqualTo(expected);
    }

    protected void checkFileAndRowSize(
            FileStoreTable table, Long expectedSnapshotId, Long timeout, int fileNum, long rowCount)
            throws Exception {
        SnapshotManager snapshotManager = table.snapshotManager();
        FileStoreScan scan = table.store().newScan();

        long start = System.currentTimeMillis();
        while (!Objects.equals(snapshotManager.latestSnapshotId(), expectedSnapshotId)) {
            Thread.sleep(500);
            if (System.currentTimeMillis() - start > timeout) {
                throw new RuntimeException("can't wait for a compaction.");
            }
        }

        List<ManifestEntry> files =
                scan.withSnapshot(snapshotManager.latestSnapshotId()).plan().files(FileKind.ADD);
        long count = 0;
        for (ManifestEntry file : files) {
            count += file.file().rowCount();
        }
        assertThat(files.size()).isEqualTo(fileNum);
        assertThat(count).isEqualTo(rowCount);
    }

    protected void checkLatestSnapshot(
            FileStoreTable table, long snapshotId, Snapshot.CommitKind commitKind) {
        SnapshotManager snapshotManager = table.snapshotManager();
        Snapshot snapshot = snapshotManager.snapshot(snapshotManager.latestSnapshotId());
        assertThat(snapshot.id()).isEqualTo(snapshotId);
        assertThat(snapshot.commitKind()).isEqualTo(commitKind);
    }

    protected FileStoreTable prepareTable(
            List<String> partitionKeys,
            List<String> primaryKeys,
            List<String> bucketKey,
            Map<String, String> tableOptions)
            throws Exception {
        FileStoreTable table =
                createFileStoreTable(ROW_TYPE, partitionKeys, primaryKeys, bucketKey, tableOptions);

        StreamWriteBuilder streamWriteBuilder =
                table.newStreamWriteBuilder().withCommitUser(commitUser);
        write = streamWriteBuilder.newWrite();
        commit = streamWriteBuilder.newCommit();

        return table;
    }
}
