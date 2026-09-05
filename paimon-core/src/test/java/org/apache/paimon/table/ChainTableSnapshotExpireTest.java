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
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests snapshot expiration maintenance for chain table branches. */
public class ChainTableSnapshotExpireTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testDeltaCommitExpiresSnapshotBranchSnapshots() throws Exception {
        Path tablePath = new Path(tempDir.toUri().toString(), "chain_snapshot_expire");
        createChainTable(tablePath);

        FileStoreTable mainTable = loadTable(tablePath);
        FileStoreTable snapshotTable = mainTable.switchToBranch("snapshot");
        FileStoreTable deltaTable = mainTable.switchToBranch("delta");
        String commitUser = UUID.randomUUID().toString();

        write(snapshotTable, commitUser, "20250101", "v1");
        write(snapshotTable, commitUser, "20250201", "v2");
        write(snapshotTable, commitUser, "20250301", "v3");

        snapshotTable = loadTable(tablePath).switchToBranch("snapshot");
        assertThat(snapshotTable.snapshotManager().snapshotCount()).isEqualTo(3);

        Map<String, String> expireOptions = new HashMap<>();
        expireOptions.put(CoreOptions.WRITE_ONLY.key(), "false");
        expireOptions.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MIN.key(), "1");
        expireOptions.put(CoreOptions.SNAPSHOT_NUM_RETAINED_MAX.key(), "1");
        expireOptions.put(CoreOptions.SNAPSHOT_TIME_RETAINED.key(), "0 ms");
        expireOptions.put(CoreOptions.SNAPSHOT_EXPIRE_EXECUTION_MODE.key(), "sync");
        deltaTable = deltaTable.copy(expireOptions);

        write(deltaTable, commitUser, "20250401", "v4");

        snapshotTable = loadTable(tablePath).switchToBranch("snapshot");
        assertThat(snapshotTable.snapshotManager().snapshotCount()).isEqualTo(1);
    }

    private void createChainTable(Path tablePath) throws Exception {
        LocalFileIO fileIO = LocalFileIO.create();
        SchemaManager schemaManager = new FileSystemSchemaManager(fileIO, tablePath);

        Map<String, String> options = new HashMap<>();
        options.put(CoreOptions.BUCKET.key(), "1");
        options.put(CoreOptions.MERGE_ENGINE.key(), "deduplicate");
        options.put(CoreOptions.SEQUENCE_FIELD.key(), "v");

        Schema schema =
                new Schema(
                        RowType.of(
                                        new org.apache.paimon.types.DataType[] {
                                            DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING()
                                        },
                                        new String[] {"dt", "pk", "v"})
                                .getFields(),
                        Collections.singletonList("dt"),
                        Arrays.asList("pk", "dt"),
                        options,
                        "");
        schemaManager.createTable(schema);

        FileStoreTable mainTable = loadTable(tablePath);
        mainTable.createBranch("snapshot");
        mainTable.createBranch("delta");

        List<SchemaChange> chainOptions =
                Arrays.asList(
                        SchemaChange.setOption(CoreOptions.CHAIN_TABLE_ENABLED.key(), "true"),
                        SchemaChange.setOption(
                                CoreOptions.SCAN_FALLBACK_SNAPSHOT_BRANCH.key(), "snapshot"),
                        SchemaChange.setOption(CoreOptions.SCAN_FALLBACK_DELTA_BRANCH.key(), "delta"),
                        SchemaChange.setOption(
                                CoreOptions.PARTITION_TIMESTAMP_PATTERN.key(), "$dt"),
                        SchemaChange.setOption(
                                CoreOptions.PARTITION_TIMESTAMP_FORMATTER.key(), "yyyyMMdd"));
        schemaManager.commitChanges(chainOptions);
        new FileSystemSchemaManager(fileIO, tablePath, "snapshot").commitChanges(chainOptions);
        new FileSystemSchemaManager(fileIO, tablePath, "delta").commitChanges(chainOptions);
    }

    private FileStoreTable loadTable(Path tablePath) {
        LocalFileIO fileIO = LocalFileIO.create();
        Options options = new Options();
        options.set(CoreOptions.PATH, tablePath.toString());
        String branchName = CoreOptions.branch(options.toMap());
        TableSchema tableSchema =
                new FileSystemSchemaManager(fileIO, tablePath, branchName).latest().get();
        return FileStoreTableFactory.create(
                fileIO, tablePath, tableSchema, CatalogEnvironment.empty());
    }

    private void write(FileStoreTable table, String commitUser, String dt, String value)
            throws Exception {
        StreamTableWrite write = table.newWrite(commitUser);
        write.write(
                GenericRow.of(
                        BinaryString.fromString(dt),
                        BinaryString.fromString(value),
                        BinaryString.fromString(value)));
        try (TableCommitImpl commit = table.newCommit(commitUser)) {
            List<CommitMessage> commitMessages = write.prepareCommit(true, Long.MAX_VALUE);
            commit.commit(Long.MAX_VALUE, commitMessages);
        }
        write.close();
    }
}
