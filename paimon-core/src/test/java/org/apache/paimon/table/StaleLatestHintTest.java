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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.HintFileUtils;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for a table whose LATEST hint is left behind while snapshots are expired. */
public class StaleLatestHintTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testCommitAfterLatestHintExpired() throws Exception {
        LatestHintSkippingFileIO fileIO = new LatestHintSkippingFileIO();
        Path tablePath = new Path(tempDir.toString());
        Schema schema =
                Schema.newBuilder()
                        .column("k", DataTypes.INT())
                        .column("v", DataTypes.INT())
                        .primaryKey("k")
                        .option("bucket", "1")
                        .option("snapshot.num-retained.min", "1")
                        .option("snapshot.num-retained.max", "3")
                        .build();
        TableSchema tableSchema =
                new FileSystemSchemaManager(fileIO, tablePath).createTable(schema);
        FileStoreTable table =
                FileStoreTableFactory.create(
                        fileIO, tablePath, tableSchema, CatalogEnvironment.empty());
        SnapshotManager snapshotManager = table.snapshotManager();

        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder();
        try (StreamTableWrite write = writeBuilder.newWrite();
                StreamTableCommit commit = writeBuilder.newCommit()) {
            long identifier = 0;
            write.write(GenericRow.of(0, 0));
            commit.commit(identifier, write.prepareCommit(false, identifier++));
            assertThat(snapshotManager.latestSnapshotId()).isEqualTo(1);

            // snapshot files are committed but the LATEST hint stays at 1, and expiration
            // removes the hinted snapshot together with the one after it
            fileIO.skipLatestHint = true;
            for (int i = 1; i <= 5; i++) {
                write.write(GenericRow.of(i, i));
                commit.commit(identifier, write.prepareCommit(false, identifier++));
            }
            assertThat(snapshotManager.snapshotExists(1)).isFalse();
            assertThat(snapshotManager.snapshotExists(2)).isFalse();
            long latestBeforeRecovery = listLatestSnapshotId(fileIO, snapshotManager);
            assertThat(snapshotManager.latestSnapshotId()).isEqualTo(latestBeforeRecovery);

            // once hint writes recover, commits should go on and refresh the hint
            fileIO.skipLatestHint = false;
            write.write(GenericRow.of(6, 6));
            commit.commit(identifier, write.prepareCommit(false, identifier));
            long latest = listLatestSnapshotId(fileIO, snapshotManager);
            assertThat(latest).isGreaterThan(latestBeforeRecovery);
            assertThat(snapshotManager.latestSnapshotId()).isEqualTo(latest);
            assertThat(
                            HintFileUtils.readHint(
                                    fileIO,
                                    HintFileUtils.LATEST,
                                    snapshotManager.snapshotDirectory()))
                    .isEqualTo(latest);
            assertThat(table.newReadBuilder().newScan().plan().splits()).isNotEmpty();
        }
    }

    private static long listLatestSnapshotId(LocalFileIO fileIO, SnapshotManager snapshotManager)
            throws IOException {
        return HintFileUtils.findByListFiles(
                fileIO, Math::max, snapshotManager.snapshotDirectory(), "snapshot-");
    }

    /** A {@link LocalFileIO} which can silently skip writing the LATEST hint. */
    private static class LatestHintSkippingFileIO extends LocalFileIO {

        private volatile boolean skipLatestHint = false;

        @Override
        public void overwriteHintFile(Path path, String content) throws IOException {
            if (skipLatestHint && path.getName().equals("LATEST")) {
                return;
            }
            super.overwriteHintFile(path, content);
        }
    }
}
