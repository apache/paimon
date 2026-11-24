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
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests validation of wholly uncommitted full-compaction checkpoint outputs. */
class CompactionChangelogRecoveryTest extends TableTestBase {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testUncommittedCompactionChangelog(boolean missing) throws Exception {
        FileStoreTable table = createTable();
        List<CommitMessage> messages = prepareCompaction(table);
        Path path = changelogPath(table, messages);
        if (missing) {
            assertThat(table.fileIO().delete(path, false)).isTrue();
        }
        assertThat(table.snapshotManager().latestSnapshotId()).isNull();

        try (StreamTableCommit commit = table.newCommit(commitUser)) {
            if (missing) {
                assertThatThrownBy(() -> commit.filterAndCommit(singletonMap(0L, messages)))
                        .hasMessageContaining("Cannot recover from this checkpoint")
                        .hasMessageContaining(path.getName());
                assertThat(table.snapshotManager().latestSnapshotId()).isNull();
            } else {
                assertThat(commit.filterAndCommit(singletonMap(0L, messages))).isEqualTo(1);
                StreamTableScan scan = table.newStreamScan();
                scan.restore(1L);
                try (CloseableIterator<InternalRow> rows =
                        table.newRead().createReader(scan.plan()).toCloseableIterator()) {
                    InternalRow row = rows.next();
                    assertThat(row.getInt(0)).isEqualTo(1);
                    assertThat(row.getInt(1)).isEqualTo(10);
                    assertThat(rows.hasNext()).isFalse();
                }
            }
        }
    }

    @Test
    void testCommittedIdentifierIsFilteredBeforeCheckingFiles() throws Exception {
        FileStoreTable table = createTable();
        List<CommitMessage> messages = prepareCompaction(table);
        try (StreamTableCommit commit = table.newCommit(commitUser)) {
            commit.commit(0L, messages);
        }
        long snapshot = table.snapshotManager().latestSnapshotId();
        assertThat(table.fileIO().delete(changelogPath(table, messages), false)).isTrue();
        try (StreamTableCommit recovered = table.newCommit(commitUser)) {
            assertThat(recovered.filterAndCommit(singletonMap(0L, messages))).isZero();
        }
        assertThat(table.snapshotManager().latestSnapshotId()).isEqualTo(snapshot);
    }

    private FileStoreTable createTable() throws Exception {
        catalog.createTable(
                identifier(),
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("v", DataTypes.INT())
                        .primaryKey("id")
                        .option(CoreOptions.BUCKET.key(), "1")
                        .option(CoreOptions.CHANGELOG_PRODUCER.key(), "full-compaction")
                        .build(),
                false);
        return (FileStoreTable) catalog.getTable(identifier());
    }

    private List<CommitMessage> prepareCompaction(FileStoreTable table) throws Exception {
        List<CommitMessage> messages;
        try (StreamTableWrite write = table.newWrite(commitUser)) {
            write.write(GenericRow.of(1, 10));
            write.compact(BinaryRow.EMPTY_ROW, 0, true);
            messages = write.prepareCommit(true, 0);
        }
        assertThat(messages).hasSize(1);
        return messages;
    }

    private Path changelogPath(FileStoreTable table, List<CommitMessage> messages) {
        CommitMessageImpl message = (CommitMessageImpl) messages.get(0);
        assertThat(message.compactIncrement().changelogFiles()).hasSize(1);
        DataFileMeta changelog = message.compactIncrement().changelogFiles().get(0);
        return changelog
                .collectFiles(
                        table.store()
                                .pathFactory()
                                .createDataFilePathFactory(message.partition(), message.bucket()))
                .get(0);
    }
}
