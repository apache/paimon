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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.TableCommitApi;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.paimon.CoreOptions.PARTITION_EXPIRATION_CHECK_INTERVAL;
import static org.apache.paimon.CoreOptions.PARTITION_EXPIRATION_TIME;
import static org.apache.paimon.CoreOptions.PARTITION_TIMESTAMP_FORMATTER;
import static org.apache.paimon.CoreOptions.WRITE_ONLY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link PartitionExpire}. */
public class PartitionExpireTest {

    @TempDir java.nio.file.Path tempDir;

    private Path path;
    private FileStoreTable table;

    @BeforeEach
    public void beforeEach() {
        path = new Path(tempDir.toUri());
    }

    @Test
    public void testNonPartitionedTable() {
        SchemaManager schemaManager = new SchemaManager(LocalFileIO.create(), path);
        assertThatThrownBy(
                        () ->
                                schemaManager.createTable(
                                        new Schema(
                                                RowType.of(VarCharType.STRING_TYPE).getFields(),
                                                Collections.emptyList(),
                                                Collections.emptyList(),
                                                Collections.singletonMap(
                                                        PARTITION_EXPIRATION_TIME.key(), "1 d"),
                                                "")))
                .hasMessageContaining(
                        "Can not set 'partition.expiration-time' for non-partitioned table");
    }

    @Test
    public void testIllegalPartition() throws Exception {
        SchemaManager schemaManager = new SchemaManager(LocalFileIO.create(), path);
        schemaManager.createTable(
                new Schema(
                        RowType.of(VarCharType.STRING_TYPE, VarCharType.STRING_TYPE).getFields(),
                        Collections.singletonList("f0"),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        ""));
        table = FileStoreTableFactory.create(LocalFileIO.create(), path);
        write("20230101", "11");
        write("abcd", "12");
        write("20230101", "12");
        write("20230103", "31");
        write("20230103", "32");
        write("20230105", "51");
        PartitionExpire expire = newExpire();
        expire.setLastCheck(date(1));
        Assertions.assertDoesNotThrow(() -> expire.expire(date(8), Long.MAX_VALUE));
        assertThat(read()).containsExactlyInAnyOrder("abcd:12");
    }

    @Test
    public void test() throws Exception {
        SchemaManager schemaManager = new SchemaManager(LocalFileIO.create(), path);
        schemaManager.createTable(
                new Schema(
                        RowType.of(VarCharType.STRING_TYPE, VarCharType.STRING_TYPE).getFields(),
                        Collections.singletonList("f0"),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        ""));
        table = FileStoreTableFactory.create(LocalFileIO.create(), path);

        write("20230101", "11");
        write("20230101", "12");
        write("20230103", "31");
        write("20230103", "32");
        write("20230105", "51");

        PartitionExpire expire = newExpire();
        expire.setLastCheck(date(1));

        expire.expire(date(3), Long.MAX_VALUE);
        assertThat(read())
                .containsExactlyInAnyOrder(
                        "20230101:11", "20230101:12", "20230103:31", "20230103:32", "20230105:51");

        expire.expire(date(5), Long.MAX_VALUE);
        assertThat(read()).containsExactlyInAnyOrder("20230103:31", "20230103:32", "20230105:51");

        // PARTITION_EXPIRATION_INTERVAL not trigger
        expire.expire(date(6), Long.MAX_VALUE);
        assertThat(read()).containsExactlyInAnyOrder("20230103:31", "20230103:32", "20230105:51");

        expire.expire(date(8), Long.MAX_VALUE);
        assertThat(read()).isEmpty();
    }

    @Test
    public void testFilterCommittedAfterExpiring() throws Exception {
        SchemaManager schemaManager = new SchemaManager(LocalFileIO.create(), path);
        schemaManager.createTable(
                new Schema(
                        RowType.of(VarCharType.STRING_TYPE, VarCharType.STRING_TYPE).getFields(),
                        Collections.singletonList("f0"),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        ""));

        table = FileStoreTableFactory.create(LocalFileIO.create(), path);
        // disable compaction and snapshot expiration
        table = table.copy(Collections.singletonMap(WRITE_ONLY.key(), "true"));
        String commitUser = UUID.randomUUID().toString();
        ThreadLocalRandom random = ThreadLocalRandom.current();

        // prepare commits
        LocalDate now = LocalDate.now();
        int preparedCommits = random.nextInt(20, 30);

        List<List<CommitMessage>> commitMessages = new ArrayList<>();
        for (int i = 0; i < preparedCommits; i++) {
            // ensure the partition will be expired
            String f0 =
                    now.minus(random.nextInt(10), ChronoUnit.DAYS)
                            .format(DateTimeFormatter.ofPattern("yyyyMMdd"));
            String f1 = String.valueOf(random.nextInt(25));
            StreamTableWrite write = table.newWrite(commitUser);
            write.write(GenericRow.of(BinaryString.fromString(f0), BinaryString.fromString(f1)));
            commitMessages.add(write.prepareCommit(false, i));
        }

        // commit a part of data and trigger partition expire
        int successCommits = random.nextInt(preparedCommits / 4, preparedCommits / 2);
        StreamTableCommit commit = table.newCommit(commitUser);
        for (int i = 0; i < successCommits - 2; i++) {
            commit.commit(i, commitMessages.get(i));
        }

        // we need two commits to trigger partition expire
        // the first commit will set the last check time to now
        // the second commit will do the partition expire
        Map<String, String> options = new HashMap<>();
        options.put(WRITE_ONLY.key(), "false");
        options.put(PARTITION_EXPIRATION_TIME.key(), "1 d");
        options.put(PARTITION_EXPIRATION_CHECK_INTERVAL.key(), "5 s");
        options.put(PARTITION_TIMESTAMP_FORMATTER.key(), "yyyyMMdd");
        table = table.copy(options);
        commit.close();
        commit = table.newCommit(commitUser);
        commit.commit(successCommits - 2, commitMessages.get(successCommits - 2));
        Thread.sleep(5000);
        commit.commit(successCommits - 1, commitMessages.get(successCommits - 1));

        // check whether partition expire is triggered
        SnapshotManager snapshotManager = table.snapshotManager();
        Snapshot snapshot = snapshotManager.latestSnapshot();
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.OVERWRITE);

        // filterAndCommit
        Map<Long, List<CommitMessage>> allCommits = new HashMap<>();
        for (int i = 0; i < preparedCommits; i++) {
            allCommits.put((long) i, commitMessages.get(i));
        }

        // no exception here
        commit.filterAndCommit(allCommits);
        commit.close();

        // check commit last
        assertThat(snapshotManager.latestSnapshot().commitIdentifier())
                .isEqualTo(allCommits.size() - 1);
    }

    private List<String> read() throws IOException {
        List<String> ret = new ArrayList<>();
        table.newRead()
                .createReader(table.newScan().plan().splits())
                .forEachRemaining(row -> ret.add(row.getString(0) + ":" + row.getString(1)));
        return ret;
    }

    private LocalDateTime date(int day) {
        return LocalDateTime.of(LocalDate.of(2023, 1, day), LocalTime.MIN);
    }

    private void write(String f0, String f1) throws Exception {
        StreamTableWrite write =
                table.copy(Collections.singletonMap(WRITE_ONLY.key(), "true")).newWrite("");
        write.write(GenericRow.of(BinaryString.fromString(f0), BinaryString.fromString(f1)));
        TableCommitApi commit = table.newCommit("");
        commit.commit(0, write.prepareCommit(true, 0));
        write.close();
        commit.close();
    }

    private PartitionExpire newExpire() {
        Map<String, String> options = new HashMap<>();
        options.put(PARTITION_EXPIRATION_TIME.key(), "2 d");
        options.put(CoreOptions.PARTITION_EXPIRATION_CHECK_INTERVAL.key(), "1 d");
        options.put(CoreOptions.PARTITION_TIMESTAMP_FORMATTER.key(), "yyyyMMdd");
        return table.copy(options).store().newPartitionExpire("");
    }
}
