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

package org.apache.paimon.flink.source;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.connector.testutils.source.reader.TestingSplitEnumeratorContext;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ConsumerProgressFailoverTest {
    @TempDir java.nio.file.Path tempDir;

    @ParameterizedTest
    @CsvSource({
        "false, false, false",
        "true, false, false",
        "false, true, false",
        "true, true, false",
        "false, true, true",
        "true, true, true"
    })
    void returnedUncheckpointedAssignmentsDoNotAdvanceConsumerPastRestoredReader(
            boolean pendingCheckpoint, boolean waitingReader, boolean emptyReturn)
            throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("consumer-id", "regional-failover");
        options.put("scan.mode", "from-snapshot");
        options.put("scan.snapshot-id", "1");
        Path path = new Path(tempDir.toUri());
        new FileSystemSchemaManager(LocalFileIO.create(), path)
                .createTable(
                        Schema.newBuilder().column("id", DataTypes.INT()).options(options).build());
        FileStoreTable table = FileStoreTableFactory.create(LocalFileIO.create(), path);
        StreamWriteBuilder builder = table.newStreamWriteBuilder().withCommitUser("writer");
        int snapshotCount = emptyReturn ? 1 : 3;
        try (StreamTableWrite write = builder.newWrite();
                StreamTableCommit commit = builder.newCommit()) {
            for (int i = 1; i <= snapshotCount; i++) {
                write.write(GenericRow.of(i));
                commit.commit(i, write.prepareCommit(true, i));
            }
        }
        StreamTableScan scan = table.newStreamScan();
        FileStoreSourceSplitGenerator generator = new FileStoreSourceSplitGenerator();
        List<FileStoreSourceSplit> splits = new ArrayList<>();
        // from-snapshot first establishes its next snapshot and returns an empty starting plan.
        for (int i = 0; i <= snapshotCount && splits.size() < snapshotCount; i++) {
            splits.addAll(generator.createSplits(scan.plan()));
        }
        assertThat(splits).hasSize(snapshotCount);
        TestingSplitEnumeratorContext<FileStoreSourceSplit> context =
                new TestingSplitEnumeratorContext<>(1);
        context.registerReader(0, "host");
        ContinuousFileSplitEnumerator enumerator =
                new ContinuousFileSplitEnumerator(
                        context,
                        splits,
                        scan.checkpoint(),
                        Long.MAX_VALUE,
                        scan,
                        false,
                        1,
                        false,
                        -1,
                        1);
        enumerator.handleSplitRequest(0, "host");
        enumerator.snapshotState(10L);
        enumerator.notifyCheckpointComplete(10L);
        assertThat(table.consumerManager().consumer("regional-failover").get().nextSnapshot())
                .isEqualTo(1L);
        if (!emptyReturn) {
            enumerator.handleSourceEvent(0, new ReaderConsumeProgressEvent(1L));
            enumerator.handleSplitRequest(0, "host");
            enumerator.handleSourceEvent(0, new ReaderConsumeProgressEvent(2L));
            enumerator.handleSplitRequest(0, "host");
        }
        if (waitingReader) {
            enumerator.handleSourceEvent(0, new ReaderConsumeProgressEvent(snapshotCount));
            enumerator.handleSplitRequest(0, "host");
            assertThat(enumerator.readersAwaitingSplit).contains(0);
        }
        if (pendingCheckpoint) {
            enumerator.snapshotState(11L);
        }
        // The reader restores split1 from checkpoint10. The coordinator returns only assignments
        // made after that checkpoint (split2 and split3), and the restored reader does not request
        // another split while it is still replaying split1.
        context.registeredReaders().remove(0);
        enumerator.addSplitsBack(
                emptyReturn
                        ? java.util.Collections.emptyList()
                        : Arrays.asList(splits.get(1), splits.get(2)),
                0);
        context.registerReader(0, "restored-host");
        enumerator.addReader(0);
        enumerator.snapshotState(12L);
        enumerator.notifyCheckpointComplete(12L);
        assertThat(table.consumerManager().consumer("regional-failover").get().nextSnapshot())
                .isEqualTo(1L);
        enumerator.handleSourceEvent(0, new ReaderConsumeProgressEvent(1L));
        enumerator.handleSplitRequest(0, "host");
        enumerator.snapshotState(13L);
        enumerator.notifyCheckpointComplete(13L);
        assertThat(table.consumerManager().consumer("regional-failover").get().nextSnapshot())
                .isEqualTo(2L);
        enumerator.close();
    }
}
