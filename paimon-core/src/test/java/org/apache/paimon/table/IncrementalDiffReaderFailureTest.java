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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.IncrementalSplit;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.TraceableFileIO;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests ownership of the two readers used by a bounded snapshot diff. */
class IncrementalDiffReaderFailureTest extends TableTestBase {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testOpenFailureClosesPreviouslyOpenedFiles(boolean failBefore) throws Exception {
        FileStoreTable table = createDiffTable();
        OpenFailingFileIO fileIO = new OpenFailingFileIO();
        FileStoreTable failingTable =
                FileStoreTableFactory.create(fileIO, table.location()).copy(table.options());
        TableScan.Plan plan = failingTable.newScan().plan();
        assertThat(plan.splits()).hasSize(1);
        IncrementalSplit split = (IncrementalSplit) plan.splits().get(0);
        assertThat(split.beforeFiles()).hasSize(1);
        assertThat(split.afterFiles()).hasSize(1);
        String before = split.beforeFiles().get(0).fileName();
        String after = split.afterFiles().get(0).fileName();
        assertThat(before).isNotEqualTo(after);
        fileIO.failFile = failBefore ? before : after;
        try {
            assertThatThrownBy(() -> failingTable.newRead().createReader(plan))
                    .satisfies(
                            anyCauseMatches(IOException.class, "injected diff file open failure"));
            assertThat(fileIO.failures).isEqualTo(1);
            if (!failBefore) {
                assertThat(fileIO.openedFiles).contains(before);
            }
            assertThat(openStreams()).isEmpty();
        } finally {
            // Keep the failing-before regression from leaking streams into later tests.
            for (SeekableInputStream stream : openStreams()) {
                stream.close();
            }
        }
    }

    @Test
    void testSuccessfulDiffClosesBothReaders() throws Exception {
        FileStoreTable table = createDiffTable();
        List<InternalRow> rows = new ArrayList<>();
        InternalRowSerializer serializer = new InternalRowSerializer(table.rowType());
        try (RecordReader<InternalRow> reader =
                table.newRead().createReader(table.newScan().plan())) {
            reader.forEachRemaining(row -> rows.add(serializer.copy(row)));
        }
        assertThat(rows).containsExactly(GenericRow.of(1, 11));
        assertThat(openStreams()).isEmpty();
    }

    private List<SeekableInputStream> openStreams() {
        return TraceableFileIO.openInputStreams(
                path -> path.toString().contains(tempPath.toString()));
    }

    private FileStoreTable createDiffTable() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("k", DataTypes.INT())
                        .column("v", DataTypes.INT())
                        .primaryKey("k")
                        .option("bucket", "1")
                        .option("num-sorted-run.compaction-trigger", "99")
                        .build();
        catalog.createTable(identifier(), schema, false);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier());
        long before;
        try (StreamTableWrite write = table.newWrite(commitUser).withIOManager(ioManager);
                StreamTableCommit commit = table.newCommit(commitUser)) {
            write.write(GenericRow.of(1, 10));
            write.write(GenericRow.of(2, 20));
            commit.commit(0, write.prepareCommit(true, 0));
            before = table.snapshotManager().latestSnapshotId();
            write.write(GenericRow.of(1, 11));
            commit.commit(1, write.prepareCommit(true, 1));
            write.compact(BinaryRow.EMPTY_ROW, 0, true);
            commit.commit(2, write.prepareCommit(true, 2));
        }
        assertThat(read(table))
                .containsExactlyInAnyOrder(GenericRow.of(1, 11), GenericRow.of(2, 20));
        Map<String, String> options = new HashMap<>();
        options.put(
                "incremental-between", before + "," + table.snapshotManager().latestSnapshotId());
        options.put("incremental-between-scan-mode", "diff");
        return table.copy(options);
    }

    private static class OpenFailingFileIO extends TraceableFileIO {
        private static final long serialVersionUID = 1L;
        private String failFile;
        private int failures;
        private final List<String> openedFiles = new ArrayList<>();

        @Override
        public SeekableInputStream newInputStream(Path path) throws IOException {
            if (path.getName().equals(failFile)) {
                failures++;
                throw new IOException("injected diff file open failure");
            }
            SeekableInputStream stream = super.newInputStream(path);
            openedFiles.add(path.getName());
            return stream;
        }
    }
}
