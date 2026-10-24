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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for committing files that already carry row ids into a row-tracking table, as {@code
 * sys.copy} does.
 */
public class RowTrackingCopiedFilesTest extends TableTestBase {

    private static final Identifier SOURCE = new Identifier("default", "src");
    private static final Identifier TARGET = new Identifier("default", "dst");

    @Test
    public void testRowsWrittenAfterCopiedFilesGetNewRowIds() throws Exception {
        FileStoreTable source = createRowTrackingTable(SOURCE);
        writeRows(source, row(1, "a"));
        writeRows(source, row(2, "b"));
        FileStoreTable target = createRowTrackingTable(TARGET);

        commitCopiedFiles(source, target, Collections.emptyList());
        target = table(TARGET);
        assertThat(rowIds(target)).containsEntry(1, 0L).containsEntry(2, 1L);
        // the next row id moves past the row ids of the copied files
        assertThat(target.snapshotManager().latestSnapshot().nextRowId()).isEqualTo(2L);

        writeRows(target, row(3, "c"), row(4, "d"));
        assertThat(rowIds(table(TARGET)))
                .containsEntry(1, 0L)
                .containsEntry(2, 1L)
                .containsEntry(3, 2L)
                .containsEntry(4, 3L);
    }

    @Test
    public void testNewFilesCommittedWithCopiedFilesGetRowIdsAfterThem() throws Exception {
        FileStoreTable source = createRowTrackingTable(SOURCE);
        writeRows(source, row(1, "a"), row(2, "b"));
        FileStoreTable target = createRowTrackingTable(TARGET);

        // a file written for the target, committed together with the copied ones
        List<DataFileMeta> newFiles;
        try (BatchTableWrite write = target.newBatchWriteBuilder().newWrite()) {
            write.write(row(3, "c"));
            newFiles =
                    ((CommitMessageImpl) write.prepareCommit().get(0))
                            .newFilesIncrement()
                            .newFiles();
        }
        commitCopiedFiles(source, target, newFiles);

        target = table(TARGET);
        assertThat(rowIds(target)).containsEntry(1, 0L).containsEntry(2, 1L).containsEntry(3, 2L);
        assertThat(target.snapshotManager().latestSnapshot().nextRowId()).isEqualTo(3L);
    }

    /**
     * Copies the data files of {@code source} into {@code target} and commits them, with {@code
     * extraFiles}, as {@code CopyFilesCommitOperator} does: with the row ids they have.
     */
    private void commitCopiedFiles(
            FileStoreTable source, FileStoreTable target, List<DataFileMeta> extraFiles)
            throws Exception {
        List<DataFileMeta> files = new ArrayList<>();
        source.newSnapshotReader().readFileIterator().forEachRemaining(e -> files.add(e.file()));
        Path sourceBucket = source.store().pathFactory().bucketPath(BinaryRow.EMPTY_ROW, 0);
        Path targetBucket = target.store().pathFactory().bucketPath(BinaryRow.EMPTY_ROW, 0);
        target.fileIO().mkdirs(targetBucket);
        for (DataFileMeta file : files) {
            target.fileIO()
                    .copyFile(
                            new Path(sourceBucket, file.fileName()),
                            new Path(targetBucket, file.fileName()),
                            false);
        }
        files.addAll(extraFiles);
        CommitMessage message =
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        0,
                        target.coreOptions().bucket(),
                        new DataIncrement(files, Collections.emptyList(), Collections.emptyList()),
                        CompactIncrement.emptyIncrement());
        try (BatchTableCommit commit = target.newBatchWriteBuilder().withOverwrite().newCommit()) {
            commit.commit(Collections.singletonList(message));
        }
    }

    private FileStoreTable createRowTrackingTable(Identifier identifier) throws Exception {
        catalog.createTable(
                identifier,
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("v", DataTypes.STRING())
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .build(),
                false);
        return table(identifier);
    }

    private FileStoreTable table(Identifier identifier) throws Exception {
        return (FileStoreTable) catalog.getTable(identifier);
    }

    private static GenericRow row(int id, String v) {
        return GenericRow.of(id, BinaryString.fromString(v));
    }

    private static void writeRows(FileStoreTable table, GenericRow... rows) throws Exception {
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite();
                BatchTableCommit commit = builder.newCommit()) {
            for (GenericRow row : rows) {
                write.write(row);
            }
            commit.commit(write.prepareCommit());
        }
    }

    private static Map<Integer, Long> rowIds(FileStoreTable table) throws Exception {
        RowType readType = SpecialFields.rowTypeWithRowTracking(table.rowType(), true, true);
        ReadBuilder readBuilder = table.newReadBuilder().withReadType(readType);
        TableRead read = readBuilder.newRead();
        InternalRowSerializer serializer = new InternalRowSerializer(readType);
        Map<Integer, Long> result = new HashMap<>();
        for (Split split : readBuilder.newScan().plan().splits()) {
            try (RecordReader<InternalRow> reader = read.createReader(split)) {
                reader.forEachRemaining(
                        r -> {
                            InternalRow row = serializer.copy(r);
                            result.put(row.getInt(0), row.isNullAt(2) ? null : row.getLong(2));
                        });
            }
        }
        return result;
    }
}
