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

package org.apache.paimon.append.dataevolution;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.DataEvolutionUtils.fileFields;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for column sequence propagation in {@link DataEvolutionNormalCompactTask}. */
public class DataEvolutionNormalCompactTaskTest extends TableTestBase {

    private static final int ROW_COUNT = 100;

    @Override
    public Schema schemaDefault() {
        return Schema.newBuilder()
                .column("dt", DataTypes.STRING())
                .column("f0", DataTypes.INT())
                .column("f1", DataTypes.STRING())
                .partitionKeys(Collections.singletonList("dt"))
                .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                .build();
    }

    @Test
    public void testPropagateColumnSequencesAcrossCompactions() throws Exception {
        write();

        int f0Id = getTableDefault().rowType().getField("f0").id();
        int f1Id = getTableDefault().rowType().getField("f1").id();
        DataFileMeta firstCompact =
                updateColumnsAndCompact(
                        Collections.singletonList("f1"),
                        1,
                        CoreOptions.GlobalIndexColumnUpdateAction.IGNORE);

        long f0Sequence = columnSequence(firstCompact, f0Id);
        assertThat(f0Sequence).isLessThan(firstCompact.maxSequenceNumber());
        assertThat(columnSequence(firstCompact, f1Id)).isEqualTo(firstCompact.maxSequenceNumber());

        DataFileMeta secondCompact =
                updateColumnsAndCompact(
                        Collections.singletonList("f1"),
                        2,
                        CoreOptions.GlobalIndexColumnUpdateAction.IGNORE);
        assertThat(columnSequence(secondCompact, f0Id)).isEqualTo(f0Sequence);
        assertThat(columnSequence(secondCompact, f1Id))
                .isEqualTo(secondCompact.maxSequenceNumber());
    }

    @Test
    public void testOmitAndReconstructRedundantColumnSequences() throws Exception {
        write();

        DataFileMeta fullUpdate =
                updateColumnsAndCompact(
                        Arrays.asList("f0", "f1"),
                        1,
                        CoreOptions.GlobalIndexColumnUpdateAction.IGNORE);
        assertThat(fullUpdate.columnMaxSequenceNumbers()).isNull();

        int f0Id = getTableDefault().rowType().getField("f0").id();
        DataFileMeta partialUpdate =
                updateColumnsAndCompact(
                        Collections.singletonList("f1"),
                        2,
                        CoreOptions.GlobalIndexColumnUpdateAction.IGNORE);
        assertThat(columnSequence(partialUpdate, f0Id))
                .isEqualTo(fullUpdate.maxSequenceNumber())
                .isLessThan(partialUpdate.maxSequenceNumber());
    }

    @Test
    public void testOmitColumnSequencesUnlessUpdatesAreIgnored() throws Exception {
        write();

        DataFileMeta compacted =
                updateColumnsAndCompact(
                        Collections.singletonList("f1"),
                        1,
                        CoreOptions.GlobalIndexColumnUpdateAction.THROW_ERROR);
        assertThat(compacted.columnMaxSequenceNumbers()).isNull();
    }

    private void write() throws Exception {
        createTableDefault();

        BatchWriteBuilder builder = getTableDefault().newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite()) {
            for (int i = 0; i < ROW_COUNT; i++) {
                write.write(
                        GenericRow.of(
                                BinaryString.fromString("p0"),
                                i,
                                BinaryString.fromString("f1_" + i)));
            }
            try (BatchTableCommit commit = builder.newCommit()) {
                commit.commit(write.prepareCommit());
            }
        }
    }

    private DataFileMeta updateColumnsAndCompact(
            List<String> columns,
            int updateRound,
            CoreOptions.GlobalIndexColumnUpdateAction updateAction)
            throws Exception {
        Map<String, String> writeOptions = new HashMap<>();
        writeOptions.put(
                CoreOptions.GLOBAL_INDEX_COLUMN_UPDATE_ACTION.key(), updateAction.toString());
        FileStoreTable table = getTableDefault().copy(writeOptions);
        List<String> writeColumns = new ArrayList<>();
        writeColumns.add("dt");
        writeColumns.addAll(columns);
        RowType writeType = table.rowType().project(writeColumns);
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        try (BatchTableWrite batchWrite = writeBuilder.newWrite().withWriteType(writeType)) {
            for (int i = 0; i < ROW_COUNT; i++) {
                List<Object> values = new ArrayList<>();
                values.add(BinaryString.fromString("p0"));
                for (String column : columns) {
                    values.add(
                            "f0".equals(column)
                                    ? i + updateRound * ROW_COUNT
                                    : BinaryString.fromString("updated_" + updateRound + "_" + i));
                }
                batchWrite.write(GenericRow.of(values.toArray()));
            }
            List<CommitMessage> messages = batchWrite.prepareCommit();
            assignFirstRowId(messages, 0L);
            try (BatchTableCommit commit = writeBuilder.newCommit()) {
                commit.commit(messages);
            }
        }

        writeOptions.put(CoreOptions.COMPACTION_MIN_FILE_NUM.key(), "2");
        table = getTableDefault().copy(writeOptions);
        Snapshot compactSnapshot = table.snapshotManager().latestSnapshot();
        DataEvolutionCompactCoordinator coordinator =
                new DataEvolutionCompactCoordinator(table, false, false, compactSnapshot);
        List<CommitMessage> compactMessages = new ArrayList<>();
        for (DataEvolutionCompactTask task : coordinator.plan()) {
            compactMessages.add(task.doCompact(table, "test-compact"));
        }
        assertThat(compactMessages).isNotEmpty();
        compactMessages.addAll(
                new DataEvolutionCompactionCommitPreparation(table, compactSnapshot)
                        .prepare(compactMessages));
        try (BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            commit.commit(compactMessages);
        }

        List<DataFileMeta> rowRangeFiles =
                getTableDefault().store().newScan().plan().files().stream()
                        .map(ManifestEntry::file)
                        .filter(file -> file.firstRowId() != null && file.firstRowId() == 0L)
                        .collect(Collectors.toList());
        assertThat(rowRangeFiles).hasSize(1);
        return rowRangeFiles.get(0);
    }

    private long columnSequence(DataFileMeta file, int fieldId) throws Exception {
        TableSchema fileSchema = getTableDefault().schemaManager().schema(file.schemaId());
        boolean nestedFieldEnabled =
                new CoreOptions(fileSchema.options()).dataEvolutionNestedFieldEnabled();
        List<DataField> fields = fileFields(fileSchema.fields(), file, nestedFieldEnabled);
        long[] sequences = file.columnMaxSequenceNumbers();
        assertThat(sequences).hasSize(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).id() == fieldId) {
                return sequences[i];
            }
        }
        throw new IllegalArgumentException("Field not found in data file: " + fieldId);
    }

    private void assignFirstRowId(List<CommitMessage> messages, long firstRowId) {
        for (CommitMessage message : messages) {
            CommitMessageImpl impl = (CommitMessageImpl) message;
            List<DataFileMeta> files = new ArrayList<>(impl.newFilesIncrement().newFiles());
            impl.newFilesIncrement().newFiles().clear();
            impl.newFilesIncrement()
                    .newFiles()
                    .addAll(
                            files.stream()
                                    .map(file -> file.assignFirstRowId(firstRowId))
                                    .collect(Collectors.toList()));
        }
    }
}
