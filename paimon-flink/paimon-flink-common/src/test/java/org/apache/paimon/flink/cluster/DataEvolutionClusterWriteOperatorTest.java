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

package org.apache.paimon.flink.cluster;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.append.dataevolution.ClusteringRowIdMapping;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.CommittableTypeInfo;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.CloseableIterator;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests that old row IDs follow actual output boundaries, including buffered spills. */
class DataEvolutionClusterWriteOperatorTest {

    @TempDir java.nio.file.Path tempDir;

    @ParameterizedTest
    @CsvSource({"false,false", "true,false", "true,true"})
    void testMappingsFollowWrittenRowsAcrossFileRolls(boolean buffered, boolean spillable)
            throws Exception {
        FileStoreTable table = createTable(buffered, spillable);
        DataEvolutionClusterWriteOperator operator =
                new DataEvolutionClusterWriteOperator(table, BinaryRow.EMPTY_ROW, "cluster-test");
        List<DataFileMeta> files;
        int rowCount = 250;
        char[] payload = new char[2048];
        java.util.Arrays.fill(payload, 'x');
        try (OneInputStreamOperatorTestHarness<InternalRow, Committable> harness =
                new OneInputStreamOperatorTestHarness<>(operator)) {
            harness.setup(new CommittableTypeInfo().createSerializer(new ExecutionConfig()));
            harness.open();
            for (int id = 0; id < rowCount; id++) {
                harness.processElement(
                        new StreamRecord<>(
                                GenericRow.of(
                                        id,
                                        BinaryString.fromString(String.valueOf(payload)),
                                        oldRowId(id, rowCount))));
            }
            operator.endInput();
            files =
                    harness.extractOutputValues().stream()
                            .flatMap(
                                    committable ->
                                            ((CommitMessageImpl) committable.commitMessage())
                                                    .newFilesIncrement().newFiles().stream())
                            .collect(Collectors.toList());
        }

        assertThat(files).hasSizeGreaterThan(2);
        assertThat(files).allSatisfy(file -> assertThat(file.firstRowId()).isNull());
        assertThat(table.latestSnapshot()).isEmpty();
        DataFilePathFactory pathFactory =
                table.store().pathFactory().createDataFilePathFactory(BinaryRow.EMPTY_ROW, 0);
        List<Integer> writtenIds = new ArrayList<>();
        for (DataFileMeta file : files) {
            List<Integer> ids = readIds(table, file);
            assertThat(ids).hasSize((int) file.rowCount());
            writtenIds.addAll(ids);
            String mappingName = ClusteringRowIdMapping.fileName(file.fileName());
            assertThat(file.extraFiles()).contains(mappingName);
            ClusteringRowIdMapping.read(
                    table.fileIO(),
                    pathFactory.toAlignedPath(mappingName, file),
                    file.rowCount(),
                    (ordinal, oldId) ->
                            assertThat(oldId)
                                    .isEqualTo(oldRowId(ids.get((int) ordinal), rowCount)));
        }
        assertThat(writtenIds)
                .containsExactlyElementsOf(
                        IntStream.range(0, rowCount).boxed().collect(Collectors.toList()));
    }

    @Test
    void testEmptyWriterProducesNoMappingsOrCommit() throws Exception {
        FileStoreTable table = createTable(true, true);
        DataEvolutionClusterWriteOperator operator =
                new DataEvolutionClusterWriteOperator(table, BinaryRow.EMPTY_ROW, "cluster-test");
        try (OneInputStreamOperatorTestHarness<InternalRow, Committable> harness =
                new OneInputStreamOperatorTestHarness<>(operator)) {
            harness.setup(new CommittableTypeInfo().createSerializer(new ExecutionConfig()));
            harness.open();
            operator.endInput();
            assertThat(harness.extractOutputValues()).isEmpty();
        }
        assertThat(table.latestSnapshot()).isEmpty();
    }

    @Test
    void testMappingFailureCleansPreparedDataAndEarlierMappings() throws Exception {
        FailingMappingFileIO fileIO = new FailingMappingFileIO();
        FileStoreTable table = createTable(true, true, fileIO);
        fileIO.recordOutputs = true;
        DataEvolutionClusterWriteOperator operator =
                new DataEvolutionClusterWriteOperator(table, BinaryRow.EMPTY_ROW, "cluster-test");
        try (OneInputStreamOperatorTestHarness<InternalRow, Committable> harness =
                new OneInputStreamOperatorTestHarness<>(operator)) {
            harness.setup(new CommittableTypeInfo().createSerializer(new ExecutionConfig()));
            harness.open();
            for (int id = 0; id < 40; id++) {
                harness.processElement(
                        new StreamRecord<>(
                                GenericRow.of(id, BinaryString.fromString("payload"), (long) id)));
            }
            assertThatThrownBy(operator::endInput)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("injected mapping failure");
            assertThat(harness.extractOutputValues()).isEmpty();
            assertThat(fileIO.mappingCount).isEqualTo(2);
            assertThat(
                            fileIO.outputs.stream()
                                    .filter(
                                            path ->
                                                    !ClusteringRowIdMapping.isMappingFile(
                                                            path.getName())))
                    .isNotEmpty();
        }
        for (Path output : fileIO.outputs) {
            assertThat(fileIO.exists(output)).as("aborted output %s", output).isFalse();
        }
        assertThat(table.latestSnapshot()).isEmpty();
    }

    private FileStoreTable createTable(boolean buffered, boolean spillable) throws Exception {
        return createTable(buffered, spillable, LocalFileIO.create());
    }

    private FileStoreTable createTable(boolean buffered, boolean spillable, LocalFileIO fileIO)
            throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("payload", DataTypes.STRING())
                        .option(CoreOptions.BUCKET.key(), "-1")
                        .option(CoreOptions.ROW_TRACKING_ENABLED.key(), "true")
                        .option(CoreOptions.DATA_EVOLUTION_ENABLED.key(), "true")
                        .option(CoreOptions.WRITE_ONLY.key(), "true")
                        .option(CoreOptions.WRITE_BUFFER_FOR_APPEND.key(), String.valueOf(buffered))
                        .option(CoreOptions.WRITE_BUFFER_SPILLABLE.key(), String.valueOf(spillable))
                        .option(CoreOptions.WRITE_BUFFER_SIZE.key(), "16 kb")
                        .option(CoreOptions.PAGE_SIZE.key(), "4 kb")
                        .option(CoreOptions.TARGET_FILE_ROW_NUM.key(), "17")
                        .build();
        Path path = new Path(tempDir.toString());
        SchemaManager schemaManager = new FileSystemSchemaManager(fileIO, path);
        return FileStoreTableFactory.create(fileIO, path, schemaManager.createTable(schema));
    }

    private List<Integer> readIds(FileStoreTable table, DataFileMeta file) throws Exception {
        DataSplit split =
                DataSplit.builder()
                        .withPartition(BinaryRow.EMPTY_ROW)
                        .withBucket(0)
                        .withTotalBuckets(1)
                        .withBucketPath(
                                table.store()
                                        .pathFactory()
                                        .bucketPath(BinaryRow.EMPTY_ROW, 0)
                                        .toString())
                        .withDataFiles(Collections.singletonList(file.assignFirstRowId(0L)))
                        .rawConvertible(false)
                        .build();
        List<Integer> result = new ArrayList<>();
        try (RecordReader<InternalRow> reader = table.newRead().createReader(split);
                CloseableIterator<InternalRow> rows = reader.toCloseableIterator()) {
            while (rows.hasNext()) {
                InternalRow row = rows.next();
                assertThat(row.getFieldCount()).isEqualTo(2);
                result.add(row.getInt(0));
            }
        }
        return result;
    }

    private static long oldRowId(int id, int rowCount) {
        return 1000L + (rowCount - id - 1) * 3L;
    }

    private static class FailingMappingFileIO extends LocalFileIO {

        private static final long serialVersionUID = 1L;
        private final List<Path> outputs = new ArrayList<>();
        private boolean recordOutputs;
        private int mappingCount;

        @Override
        public PositionOutputStream newOutputStream(Path path, boolean overwrite)
                throws IOException {
            if (recordOutputs) {
                outputs.add(path);
                if (ClusteringRowIdMapping.isMappingFile(path.getName()) && ++mappingCount == 2) {
                    throw new IOException("injected mapping failure");
                }
            }
            return super.newOutputStream(path, overwrite);
        }
    }
}
