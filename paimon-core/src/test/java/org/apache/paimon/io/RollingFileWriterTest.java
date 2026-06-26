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

package org.apache.paimon.io;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatReaderContext;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongCounter;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RollingFileWriterImpl}. */
public class RollingFileWriterTest {

    private static final RowType SCHEMA =
            RowType.of(new DataType[] {new IntType()}, new String[] {"id"});

    /**
     * Set a very small target file size, so that we will roll over to a new file even if writing
     * one record.
     */
    private static final Long TARGET_FILE_SIZE = 64L;

    @TempDir java.nio.file.Path tempDir;

    private RollingFileWriterImpl<InternalRow, DataFileMeta> rollingFileWriter;

    public void initialize(String identifier) {
        initialize(identifier, false);
    }

    public void initialize(String identifier, boolean statsDenseStore) {
        FileFormat fileFormat = FileFormat.fromIdentifier(identifier, new Options());
        rollingFileWriter =
                new RollingFileWriterImpl<>(
                        () ->
                                new RowDataFileWriter(
                                        LocalFileIO.create(),
                                        RollingFileWriter.createFileWriterContext(
                                                fileFormat,
                                                SCHEMA,
                                                SimpleColStatsCollector.createFullStatsFactories(
                                                        SCHEMA.getFieldCount()),
                                                CoreOptions.FILE_COMPRESSION.defaultValue()),
                                        new DataFilePathFactory(
                                                        new Path(tempDir + "/bucket-0"),
                                                        CoreOptions.FILE_FORMAT
                                                                .defaultValue()
                                                                .toString(),
                                                        CoreOptions.DATA_FILE_PREFIX.defaultValue(),
                                                        CoreOptions.CHANGELOG_FILE_PREFIX
                                                                .defaultValue(),
                                                        CoreOptions.FILE_SUFFIX_INCLUDE_COMPRESSION
                                                                .defaultValue(),
                                                        CoreOptions.FILE_COMPRESSION.defaultValue(),
                                                        null)
                                                .newPath(),
                                        SCHEMA,
                                        0L,
                                        () -> new LongCounter(0),
                                        new FileIndexOptions(),
                                        FileSource.APPEND,
                                        true,
                                        statsDenseStore,
                                        false,
                                        null),
                        TARGET_FILE_SIZE);
    }

    @ParameterizedTest
    @ValueSource(strings = {"orc", "avro", "parquet"})
    public void testRolling(String formatType) throws IOException {
        initialize(formatType);
        int checkInterval =
                formatType.equals(CoreOptions.FILE_FORMAT_ORC)
                        ? VectorizedRowBatch.DEFAULT_SIZE
                        : 1000;
        for (int i = 0; i < 3000; i++) {
            rollingFileWriter.write(GenericRow.of(i));
            if (i < checkInterval) {
                assertFileNum(1);
            } else if (i < checkInterval * 2) {
                assertFileNum(2);
            } else {
                assertFileNum(3);
            }
        }
    }

    @Test
    public void testWriteRowSidecar() throws IOException {
        FileFormat fileFormat = FileFormat.fromIdentifier("parquet", new Options());
        FileFormat rowFormat = FileFormat.fromIdentifier("row", new Options());
        DataFilePathFactory pathFactory =
                new DataFilePathFactory(
                        new Path(tempDir + "/bucket-0"),
                        CoreOptions.FILE_FORMAT.defaultValue().toString(),
                        CoreOptions.DATA_FILE_PREFIX.defaultValue(),
                        CoreOptions.CHANGELOG_FILE_PREFIX.defaultValue(),
                        CoreOptions.FILE_SUFFIX_INCLUDE_COMPRESSION.defaultValue(),
                        CoreOptions.FILE_COMPRESSION.defaultValue(),
                        null);
        RowDataRollingFileWriter writer =
                new RowDataRollingFileWriter(
                        LocalFileIO.create(),
                        0L,
                        fileFormat,
                        TARGET_FILE_SIZE,
                        SCHEMA,
                        pathFactory,
                        () -> new LongCounter(0),
                        CoreOptions.FILE_COMPRESSION.defaultValue(),
                        SimpleColStatsCollector.createFullStatsFactories(SCHEMA.getFieldCount()),
                        new FileIndexOptions(),
                        FileSource.APPEND,
                        true,
                        false,
                        null,
                        rowFormat);

        writer.write(GenericRow.of(1));
        writer.close();

        List<DataFileMeta> files = writer.result();
        assertThat(files).hasSize(1);
        DataFileMeta file = files.get(0);
        assertThat(file.extraFiles()).hasSize(1);
        String rowSidecar = file.extraFiles().get(0);
        assertThat(rowSidecar).endsWith(".row");
        Path dataPath = pathFactory.toPath(file);
        Path rowSidecarPath = pathFactory.toAlignedPath(rowSidecar, file);
        assertThat(LocalFileIO.create().exists(dataPath)).isTrue();
        assertThat(LocalFileIO.create().exists(rowSidecarPath)).isTrue();

        writer.abort();
        assertThat(LocalFileIO.create().exists(dataPath)).isFalse();
        assertThat(LocalFileIO.create().exists(rowSidecarPath)).isFalse();
    }

    @Test
    public void testWriteRowSidecarWithBundle() throws IOException {
        FileFormat fileFormat = FileFormat.fromIdentifier("parquet", new Options());
        FileFormat rowFormat = FileFormat.fromIdentifier("row", new Options());
        DataFilePathFactory pathFactory =
                new DataFilePathFactory(
                        new Path(tempDir + "/bucket-0"),
                        CoreOptions.FILE_FORMAT.defaultValue().toString(),
                        CoreOptions.DATA_FILE_PREFIX.defaultValue(),
                        CoreOptions.CHANGELOG_FILE_PREFIX.defaultValue(),
                        CoreOptions.FILE_SUFFIX_INCLUDE_COMPRESSION.defaultValue(),
                        CoreOptions.FILE_COMPRESSION.defaultValue(),
                        null);
        RowDataRollingFileWriter writer =
                new RowDataRollingFileWriter(
                        LocalFileIO.create(),
                        0L,
                        fileFormat,
                        TARGET_FILE_SIZE,
                        SCHEMA,
                        pathFactory,
                        () -> new LongCounter(0),
                        CoreOptions.FILE_COMPRESSION.defaultValue(),
                        SimpleColStatsCollector.createFullStatsFactories(SCHEMA.getFieldCount()),
                        new FileIndexOptions(),
                        FileSource.APPEND,
                        true,
                        false,
                        null,
                        rowFormat);

        writer.writeBundle(
                new SingleUseBundleRecords(Arrays.asList(GenericRow.of(1), GenericRow.of(2))));
        writer.close();

        List<DataFileMeta> files = writer.result();
        assertThat(files).hasSize(1);
        DataFileMeta file = files.get(0);
        assertThat(file.rowCount()).isEqualTo(2);
        assertThat(file.minSequenceNumber()).isEqualTo(0);
        assertThat(file.maxSequenceNumber()).isEqualTo(1);
        assertThat(file.extraFiles()).hasSize(1);
        assertThat(
                        readIntsFromRowFile(
                                rowFormat, pathFactory.toAlignedPath(file.extraFiles().get(0), file)))
                .containsExactly(1, 2);
    }

    private static List<Integer> readIntsFromRowFile(FileFormat rowFormat, Path path)
            throws IOException {
        LocalFileIO fileIO = LocalFileIO.create();
        List<Integer> result = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                rowFormat.createReaderFactory(SCHEMA, SCHEMA, Collections.emptyList())
                        .createReader(
                                new FormatReaderContext(fileIO, path, fileIO.getFileSize(path)))) {
            reader.forEachRemaining(row -> result.add(row.getInt(0)));
        }
        return result;
    }

    private void assertFileNum(int expected) {
        File dataDir = tempDir.resolve("bucket-0").toFile();
        File[] files = dataDir.listFiles();
        assertThat(files).isNotNull().hasSize(expected);
    }

    @Test
    public void testStatsDenseStore() throws IOException {
        initialize("parquet", true);
        for (int i = 0; i < 1000; i++) {
            rollingFileWriter.write(GenericRow.of(i));
        }
        rollingFileWriter.close();
        DataFileMeta file = rollingFileWriter.result().get(0);
        assertThat(file.valueStatsCols()).isNull();
        assertThat(file.valueStats().minValues().getFieldCount()).isEqualTo(SCHEMA.getFieldCount());
    }

    private static class SingleUseBundleRecords implements BundleRecords {

        private final List<InternalRow> rows;
        private boolean iterated;

        private SingleUseBundleRecords(List<InternalRow> rows) {
            this.rows = rows;
        }

        @Override
        public Iterator<InternalRow> iterator() {
            if (iterated) {
                throw new IllegalStateException("Bundle should only be consumed once.");
            }
            iterated = true;
            return rows.iterator();
        }

        @Override
        public long rowCount() {
            return rows.size();
        }
    }
}
