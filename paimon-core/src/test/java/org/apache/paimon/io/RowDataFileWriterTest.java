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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.format.BundleFormatWriter;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.SimpleColStats;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongCounter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link RowDataFileWriter}. */
public class RowDataFileWriterTest {

    private static final RowType ROW_TYPE =
            RowType.of(new DataType[] {new IntType()}, new String[] {"id"});

    @TempDir java.nio.file.Path tempDir;

    private FileIO fileIO;
    private int fileId;

    @BeforeEach
    public void beforeEach() {
        fileIO = LocalFileIO.create();
        fileId = 0;
    }

    @Test
    public void testBundleFastPath() throws IOException {
        TrackingBundleFormatWriter formatWriter = new TrackingBundleFormatWriter();
        RowDataFileWriter writer =
                newWriter(
                        ROW_TYPE,
                        formatWriter,
                        SimpleStatsProducer.disabledProducer(),
                        () -> new LongCounter(5));

        writer.writeBundle(bundle(GenericRow.of(1), GenericRow.of(2)));
        writer.close();

        DataFileMeta result = writer.result();
        assertThat(formatWriter.bundleWriteCount).isEqualTo(1);
        assertThat(formatWriter.elementWriteCount).isZero();
        assertThat(formatWriter.values).containsExactly(1, 2);
        assertThat(result.rowCount()).isEqualTo(2);
        assertThat(result.minSequenceNumber()).isEqualTo(5);
        assertThat(result.maxSequenceNumber()).isEqualTo(6);
    }

    @Test
    public void testBundleFallsBackForPerRecordStats() throws IOException {
        TrackingBundleFormatWriter formatWriter = new TrackingBundleFormatWriter();
        PerRecordStatsProducer statsProducer = new PerRecordStatsProducer();
        RowDataFileWriter writer =
                newWriter(ROW_TYPE, formatWriter, statsProducer, LongCounter::new);

        writer.writeBundle(bundle(GenericRow.of(1), GenericRow.of(2)));
        writer.close();

        assertThat(formatWriter.bundleWriteCount).isZero();
        assertThat(formatWriter.elementWriteCount).isEqualTo(2);
        assertThat(statsProducer.collectedCount).isEqualTo(2);
    }

    @Test
    public void testBundleFallsBackForRowTracking() throws IOException {
        RowType rowTrackingType = SpecialFields.rowTypeWithRowTracking(ROW_TYPE);
        TrackingBundleFormatWriter formatWriter = new TrackingBundleFormatWriter();
        RowDataFileWriter writer =
                newWriter(
                        rowTrackingType,
                        formatWriter,
                        SimpleStatsProducer.disabledProducer(),
                        LongCounter::new);

        writer.writeBundle(bundle(GenericRow.of(1, 100L, 7L), GenericRow.of(2, 101L, 11L)));
        writer.close();

        DataFileMeta result = writer.result();
        assertThat(formatWriter.bundleWriteCount).isZero();
        assertThat(formatWriter.elementWriteCount).isEqualTo(2);
        assertThat(result.minSequenceNumber()).isEqualTo(7);
        assertThat(result.maxSequenceNumber()).isEqualTo(11);
    }

    @Test
    public void testBundleFallsBackForAuxiliaryWriter() throws IOException {
        TrackingBundleFormatWriter formatWriter = new TrackingBundleFormatWriter();
        FileFormat rowFormat = FileFormat.fromIdentifier("row", new Options());
        Path dataPath = nextPath("data");
        Path sidecarPath = nextPath("sidecar.row");
        RowDataFileWriter writer =
                new RowDataFileWriter(
                        fileIO,
                        context(formatWriter, SimpleStatsProducer.disabledProducer()),
                        dataPath,
                        ROW_TYPE,
                        0L,
                        LongCounter::new,
                        new FileIndexOptions(),
                        FileSource.APPEND,
                        false,
                        false,
                        false,
                        null,
                        rowFormat,
                        sidecarPath);

        writer.writeBundle(bundle(GenericRow.of(1), GenericRow.of(2)));
        writer.close();

        assertThat(formatWriter.bundleWriteCount).isZero();
        assertThat(formatWriter.elementWriteCount).isEqualTo(2);
        assertThat(fileIO.exists(sidecarPath)).isTrue();
    }

    private RowDataFileWriter newWriter(
            RowType rowType,
            TrackingBundleFormatWriter formatWriter,
            SimpleStatsProducer statsProducer,
            Supplier<LongCounter> counterSupplier) {
        return new RowDataFileWriter(
                fileIO,
                context(formatWriter, statsProducer),
                nextPath("data"),
                rowType,
                0L,
                counterSupplier,
                new FileIndexOptions(),
                FileSource.APPEND,
                false,
                false,
                false,
                null);
    }

    private FileWriterContext context(
            TrackingBundleFormatWriter formatWriter, SimpleStatsProducer statsProducer) {
        return new FileWriterContext((out, compression) -> formatWriter, statsProducer, "zstd");
    }

    private Path nextPath(String suffix) {
        return new Path(tempDir.resolve("file-" + fileId++ + "-" + suffix).toString());
    }

    private static BundleRecords bundle(InternalRow... rows) {
        return new TestBundleRecords(Arrays.asList(rows));
    }

    private static class TestBundleRecords implements BundleRecords {

        private final List<InternalRow> rows;

        private TestBundleRecords(List<InternalRow> rows) {
            this.rows = rows;
        }

        @Override
        public Iterator<InternalRow> iterator() {
            return rows.iterator();
        }

        @Override
        public long rowCount() {
            return rows.size();
        }
    }

    private static class TrackingBundleFormatWriter implements BundleFormatWriter {

        private int bundleWriteCount;
        private int elementWriteCount;
        private final List<Integer> values = new ArrayList<>();

        @Override
        public void addElement(InternalRow row) {
            elementWriteCount++;
            values.add(row.getInt(0));
        }

        @Override
        public void writeBundle(BundleRecords bundle) {
            bundleWriteCount++;
            for (InternalRow row : bundle) {
                values.add(row.getInt(0));
            }
        }

        @Override
        public boolean reachTargetSize(boolean suggestedCheck, long targetSize) {
            return false;
        }

        @Override
        public void close() {}
    }

    private static class PerRecordStatsProducer implements SimpleStatsProducer {

        private int collectedCount;

        @Override
        public boolean isStatsDisabled() {
            return false;
        }

        @Override
        public boolean requirePerRecord() {
            return true;
        }

        @Override
        public void collect(InternalRow row) {
            collectedCount++;
        }

        @Override
        public SimpleColStats[] extract(FileIO fileIO, Path path, long length) {
            return new SimpleColStats[] {SimpleColStats.NONE};
        }
    }
}
