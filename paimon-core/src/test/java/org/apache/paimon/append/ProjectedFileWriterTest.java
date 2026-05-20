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

package org.apache.paimon.append;

import org.apache.paimon.arrow.ArrowBundleRecords;
import org.apache.paimon.arrow.vector.ArrowFormatWriter;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.io.FileWriter;
import org.apache.paimon.io.ProjectableBundleRecords;
import org.apache.paimon.io.ReplayableBundleRecords;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ProjectedFileWriter}. */
public class ProjectedFileWriterTest {

    @Test
    public void testWriteBundlePassThroughForReplayableBundle() throws IOException {
        RecordingWriter writer = new RecordingWriter(true, true);
        ProjectedFileWriter<RecordingWriter, List<String>> projectedWriter =
                new ProjectedFileWriter<>(writer, new int[] {0, 2});

        projectedWriter.writeBundle(
                new ReplayableTestBundleRecords(
                        Arrays.<InternalRow>asList(
                                GenericRow.of(1, 2, 3), GenericRow.of(4, 5, 6))));

        assertThat(writer.bundleWriteCount).isEqualTo(1);
        assertThat(writer.rowWriteCount).isEqualTo(0);
        assertThat(writer.firstPassRows).containsExactly("1,3", "4,6");
        assertThat(writer.secondPassRows).containsExactly("1,3", "4,6");
    }

    @Test
    public void testWriteBundlePassThroughForProjectableArrowBundle() throws IOException {
        RecordingWriter writer = new RecordingWriter(true, true);
        ProjectedFileWriter<RecordingWriter, List<String>> projectedWriter =
                new ProjectedFileWriter<>(writer, new int[] {0, 2});
        RowType rowType =
                RowType.builder()
                        .field("f0", DataTypes.INT())
                        .field("f1", DataTypes.INT())
                        .field("f2", DataTypes.INT())
                        .build();

        try (ArrowFormatWriter arrowWriter = new ArrowFormatWriter(rowType, 16, true)) {
            arrowWriter.write(GenericRow.of(1, 2, 3));
            arrowWriter.write(GenericRow.of(4, 5, 6));
            arrowWriter.flush();

            projectedWriter.writeBundle(
                    new ArrowBundleRecords(arrowWriter.getVectorSchemaRoot(), rowType, true));
        }

        assertThat(writer.bundleWriteCount).isEqualTo(1);
        assertThat(writer.rowWriteCount).isEqualTo(0);
        assertThat(writer.projectableBundleWriteCount).isEqualTo(1);
        assertThat(writer.arrowBundleWriteCount).isEqualTo(1);
        assertThat(writer.firstPassRows).containsExactly("1,3", "4,6");
        assertThat(writer.secondPassRows).containsExactly("1,3", "4,6");
    }

    @Test
    public void testWriteBundleFallbackToRowWritesWhenWriterDoesNotSupportPassThrough()
            throws IOException {
        RecordingWriter writer = new RecordingWriter(false);
        ProjectedFileWriter<RecordingWriter, List<String>> projectedWriter =
                new ProjectedFileWriter<>(writer, new int[] {0, 2});

        projectedWriter.writeBundle(
                new ReplayableTestBundleRecords(
                        Arrays.<InternalRow>asList(
                                GenericRow.of(1, 2, 3), GenericRow.of(4, 5, 6))));

        assertThat(writer.bundleWriteCount).isEqualTo(0);
        assertThat(writer.rowWriteCount).isEqualTo(2);
        assertThat(writer.firstPassRows).containsExactly("1,3", "4,6");
    }

    @Test
    public void testWriteBundleFallbackToRowWritesWhenBundleIsNotReplayable() throws IOException {
        RecordingWriter writer = new RecordingWriter(true, true);
        ProjectedFileWriter<RecordingWriter, List<String>> projectedWriter =
                new ProjectedFileWriter<>(writer, new int[] {0, 2});

        projectedWriter.writeBundle(
                new SingleUseBundleRecords(
                        Arrays.<InternalRow>asList(
                                GenericRow.of(1, 2, 3), GenericRow.of(4, 5, 6))));

        assertThat(writer.bundleWriteCount).isEqualTo(0);
        assertThat(writer.rowWriteCount).isEqualTo(2);
        assertThat(writer.firstPassRows).containsExactly("1,3", "4,6");
        assertThat(writer.secondPassRows).isEmpty();
    }

    private static class RecordingWriter
            implements FileWriter<InternalRow, List<String>>, BundlePassThroughWriter {

        private final boolean supportsBundlePassThrough;
        private final boolean iterateBundleTwice;
        private final List<String> firstPassRows = new ArrayList<>();
        private final List<String> secondPassRows = new ArrayList<>();
        private int bundleWriteCount;
        private int projectableBundleWriteCount;
        private int arrowBundleWriteCount;
        private int rowWriteCount;

        private RecordingWriter(boolean supportsBundlePassThrough) {
            this(supportsBundlePassThrough, false);
        }

        private RecordingWriter(boolean supportsBundlePassThrough, boolean iterateBundleTwice) {
            this.supportsBundlePassThrough = supportsBundlePassThrough;
            this.iterateBundleTwice = iterateBundleTwice;
        }

        @Override
        public boolean supportsBundlePassThrough() {
            return supportsBundlePassThrough;
        }

        @Override
        public void writeReplayableBundle(ReplayableBundleRecords bundle) {
            bundleWriteCount++;
            if (bundle instanceof ProjectableBundleRecords) {
                projectableBundleWriteCount++;
            }
            if (bundle instanceof ArrowBundleRecords) {
                arrowBundleWriteCount++;
            }
            for (InternalRow row : bundle) {
                firstPassRows.add(format(row));
            }
            if (iterateBundleTwice) {
                for (InternalRow row : bundle) {
                    secondPassRows.add(format(row));
                }
            }
        }

        @Override
        public void write(InternalRow record) {
            rowWriteCount++;
            firstPassRows.add(format(record));
        }

        @Override
        public long recordCount() {
            return firstPassRows.size();
        }

        @Override
        public void abort() {}

        @Override
        public List<String> result() {
            return firstPassRows;
        }

        @Override
        public void close() {}

        private String format(InternalRow row) {
            return row.getInt(0) + "," + row.getInt(1);
        }
    }

    private static class ReplayableTestBundleRecords implements ReplayableBundleRecords {

        private final List<InternalRow> rows;

        private ReplayableTestBundleRecords(List<InternalRow> rows) {
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
