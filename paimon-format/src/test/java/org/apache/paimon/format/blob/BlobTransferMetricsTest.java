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

package org.apache.paimon.format.blob;

import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.BlobFetchMetricReporter;
import org.apache.paimon.data.BlobRef;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.PositionOutputStreamWrapper;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.UriReader;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Payload-copy accounting without changing Blob write behavior. */
class BlobTransferMetricsTest {

    @Test
    void disabledUsesOriginalCopyPath(@TempDir Path dir) throws Exception {
        Reporter disabled = new Reporter(false);
        Reporter enabled = new Reporter(true);
        Path oldFile = dir.resolve("old.blob");
        Path measuredFile = dir.resolve("measured.blob");
        for (boolean active : new boolean[] {false, true}) {
            try (BlobFormatWriter writer =
                    writer(active ? measuredFile : oldFile, active ? enabled : disabled)) {
                writer.addElement(GenericRow.of(Blob.fromData(new byte[] {1, 2, 3})));
                writer.addElement(GenericRow.of((Object) null));
                writer.addElement(GenericRow.of(Blob.fromData(new byte[0])));
            }
        }
        assertThat(Files.readAllBytes(measuredFile)).isEqualTo(Files.readAllBytes(oldFile));
        assertThat(disabled.transfers).isEmpty();
        assertThat(enabled.transfers).hasSize(2);
        assertTransfer(enabled.transfers.get(0), 3, 3, true);
        assertTransfer(enabled.transfers.get(1), 0, 0, true);
        assertThat(disabled.successes).isEqualTo(enabled.successes).isEqualTo(2);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void readFailurePreservesPartialBytesAndOriginalError(boolean bounded, @TempDir Path dir)
            throws Exception {
        IOException failure = new IOException("source failed");
        Reporter reporter = new Reporter(true);
        SeekableInputStream stream =
                new ByteArraySeekableStream(new byte[] {1, 2, 3, 4}) {
                    @Override
                    public int read(byte[] b) throws IOException {
                        return read(b, 0, b.length);
                    }

                    @Override
                    public int read(byte[] b, int off, int len) throws IOException {
                        if (getPos() >= 2) {
                            throw failure;
                        }
                        return super.read(b, off, len);
                    }
                };
        Blob blob =
                bounded
                        ? new BlobRef(uri -> stream, new BlobDescriptor("test://source", 0, 4))
                        : Blob.fromInputStream(() -> stream);
        // Telemetry must not replace the original I/O exception.
        reporter.throwOnTransfer = true;
        try (BlobFormatWriter writer = writer(dir.resolve("partial.blob"), reporter)) {
            assertThatThrownBy(() -> writer.addElement(GenericRow.of(blob))).isSameAs(failure);
        }
        assertThat(reporter.transfers).hasSize(1);
        assertTransfer(reporter.transfers.get(0), 2, 2, false);
        assertThat(reporter.failures).isEqualTo(1);
    }

    @Test
    void throwingDestinationOnlyCountsNormallyReturnedWrites(@TempDir Path dir) throws Exception {
        Reporter reporter = new Reporter(true);
        IOException failure = new IOException("destination failed");
        PositionOutputStream out =
                new PositionOutputStreamWrapper(
                        new LocalFileIO.LocalPositionOutputStream(
                                dir.resolve("out.blob").toFile())) {
                    @Override
                    public void write(byte[] b, int off, int len) throws IOException {
                        if (getPos() >= 6) { // four framing bytes plus one successful payload write
                            super.write(
                                    b, off, 1); // unknown partial side effect of the throwing call
                            throw failure;
                        }
                        super.write(b, off, len);
                    }
                };
        try (BlobFormatWriter writer =
                new BlobFormatWriter(
                        out, null, RowType.of(DataTypes.BLOB()), false, false, reporter, 2)) {
            assertThatThrownBy(
                            () ->
                                    writer.addElement(
                                            GenericRow.of(Blob.fromData(new byte[] {1, 2, 3, 4}))))
                    .isSameAs(failure);
        }
        assertTransfer(reporter.transfers.get(0), 4, 2, false);
        assertThat(reporter.failures).isEqualTo(1);
    }

    @Test
    void unknownLengthRetainsSingleArgumentReadDispatch(@TempDir Path dir) throws Exception {
        Reporter reporter = new Reporter(true);
        int[] reads = {0};
        SeekableInputStream stream =
                new ByteArraySeekableStream(new byte[] {1, 2, 3}) {
                    @Override
                    public int read(byte[] b) throws IOException {
                        reads[0]++;
                        return super.read(b);
                    }

                    @Override
                    public int read(byte[] b, int off, int len) {
                        throw new AssertionError("Unknown-length copy changed read overload");
                    }
                };
        try (BlobFormatWriter writer = writer(dir.resolve("dispatch.blob"), reporter)) {
            writer.addElement(GenericRow.of(Blob.fromInputStream(() -> stream)));
        }
        assertThat(reads[0]).isGreaterThan(0);
        assertTransfer(reporter.transfers.get(0), 3, 3, true);
    }

    @Test
    void telemetryFailureDoesNotBreakSuccessfulCopy(@TempDir Path dir) throws Exception {
        Reporter reporter = new Reporter(true);
        reporter.throwOnTransfer = true;
        try (BlobFormatWriter writer = writer(dir.resolve("ok.blob"), reporter)) {
            writer.addElement(GenericRow.of(Blob.fromData(new byte[] {1, 2, 3})));
        }
        assertThat(reporter.successes).isEqualTo(1);
        assertTransfer(reporter.transfers.get(0), 3, 3, true);
    }

    @Test
    void capabilityFailureUsesOriginalCopy(@TempDir Path dir) throws Exception {
        Reporter reporter =
                new Reporter(true) {
                    @Override
                    public boolean isTransferMetricsEnabled() {
                        throw new IllegalStateException("telemetry unavailable");
                    }
                };
        try (BlobFormatWriter writer = writer(dir.resolve("ok.blob"), reporter)) {
            writer.addElement(GenericRow.of(Blob.fromData(new byte[] {1})));
        }
        assertThat(reporter.transfers).isEmpty();
        assertThat(reporter.successes).isEqualTo(1);
    }

    @Test
    void openingFailureHasNoPayloadTiming(@TempDir Path dir) throws Exception {
        Reporter reporter = new Reporter(true);
        Blob blob =
                Blob.fromInputStream(
                        () -> {
                            throw new IllegalStateException("open failed");
                        });
        try (BlobFormatWriter writer =
                new BlobFormatWriter(
                        new LocalFileIO.LocalPositionOutputStream(
                                dir.resolve("null.blob").toFile()),
                        null,
                        RowType.of(DataTypes.BLOB()),
                        false,
                        true,
                        reporter,
                        2)) {
            writer.addElement(GenericRow.of(blob));
        }
        assertThat(reporter.transfers).isEmpty();
        assertThat(reporter.nullFailures).isEqualTo(1);
    }

    @Test
    void exactRefsStillReuseSourceAndCountEmptyCopy(@TempDir Path dir) throws Exception {
        Reporter reporter = new Reporter(true);
        int[] opens = {0};
        UriReader reader =
                uri -> {
                    opens[0]++;
                    return new ByteArraySeekableStream(new byte[] {1, 2, 3, 4});
                };
        try (BlobFormatWriter writer = writer(dir.resolve("refs.blob"), reporter)) {
            writer.addElement(
                    GenericRow.of(new BlobRef(reader, new BlobDescriptor("test://source", 0, 2))));
            writer.addElement(
                    GenericRow.of(new BlobRef(reader, new BlobDescriptor("test://source", 2, 2))));
            writer.addElement(
                    GenericRow.of(new BlobRef(reader, new BlobDescriptor("test://source", 4, 0))));
        }
        assertThat(opens[0]).isEqualTo(1);
        assertThat(reporter.transfers).hasSize(3);
        assertTransfer(reporter.transfers.get(0), 2, 2, true);
        assertTransfer(reporter.transfers.get(1), 2, 2, true);
        assertTransfer(reporter.transfers.get(2), 0, 0, true);
    }

    @Test
    void nestedArrayAndMapUseSamePayloadBoundary(@TempDir Path dir) throws Exception {
        Reporter reporter = new Reporter(true);
        try (BlobFormatWriter writer =
                new BlobFormatWriter(
                        new LocalFileIO.LocalPositionOutputStream(
                                dir.resolve("array.blob").toFile()),
                        null,
                        RowType.of(DataTypes.ARRAY(DataTypes.BLOB())),
                        false,
                        false,
                        reporter,
                        2)) {
            writer.addElement(
                    GenericRow.of(
                            new GenericArray(
                                    new Object[] {Blob.fromData(new byte[] {1, 2, 3}), null})));
        }
        try (BlobFormatWriter writer =
                new BlobFormatWriter(
                        new LocalFileIO.LocalPositionOutputStream(dir.resolve("map.blob").toFile()),
                        null,
                        RowType.of(DataTypes.MAP(DataTypes.INT(), DataTypes.BLOB())),
                        false,
                        false,
                        reporter,
                        2)) {
            writer.addElement(
                    GenericRow.of(
                            new GenericMap(
                                    Collections.singletonMap(
                                            1, Blob.fromData(new byte[] {4, 5})))));
        }
        assertThat(reporter.transfers).hasSize(2);
        assertTransfer(reporter.transfers.get(0), 3, 3, true);
        assertTransfer(reporter.transfers.get(1), 2, 2, true);
    }

    private static BlobFormatWriter writer(Path path, Reporter reporter) throws IOException {
        return new BlobFormatWriter(
                new LocalFileIO.LocalPositionOutputStream(path.toFile()),
                null,
                RowType.of(DataTypes.BLOB()),
                false,
                false,
                reporter,
                2);
    }

    private static void assertTransfer(
            long[] transfer, long read, long written, boolean completed) {
        assertThat(transfer[0]).isEqualTo(read);
        assertThat(transfer[1]).isEqualTo(written);
        assertThat(transfer[2]).isGreaterThanOrEqualTo(0);
        assertThat(transfer[3]).isEqualTo(completed ? 1 : 0);
    }

    private static class Reporter implements BlobFetchMetricReporter {
        private final boolean enabled;
        private final List<long[]> transfers = new ArrayList<>();
        private boolean throwOnTransfer;
        private int successes;
        private int failures;
        private int nullFailures;

        private Reporter(boolean enabled) {
            this.enabled = enabled;
        }

        @Override
        public boolean isTransferMetricsEnabled() {
            return enabled;
        }

        @Override
        public void recordTransfer(long read, long written, long nanos, boolean completed) {
            transfers.add(new long[] {read, written, nanos, completed ? 1 : 0});
            if (throwOnTransfer) {
                throw new IllegalStateException("telemetry unavailable");
            }
        }

        @Override
        public void recordSuccess(long bytes) {
            successes++;
        }

        @Override
        public void recordMissingFileNullWritten(boolean httpNotFound) {}

        @Override
        public void recordFetchFailureNullWritten(Throwable throwable) {
            nullFailures++;
        }

        @Override
        public void recordFetchFailure(Throwable throwable) {
            failures++;
        }
    }
}
