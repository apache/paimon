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

package org.apache.paimon.format.mosaic;

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.mosaic.MosaicReader;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/** Test for {@link MosaicRecordsReader}. */
class MosaicRecordsReaderTest {

    @Test
    void testConstructorRuntimeExceptionClosesCreatedResources() throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        RuntimeException failure = new RuntimeException("native reader failed");

        assertThatThrownBy(
                        () ->
                                new MosaicRecordsReader(
                                        inputFileAdapter,
                                        0,
                                        rowType(),
                                        rowType(),
                                        null,
                                        new Path("file:/tmp/mosaic-reader-test"),
                                        allocator,
                                        (inputFile, fileSize, bufferAllocator) -> {
                                            throw failure;
                                        }))
                .isSameAs(failure);

        assertThat(allocator.closeCount()).isEqualTo(1);
        assertThat(inputStream.closeCount()).isEqualTo(1);
    }

    @Test
    void testConstructorErrorClosesCreatedResources() throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        UnsatisfiedLinkError failure = new UnsatisfiedLinkError("native library failed");

        assertThatThrownBy(
                        () ->
                                new MosaicRecordsReader(
                                        inputFileAdapter,
                                        0,
                                        rowType(),
                                        rowType(),
                                        null,
                                        new Path("file:/tmp/mosaic-reader-test"),
                                        allocator,
                                        (inputFile, fileSize, bufferAllocator) -> {
                                            throw failure;
                                        }))
                .isSameAs(failure);

        assertThat(allocator.closeCount()).isEqualTo(1);
        assertThat(inputStream.closeCount()).isEqualTo(1);
    }

    @Test
    void testConstructorFailureAfterReaderCreatedClosesReaderAndOtherResources()
            throws IOException {
        CloseCountingSeekableInputStream inputStream = new CloseCountingSeekableInputStream();
        MosaicInputFileAdapter inputFileAdapter = createInputFileAdapter(inputStream);
        CloseCountingRootAllocator allocator = new CloseCountingRootAllocator();
        MosaicReader reader = mock(MosaicReader.class);
        RuntimeException failure = new RuntimeException("schema failed");
        doThrow(failure).when(reader).getSchema();

        assertThatThrownBy(
                        () ->
                                new MosaicRecordsReader(
                                        inputFileAdapter,
                                        0,
                                        rowType(),
                                        rowType(),
                                        null,
                                        new Path("file:/tmp/mosaic-reader-test"),
                                        allocator,
                                        (inputFile, fileSize, bufferAllocator) -> reader))
                .isSameAs(failure);

        verify(reader).close();
        assertThat(allocator.closeCount()).isEqualTo(1);
        assertThat(inputStream.closeCount()).isEqualTo(1);
    }

    private static MosaicInputFileAdapter createInputFileAdapter(
            CloseCountingSeekableInputStream inputStream) throws IOException {
        return new MosaicInputFileAdapter(
                new CloseCountingFileIO(inputStream), new Path("file:/tmp/mosaic-reader-test"));
    }

    private static RowType rowType() {
        return DataTypes.ROW(DataTypes.INT());
    }

    private static class CloseCountingFileIO extends LocalFileIO {

        private final CloseCountingSeekableInputStream inputStream;

        private CloseCountingFileIO(CloseCountingSeekableInputStream inputStream) {
            this.inputStream = inputStream;
        }

        @Override
        public SeekableInputStream newInputStream(Path path) {
            return inputStream;
        }
    }

    private static class CloseCountingSeekableInputStream extends SeekableInputStream {

        private int closeCount;

        @Override
        public void seek(long desired) {}

        @Override
        public long getPos() {
            return 0;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            return -1;
        }

        @Override
        public int read() {
            return -1;
        }

        @Override
        public void close() {
            closeCount++;
        }

        int closeCount() {
            return closeCount;
        }
    }

    private static class CloseCountingRootAllocator extends RootAllocator {

        private int closeCount;

        @Override
        public void close() {
            closeCount++;
            super.close();
        }

        int closeCount() {
            return closeCount;
        }
    }
}
