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

package org.apache.paimon.lumina.index;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link LuminaVectorGlobalIndexWriter.FileBackedDataset} temp file format. */
public class FileBackedDatasetTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testBasicReadback() throws IOException {
        int dim = 3;
        int numVectors = 4;
        long[] rowIds = {0, 2, 5, 7};
        float[][] vectors = {
            {1.0f, 2.0f, 3.0f},
            {4.0f, 5.0f, 6.0f},
            {7.0f, 8.0f, 9.0f},
            {10.0f, 11.0f, 12.0f}
        };

        File tempFile = writeTempFile(dim, rowIds, vectors);

        try (LuminaVectorGlobalIndexWriter.FileBackedDataset ds =
                new LuminaVectorGlobalIndexWriter.FileBackedDataset(
                        tempFile, dim, numVectors, "test")) {
            assertThat(ds.dim()).isEqualTo(dim);
            assertThat(ds.totalSize()).isEqualTo(numVectors);

            float[] vectorBuf = new float[dim * numVectors];
            long[] idBuf = new long[numVectors];
            long read = ds.getNextBatch(vectorBuf, idBuf);

            assertThat(read).isEqualTo(numVectors);
            assertThat(idBuf).containsExactly(0, 2, 5, 7);
            assertThat(vectorBuf[0]).isEqualTo(1.0f);
            assertThat(vectorBuf[3]).isEqualTo(4.0f);
            assertThat(vectorBuf[9]).isEqualTo(10.0f);
            assertThat(vectorBuf[11]).isEqualTo(12.0f);

            assertThat(ds.getNextBatch(vectorBuf, idBuf)).isEqualTo(0);
        }
    }

    @Test
    public void testSmallBufferForcesBoundary() throws IOException {
        int dim = 2;
        int numVectors = 5;
        long[] rowIds = {0, 1, 3, 6, 10};
        float[][] vectors = {
            {1.0f, 2.0f},
            {3.0f, 4.0f},
            {5.0f, 6.0f},
            {7.0f, 8.0f},
            {9.0f, 10.0f}
        };

        File tempFile = writeTempFile(dim, rowIds, vectors);

        // Record size = 8 + 2*4 = 16 bytes. Use buffer of 20 bytes so records always cross
        // boundary.
        int smallBuffer = 20;
        try (LuminaVectorGlobalIndexWriter.FileBackedDataset ds =
                new LuminaVectorGlobalIndexWriter.FileBackedDataset(
                        tempFile, dim, numVectors, "test", smallBuffer)) {
            float[] vectorBuf = new float[dim * 2];
            long[] idBuf = new long[2];

            // Read in batches of 2
            long read = ds.getNextBatch(vectorBuf, idBuf);
            assertThat(read).isEqualTo(2);
            assertThat(idBuf[0]).isEqualTo(0);
            assertThat(idBuf[1]).isEqualTo(1);
            assertThat(vectorBuf[0]).isEqualTo(1.0f);
            assertThat(vectorBuf[2]).isEqualTo(3.0f);

            read = ds.getNextBatch(vectorBuf, idBuf);
            assertThat(read).isEqualTo(2);
            assertThat(idBuf[0]).isEqualTo(3);
            assertThat(idBuf[1]).isEqualTo(6);

            read = ds.getNextBatch(vectorBuf, idBuf);
            assertThat(read).isEqualTo(1);
            assertThat(idBuf[0]).isEqualTo(10);
            assertThat(vectorBuf[0]).isEqualTo(9.0f);
            assertThat(vectorBuf[1]).isEqualTo(10.0f);

            assertThat(ds.getNextBatch(vectorBuf, idBuf)).isEqualTo(0);
        }
    }

    @Test
    public void testSingleRecordPerBuffer() throws IOException {
        int dim = 4;
        int numVectors = 3;
        long[] rowIds = {100, 200, 300};
        float[][] vectors = {
            {1.0f, 2.0f, 3.0f, 4.0f},
            {5.0f, 6.0f, 7.0f, 8.0f},
            {9.0f, 10.0f, 11.0f, 12.0f}
        };

        File tempFile = writeTempFile(dim, rowIds, vectors);

        // Record size = 8 + 4*4 = 24. Buffer exactly fits one record.
        int exactBuffer = 24;
        try (LuminaVectorGlobalIndexWriter.FileBackedDataset ds =
                new LuminaVectorGlobalIndexWriter.FileBackedDataset(
                        tempFile, dim, numVectors, "test", exactBuffer)) {
            float[] vectorBuf = new float[dim];
            long[] idBuf = new long[1];

            for (int i = 0; i < numVectors; i++) {
                long read = ds.getNextBatch(vectorBuf, idBuf);
                assertThat(read).isEqualTo(1);
                assertThat(idBuf[0]).isEqualTo(rowIds[i]);
                for (int d = 0; d < dim; d++) {
                    assertThat(vectorBuf[d]).isEqualTo(vectors[i][d]);
                }
            }
            assertThat(ds.getNextBatch(vectorBuf, idBuf)).isEqualTo(0);
        }
    }

    private File writeTempFile(int dim, long[] rowIds, float[][] vectors) throws IOException {
        File tempFile = new File(tempDir.toFile(), "test-vectors.bin");
        try (RandomAccessFile raf = new RandomAccessFile(tempFile, "rw");
                FileChannel channel = raf.getChannel()) {
            int recordSize = Long.BYTES + dim * Float.BYTES;
            ByteBuffer buf = ByteBuffer.allocate(recordSize);
            buf.order(ByteOrder.nativeOrder());
            for (int i = 0; i < rowIds.length; i++) {
                buf.clear();
                buf.putLong(rowIds[i]);
                for (int d = 0; d < dim; d++) {
                    buf.putFloat(vectors[i][d]);
                }
                buf.flip();
                while (buf.hasRemaining()) {
                    channel.write(buf);
                }
            }
        }
        return tempFile;
    }
}
