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

package org.apache.paimon.ivfpq.index;

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.globalindex.GlobalIndexSingletonWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.index.ivfpq.IVFPQWriter;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.VectorType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Vector global index writer using IVF-PQ.
 *
 * <p>Vectors are spilled to a temporary file on disk as they arrive via {@link #write(Object)},
 * keeping Java heap usage constant (~8 MB buffer). During index build, vectors are read back for
 * training (with optional reservoir sampling) and batch insertion.
 *
 * <p><b>Thread safety:</b> This class is <b>not</b> thread-safe.
 */
public class IvfpqVectorGlobalIndexWriter implements GlobalIndexSingletonWriter, Closeable {

    private static final String FILE_NAME_PREFIX = "ivfpq";

    private static final Logger LOG = LoggerFactory.getLogger(IvfpqVectorGlobalIndexWriter.class);

    private static final int IO_BUFFER_SIZE = 8 * 1024 * 1024;

    private final GlobalIndexFileWriter fileWriter;
    private final IvfpqVectorIndexOptions options;
    private final int dim;

    private File tempVectorFile;
    private FileChannel writeChannel;
    private ByteBuffer writeBuf;

    private final int recordSizeInBytes;
    private final float[] vectorBuf;
    private long count;
    private boolean closed;

    private long logicalRowId;

    public IvfpqVectorGlobalIndexWriter(
            GlobalIndexFileWriter fileWriter,
            DataType fieldType,
            IvfpqVectorIndexOptions options) {
        this.fileWriter = fileWriter;
        this.options = options;
        this.dim = options.dimension();
        this.count = 0;
        this.closed = false;
        this.recordSizeInBytes = checkedRecordSize(dim, IO_BUFFER_SIZE);
        this.vectorBuf = new float[dim];

        validateFieldType(fieldType);

        try {
            this.tempVectorFile = File.createTempFile("ivfpq-vectors-", ".bin");
            this.tempVectorFile.deleteOnExit();
            @SuppressWarnings("resource")
            RandomAccessFile raf = new RandomAccessFile(tempVectorFile, "rw");
            this.writeChannel = raf.getChannel();
            this.writeBuf = ByteBuffer.allocateDirect(IO_BUFFER_SIZE);
            this.writeBuf.order(ByteOrder.nativeOrder());
        } catch (IOException e) {
            throw new RuntimeException("Failed to create temp vector file", e);
        }
    }

    private void validateFieldType(DataType dataType) {
        if (dataType instanceof VectorType) {
            DataType elementType = ((VectorType) dataType).getElementType();
            if (!(elementType instanceof FloatType)) {
                throw new IllegalArgumentException(
                        "IVF-PQ index requires float vector, but got: " + elementType);
            }
            return;
        }
        if (dataType instanceof ArrayType) {
            DataType elementType = ((ArrayType) dataType).getElementType();
            if (!(elementType instanceof FloatType)) {
                throw new IllegalArgumentException(
                        "IVF-PQ index requires float array, but got: " + elementType);
            }
            return;
        }
        throw new IllegalArgumentException(
                "IVF-PQ index requires VectorType or ArrayType<FLOAT>, but got: " + dataType);
    }

    @Override
    public void write(Object fieldData) {
        if (fieldData == null) {
            logicalRowId++;
            return;
        }

        float[] src = materializeAndValidate(fieldData);

        if (writeBuf.remaining() < recordSizeInBytes) {
            flushWriteBuffer();
        }
        writeBuf.putLong(logicalRowId);
        for (int i = 0; i < dim; i++) {
            writeBuf.putFloat(src[i]);
        }
        logicalRowId++;
        count++;
    }

    private float[] materializeAndValidate(Object fieldData) {
        if (fieldData instanceof float[]) {
            float[] vector = (float[]) fieldData;
            checkDimension(vector.length);
            for (int i = 0; i < dim; i++) {
                checkFinite(vector[i], i);
            }
            return vector;
        } else if (fieldData instanceof InternalVector) {
            InternalVector vector = (InternalVector) fieldData;
            checkDimension(vector.size());
            for (int i = 0; i < dim; i++) {
                float v = vector.getFloat(i);
                checkFinite(v, i);
                vectorBuf[i] = v;
            }
            return vectorBuf;
        } else if (fieldData instanceof InternalArray) {
            InternalArray array = (InternalArray) fieldData;
            checkDimension(array.size());
            for (int i = 0; i < dim; i++) {
                if (array.isNullAt(i)) {
                    throw new IllegalArgumentException("Vector element at index " + i + " is null");
                }
                float v = array.getFloat(i);
                checkFinite(v, i);
                vectorBuf[i] = v;
            }
            return vectorBuf;
        } else {
            throw new RuntimeException(
                    "Unsupported vector type: " + fieldData.getClass().getName());
        }
    }

    private void flushWriteBuffer() {
        try {
            writeBuf.flip();
            while (writeBuf.hasRemaining()) {
                writeChannel.write(writeBuf);
            }
            writeBuf.clear();
        } catch (IOException e) {
            throw new RuntimeException("Failed to flush vector buffer to disk", e);
        }
    }

    @Override
    public List<ResultEntry> finish() {
        try {
            if (count == 0) {
                writeChannel.close();
                writeChannel = null;
                writeBuf = null;
                return Collections.emptyList();
            }
            flushWriteBuffer();
            writeChannel.close();
            writeChannel = null;
            writeBuf = null;
            return Collections.singletonList(buildIndex());
        } catch (IOException e) {
            throw new RuntimeException("Failed to write IVF-PQ vector global index", e);
        } finally {
            if (tempVectorFile != null) {
                tempVectorFile.delete();
                tempVectorFile = null;
            }
        }
    }

    private ResultEntry buildIndex() throws IOException {
        int effectiveNlist = (int) Math.min(options.nlist(), count);

        LOG.info(
                "IVF-PQ index build started: {} vectors, dim={}, nlist={}, m={}, metric={}",
                count,
                dim,
                effectiveNlist,
                options.m(),
                options.metric());
        long buildStart = System.currentTimeMillis();

        try (IVFPQWriter writer =
                new IVFPQWriter(
                        dim,
                        effectiveNlist,
                        options.m(),
                        options.metric().toNativeMetric(),
                        options.useOpq())) {

            // Phase 1: Train
            long phaseStart = System.currentTimeMillis();
            LOG.info(
                    "IVF-PQ train phase started (sample_ratio={})", options.trainSampleRatio());
            trainFromTempFile(writer);
            LOG.info("IVF-PQ train phase done in {} ms", System.currentTimeMillis() - phaseStart);

            // Phase 2: Add all vectors in batches
            phaseStart = System.currentTimeMillis();
            LOG.info("IVF-PQ add phase started");
            addVectorsFromTempFile(writer);
            LOG.info("IVF-PQ add phase done in {} ms", System.currentTimeMillis() - phaseStart);

            // Phase 3: Write index
            phaseStart = System.currentTimeMillis();
            LOG.info("IVF-PQ write phase started");
            String fileName = fileWriter.newFileName(FILE_NAME_PREFIX);
            try (PositionOutputStream out = fileWriter.newOutputStream(fileName)) {
                writer.writeIndex(out);
                out.flush();
            }
            LOG.info("IVF-PQ write phase done in {} ms", System.currentTimeMillis() - phaseStart);

            LOG.info(
                    "IVF-PQ index build completed in {} ms",
                    System.currentTimeMillis() - buildStart);

            IvfpqIndexMeta meta = new IvfpqIndexMeta(options);
            return new ResultEntry(fileName, logicalRowId, meta.serialize());
        }
    }

    private void trainFromTempFile(IVFPQWriter writer) throws IOException {
        double sampleRatio = options.trainSampleRatio();
        int minTrainSize = (int) Math.min(count, Math.max(options.nlist() * 39L, 256));
        int sampleCount;
        if (sampleRatio >= 1.0) {
            sampleCount = (int) count;
        } else {
            sampleCount = Math.max((int) (count * sampleRatio), minTrainSize);
            sampleCount = (int) Math.min(sampleCount, count);
        }

        float[] trainData = new float[sampleCount * dim];

        try (RandomAccessFile raf = new RandomAccessFile(tempVectorFile, "r");
                FileChannel channel = raf.getChannel()) {
            ByteBuffer readBuf = ByteBuffer.allocateDirect(IO_BUFFER_SIZE);
            readBuf.order(ByteOrder.nativeOrder());
            readBuf.limit(0);

            if (sampleCount == (int) count) {
                // Read all vectors
                for (int i = 0; i < sampleCount; i++) {
                    ensureAvailable(readBuf, channel, recordSizeInBytes);
                    readBuf.getLong(); // skip rowId
                    for (int d = 0; d < dim; d++) {
                        trainData[i * dim + d] = readBuf.getFloat();
                    }
                }
            } else {
                // Reservoir sampling
                Random rng = new Random(42);
                int collected = 0;
                for (long i = 0; i < count; i++) {
                    ensureAvailable(readBuf, channel, recordSizeInBytes);
                    readBuf.getLong(); // skip rowId
                    if (collected < sampleCount) {
                        for (int d = 0; d < dim; d++) {
                            trainData[collected * dim + d] = readBuf.getFloat();
                        }
                        collected++;
                    } else {
                        int j = rng.nextInt((int) (i + 1));
                        if (j < sampleCount) {
                            for (int d = 0; d < dim; d++) {
                                trainData[j * dim + d] = readBuf.getFloat();
                            }
                        } else {
                            readBuf.position(readBuf.position() + dim * Float.BYTES);
                        }
                    }
                }
            }
        }

        writer.train(trainData, sampleCount);
    }

    private void addVectorsFromTempFile(IVFPQWriter writer) throws IOException {
        int batchSize = options.addBatchSize();
        long[] batchIds = new long[batchSize];
        float[] batchVectors = new float[batchSize * dim];

        try (RandomAccessFile raf = new RandomAccessFile(tempVectorFile, "r");
                FileChannel channel = raf.getChannel()) {
            ByteBuffer readBuf = ByteBuffer.allocateDirect(IO_BUFFER_SIZE);
            readBuf.order(ByteOrder.nativeOrder());
            readBuf.limit(0);

            long remaining = count;
            int lastLoggedPercent = -1;

            while (remaining > 0) {
                int thisBatch = (int) Math.min(batchSize, remaining);
                for (int i = 0; i < thisBatch; i++) {
                    ensureAvailable(readBuf, channel, recordSizeInBytes);
                    batchIds[i] = readBuf.getLong();
                    for (int d = 0; d < dim; d++) {
                        batchVectors[i * dim + d] = readBuf.getFloat();
                    }
                }
                writer.addVectors(batchIds, batchVectors, thisBatch);
                remaining -= thisBatch;

                int percent = (int) ((count - remaining) * 100 / count);
                if (percent / 10 > lastLoggedPercent / 10) {
                    LOG.info(
                            "IVF-PQ add progress: {}/{} vectors ({}%)",
                            count - remaining,
                            count,
                            percent);
                    lastLoggedPercent = percent;
                }
            }
        }
    }

    private static void ensureAvailable(ByteBuffer readBuf, FileChannel channel, int minBytes)
            throws IOException {
        int zeroReadCount = 0;
        while (readBuf.remaining() < minBytes) {
            readBuf.compact();
            int bytesRead = channel.read(readBuf);
            readBuf.flip();
            if (bytesRead == -1) {
                throw new IOException("Unexpected end of temp file");
            }
            if (bytesRead == 0) {
                if (++zeroReadCount > 100) {
                    throw new IOException(
                            "Unable to read from temp file: repeated zero-byte reads");
                }
            } else {
                zeroReadCount = 0;
            }
        }
    }

    private void checkDimension(int actualDim) {
        if (actualDim != dim) {
            throw new IllegalArgumentException(
                    String.format(
                            "Vector dimension mismatch: expected %d, but got %d", dim, actualDim));
        }
    }

    private void checkFinite(float value, int elementIndex) {
        if (!Float.isFinite(value)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Vector element at rowId=%d, index=%d is %s",
                            logicalRowId, elementIndex, Float.toString(value)));
        }
    }

    private static int checkedRecordSize(int dim, int bufferCapacity) {
        long recordSize = Long.BYTES + (long) dim * Float.BYTES;
        if (recordSize > bufferCapacity || recordSize > Integer.MAX_VALUE) {
            throw new IllegalStateException(
                    "Vector record size "
                            + recordSize
                            + " exceeds buffer capacity "
                            + bufferCapacity);
        }
        return (int) recordSize;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            try {
                if (writeChannel != null) {
                    writeChannel.close();
                }
            } catch (IOException ignored) {
            }
            writeBuf = null;
            if (tempVectorFile != null) {
                tempVectorFile.delete();
                tempVectorFile = null;
            }
        }
    }
}
