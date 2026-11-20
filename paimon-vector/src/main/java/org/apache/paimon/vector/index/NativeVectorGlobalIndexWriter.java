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

package org.apache.paimon.vector.index;

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.fs.ByteArraySeekableStream;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.fs.PositionOutputStreamWrapper;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.index.vector.CentroidRouter;
import org.apache.paimon.index.vector.VectorIndexMetadata;
import org.apache.paimon.index.vector.VectorIndexReader;
import org.apache.paimon.index.vector.VectorIndexTrainer;
import org.apache.paimon.index.vector.VectorIndexTraining;
import org.apache.paimon.index.vector.VectorIndexWriter;
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
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Vector global index writer using paimon-vector-index-java.
 *
 * <p>Vectors are spilled to a temporary file on disk as they arrive via {@link #write(Object,
 * long)}, keeping Java heap usage constant (~8 MB buffer). During index build, vectors are read
 * back for training and batch insertion.
 *
 * <p><b>Thread safety:</b> This class is <b>not</b> thread-safe.
 */
public class NativeVectorGlobalIndexWriter implements GlobalIndexSingleColumnWriter, Closeable {

    private static final String FILE_NAME_PREFIX = "vector";

    private static final Logger LOG = LoggerFactory.getLogger(NativeVectorGlobalIndexWriter.class);

    private static final int IO_BUFFER_SIZE = 8 * 1024 * 1024;
    private static final int ADD_BATCH_SIZE = 10000;
    private static final int TRAIN_BATCH_SIZE = 4096;
    private static final long WRITE_PROGRESS_INTERVAL_BYTES = 64L * 1024 * 1024;
    static final int MAX_FLOAT_ARRAY_LENGTH = Integer.MAX_VALUE - 8;
    private static final long TRAIN_MEMORY_WARNING_BYTES = 4L * 1024 * 1024 * 1024;

    private final GlobalIndexFileWriter fileWriter;
    private final String identifier;
    private final Map<String, String> nativeOptions;
    private final int dim;
    private final double trainSampleRatio;
    private final boolean centroidShard;

    private File tempVectorFile;
    private FileChannel writeChannel;
    // Writes, training reads and index insertion use this buffer in sequence.
    private ByteBuffer spillBuffer;

    private final int recordSizeInBytes;
    private float[] vectorBuf;
    private long count;
    private boolean closed;

    private long rowCount;

    public NativeVectorGlobalIndexWriter(
            GlobalIndexFileWriter fileWriter,
            DataType fieldType,
            Map<String, String> options,
            String identifier) {
        this(
                fileWriter,
                fieldType,
                options,
                identifier,
                NativeVectorGlobalIndexerFactory.DEFAULT_TRAIN_SAMPLE_RATIO);
    }

    public NativeVectorGlobalIndexWriter(
            GlobalIndexFileWriter fileWriter,
            DataType fieldType,
            Map<String, String> options,
            String identifier,
            double trainSampleRatio) {
        this(fileWriter, fieldType, options, identifier, trainSampleRatio, false);
    }

    NativeVectorGlobalIndexWriter(
            GlobalIndexFileWriter fileWriter,
            DataType fieldType,
            Map<String, String> options,
            String identifier,
            double trainSampleRatio,
            boolean centroidShard) {
        this.centroidShard = centroidShard;
        this.fileWriter = fileWriter;
        this.identifier = identifier;
        validateFieldType(fieldType);
        this.nativeOptions = options;
        this.dim = Integer.parseInt(options.get("dimension"));
        if (Double.isNaN(trainSampleRatio)
                || Double.isInfinite(trainSampleRatio)
                || trainSampleRatio <= 0
                || trainSampleRatio > 1) {
            throw new IllegalArgumentException(
                    "trainSampleRatio must be greater than 0 and less than or equal to 1: "
                            + trainSampleRatio);
        }
        this.trainSampleRatio = trainSampleRatio;
        this.count = 0;
        this.closed = false;
        this.recordSizeInBytes = checkedRecordSize(dim, IO_BUFFER_SIZE);
    }

    private void openSpillFile() {
        this.vectorBuf = new float[dim];
        this.spillBuffer =
                ByteBuffer.allocateDirect(Math.max(8192, recordSizeInBytes))
                        .order(ByteOrder.nativeOrder());

        try {
            this.tempVectorFile = File.createTempFile("paimon-vector-vectors-", ".bin");
            this.tempVectorFile.deleteOnExit();
            @SuppressWarnings("resource")
            RandomAccessFile raf = new RandomAccessFile(tempVectorFile, "rw");
            this.writeChannel = raf.getChannel();
        } catch (IOException e) {
            close();
            throw new RuntimeException("Failed to create temp vector file", e);
        }
    }

    private void validateFieldType(DataType dataType) {
        if (dataType instanceof VectorType) {
            DataType elementType = ((VectorType) dataType).getElementType();
            if (!(elementType instanceof FloatType)) {
                throw new IllegalArgumentException(
                        "Vector index requires float vector, but got: " + elementType);
            }
            return;
        }
        if (dataType instanceof ArrayType) {
            DataType elementType = ((ArrayType) dataType).getElementType();
            if (!(elementType instanceof FloatType)) {
                throw new IllegalArgumentException(
                        "Vector index requires float array, but got: " + elementType);
            }
            return;
        }
        throw new IllegalArgumentException(
                "Vector index requires VectorType or ArrayType<FLOAT>, but got: " + dataType);
    }

    @Override
    public void write(Object fieldData, long rowId) {
        if (closed) {
            throw new IllegalStateException("Vector index writer is closed.");
        }
        if (fieldData == null) {
            rowCount++;
            return;
        }

        if (centroidShard && rowId < 0) {
            throw new IllegalArgumentException(
                    "Centroid shards require nonnegative relative row IDs.");
        }
        if (spillBuffer == null) {
            openSpillFile();
        }
        float[] src = materializeAndValidate(fieldData, rowId);

        if (spillBuffer.remaining() < recordSizeInBytes) {
            flushWriteBuffer();
            if (spillBuffer.capacity() < IO_BUFFER_SIZE) {
                spillBuffer =
                        ByteBuffer.allocateDirect(
                                        Math.min(IO_BUFFER_SIZE, spillBuffer.capacity() * 2))
                                .order(ByteOrder.nativeOrder());
            }
        }
        spillBuffer.putLong(rowId);
        for (int i = 0; i < dim; i++) {
            spillBuffer.putFloat(src[i]);
        }
        rowCount++;
        count++;
    }

    private float[] materializeAndValidate(Object fieldData, long relativeRowId) {
        if (fieldData instanceof float[]) {
            float[] vector = (float[]) fieldData;
            checkDimension(vector.length);
            for (int i = 0; i < dim; i++) {
                checkFinite(vector[i], relativeRowId, i);
            }
            return vector;
        } else if (fieldData instanceof InternalVector) {
            InternalVector vector = (InternalVector) fieldData;
            checkDimension(vector.size());
            for (int i = 0; i < dim; i++) {
                float v = vector.getFloat(i);
                checkFinite(v, relativeRowId, i);
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
                checkFinite(v, relativeRowId, i);
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
            spillBuffer.flip();
            while (spillBuffer.hasRemaining()) {
                writeChannel.write(spillBuffer);
            }
            spillBuffer.clear();
        } catch (IOException e) {
            throw new RuntimeException("Failed to flush vector buffer to disk", e);
        }
    }

    @Override
    public List<ResultEntry> finish() {
        if (closed) {
            throw new IllegalStateException("Vector index writer is closed.");
        }
        try {
            if (count == 0) {
                if (!centroidShard) {
                    return Collections.emptyList();
                }
                String name = fileWriter.newFileName(fileNamePrefix());
                try (PositionOutputStream out = fileWriter.newOutputStream(name)) {
                    out.write(new byte[] {'P', 'C', 'E', 'M', 'P', 'T', 'Y', '1'});
                }
                return Collections.singletonList(new ResultEntry(name, 0, null));
            }
            flushWriteBuffer();
            writeChannel.close();
            writeChannel = null;
            return Collections.singletonList(buildIndex());
        } catch (IOException e) {
            throw new RuntimeException("Failed to write vector global index", e);
        } finally {
            close();
        }
    }

    private ResultEntry buildIndex() throws IOException {
        LOG.info("{} vector index build started: {} vectors, dim={}", identifier, count, dim);
        long buildStart = System.currentTimeMillis();

        // Phase 1: Train
        long phaseStart = System.currentTimeMillis();
        LOG.info("{} train phase started", identifier);
        try (VectorIndexTraining training = trainFromTempFile();
                VectorIndexWriter writer = new VectorIndexWriter(training)) {
            LOG.info(
                    "{} train phase done in {} ms",
                    identifier,
                    System.currentTimeMillis() - phaseStart);

            // Phase 2: Add all vectors in batches
            phaseStart = System.currentTimeMillis();
            LOG.info("{} add phase started", identifier);
            addVectorsFromTempFile(writer);
            LOG.info(
                    "{} add phase done in {} ms",
                    identifier,
                    System.currentTimeMillis() - phaseStart);

            // Phase 3: Write index
            phaseStart = System.currentTimeMillis();
            LOG.info("{} write phase started", identifier);
            String fileName = fileWriter.newFileName(fileNamePrefix());
            // Native IVF-FLAT and IVF-PQ readers obtain their geometry from the 64-byte header.
            // Capture it as it is written so automatic local nlist is resolved by native itself.
            byte[] header =
                    centroidShard && ("ivf-flat".equals(identifier) || "ivf-pq".equals(identifier))
                            ? new byte[64]
                            : null;
            try (PositionOutputStream out = fileWriter.newOutputStream(fileName)) {
                writer.writeIndex(
                        new PositionOutputStreamWrapper(out) {
                            private long bytesWritten;

                            @Override
                            public void write(byte[] b) throws IOException {
                                super.write(b);
                                if (header != null && bytesWritten < header.length) {
                                    int offset = (int) bytesWritten;
                                    System.arraycopy(
                                            b,
                                            0,
                                            header,
                                            offset,
                                            Math.min(b.length, header.length - offset));
                                }
                                long previous = bytesWritten;
                                bytesWritten += b.length;
                                if (bytesWritten / WRITE_PROGRESS_INTERVAL_BYTES
                                        > previous / WRITE_PROGRESS_INTERVAL_BYTES) {
                                    LOG.info(
                                            "{} write progress: {} MiB written",
                                            identifier,
                                            bytesWritten / (1024 * 1024));
                                }
                            }
                        });
                out.flush();
            }
            LOG.info(
                    "{} write phase done in {} ms",
                    identifier,
                    System.currentTimeMillis() - phaseStart);

            LOG.info(
                    "{} vector index build completed in {} ms",
                    identifier,
                    System.currentTimeMillis() - buildStart);

            VectorIndexMeta descriptor = new VectorIndexMeta();
            if (header != null) {
                try (VectorIndexReader reader =
                        new VectorIndexReader(
                                new NativeVectorGlobalIndexReader.SeekableStreamVectorIndexInput(
                                        new ByteArraySeekableStream(header)))) {
                    VectorIndexMetadata local = reader.metadata();
                    descriptor = new VectorIndexMeta(local.nlist(), local.metric());
                }
            }
            return new ResultEntry(
                    fileName, !centroidShard ? rowCount : count, descriptor.serialize());
        }
    }

    private String fileNamePrefix() {
        return FILE_NAME_PREFIX + "-" + identifier;
    }

    private VectorIndexTraining trainFromTempFile() throws IOException {
        Map<String, String> options = trainingOptions(nativeOptions, count);
        if (centroidShard) {
            options.put("expected-vector-count", Long.toString(count));
            String nlist = options.get("nlist");
            if (nlist != null && !"auto".equals(nlist)) {
                options.put(
                        "nlist",
                        Integer.toString(
                                Math.min(
                                        trainingVectorCount(count, trainSampleRatio),
                                        Integer.parseInt(nlist))));
            }
        }
        if ("diskann".equals(identifier)) {
            options.remove("nlist");
        }
        try (VectorIndexTrainer trainer = VectorIndexTrainer.create(options)) {
            int trainCount = trainingVectorCount(count, trainSampleRatio);
            logTrainingMemoryEstimate(trainCount);
            addTrainingBatches(trainer, trainCount);
            return trainer.finishTraining();
        }
    }

    private void addTrainingBatches(VectorIndexTrainer trainer, long trainCount)
            throws IOException {
        int trainBatchSize =
                (int)
                        Math.min(
                                trainCount,
                                Math.min(TRAIN_BATCH_SIZE, IO_BUFFER_SIZE / recordSizeInBytes));
        float[] batchVectors = new float[trainBatchSize * dim];

        try (RandomAccessFile raf = new RandomAccessFile(tempVectorFile, "r");
                FileChannel channel = raf.getChannel()) {
            ByteBuffer readBuf = spillBuffer;
            readBuf.limit(0);

            long selected = 0;
            long nextSampleIndex = 0;
            int batchCount = 0;

            for (long recordIndex = 0;
                    recordIndex < count && selected < trainCount;
                    recordIndex++) {
                ensureAvailable(readBuf, channel, recordSizeInBytes);
                readBuf.getLong(); // skip rowId
                if (recordIndex == nextSampleIndex) {
                    for (int d = 0; d < dim; d++) {
                        batchVectors[batchCount * dim + d] = readBuf.getFloat();
                    }
                    selected++;
                    batchCount++;
                    if (batchCount == trainBatchSize) {
                        trainer.addTrainingVectors(batchVectors, batchCount);
                        batchCount = 0;
                    }
                    if (selected < trainCount) {
                        nextSampleIndex = sampleIndex(selected, count, trainCount);
                    }
                } else {
                    readBuf.position(readBuf.position() + dim * Float.BYTES);
                }
            }

            if (batchCount > 0) {
                trainer.addTrainingVectors(
                        Arrays.copyOf(batchVectors, batchCount * dim), batchCount);
            }
            if (selected != trainCount) {
                throw new IOException(
                        "Expected to select "
                                + trainCount
                                + " training vectors, but selected "
                                + selected);
            }
        }
    }

    CentroidRouter finishRoutingModel(NativeCentroidVectorGlobalIndexer indexer)
            throws IOException {
        if (count == 0) {
            throw new IllegalArgumentException(
                    "Cannot train a centroid model without non-null vectors.");
        }
        flushWriteBuffer();
        try (VectorIndexTrainer trainer = indexer.createCoarseTrainer(count)) {
            addTrainingBatches(trainer, trainingCandidateCount(count, trainSampleRatio));
            try (VectorIndexTraining coarse = trainer.finishTraining()) {
                return CentroidRouter.fromTraining(coarse);
            }
        }
    }

    private static Map<String, String> trainingOptions(
            Map<String, String> nativeOptions, long count) {
        Map<String, String> options = new LinkedHashMap<>(nativeOptions);
        options.remove("centroid.nlist");
        String indexType = options.get("index.type");
        String nlist = options.get("nlist");
        if (indexType != null
                && indexType.startsWith("ivf_")
                && (nlist == null || "auto".equals(nlist))
                && !options.containsKey("expected-vector-count")) {
            options.put("expected-vector-count", Long.toString(count));
        }
        return options;
    }

    private void addVectorsFromTempFile(VectorIndexWriter writer) throws IOException {
        int addBatchSize = vectorBatchSize((int) Math.min(count, ADD_BATCH_SIZE), dim);
        long[] batchIds = new long[addBatchSize];
        float[] batchVectors = new float[addBatchSize * dim];

        try (RandomAccessFile raf = new RandomAccessFile(tempVectorFile, "r");
                FileChannel channel = raf.getChannel()) {
            ByteBuffer readBuf = spillBuffer;
            readBuf.limit(0);

            long remaining = count;
            int lastLoggedPercent = -1;

            while (remaining > 0) {
                int thisBatch = (int) Math.min(addBatchSize, remaining);
                for (int i = 0; i < thisBatch; i++) {
                    ensureAvailable(readBuf, channel, recordSizeInBytes);
                    batchIds[i] = readBuf.getLong();
                    for (int d = 0; d < dim; d++) {
                        batchVectors[i * dim + d] = readBuf.getFloat();
                    }
                }
                if (thisBatch == addBatchSize) {
                    writer.addVectors(batchIds, batchVectors, thisBatch);
                } else {
                    writer.addVectors(
                            Arrays.copyOf(batchIds, thisBatch),
                            Arrays.copyOf(batchVectors, thisBatch * dim),
                            thisBatch);
                }
                remaining -= thisBatch;

                int percent = (int) ((count - remaining) * 100 / count);
                if (percent / 10 > lastLoggedPercent / 10) {
                    LOG.info(
                            "{} add progress: {}/{} vectors ({}%)",
                            identifier, count - remaining, count, percent);
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

    private void checkFinite(float value, long relativeRowId, int elementIndex) {
        if (!Float.isFinite(value)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Vector element at rowId=%d, index=%d is %s",
                            relativeRowId, elementIndex, Float.toString(value)));
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

    static int trainingVectorCount(long vectorCount, double trainSampleRatio) {
        long trainCount = trainingCandidateCount(vectorCount, trainSampleRatio);
        if (trainCount > Integer.MAX_VALUE) {
            throw new IllegalStateException(
                    "Training vector count "
                            + trainCount
                            + " exceeds Java integer capacity. Reduce train.sample-ratio.");
        }
        return (int) trainCount;
    }

    static long trainingCandidateCount(long vectorCount, double trainSampleRatio) {
        if (vectorCount <= 0) {
            return 0;
        }
        if (Double.isNaN(trainSampleRatio)
                || Double.isInfinite(trainSampleRatio)
                || trainSampleRatio <= 0
                || trainSampleRatio > 1) {
            throw new IllegalArgumentException(
                    "trainSampleRatio must be greater than 0 and less than or equal to 1: "
                            + trainSampleRatio);
        }
        long trainCount = (long) Math.ceil(vectorCount * trainSampleRatio);
        return Math.max(1L, Math.min(vectorCount, trainCount));
    }

    static int vectorBatchSize(int requestedBatchSize, int dim) {
        if (requestedBatchSize <= 0) {
            throw new IllegalArgumentException(
                    "requestedBatchSize must be a positive integer: " + requestedBatchSize);
        }
        if (dim <= 0) {
            throw new IllegalArgumentException("dim must be a positive integer: " + dim);
        }
        int maxBatchSize = MAX_FLOAT_ARRAY_LENGTH / dim;
        if (maxBatchSize <= 0) {
            throw new IllegalStateException(
                    "Vector dimension " + dim + " exceeds Java float array capacity");
        }
        return Math.min(requestedBatchSize, maxBatchSize);
    }

    private void logTrainingMemoryEstimate(int trainCount) {
        long rawBytes = saturatedMultiply(saturatedMultiply(trainCount, dim), Float.BYTES);
        long estimatedPeakBytes = saturatedMultiply(rawBytes, 2);
        if (estimatedPeakBytes >= TRAIN_MEMORY_WARNING_BYTES) {
            LOG.warn(
                    "{} training uses {} samples out of {} vectors (dim={}). Estimated native "
                            + "training peak is at least {} bytes (~{} GiB) before OPQ and "
                            + "temporary buffers.",
                    identifier,
                    trainCount,
                    count,
                    dim,
                    estimatedPeakBytes,
                    String.format("%.2f", estimatedPeakBytes / 1024.0 / 1024.0 / 1024.0));
        } else {
            LOG.info(
                    "{} training uses {} samples out of {} vectors (dim={})",
                    identifier,
                    trainCount,
                    count,
                    dim);
        }
    }

    private static long saturatedMultiply(long left, long right) {
        if (left == 0 || right == 0) {
            return 0;
        }
        if (left > Long.MAX_VALUE / right) {
            return Long.MAX_VALUE;
        }
        return left * right;
    }

    static long sampleIndex(long sampleOrdinal, long vectorCount, long trainCount) {
        long quotient = vectorCount / trainCount;
        long remainder = vectorCount % trainCount;
        if (sampleOrdinal != 0 && remainder > Long.MAX_VALUE / sampleOrdinal) {
            return BigInteger.valueOf(sampleOrdinal)
                    .multiply(BigInteger.valueOf(vectorCount))
                    .divide(BigInteger.valueOf(trainCount))
                    .longValueExact();
        }
        return sampleOrdinal * quotient + sampleOrdinal * remainder / trainCount;
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
            spillBuffer = null;
            vectorBuf = null;
            if (tempVectorFile != null) {
                tempVectorFile.delete();
                tempVectorFile = null;
            }
        }
    }
}
