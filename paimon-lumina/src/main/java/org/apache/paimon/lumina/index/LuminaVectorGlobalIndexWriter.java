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

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.globalindex.GlobalIndexSingletonWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.FloatType;

import org.aliyun.lumina.LuminaDataset;
import org.aliyun.lumina.LuminaFileOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Vector global index writer using Lumina. Builds a single index file per shard.
 *
 * <p>Vectors are spilled to a temporary file on disk as they arrive via {@link #write(Object)},
 * keeping Java heap usage constant (~8 MB buffer) regardless of dataset size. During index build,
 * the Lumina builder streams vectors from the temp file via the {@link LuminaDataset} callback API.
 *
 * <p><b>Thread safety:</b> This class is <b>not</b> thread-safe. The underlying {@code
 * LuminaBuilder} must be used from a single thread or the caller must provide external
 * synchronization. The internal Lumina executor thread pool size is controlled globally by the
 * {@code LUMINA_EXECUTOR_THREAD_COUNT} environment variable and cannot be configured per-instance.
 */
public class LuminaVectorGlobalIndexWriter implements GlobalIndexSingletonWriter, Closeable {

    private static final String FILE_NAME_PREFIX = "lumina";

    private static final Logger LOG = LoggerFactory.getLogger(LuminaVectorGlobalIndexWriter.class);

    /**
     * Tracks which environment variables have already been set in this JVM. Used to avoid
     * concurrent reflection-based modification of {@code System.getenv()} when multiple shard tasks
     * run in parallel on the same TaskManager.
     */
    private static final ConcurrentHashMap<String, String> CONFIGURED_ENV_VARS =
            new ConcurrentHashMap<>();

    /** I/O buffer size for reading/writing the temp vector file (~8 MB). */
    private static final int IO_BUFFER_SIZE = 8 * 1024 * 1024;

    private final GlobalIndexFileWriter fileWriter;
    private final LuminaVectorIndexOptions options;
    private final int dim;

    /** Temporary file storing vectors as raw native-order floats. */
    private File tempVectorFile;

    private FileChannel writeChannel;
    private ByteBuffer writeBuf;

    private int count;
    private boolean closed;

    public LuminaVectorGlobalIndexWriter(
            GlobalIndexFileWriter fileWriter,
            DataType fieldType,
            LuminaVectorIndexOptions options) {
        this.fileWriter = fileWriter;
        this.options = options;
        this.dim = options.dimension();
        this.count = 0;
        this.closed = false;

        validateFieldType(fieldType);

        try {
            this.tempVectorFile = File.createTempFile("lumina-vectors-", ".bin");
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
        if (!(dataType instanceof ArrayType)) {
            throw new IllegalArgumentException(
                    "Lumina vector index requires ArrayType, but got: " + dataType);
        }
        DataType elementType = ((ArrayType) dataType).getElementType();
        if (!(elementType instanceof FloatType)) {
            throw new IllegalArgumentException(
                    "Lumina vector index requires float array, but got: " + elementType);
        }
    }

    @Override
    public void write(Object fieldData) {
        if (fieldData == null) {
            throw new IllegalArgumentException("Field data must not be null");
        }

        int bytesNeeded = dim * Float.BYTES;
        if (writeBuf.remaining() < bytesNeeded) {
            flushWriteBuffer();
        }

        if (fieldData instanceof float[]) {
            float[] vector = (float[]) fieldData;
            checkDimension(vector.length);
            for (int i = 0; i < dim; i++) {
                writeBuf.putFloat(vector[i]);
            }
        } else if (fieldData instanceof InternalArray) {
            InternalArray array = (InternalArray) fieldData;
            checkDimension(array.size());
            for (int i = 0; i < dim; i++) {
                if (array.isNullAt(i)) {
                    throw new IllegalArgumentException("Vector element at index " + i + " is null");
                }
                writeBuf.putFloat(array.getFloat(i));
            }
        } else {
            throw new RuntimeException(
                    "Unsupported vector type: " + fieldData.getClass().getName());
        }
        count++;
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
                return Collections.emptyList();
            }
            // Flush remaining data and close the write channel
            flushWriteBuffer();
            writeChannel.close();
            writeChannel = null;
            writeBuf = null;
            return Collections.singletonList(buildIndex());
        } catch (IOException e) {
            throw new RuntimeException("Failed to write Lumina vector global index", e);
        } finally {
            // Delete the temp file eagerly so index builds don't exhaust disk.
            // Production callers (e.g. DefaultGlobalIndexBuilder) call finish()
            // but never close(), so we must clean up here.
            if (tempVectorFile != null) {
                tempVectorFile.delete();
                tempVectorFile = null;
            }
        }
    }

    private ResultEntry buildIndex() throws IOException {
        configureExecutorThreadCount();
        LOG.info(
                "Lumina index build started: {} vectors, dim={}, type={}, metric={}",
                count,
                dim,
                options.indexType(),
                options.metric());
        long buildStart = System.currentTimeMillis();

        try (LuminaIndex index =
                LuminaIndex.createForBuild(
                        options.indexType(), dim, options.metric(), options.toLuminaOptions())) {

            // Pretrain and insert via streaming file-backed Dataset API
            long phaseStart = System.currentTimeMillis();
            LOG.info("Lumina pretrain phase started");
            try (FileBackedDataset ds =
                    new FileBackedDataset(tempVectorFile, dim, count, "pretrain")) {
                index.pretrainFrom(ds);
            }
            LOG.info(
                    "Lumina pretrain phase done in {} ms", System.currentTimeMillis() - phaseStart);

            phaseStart = System.currentTimeMillis();
            LOG.info("Lumina insert phase started");
            try (FileBackedDataset ds =
                    new FileBackedDataset(tempVectorFile, dim, count, "insert")) {
                index.insertFrom(ds);
            }
            LOG.info("Lumina insert phase done in {} ms", System.currentTimeMillis() - phaseStart);

            phaseStart = System.currentTimeMillis();
            LOG.info("Lumina dump phase started");
            String fileName = fileWriter.newFileName(FILE_NAME_PREFIX);
            try (PositionOutputStream out = fileWriter.newOutputStream(fileName)) {
                index.dump(new OutputStreamFileOutput(out));
                out.flush();
            }
            LOG.info("Lumina dump phase done in {} ms", System.currentTimeMillis() - phaseStart);

            LOG.info(
                    "Lumina index build completed in {} ms",
                    System.currentTimeMillis() - buildStart);

            LuminaIndexMeta meta = new LuminaIndexMeta(options.toLuminaOptions());
            return new ResultEntry(fileName, count, meta.serialize());
        }
    }

    /**
     * Sets the {@code LUMINA_EXECUTOR_THREAD_COUNT} environment variable from the {@link
     * LuminaVectorIndexOptions#DISKANN_BUILD_THREAD_COUNT} option so the native Lumina executor
     * thread pool is sized correctly before the builder is created.
     */
    private void configureExecutorThreadCount() {
        Map<String, String> luminaOpts = options.toLuminaOptions();
        String threadCountKey =
                LuminaVectorIndexOptions.toLuminaKey(
                        LuminaVectorIndexOptions.DISKANN_BUILD_THREAD_COUNT);
        String threadCount = luminaOpts.get(threadCountKey);
        if (threadCount != null && !threadCount.isEmpty()) {
            setEnv("LUMINA_EXECUTOR_THREAD_COUNT", threadCount);
        }
    }

    /**
     * Best-effort, thread-safe, idempotent attempt to set an environment variable in the current
     * process. Uses reflection to modify the JVM's process environment map. This is necessary
     * because Lumina's native executor thread pool reads {@code LUMINA_EXECUTOR_THREAD_COUNT} from
     * the OS environment.
     *
     * <p>When multiple shard tasks run in parallel on the same TaskManager JVM, this method ensures
     * the reflection-based map modification happens at most once per key-value pair.
     */
    @SuppressWarnings("unchecked")
    private static void setEnv(String key, String value) {
        // Skip if already set to the same value in this JVM
        if (value.equals(CONFIGURED_ENV_VARS.get(key))) {
            return;
        }
        synchronized (CONFIGURED_ENV_VARS) {
            // Double-check after acquiring lock
            if (value.equals(CONFIGURED_ENV_VARS.get(key))) {
                return;
            }
            try {
                Map<String, String> env = System.getenv();
                Field field = env.getClass().getDeclaredField("m");
                field.setAccessible(true);
                ((Map<String, String>) field.get(env)).put(key, value);
                CONFIGURED_ENV_VARS.put(key, value);
            } catch (Exception e) {
                LOG.warn(
                        "Failed to set environment variable '{}' for Lumina executor. "
                                + "Thread-count tuning may not take effect.",
                        key,
                        e);
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

    /**
     * A {@link LuminaDataset} that streams vectors from a temporary file. Reads are sequential and
     * buffered for efficiency. Each instance reads the file from the beginning independently.
     */
    static class FileBackedDataset implements LuminaDataset, Closeable {
        private final RandomAccessFile raf;
        private final FileChannel channel;
        private final int dim;
        private final int totalCount;
        private int cursor;
        private final ByteBuffer readBuf;
        private final String phase;
        private int lastLoggedPercent;

        FileBackedDataset(File file, int dim, int totalCount, String phase) throws IOException {
            this.raf = new RandomAccessFile(file, "r");
            this.channel = raf.getChannel();
            this.dim = dim;
            this.totalCount = totalCount;
            this.cursor = 0;
            this.readBuf = ByteBuffer.allocateDirect(IO_BUFFER_SIZE);
            this.readBuf.order(ByteOrder.nativeOrder());
            this.readBuf.limit(0); // empty initially
            this.phase = phase;
            this.lastLoggedPercent = -1;
        }

        @Override
        public int dim() {
            return dim;
        }

        @Override
        public long totalSize() {
            return totalCount;
        }

        @Override
        public long getNextBatch(float[] vectorBuf, long[] idBuf) {
            if (cursor >= totalCount) {
                return 0;
            }
            int remaining = totalCount - cursor;
            int maxByVectorBuf = vectorBuf.length / dim;
            int batchSize = Math.min(Math.min(maxByVectorBuf, idBuf.length), remaining);

            int floatsNeeded = batchSize * dim;
            int destOffset = 0;
            try {
                while (destOffset < floatsNeeded) {
                    if (readBuf.remaining() < Float.BYTES) {
                        // Compact any partial float bytes and refill
                        readBuf.compact();
                        int bytesRead = channel.read(readBuf);
                        readBuf.flip();
                        if (bytesRead == -1 && readBuf.remaining() < Float.BYTES) {
                            throw new IOException(
                                    "Unexpected end of temp file: read "
                                            + destOffset
                                            + " floats but need "
                                            + floatsNeeded);
                        }
                    }
                    int availableFloats = readBuf.remaining() / Float.BYTES;
                    int toRead = Math.min(availableFloats, floatsNeeded - destOffset);
                    readBuf.asFloatBuffer().get(vectorBuf, destOffset, toRead);
                    readBuf.position(readBuf.position() + toRead * Float.BYTES);
                    destOffset += toRead;
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to read vectors from temp file", e);
            }

            for (int i = 0; i < batchSize; i++) {
                idBuf[i] = cursor + i;
            }
            cursor += batchSize;

            int percent = (int) ((long) cursor * 100 / totalCount);
            if (percent / 10 > lastLoggedPercent / 10) {
                LOG.info(
                        "Lumina {} progress: {}/{} vectors ({}%)",
                        phase, cursor, totalCount, percent);
                lastLoggedPercent = percent;
            }

            return batchSize;
        }

        @Override
        public void close() throws IOException {
            channel.close();
            raf.close();
        }
    }

    /** Adapts a {@link PositionOutputStream} to the {@link LuminaFileOutput} JNI callback API. */
    static class OutputStreamFileOutput implements LuminaFileOutput {
        private final PositionOutputStream out;

        OutputStreamFileOutput(PositionOutputStream out) {
            this.out = out;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            out.flush();
        }

        @Override
        public long getPos() throws IOException {
            return out.getPos();
        }

        @Override
        public void close() {}
    }
}
