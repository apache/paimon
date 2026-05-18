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

package org.apache.paimon.elasticsearch.index;

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalVector;
import org.apache.paimon.elasticsearch.index.model.ESIndexEntryMeta;
import org.apache.paimon.elasticsearch.index.model.ESVectorIndexOptions;
import org.apache.paimon.elasticsearch.index.util.ESIndexArchiveUtils;
import org.apache.paimon.elasticsearch.index.util.Slf4jBridgeESLoggerFactory;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.globalindex.GlobalIndexSingletonWriter;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.VectorType;

import org.elasticsearch.vectorindex.api.VectorIndexBuilder;
import org.elasticsearch.vectorindex.impl.DiskBBQVectorIndexBuilder;
import org.elasticsearch.vectorindex.model.VectorIndexConfig;
import org.elasticsearch.vectorindex.model.VectorIndexMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Vector global index writer using ES DiskBBQ.
 *
 * <p>Vectors are added to a {@link DiskBBQVectorIndexBuilder} which builds an IVF index with binary
 * quantization internally. On finish, all Lucene segment files are packed into an archive, and a
 * small offset table file is written alongside it to enable selective Range Read during search.
 */
public class ESVectorGlobalIndexWriter implements GlobalIndexSingletonWriter, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(ESVectorGlobalIndexWriter.class);

    private final GlobalIndexFileWriter fileWriter;
    private final VectorIndexConfig config;
    private final int dim;
    private final VectorIndexBuilder builder;

    private Path tempIndexDir;
    private int count;
    private boolean closed;

    public ESVectorGlobalIndexWriter(
            GlobalIndexFileWriter fileWriter,
            String fieldName,
            DataType fieldType,
            ESVectorIndexOptions options)
            throws IOException {
        Slf4jBridgeESLoggerFactory.ensureInitialized();
        this.fileWriter = fileWriter;
        this.config = options.toVectorIndexConfig(fieldName);
        this.dim = options.dimension();
        this.count = 0;
        this.closed = false;

        validateFieldType(fieldType);

        // Allocate the work directory up-front and hand it to the builder so its forceMerge
        // output lands directly here. Avoids a redundant Files.copy in finish().
        this.tempIndexDir = Files.createTempDirectory("es-diskbbq-index-");
        this.builder = new DiskBBQVectorIndexBuilder(config, tempIndexDir);
    }

    private void validateFieldType(DataType dataType) {
        if (dataType instanceof VectorType) {
            DataType elementType = ((VectorType) dataType).getElementType();
            if (!(elementType instanceof FloatType)) {
                throw new IllegalArgumentException(
                        "ES vector index requires float vector, but got: " + elementType);
            }
            return;
        }
        if (dataType instanceof ArrayType) {
            DataType elementType = ((ArrayType) dataType).getElementType();
            if (!(elementType instanceof FloatType)) {
                throw new IllegalArgumentException(
                        "ES vector index requires float array, but got: " + elementType);
            }
            return;
        }
        throw new IllegalArgumentException(
                "ES vector index requires VectorType or ArrayType<FLOAT>, but got: " + dataType);
    }

    @Override
    public void write(Object fieldData) {
        if (fieldData == null) {
            throw new IllegalArgumentException("Field data must not be null");
        }

        float[] vector;
        if (fieldData instanceof float[]) {
            vector = (float[]) fieldData;
        } else if (fieldData instanceof InternalVector) {
            InternalVector iv = (InternalVector) fieldData;
            checkDimension(iv.size());
            vector = new float[dim];
            for (int i = 0; i < dim; i++) {
                vector[i] = iv.getFloat(i);
            }
        } else if (fieldData instanceof InternalArray) {
            InternalArray array = (InternalArray) fieldData;
            checkDimension(array.size());
            vector = new float[dim];
            for (int i = 0; i < dim; i++) {
                if (array.isNullAt(i)) {
                    throw new IllegalArgumentException("Vector element at index " + i + " is null");
                }
                vector[i] = array.getFloat(i);
            }
        } else {
            throw new RuntimeException(
                    "Unsupported vector type: " + fieldData.getClass().getName());
        }

        checkDimension(vector.length);
        try {
            builder.addVector(count, vector);
        } catch (IOException e) {
            throw new RuntimeException("Failed to add vector to index builder", e);
        }
        count++;
    }

    @Override
    public List<ResultEntry> finish() {
        try {
            if (count == 0) {
                return Collections.emptyList();
            }

            LOG.info(
                    "ES DiskBBQ index build started: {} vectors, dim={}, metric={}",
                    count,
                    dim,
                    config.similarity());
            long buildStart = System.currentTimeMillis();

            builder.build();
            LOG.info(
                    "ES DiskBBQ index build done in {} ms",
                    System.currentTimeMillis() - buildStart);

            // Builder was constructed with `tempIndexDir` as its workDir, so forceMerge output
            // is already in `tempIndexDir`. Skip the redundant Files.copy that the historical
            // `writeToDir(tempIndexDir)` step used to perform.
            LOG.info(
                    "ES DiskBBQ build output ready: {} files in {}",
                    tempIndexDir.toFile().listFiles().length,
                    tempIndexDir);

            VectorIndexMeta meta = new VectorIndexMeta(config, count);
            byte[] metaSerialized = meta.serialize();

            Map<String, long[]> fileOffsets = new LinkedHashMap<>();
            ResultEntry archiveEntry = packArchive(metaSerialized, fileOffsets);
            ResultEntry offsetEntry = writeOffsetTable(metaSerialized, fileOffsets);

            List<ResultEntry> entries = new ArrayList<>(2);
            entries.add(archiveEntry);
            entries.add(offsetEntry);
            return entries;
        } catch (IOException e) {
            throw new RuntimeException("Failed to write ES vector global index", e);
        } finally {
            deleteTempDir();
        }
    }

    private ResultEntry packArchive(byte[] metaSerialized, Map<String, long[]> fileOffsets)
            throws IOException {
        String fileName = fileWriter.newFileName("es-archive");
        long writeStart = System.currentTimeMillis();

        try (PositionOutputStream out = fileWriter.newOutputStream(fileName)) {
            fileOffsets.putAll(ESIndexArchiveUtils.pack(tempIndexDir, out));
        }

        LOG.info("ES DiskBBQ archive packed in {} ms", System.currentTimeMillis() - writeStart);

        ESIndexEntryMeta entryMeta =
                new ESIndexEntryMeta(ESIndexEntryMeta.FILE_ROLE_ARCHIVE, metaSerialized);
        return new ResultEntry(fileName, count, entryMeta.serialize());
    }

    private ResultEntry writeOffsetTable(byte[] metaSerialized, Map<String, long[]> fileOffsets)
            throws IOException {
        String fileName = fileWriter.newFileName("es-offsets");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeInt(1);
        dos.writeInt(fileOffsets.size());
        for (Map.Entry<String, long[]> entry : fileOffsets.entrySet()) {
            dos.writeUTF(entry.getKey());
            dos.writeLong(entry.getValue()[0]);
            dos.writeLong(entry.getValue()[1]);
        }
        dos.flush();
        byte[] tableBytes = baos.toByteArray();

        try (PositionOutputStream out = fileWriter.newOutputStream(fileName)) {
            out.write(tableBytes);
            out.flush();
        }

        LOG.info(
                "ES DiskBBQ offset table written: {} files, {} bytes",
                fileOffsets.size(),
                tableBytes.length);

        ESIndexEntryMeta entryMeta =
                new ESIndexEntryMeta(ESIndexEntryMeta.FILE_ROLE_OFFSETS, metaSerialized);
        return new ResultEntry(fileName, count, entryMeta.serialize());
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
                builder.close();
            } catch (IOException ignored) {
            }
            deleteTempDir();
        }
    }

    private void deleteTempDir() {
        if (tempIndexDir != null) {
            try {
                Files.walkFileTree(
                        tempIndexDir,
                        new SimpleFileVisitor<Path>() {
                            @Override
                            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                                    throws IOException {
                                Files.delete(file);
                                return FileVisitResult.CONTINUE;
                            }

                            @Override
                            public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                                    throws IOException {
                                Files.delete(dir);
                                return FileVisitResult.CONTINUE;
                            }
                        });
            } catch (IOException ignored) {
            }
            tempIndexDir = null;
        }
    }
}
