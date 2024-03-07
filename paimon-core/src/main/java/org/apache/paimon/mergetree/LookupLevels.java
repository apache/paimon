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

package org.apache.paimon.mergetree;

import org.apache.paimon.KeyValue;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.RowCompactedSerializer;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.lookup.LookupStoreFactory;
import org.apache.paimon.lookup.LookupStoreReader;
import org.apache.paimon.lookup.LookupStoreWriter;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BloomFilter;
import org.apache.paimon.utils.FileIOUtils;
import org.apache.paimon.utils.IOFunction;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.RemovalCause;
import org.apache.paimon.shade.guava30.com.google.common.util.concurrent.MoreExecutors;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Comparator;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.paimon.mergetree.LookupUtils.fileKibiBytes;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Provide lookup by key. */
public class LookupLevels<T> implements Levels.DropFileCallback, Closeable {

    private final Levels levels;
    private final Comparator<InternalRow> keyComparator;
    private final RowCompactedSerializer keySerializer;
    private final ValueProcessor<T> valueProcessor;
    private final IOFunction<DataFileMeta, RecordReader<KeyValue>> fileReaderFactory;
    private final Supplier<File> localFileFactory;
    private final LookupStoreFactory lookupStoreFactory;
    private final Cache<String, LookupFile> lookupFiles;
    private final Function<Long, BloomFilter.Builder> bfGenerator;

    public LookupLevels(
            Levels levels,
            Comparator<InternalRow> keyComparator,
            RowType keyType,
            ValueProcessor<T> valueProcessor,
            IOFunction<DataFileMeta, RecordReader<KeyValue>> fileReaderFactory,
            Supplier<File> localFileFactory,
            LookupStoreFactory lookupStoreFactory,
            Duration fileRetention,
            MemorySize maxDiskSize,
            Function<Long, BloomFilter.Builder> bfGenerator) {
        this.levels = levels;
        this.keyComparator = keyComparator;
        this.keySerializer = new RowCompactedSerializer(keyType);
        this.valueProcessor = valueProcessor;
        this.fileReaderFactory = fileReaderFactory;
        this.localFileFactory = localFileFactory;
        this.lookupStoreFactory = lookupStoreFactory;
        this.lookupFiles =
                Caffeine.newBuilder()
                        .expireAfterAccess(fileRetention)
                        .maximumWeight(maxDiskSize.getKibiBytes())
                        .weigher(this::fileWeigh)
                        .removalListener(this::removalCallback)
                        .executor(MoreExecutors.directExecutor())
                        .build();
        this.bfGenerator = bfGenerator;
        levels.addDropFileCallback(this);
    }

    public Levels getLevels() {
        return levels;
    }

    @VisibleForTesting
    Cache<String, LookupFile> lookupFiles() {
        return lookupFiles;
    }

    @Override
    public void notifyDropFile(String file) {
        lookupFiles.invalidate(file);
    }

    @Nullable
    public T lookup(InternalRow key, int startLevel) throws IOException {
        return LookupUtils.lookup(levels, key, startLevel, this::lookup, this::lookupLevel0);
    }

    @Nullable
    private T lookupLevel0(InternalRow key, TreeSet<DataFileMeta> level0) throws IOException {
        return LookupUtils.lookupLevel0(keyComparator, key, level0, this::lookup);
    }

    @Nullable
    private T lookup(InternalRow key, SortedRun level) throws IOException {
        return LookupUtils.lookup(keyComparator, key, level, this::lookup);
    }

    @Nullable
    private T lookup(InternalRow key, DataFileMeta file) throws IOException {
        LookupFile lookupFile = lookupFiles.getIfPresent(file.fileName());

        while (lookupFile == null || lookupFile.isClosed) {
            lookupFile = createLookupFile(file);
            lookupFiles.put(file.fileName(), lookupFile);
        }

        byte[] keyBytes = keySerializer.serializeToBytes(key);
        byte[] valueBytes = lookupFile.get(keyBytes);
        if (valueBytes == null) {
            return null;
        }

        return valueProcessor.readFromDisk(key, lookupFile.remoteFile().level(), valueBytes);
    }

    private int fileWeigh(String file, LookupFile lookupFile) {
        return fileKibiBytes(lookupFile.localFile);
    }

    private void removalCallback(String key, LookupFile file, RemovalCause cause) {
        if (file != null) {
            try {
                file.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private LookupFile createLookupFile(DataFileMeta file) throws IOException {
        File localFile = localFileFactory.get();
        if (!localFile.createNewFile()) {
            throw new IOException("Can not create new file: " + localFile);
        }
        LookupStoreWriter kvWriter =
                lookupStoreFactory.createWriter(localFile, bfGenerator.apply(file.rowCount()));
        LookupStoreFactory.Context context;
        try (RecordReader<KeyValue> reader = fileReaderFactory.apply(file)) {
            RecordReader.RecordIterator<KeyValue> batch;
            KeyValue kv;
            while ((batch = reader.readBatch()) != null) {
                while ((kv = batch.next()) != null) {
                    byte[] keyBytes = keySerializer.serializeToBytes(kv.key());
                    byte[] valueBytes = valueProcessor.persistToDisk(kv);
                    kvWriter.put(keyBytes, valueBytes);
                }
                batch.releaseBatch();
            }
        } catch (IOException e) {
            FileIOUtils.deleteFileOrDirectory(localFile);
            throw e;
        } finally {
            context = kvWriter.close();
        }

        return new LookupFile(localFile, file, lookupStoreFactory.createReader(localFile, context));
    }

    @Override
    public void close() throws IOException {
        lookupFiles.invalidateAll();
    }

    private static class LookupFile implements Closeable {

        private final File localFile;
        private final DataFileMeta remoteFile;
        private final LookupStoreReader reader;

        private boolean isClosed = false;

        public LookupFile(File localFile, DataFileMeta remoteFile, LookupStoreReader reader) {
            this.localFile = localFile;
            this.remoteFile = remoteFile;
            this.reader = reader;
        }

        @Nullable
        public byte[] get(byte[] key) throws IOException {
            checkArgument(!isClosed);
            return reader.lookup(key);
        }

        public DataFileMeta remoteFile() {
            return remoteFile;
        }

        @Override
        public void close() throws IOException {
            reader.close();
            isClosed = true;
            FileIOUtils.deleteFileOrDirectory(localFile);
        }
    }

    /** Processor to process value. */
    public interface ValueProcessor<T> {

        byte[] persistToDisk(KeyValue kv);

        T readFromDisk(InternalRow key, int level, byte[] valueBytes);
    }

    /** A {@link ValueProcessor} to return {@link KeyValue}. */
    public static class KeyValueProcessor implements ValueProcessor<KeyValue> {

        private final RowCompactedSerializer valueSerializer;

        public KeyValueProcessor(RowType valueType) {
            this.valueSerializer = new RowCompactedSerializer(valueType);
        }

        @Override
        public byte[] persistToDisk(KeyValue kv) {
            byte[] vBytes = valueSerializer.serializeToBytes(kv.value());
            byte[] bytes = new byte[vBytes.length + 8 + 1];
            MemorySegment segment = MemorySegment.wrap(bytes);
            segment.put(0, vBytes);
            segment.putLong(bytes.length - 9, kv.sequenceNumber());
            segment.put(bytes.length - 1, kv.valueKind().toByteValue());
            return bytes;
        }

        @Override
        public KeyValue readFromDisk(InternalRow key, int level, byte[] bytes) {
            InternalRow value = valueSerializer.deserialize(bytes);
            long sequenceNumber = MemorySegment.wrap(bytes).getLong(bytes.length - 9);
            RowKind rowKind = RowKind.fromByteValue(bytes[bytes.length - 1]);
            return new KeyValue().replace(key, sequenceNumber, rowKind, value).setLevel(level);
        }
    }

    /** A {@link ValueProcessor} to return {@link Boolean} only. */
    public static class ContainsValueProcessor implements ValueProcessor<Boolean> {

        private static final byte[] EMPTY_BYTES = new byte[0];

        @Override
        public byte[] persistToDisk(KeyValue kv) {
            return EMPTY_BYTES;
        }

        @Override
        public Boolean readFromDisk(InternalRow key, int level, byte[] bytes) {
            return Boolean.TRUE;
        }
    }
}
