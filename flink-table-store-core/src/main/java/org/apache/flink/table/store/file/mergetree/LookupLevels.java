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

package org.apache.flink.table.store.file.mergetree;

import org.apache.flink.table.store.annotation.VisibleForTesting;
import org.apache.flink.table.store.data.BinaryRow;
import org.apache.flink.table.store.data.InternalRow;
import org.apache.flink.table.store.data.serializer.InternalRowSerializer;
import org.apache.flink.table.store.file.KeyValue;
import org.apache.flink.table.store.file.io.DataFileMeta;
import org.apache.flink.table.store.io.DataOutputSerializer;
import org.apache.flink.table.store.lookup.LookupStoreFactory;
import org.apache.flink.table.store.lookup.LookupStoreReader;
import org.apache.flink.table.store.lookup.LookupStoreWriter;
import org.apache.flink.table.store.memory.MemorySegment;
import org.apache.flink.table.store.options.MemorySize;
import org.apache.flink.table.store.reader.RecordReader;
import org.apache.flink.table.store.types.RowKind;
import org.apache.flink.table.store.utils.FileIOUtils;
import org.apache.flink.table.store.utils.IOFunction;

import org.apache.flink.shaded.guava30.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava30.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava30.com.google.common.cache.RemovalNotification;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import static org.apache.flink.table.store.options.ConfigOptions.key;

/** Provide lookup by key. */
public class LookupLevels implements Levels.DropFileCallback, Closeable {

    private final Levels levels;
    private final Comparator<InternalRow> keyComparator;
    private final InternalRowSerializer keySerializer;
    private final InternalRowSerializer valueSerializer;
    private final IOFunction<DataFileMeta, RecordReader<KeyValue>> fileReaderFactory;
    private final Supplier<File> localFileFactory;
    private final LookupStoreFactory lookupStoreFactory;

    private final Cache<String, LookupFile> lookupFiles;

    public LookupLevels(
            Levels levels,
            Comparator<InternalRow> keyComparator,
            InternalRowSerializer keySerializer,
            InternalRowSerializer valueSerializer,
            IOFunction<DataFileMeta, RecordReader<KeyValue>> fileReaderFactory,
            Supplier<File> localFileFactory,
            LookupStoreFactory lookupStoreFactory,
            Duration fileRetention,
            MemorySize maxDiskSize) {
        this.levels = levels;
        this.keyComparator = keyComparator;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.fileReaderFactory = fileReaderFactory;
        this.localFileFactory = localFileFactory;
        this.lookupStoreFactory = lookupStoreFactory;
        this.lookupFiles =
                CacheBuilder.newBuilder()
                        .expireAfterAccess(fileRetention)
                        .maximumWeight(maxDiskSize.getKibiBytes())
                        .weigher(this::fileWeigh)
                        .removalListener(this::removalCallback)
                        .build();
        levels.addDropFileCallback(this);
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
    public KeyValue lookup(InternalRow key, int startLevel) throws IOException {
        if (startLevel == 0) {
            throw new IllegalArgumentException("Start level can not be zero.");
        }

        KeyValue kv = null;
        for (int i = startLevel; i < levels.numberOfLevels(); i++) {
            SortedRun level = levels.runOfLevel(i);
            kv = lookup(key, level);
            if (kv != null) {
                break;
            }
        }

        return kv;
    }

    @Nullable
    private KeyValue lookup(InternalRow target, SortedRun level) throws IOException {
        List<DataFileMeta> files = level.files();
        int left = 0;
        int right = files.size() - 1;

        // binary search restart positions to find the restart position immediately before the
        // targetKey
        while (left < right) {
            int mid = (left + right) / 2;

            if (keyComparator.compare(files.get(mid).maxKey(), target) < 0) {
                // Key at "mid.max" is < "target".  Therefore all
                // files at or before "mid" are uninteresting.
                left = mid + 1;
            } else {
                // Key at "mid.max" is >= "target".  Therefore all files
                // after "mid" are uninteresting.
                right = mid;
            }
        }

        int index = right;

        // if the index is now pointing to the last file, check if the largest key in the block is
        // than the target key.  If so, we need to seek beyond the end of this file
        if (index == files.size() - 1
                && keyComparator.compare(files.get(index).maxKey(), target) < 0) {
            index++;
        }

        // if files does not have a next, it means the key does not exist in this level
        return index < files.size() ? lookup(target, files.get(index)) : null;
    }

    @Nullable
    private KeyValue lookup(InternalRow key, DataFileMeta file) throws IOException {
        LookupFile lookupFile;
        try {
            lookupFile = lookupFiles.get(file.fileName(), () -> createLookupFile(file));
        } catch (ExecutionException e) {
            throw new IOException(e);
        }
        byte[] keyBytes = keySerializer.toBinaryRow(key).toBytes();
        byte[] valueBytes = lookupFile.get(keyBytes);
        if (valueBytes == null) {
            return null;
        }
        MemorySegment memorySegment = MemorySegment.wrap(valueBytes);
        long sequenceNumber = memorySegment.getLong(0);
        RowKind rowKind = RowKind.fromByteValue(valueBytes[8]);
        BinaryRow value = new BinaryRow(valueSerializer.getArity());
        value.pointTo(memorySegment, 9, valueBytes.length - 9);
        return new KeyValue()
                .replace(key, sequenceNumber, rowKind, value)
                .setLevel(lookupFile.remoteFile().level());
    }

    private int fileWeigh(String file, LookupFile lookupFile) {
        return lookupFile.fileKibiBytes();
    }

    private void removalCallback(RemovalNotification<String, LookupFile> notification) {
        LookupFile reader = notification.getValue();
        if (reader != null) {
            try {
                reader.close();
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
        try (LookupStoreWriter kvWriter = lookupStoreFactory.createWriter(localFile);
                RecordReader<KeyValue> reader = fileReaderFactory.apply(file)) {
            DataOutputSerializer valueOut = new DataOutputSerializer(32);
            RecordReader.RecordIterator<KeyValue> batch;
            KeyValue kv;
            while ((batch = reader.readBatch()) != null) {
                while ((kv = batch.next()) != null) {
                    byte[] keyBytes = keySerializer.toBinaryRow(kv.key()).toBytes();
                    valueOut.clear();
                    valueOut.writeLong(kv.sequenceNumber());
                    valueOut.writeByte(kv.valueKind().toByteValue());
                    valueOut.write(valueSerializer.toBinaryRow(kv.value()).toBytes());
                    byte[] valueBytes = valueOut.getCopyOfBuffer();
                    kvWriter.put(keyBytes, valueBytes);
                }
                batch.releaseBatch();
            }
        } catch (IOException e) {
            FileIOUtils.deleteFileOrDirectory(localFile);
            throw e;
        }

        return new LookupFile(localFile, file, lookupStoreFactory.createReader(localFile));
    }

    @Override
    public void close() throws IOException {
        lookupFiles.invalidateAll();
    }

    private static class LookupFile implements Closeable {

        private final File localFile;
        private final DataFileMeta remoteFile;
        private final LookupStoreReader reader;

        public LookupFile(File localFile, DataFileMeta remoteFile, LookupStoreReader reader) {
            this.localFile = localFile;
            this.remoteFile = remoteFile;
            this.reader = reader;
        }

        @Nullable
        public byte[] get(byte[] key) throws IOException {
            return reader.lookup(key);
        }

        public int fileKibiBytes() {
            long kibiBytes = localFile.length() >> 10;
            if (kibiBytes > Integer.MAX_VALUE) {
                throw new RuntimeException(
                        "Lookup file is too big: " + MemorySize.ofKibiBytes(kibiBytes));
            }
            return (int) kibiBytes;
        }

        public DataFileMeta remoteFile() {
            return remoteFile;
        }

        @Override
        public void close() throws IOException {
            reader.close();
            FileIOUtils.deleteFileOrDirectory(localFile);
        }
    }
}
