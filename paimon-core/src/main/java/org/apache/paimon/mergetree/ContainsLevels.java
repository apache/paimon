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
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.reader.RecordReader;
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

/** Provide contains key. */
public class ContainsLevels implements Levels.DropFileCallback, Closeable {

    private static final byte[] EMPTY_VALUE = new byte[0];

    private final Levels levels;
    private final Comparator<InternalRow> keyComparator;
    private final RowCompactedSerializer keySerializer;
    private final IOFunction<DataFileMeta, RecordReader<KeyValue>> fileReaderFactory;
    private final Supplier<File> localFileFactory;
    private final LookupStoreFactory lookupStoreFactory;
    private final Cache<String, ContainsFile> containsFiles;
    private final Function<Long, BloomFilter.Builder> bfGenerator;

    public ContainsLevels(
            Levels levels,
            Comparator<InternalRow> keyComparator,
            RowType keyType,
            IOFunction<DataFileMeta, RecordReader<KeyValue>> fileReaderFactory,
            Supplier<File> localFileFactory,
            LookupStoreFactory lookupStoreFactory,
            Duration fileRetention,
            MemorySize maxDiskSize,
            Function<Long, BloomFilter.Builder> bfGenerator) {
        this.levels = levels;
        this.keyComparator = keyComparator;
        this.keySerializer = new RowCompactedSerializer(keyType);
        this.fileReaderFactory = fileReaderFactory;
        this.localFileFactory = localFileFactory;
        this.lookupStoreFactory = lookupStoreFactory;
        this.containsFiles =
                Caffeine.newBuilder()
                        .expireAfterAccess(fileRetention)
                        .maximumWeight(maxDiskSize.getKibiBytes())
                        .weigher(this::fileWeigh)
                        .removalListener(this::removalCallback)
                        .executor(MoreExecutors.directExecutor())
                        .build();
        levels.addDropFileCallback(this);
        this.bfGenerator = bfGenerator;
    }

    @VisibleForTesting
    Cache<String, ContainsFile> containsFiles() {
        return containsFiles;
    }

    @Override
    public void notifyDropFile(String file) {
        containsFiles.invalidate(file);
    }

    public boolean contains(InternalRow key, int startLevel) throws IOException {
        Boolean result =
                LookupUtils.lookup(levels, key, startLevel, this::contains, this::containsLevel0);
        return result != null && result;
    }

    @Nullable
    private Boolean containsLevel0(InternalRow key, TreeSet<DataFileMeta> level0)
            throws IOException {
        return LookupUtils.lookupLevel0(keyComparator, key, level0, this::contains);
    }

    @Nullable
    private Boolean contains(InternalRow key, SortedRun level) throws IOException {
        return LookupUtils.lookup(keyComparator, key, level, this::contains);
    }

    @Nullable
    private Boolean contains(InternalRow key, DataFileMeta file) throws IOException {
        ContainsFile containsFile = containsFiles.getIfPresent(file.fileName());
        while (containsFile == null || containsFile.isClosed) {
            containsFile = createContainsFile(file);
            containsFiles.put(file.fileName(), containsFile);
        }
        if (containsFile.get(keySerializer.serializeToBytes(key)) != null) {
            return true;
        }
        return null;
    }

    private int fileWeigh(String file, ContainsFile containsFile) {
        return fileKibiBytes(containsFile.localFile);
    }

    private void removalCallback(String key, ContainsFile file, RemovalCause cause) {
        if (file != null) {
            try {
                file.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private ContainsFile createContainsFile(DataFileMeta file) throws IOException {
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
                    kvWriter.put(keyBytes, EMPTY_VALUE);
                }
                batch.releaseBatch();
            }
        } catch (IOException e) {
            FileIOUtils.deleteFileOrDirectory(localFile);
            throw e;
        } finally {
            context = kvWriter.close();
        }

        return new ContainsFile(localFile, lookupStoreFactory.createReader(localFile, context));
    }

    @Override
    public void close() throws IOException {
        containsFiles.invalidateAll();
    }

    private static class ContainsFile implements Closeable {

        private final File localFile;
        private final LookupStoreReader reader;

        private boolean isClosed = false;

        public ContainsFile(File localFile, LookupStoreReader reader) {
            this.localFile = localFile;
            this.reader = reader;
        }

        @Nullable
        public byte[] get(byte[] key) throws IOException {
            checkArgument(!isClosed);
            return reader.lookup(key);
        }

        @Override
        public void close() throws IOException {
            reader.close();
            isClosed = true;
            FileIOUtils.deleteFileOrDirectory(localFile);
        }
    }
}
