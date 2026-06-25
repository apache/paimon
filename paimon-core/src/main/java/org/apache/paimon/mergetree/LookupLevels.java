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
import org.apache.paimon.lookup.LookupStoreWriter;
import org.apache.paimon.mergetree.lookup.LookupSerializerFactory;
import org.apache.paimon.mergetree.lookup.PersistProcessor;
import org.apache.paimon.mergetree.lookup.RemoteFileDownloader;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BloomFilter;
import org.apache.paimon.utils.FileIOUtils;
import org.apache.paimon.utils.IOFunction;
import org.apache.paimon.utils.Pair;

import org.apache.paimon.shade.caffeine2.com.github.benmanes.caffeine.cache.Cache;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/** Provide lookup by key. */
public class LookupLevels<T> implements Levels.DropFileCallback, Closeable {

    public static final String REMOTE_LOOKUP_FILE_SUFFIX = ".lookup";
    private static final int LOOKUP_FILE_LOCK_STRIPES = 1024;

    private final Function<Long, RowType> schemaFunction;
    private final long currentSchemaId;
    private final Levels levels;
    private final Comparator<InternalRow> keyComparator;
    private final RowCompactedSerializer keySerializer;
    private final PersistProcessor.Factory<T> processorFactory;
    private final LookupSerializerFactory serializerFactory;
    private final IOFunction<DataFileMeta, RecordReader<KeyValue>> fileReaderFactory;
    private final Function<String, File> localFileFactory;
    private final LookupStoreFactory lookupStoreFactory;
    private final Function<Long, BloomFilter.Builder> bfGenerator;
    private final Cache<String, LookupFile> lookupFileCache;
    private final Set<String> ownCachedFiles;
    private final Object[] lookupFileLocks;
    private final Map<Pair<Long, String>, PersistProcessor<T>> schemaIdAndSerVersionToProcessors;

    @Nullable private RemoteFileDownloader remoteFileDownloader;

    public LookupLevels(
            Function<Long, RowType> schemaFunction,
            long currentSchemaId,
            Levels levels,
            Comparator<InternalRow> keyComparator,
            RowType keyType,
            PersistProcessor.Factory<T> processorFactory,
            LookupSerializerFactory serializerFactory,
            IOFunction<DataFileMeta, RecordReader<KeyValue>> fileReaderFactory,
            Function<String, File> localFileFactory,
            LookupStoreFactory lookupStoreFactory,
            Function<Long, BloomFilter.Builder> bfGenerator,
            Cache<String, LookupFile> lookupFileCache) {
        this.schemaFunction = schemaFunction;
        this.currentSchemaId = currentSchemaId;
        this.levels = levels;
        this.keyComparator = keyComparator;
        this.keySerializer = new RowCompactedSerializer(keyType);
        this.processorFactory = processorFactory;
        this.serializerFactory = serializerFactory;
        this.fileReaderFactory = fileReaderFactory;
        this.localFileFactory = localFileFactory;
        this.lookupStoreFactory = lookupStoreFactory;
        this.bfGenerator = bfGenerator;
        this.lookupFileCache = lookupFileCache;
        this.ownCachedFiles = ConcurrentHashMap.newKeySet();
        this.lookupFileLocks = new Object[LOOKUP_FILE_LOCK_STRIPES];
        for (int i = 0; i < lookupFileLocks.length; i++) {
            lookupFileLocks[i] = new Object();
        }
        this.schemaIdAndSerVersionToProcessors = new ConcurrentHashMap<>();
        levels.addDropFileCallback(this);
    }

    public void setRemoteFileDownloader(@Nullable RemoteFileDownloader remoteFileDownloader) {
        this.remoteFileDownloader = remoteFileDownloader;
    }

    public Levels getLevels() {
        return levels;
    }

    @VisibleForTesting
    Cache<String, LookupFile> lookupFiles() {
        return lookupFileCache;
    }

    @VisibleForTesting
    Set<String> cachedFiles() {
        return ownCachedFiles;
    }

    @Override
    public void notifyDropFile(String file) {
        lookupFileCache.invalidate(file);
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
        byte[] keyBytes = serializeKey(key);
        LookupResult lookupResult = lookupFile(file, keyBytes);
        byte[] valueBytes = lookupResult.valueBytes;
        if (valueBytes == null) {
            return null;
        }

        return readFromDisk(
                getOrCreateProcessor(lookupResult.schemaId, lookupResult.serVersion),
                key,
                lookupResult.level,
                valueBytes,
                file.fileName());
    }

    private LookupResult lookupFile(DataFileMeta file, byte[] keyBytes) throws IOException {
        String fileName = file.fileName();
        LookupFile lookupFile = lookupFileCache.getIfPresent(fileName);
        LookupResult lookupResult = lookupCachedFile(fileName, lookupFile, keyBytes);
        if (lookupResult != null) {
            return lookupResult;
        }

        Object lock = lookupFileLock(fileName);
        synchronized (lock) {
            lookupFile = lookupFileCache.getIfPresent(fileName);
            lookupResult = lookupCachedFile(fileName, lookupFile, keyBytes);
            if (lookupResult != null) {
                return lookupResult;
            }

            lookupFile = createLookupFile(file);

            try {
                return LookupResult.of(lookupFile, lookupFile.get(keyBytes));
            } finally {
                addLocalFile(file, lookupFile);
            }
        }
    }

    private Object lookupFileLock(String fileName) {
        return lookupFileLocks[Math.floorMod(fileName.hashCode(), lookupFileLocks.length)];
    }

    @Nullable
    private LookupResult lookupCachedFile(
            String fileName, @Nullable LookupFile lookupFile, byte[] keyBytes) throws IOException {
        if (lookupFile == null) {
            return null;
        }

        byte[] valueBytes = lookupFile.get(keyBytes);
        if (lookupFile.isClosed()) {
            lookupFileCache.asMap().remove(fileName, lookupFile);
            return null;
        }
        return LookupResult.of(lookupFile, valueBytes);
    }

    private static class LookupResult {

        private final int level;
        private final long schemaId;
        private final String serVersion;
        private final byte[] valueBytes;

        private LookupResult(int level, long schemaId, String serVersion, byte[] valueBytes) {
            this.level = level;
            this.schemaId = schemaId;
            this.serVersion = serVersion;
            this.valueBytes = valueBytes;
        }

        private static LookupResult of(LookupFile lookupFile, byte[] valueBytes) {
            return new LookupResult(
                    lookupFile.level(), lookupFile.schemaId(), lookupFile.serVersion(), valueBytes);
        }
    }

    private PersistProcessor<T> getOrCreateProcessor(long schemaId, String serVersion) {
        return schemaIdAndSerVersionToProcessors.computeIfAbsent(
                Pair.of(schemaId, serVersion),
                id -> {
                    RowType fileSchema =
                            schemaId == currentSchemaId ? null : schemaFunction.apply(schemaId);
                    return processorFactory.create(serVersion, serializerFactory, fileSchema);
                });
    }

    private T readFromDisk(
            PersistProcessor<T> processor,
            InternalRow key,
            int level,
            byte[] valueBytes,
            String fileName) {
        synchronized (processor) {
            return processor.readFromDisk(key, level, valueBytes, fileName);
        }
    }

    private byte[] persistToDisk(PersistProcessor<T> processor, KeyValue kv) {
        synchronized (processor) {
            return processor.persistToDisk(kv);
        }
    }

    private byte[] persistToDisk(PersistProcessor<T> processor, KeyValue kv, long rowPosition) {
        synchronized (processor) {
            return processor.persistToDisk(kv, rowPosition);
        }
    }

    private byte[] serializeKey(InternalRow key) {
        synchronized (keySerializer) {
            return keySerializer.serializeToBytes(key);
        }
    }

    public LookupFile createLookupFile(DataFileMeta file) throws IOException {
        File localFile = localFileFactory.apply(file.fileName());
        if (!localFile.createNewFile()) {
            throw new IOException("Can not create new file: " + localFile);
        }

        try {
            long schemaId = this.currentSchemaId;
            String fileSerVersion = serializerFactory.version();
            Optional<String> downloadSerVersion = tryToDownloadRemoteSst(file, localFile);
            if (downloadSerVersion.isPresent()) {
                // use schema id from remote file
                schemaId = file.schemaId();
                fileSerVersion = downloadSerVersion.get();
            } else {
                createSstFileFromDataFile(file, localFile);
            }

            LookupFile lookupFile =
                    new LookupFile(
                            localFile,
                            file.level(),
                            schemaId,
                            fileSerVersion,
                            lookupStoreFactory.createReader(localFile),
                            () -> ownCachedFiles.remove(file.fileName()));
            ownCachedFiles.add(file.fileName());
            return lookupFile;
        } catch (Throwable t) {
            try {
                FileIOUtils.deleteFileOrDirectory(localFile);
            } catch (Throwable deleteException) {
                t.addSuppressed(deleteException);
            }
            if (t instanceof IOException) {
                throw (IOException) t;
            }
            if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            }
            if (t instanceof Error) {
                throw (Error) t;
            }
            throw new IOException(t);
        }
    }

    private Optional<String> tryToDownloadRemoteSst(DataFileMeta file, File localFile) {
        if (remoteFileDownloader == null) {
            return Optional.empty();
        }
        Optional<RemoteSstFile> remoteSstFile = remoteSst(file);
        if (!remoteSstFile.isPresent()) {
            return Optional.empty();
        }

        RemoteSstFile remoteSst = remoteSstFile.get();

        // validate schema matched, no exception here
        try {
            getOrCreateProcessor(file.schemaId(), remoteSst.serVersion);
        } catch (UnsupportedOperationException e) {
            return Optional.empty();
        }
        boolean success =
                remoteFileDownloader.tryToDownload(file, remoteSst.sstFileName, localFile);
        if (!success) {
            return Optional.empty();
        }

        return Optional.of(remoteSst.serVersion);
    }

    public void addLocalFile(DataFileMeta file, LookupFile lookupFile) {
        lookupFileCache.put(file.fileName(), lookupFile);
    }

    private void createSstFileFromDataFile(DataFileMeta file, File localFile) throws IOException {
        try (LookupStoreWriter kvWriter =
                        lookupStoreFactory.createWriter(
                                localFile, bfGenerator.apply(file.rowCount()));
                RecordReader<KeyValue> reader = fileReaderFactory.apply(file)) {
            PersistProcessor<T> processor =
                    getOrCreateProcessor(currentSchemaId, serializerFactory.version());
            KeyValue kv;
            if (processor.withPosition()) {
                FileRecordIterator<KeyValue> batch;
                while ((batch = (FileRecordIterator<KeyValue>) reader.readBatch()) != null) {
                    while ((kv = batch.next()) != null) {
                        byte[] keyBytes = serializeKey(kv.key());
                        byte[] valueBytes = persistToDisk(processor, kv, batch.returnedPosition());
                        kvWriter.put(keyBytes, valueBytes);
                    }
                    batch.releaseBatch();
                }
            } else {
                RecordReader.RecordIterator<KeyValue> batch;
                while ((batch = reader.readBatch()) != null) {
                    while ((kv = batch.next()) != null) {
                        byte[] keyBytes = serializeKey(kv.key());
                        byte[] valueBytes = persistToDisk(processor, kv);
                        kvWriter.put(keyBytes, valueBytes);
                    }
                    batch.releaseBatch();
                }
            }
        } catch (IOException e) {
            FileIOUtils.deleteFileOrDirectory(localFile);
            throw e;
        }
    }

    public Optional<RemoteSstFile> remoteSst(DataFileMeta file) {
        Optional<String> sstFile =
                file.extraFiles().stream()
                        .filter(f -> f.endsWith(REMOTE_LOOKUP_FILE_SUFFIX))
                        .findFirst();
        if (!sstFile.isPresent()) {
            return Optional.empty();
        }

        String sstFileName = sstFile.get();
        String[] split = sstFileName.split("\\.");
        if (split.length < 3) {
            return Optional.empty();
        }

        String processorId = split[split.length - 3];
        if (!processorFactory.identifier().equals(processorId)) {
            return Optional.empty();
        }

        String serVersion = split[split.length - 2];
        return Optional.of(new RemoteSstFile(sstFileName, serVersion));
    }

    public String newRemoteSst(DataFileMeta file, long length) {
        return file.fileName()
                + "."
                + length
                + "."
                + processorFactory.identifier()
                + "."
                + serializerFactory.version()
                + REMOTE_LOOKUP_FILE_SUFFIX;
    }

    @Override
    public void close() throws IOException {
        Set<String> toClean = new HashSet<>(ownCachedFiles);
        for (String cachedFile : toClean) {
            lookupFileCache.invalidate(cachedFile);
        }
    }

    /** Remote sst file with serVersion. */
    public static class RemoteSstFile {

        private final String sstFileName;
        private final String serVersion;

        private RemoteSstFile(String sstFileName, String serVersion) {
            this.sstFileName = sstFileName;
            this.serVersion = serVersion;
        }
    }
}
