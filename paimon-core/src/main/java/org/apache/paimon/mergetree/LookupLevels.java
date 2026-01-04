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
        this.ownCachedFiles = new HashSet<>();
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
        LookupFile lookupFile = lookupFileCache.getIfPresent(file.fileName());

        boolean newCreatedLookupFile = false;
        if (lookupFile == null) {
            lookupFile = createLookupFile(file);
            newCreatedLookupFile = true;
        }

        byte[] valueBytes;
        try {
            byte[] keyBytes = keySerializer.serializeToBytes(key);
            valueBytes = lookupFile.get(keyBytes);
        } finally {
            if (newCreatedLookupFile) {
                addLocalFile(file, lookupFile);
            }
        }
        if (valueBytes == null) {
            return null;
        }

        return getOrCreateProcessor(lookupFile.schemaId(), lookupFile.serVersion())
                .readFromDisk(key, lookupFile.level(), valueBytes, file.fileName());
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

    public LookupFile createLookupFile(DataFileMeta file) throws IOException {
        File localFile = localFileFactory.apply(file.fileName());
        if (!localFile.createNewFile()) {
            throw new IOException("Can not create new file: " + localFile);
        }

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

        ownCachedFiles.add(file.fileName());
        return new LookupFile(
                localFile,
                file.level(),
                schemaId,
                fileSerVersion,
                lookupStoreFactory.createReader(localFile),
                () -> ownCachedFiles.remove(file.fileName()));
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
                        byte[] keyBytes = keySerializer.serializeToBytes(kv.key());
                        byte[] valueBytes = processor.persistToDisk(kv, batch.returnedPosition());
                        kvWriter.put(keyBytes, valueBytes);
                    }
                    batch.releaseBatch();
                }
            } else {
                RecordReader.RecordIterator<KeyValue> batch;
                while ((batch = reader.readBatch()) != null) {
                    while ((kv = batch.next()) != null) {
                        byte[] keyBytes = keySerializer.serializeToBytes(kv.key());
                        byte[] valueBytes = processor.persistToDisk(kv);
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
