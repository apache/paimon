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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.RecordComparator;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestEntrySerializer;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.sort.BinaryExternalSortBuffer;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.MutableObjectIterator;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.apache.paimon.utils.ManifestReadThreadPool.sequentialBatchedExecute;

/** Spillable external sort utilities for manifest entries. */
public class ManifestEntryExternalSort {

    static Pair<List<ManifestFileMeta>, List<ManifestFileMeta>> sortAndWriteMinorEntries(
            List<ManifestFileMeta> section,
            ManifestFileSorter.ManifestSortKey sortKey,
            ExternalSortConfig config,
            ManifestFile manifestFile,
            List<ManifestFileMeta> newFilesForAbort,
            @Nullable Integer manifestReadParallelism)
            throws Exception {
        try (EntrySorter sorter = new EntrySorter(sortKey, config)) {
            Map<FileEntry.Identifier, ManifestEntry> deleteEntries = new HashMap<>();
            Function<ManifestFileMeta, List<ManifestEntry>> reader =
                    meta -> manifestFile.read(meta.fileName(), meta.fileSize());
            for (ManifestEntry entry :
                    sequentialBatchedExecute(reader, section, manifestReadParallelism)) {
                if (entry.kind() == FileKind.DELETE) {
                    deleteEntries.put(entry.identifier(), entry);
                } else {
                    sorter.write(entry);
                }
            }

            List<ManifestFileMeta> addFiles =
                    sorter.writeSurvivingAddsToManifest(manifestFile, deleteEntries);
            // Register ADD files for abort cleanup right after they are written, before the
            // DELETE files below. Otherwise, if sortAndWriteDeleteEntries throws, the already
            // written ADD manifest files would not be in newFilesForAbort and would leak as
            // orphan files on commit abort.
            newFilesForAbort.addAll(addFiles);
            List<ManifestFileMeta> deleteFiles =
                    sortAndWriteDeleteEntries(deleteEntries.values(), sortKey, manifestFile);
            newFilesForAbort.addAll(deleteFiles);
            return Pair.of(addFiles, deleteFiles);
        }
    }

    static List<ManifestFileMeta> sortAndWriteFullEntries(
            List<ManifestFileMeta> section,
            ManifestFileSorter.ManifestSortKey sortKey,
            ExternalSortConfig config,
            ManifestFile manifestFile,
            List<ManifestFileMeta> newFilesForAbort,
            Set<FileEntry.Identifier> deleteEntries,
            @Nullable Integer manifestReadParallelism)
            throws Exception {
        try (EntrySorter sorter = new EntrySorter(sortKey, config)) {
            Function<ManifestFileMeta, List<ManifestEntry>> reader =
                    meta -> {
                        List<ManifestEntry> batch = new ArrayList<>();
                        for (ManifestEntry entry :
                                manifestFile.read(
                                        meta.fileName(),
                                        meta.fileSize(),
                                        FileEntry.addFilter(),
                                        Filter.alwaysTrue())) {
                            if (!deleteEntries.contains(entry.identifier())) {
                                batch.add(entry);
                            }
                        }
                        return batch;
                    };
            for (ManifestEntry entry :
                    sequentialBatchedExecute(reader, section, manifestReadParallelism)) {
                sorter.write(entry);
            }
            List<ManifestFileMeta> files = sorter.writeToManifest(manifestFile);
            newFilesForAbort.addAll(files);
            return files;
        }
    }

    /** Config used by manifest entry external sort. */
    static class ExternalSortConfig {
        final long bufferSize;
        final int pageSize;
        final int maxNumFileHandles;
        final CompressOptions compression;
        final MemorySize maxDiskSize;
        @Nullable final IOManager ioManager;

        ExternalSortConfig(
                long bufferSize,
                int pageSize,
                int maxNumFileHandles,
                CompressOptions compression,
                MemorySize maxDiskSize,
                @Nullable IOManager ioManager) {
            this.bufferSize = bufferSize;
            this.pageSize = pageSize;
            this.maxNumFileHandles = maxNumFileHandles;
            this.compression = compression;
            this.maxDiskSize = maxDiskSize;
            this.ioManager = ioManager;
        }

        static ExternalSortConfig from(CoreOptions options, @Nullable IOManager ioManager) {
            return new ExternalSortConfig(
                    options.sortSpillBufferSize(),
                    options.pageSize(),
                    options.localSortMaxNumFileHandles(),
                    options.spillCompressOptions(),
                    options.writeBufferSpillDiskSize(),
                    ioManager);
        }
    }

    /** Spillable sorter that stores sort keys plus serialized manifest entries in BinaryRow. */
    private static class EntrySorter implements AutoCloseable {
        private final ManifestFileSorter.ManifestSortKey sortKey;
        private final ManifestEntrySerializer entrySerializer;
        private final IOManager ioManager;
        private final boolean ownedIOManager;
        private final BinaryExternalSortBuffer sortBuffer;

        private EntrySorter(ManifestFileSorter.ManifestSortKey sortKey, ExternalSortConfig config) {
            this.sortKey = sortKey;
            this.entrySerializer = new ManifestEntrySerializer();
            this.ioManager =
                    config.ioManager == null
                            ? IOManager.create(System.getProperty("java.io.tmpdir"))
                            : config.ioManager;
            this.ownedIOManager = config.ioManager == null;
            this.sortBuffer =
                    BinaryExternalSortBuffer.create(
                            ioManager,
                            sortKey.externalSortRowType(),
                            sortKey.externalSortKeyFields(),
                            config.bufferSize,
                            config.pageSize,
                            config.maxNumFileHandles,
                            config.compression,
                            config.maxDiskSize);
        }

        private void write(ManifestEntry entry) throws Exception {
            sortBuffer.write(
                    sortKey.toExternalSortRow(entry, entrySerializer.serializeToBytes(entry)));
        }

        private boolean isEmpty() {
            return sortBuffer.isEmpty();
        }

        private List<ManifestFileMeta> writeToManifest(ManifestFile manifestFile) throws Exception {
            if (isEmpty()) {
                return Collections.emptyList();
            }

            RollingFileWriter<ManifestEntry, ManifestFileMeta> writer =
                    manifestFile.createRollingWriter();
            Exception exception = null;
            try {
                MutableObjectIterator<BinaryRow> iterator = sortBuffer.sortedIterator();
                BinaryRow reuse = new BinaryRow(sortKey.externalSortRowType().getFieldCount());
                BinaryRow row;
                while ((row = iterator.next(reuse)) != null) {
                    writer.write(entrySerializer.deserializeFromBytes(sortKey.entryBytes(row)));
                }
            } catch (Exception e) {
                exception = e;
            } finally {
                if (exception != null) {
                    writer.abort();
                    throw exception;
                }
                writer.close();
            }
            return writer.result();
        }

        private List<ManifestFileMeta> writeSurvivingAddsToManifest(
                ManifestFile manifestFile, Map<FileEntry.Identifier, ManifestEntry> deleteEntries)
                throws Exception {
            if (isEmpty()) {
                return Collections.emptyList();
            }

            RollingFileWriter<ManifestEntry, ManifestFileMeta> writer =
                    manifestFile.createRollingWriter();
            Exception exception = null;
            try {
                MutableObjectIterator<BinaryRow> iterator = sortBuffer.sortedIterator();
                BinaryRow reuse = new BinaryRow(sortKey.externalSortRowType().getFieldCount());
                BinaryRow row;
                while ((row = iterator.next(reuse)) != null) {
                    ManifestEntry entry =
                            entrySerializer.deserializeFromBytes(sortKey.entryBytes(row));
                    if (deleteEntries.remove(entry.identifier()) != null) {
                        continue;
                    }
                    writer.write(entry);
                }
            } catch (Exception e) {
                exception = e;
            } finally {
                if (exception != null) {
                    writer.abort();
                    throw exception;
                }
                writer.close();
            }
            return writer.result();
        }

        @Override
        public void close() throws Exception {
            sortBuffer.clear();
            if (ownedIOManager) {
                ioManager.close();
            }
        }
    }

    private static List<ManifestFileMeta> sortAndWriteDeleteEntries(
            Collection<ManifestEntry> entries,
            ManifestFileSorter.ManifestSortKey sortKey,
            ManifestFile manifestFile)
            throws Exception {
        if (entries.isEmpty()) {
            return Collections.emptyList();
        }

        List<ManifestEntry> sorted = new ArrayList<>(entries);
        RecordComparator comparator =
                CodeGenUtils.newRecordComparator(
                        sortKey.externalSortRowType().getFieldTypes(),
                        sortKey.externalSortKeyFields());
        sorted.sort(
                (a, b) ->
                        comparator.compare(
                                sortKey.toExternalSortRow(a, new byte[0]),
                                sortKey.toExternalSortRow(b, new byte[0])));

        RollingFileWriter<ManifestEntry, ManifestFileMeta> writer =
                manifestFile.createRollingWriter();
        Exception exception = null;
        try {
            writer.write(sorted);
        } catch (Exception e) {
            exception = e;
        } finally {
            if (exception != null) {
                writer.abort();
                throw exception;
            }
            writer.close();
        }
        return writer.result();
    }
}
