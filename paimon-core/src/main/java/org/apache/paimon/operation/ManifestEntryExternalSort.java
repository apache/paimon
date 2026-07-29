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
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.RollingFileWriter;
import org.apache.paimon.manifest.BinaryManifestEntry;
import org.apache.paimon.manifest.BinaryManifestEntry.ReusableIdentifier;
import org.apache.paimon.manifest.DeletedIdentifierSet;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.sort.BinaryExternalSortBuffer;
import org.apache.paimon.utils.CloseableIterator;
import org.apache.paimon.utils.MutableObjectIterator;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
            DeletedIdentifierSet deleteEntries = new DeletedIdentifierSet();
            try {
                scanEntries(
                        section,
                        manifestFile,
                        manifestReadParallelism,
                        entry -> {
                            if (entry.isDelete()) {
                                deleteEntries.add(entry);
                            }
                            sorter.write(entry);
                        });

                return sorter.writeMinorToManifest(manifestFile, deleteEntries, newFilesForAbort);
            } finally {
                deleteEntries.release();
            }
        }
    }

    static List<ManifestFileMeta> sortAndWriteFullEntries(
            List<ManifestFileMeta> section,
            ManifestFileSorter.ManifestSortKey sortKey,
            ExternalSortConfig config,
            ManifestFile manifestFile,
            List<ManifestFileMeta> newFilesForAbort,
            DeletedIdentifierSet deleteEntries,
            @Nullable Integer manifestReadParallelism)
            throws Exception {
        try (EntrySorter sorter = new EntrySorter(sortKey, config)) {
            scanEntries(
                    section,
                    manifestFile,
                    manifestReadParallelism,
                    entry -> {
                        if (entry.isAdd()
                                && (deleteEntries.isEmpty() || !deleteEntries.contains(entry))) {
                            sorter.write(entry);
                        }
                    });
            List<ManifestFileMeta> files = sorter.writeToManifest(manifestFile);
            newFilesForAbort.addAll(files);
            return files;
        }
    }

    private static void scanEntries(
            List<ManifestFileMeta> section,
            ManifestFile manifestFile,
            @Nullable Integer manifestReadParallelism,
            BinaryEntryConsumer consumer)
            throws Exception {
        if (section.size() <= 1
                || (manifestReadParallelism != null && manifestReadParallelism <= 1)) {
            for (ManifestFileMeta meta : section) {
                try (CloseableIterator<BinaryManifestEntry> entries =
                        manifestFile.scan(
                                meta.fileName(),
                                meta.fileSize(),
                                BinaryManifestEntry.fullProjection())) {
                    while (entries.hasNext()) {
                        consumer.accept(entries.next());
                    }
                }
            }
            return;
        }

        Function<ManifestFileMeta, List<BinaryRow>> reader =
                meta -> readBinaryRows(manifestFile, meta);
        BinaryManifestEntry entry = BinaryManifestEntry.fullProjection().createEntry();
        for (BinaryRow row : sequentialBatchedExecute(reader, section, manifestReadParallelism)) {
            consumer.accept(entry.replace(row));
        }
        entry.clear();
    }

    private static List<BinaryRow> readBinaryRows(
            ManifestFile manifestFile, ManifestFileMeta meta) {
        long entryCount = meta.numAddedFiles() + meta.numDeletedFiles();
        List<BinaryRow> rows = new ArrayList<>((int) Math.min(entryCount, 1 << 20));
        InternalRowSerializer serializer =
                new InternalRowSerializer(ManifestEntry.MANIFEST_ROW_TYPE);
        try (CloseableIterator<BinaryManifestEntry> entries =
                manifestFile.scan(
                        meta.fileName(), meta.fileSize(), BinaryManifestEntry.fullProjection())) {
            while (entries.hasNext()) {
                rows.add(serializer.toBinaryRow(entries.next().fullRow()).copy());
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Failed to scan manifest file '%s'.", meta.fileName()), e);
        }
        return rows;
    }

    @FunctionalInterface
    private interface BinaryEntryConsumer {
        void accept(BinaryManifestEntry entry) throws Exception;
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

    /** Spillable sorter that stores sort keys plus complete binary manifest rows. */
    private static class EntrySorter implements AutoCloseable {
        private final ManifestFileSorter.ManifestSortKey sortKey;
        private final GenericRow externalSortRow;
        private final IOManager ioManager;
        private final boolean ownedIOManager;
        private final BinaryExternalSortBuffer sortBuffer;

        private EntrySorter(ManifestFileSorter.ManifestSortKey sortKey, ExternalSortConfig config) {
            this.sortKey = sortKey;
            this.externalSortRow = new GenericRow(sortKey.externalSortRowType().getFieldCount());
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

        private void write(BinaryManifestEntry entry) throws Exception {
            sortKey.replaceExternalSortRow(externalSortRow, entry, entry.fullRow());
            sortBuffer.write(externalSortRow);
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
                BinaryManifestEntry entry = BinaryManifestEntry.fullProjection().createEntry();
                BinaryRow row;
                while ((row = iterator.next(reuse)) != null) {
                    writer.write(entry.replace(sortKey.binaryManifestRow(row)));
                }
                entry.clear();
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

        private Pair<List<ManifestFileMeta>, List<ManifestFileMeta>> writeMinorToManifest(
                ManifestFile manifestFile,
                DeletedIdentifierSet deleteEntries,
                List<ManifestFileMeta> newFilesForAbort)
                throws Exception {
            if (isEmpty()) {
                return Pair.of(Collections.emptyList(), Collections.emptyList());
            }

            RollingFileWriter<ManifestEntry, ManifestFileMeta> addWriter =
                    manifestFile.createRollingWriter();
            RollingFileWriter<ManifestEntry, ManifestFileMeta> deleteWriter =
                    manifestFile.createRollingWriter();
            DeletedIdentifierSet matchedEntries = new DeletedIdentifierSet();
            DeletedIdentifierSet emittedDeletes = new DeletedIdentifierSet();
            ReusableIdentifier identifier = new ReusableIdentifier();
            Exception exception = null;
            try {
                MutableObjectIterator<BinaryRow> iterator = sortBuffer.sortedIterator();
                BinaryRow reuse = new BinaryRow(sortKey.externalSortRowType().getFieldCount());
                BinaryManifestEntry entry = BinaryManifestEntry.fullProjection().createEntry();
                BinaryRow row;
                while ((row = iterator.next(reuse)) != null) {
                    entry.replace(sortKey.binaryManifestRow(row));
                    identifier.replaceWithPartition(entry);
                    if (entry.isAdd()) {
                        if (deleteEntries.contains(identifier)) {
                            matchedEntries.add(identifier);
                        } else {
                            addWriter.write(entry);
                        }
                    } else if (!matchedEntries.contains(identifier)
                            && !emittedDeletes.contains(identifier)) {
                        emittedDeletes.add(identifier);
                        deleteWriter.write(entry);
                    }
                }
                entry.clear();
                addWriter.close();
                newFilesForAbort.addAll(addWriter.result());
                deleteWriter.close();
                newFilesForAbort.addAll(deleteWriter.result());
            } catch (Exception e) {
                exception = e;
            } finally {
                identifier.release();
                matchedEntries.release();
                emittedDeletes.release();
                if (exception != null) {
                    addWriter.abort();
                    deleteWriter.abort();
                    throw exception;
                }
            }
            return Pair.of(addWriter.result(), deleteWriter.result());
        }

        @Override
        public void close() throws Exception {
            sortBuffer.clear();
            if (ownedIOManager) {
                ioManager.close();
            }
        }
    }
}
