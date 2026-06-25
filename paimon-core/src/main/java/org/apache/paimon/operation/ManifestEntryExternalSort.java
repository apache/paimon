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
import org.apache.paimon.utils.MutableObjectIterator;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.paimon.utils.ManifestReadThreadPool.sequentialBatchedExecute;

/** Spillable external sort utilities for manifest entries. */
public class ManifestEntryExternalSort {

    static Pair<List<ManifestFileMeta>, List<ManifestFileMeta>> sortAndWriteMinorEntries(
            List<ManifestFileMeta> section,
            ManifestFileSorter.ManifestSortKey sortKey,
            Config config,
            ManifestFile manifestFile,
            @Nullable Integer manifestReadParallelism)
            throws Exception {
        try (EntrySorter sorter = new EntrySorter(sortKey, config)) {
            Map<FileEntry.Identifier, ManifestEntry> deleteEntries = new HashMap<>();
            Function<ManifestFileMeta, List<ManifestEntry>> reader =
                    meta -> manifestFile.read(meta.fileName(), meta.fileSize());
            for (ManifestEntry entry :
                    sequentialBatchedExecute(reader, section, manifestReadParallelism)) {
                sorter.write(entry);
                if (entry.kind() == FileKind.DELETE) {
                    deleteEntries.put(entry.identifier(), entry);
                }
            }

            List<ManifestFileMeta> addFiles =
                    sorter.writeSurvivingAddsToManifest(manifestFile, deleteEntries);
            List<ManifestFileMeta> deleteFiles =
                    sortAndWriteDeleteEntries(deleteEntries.values(), sortKey, manifestFile);
            return Pair.of(addFiles, deleteFiles);
        }
    }

    static List<ManifestFileMeta> sortAndWriteFullEntries(
            Iterable<ManifestEntry> entries,
            ManifestFileSorter.ManifestSortKey sortKey,
            Config config,
            ManifestFile manifestFile)
            throws Exception {
        try (EntrySorter sorter = new EntrySorter(sortKey, config)) {
            for (ManifestEntry entry : entries) {
                sorter.write(entry);
            }
            if (sorter.isEmpty()) {
                return Collections.emptyList();
            }
            return sorter.writeToManifest(manifestFile);
        }
    }

    /** Config used by manifest entry external sort. */
    static class Config {
        final long bufferSize;
        final int pageSize;
        final int maxNumFileHandles;
        final CompressOptions compression;
        final MemorySize maxDiskSize;

        Config(
                long bufferSize,
                int pageSize,
                int maxNumFileHandles,
                CompressOptions compression,
                MemorySize maxDiskSize) {
            this.bufferSize = bufferSize;
            this.pageSize = pageSize;
            this.maxNumFileHandles = maxNumFileHandles;
            this.compression = compression;
            this.maxDiskSize = maxDiskSize;
        }

        static Config from(CoreOptions options) {
            return new Config(
                    options.sortSpillBufferSize(),
                    options.pageSize(),
                    options.localSortMaxNumFileHandles(),
                    options.spillCompressOptions(),
                    options.writeBufferSpillDiskSize());
        }
    }

    /** Spillable sorter that stores sort keys plus serialized manifest entries in BinaryRow. */
    private static class EntrySorter implements AutoCloseable {
        private final ManifestFileSorter.ManifestSortKey sortKey;
        private final ManifestEntrySerializer entrySerializer;
        private final IOManager ioManager;
        private final BinaryExternalSortBuffer sortBuffer;

        private EntrySorter(ManifestFileSorter.ManifestSortKey sortKey, Config config) {
            this.sortKey = sortKey;
            this.entrySerializer = new ManifestEntrySerializer();
            this.ioManager = IOManager.create(System.getProperty("java.io.tmpdir"));
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

            RollingFileWriter<ManifestEntry, ManifestFileMeta> writer = null;
            Exception exception = null;
            try {
                MutableObjectIterator<BinaryRow> iterator = sortBuffer.sortedIterator();
                BinaryRow reuse = new BinaryRow(sortKey.externalSortRowType().getFieldCount());
                BinaryRow row;
                while ((row = iterator.next(reuse)) != null) {
                    ManifestEntry entry =
                            entrySerializer.deserializeFromBytes(sortKey.entryBytes(row));
                    if (entry.kind() == FileKind.DELETE) {
                        continue;
                    }
                    if (deleteEntries.remove(entry.identifier()) != null) {
                        continue;
                    }
                    if (writer == null) {
                        writer = manifestFile.createRollingWriter();
                    }
                    writer.write(entry);
                }
            } catch (Exception e) {
                exception = e;
            } finally {
                if (exception != null) {
                    if (writer != null) {
                        writer.abort();
                    }
                    throw exception;
                }
                if (writer != null) {
                    writer.close();
                }
            }
            return writer == null ? Collections.emptyList() : writer.result();
        }

        @Override
        public void close() throws Exception {
            sortBuffer.clear();
            ioManager.close();
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
