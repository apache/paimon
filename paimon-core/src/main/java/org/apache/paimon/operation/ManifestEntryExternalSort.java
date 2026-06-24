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
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.MutableObjectIterator;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.apache.paimon.utils.ManifestReadThreadPool.sequentialBatchedExecute;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/** Spillable external sort utilities for manifest entries. */
final class ManifestEntryExternalSort {

    private ManifestEntryExternalSort() {}

    static Pair<List<ManifestFileMeta>, List<ManifestFileMeta>> cancelAndWriteEntries(
            List<ManifestFileMeta> section,
            ManifestFileSorter.ManifestSortKey sortKey,
            Config config,
            ManifestFile manifestFile,
            @Nullable Integer manifestReadParallelism,
            CancelMode cancelMode)
            throws Exception {
        try (EntryCanceler canceler = new EntryCanceler(config);
                EntrySorter addSorter = new EntrySorter(sortKey, config);
                EntrySorter deleteSorter =
                        cancelMode == CancelMode.MINOR ? new EntrySorter(sortKey, config) : null) {
            Function<ManifestFileMeta, List<ManifestEntry>> reader =
                    meta -> manifestFile.read(meta.fileName(), meta.fileSize());
            for (ManifestEntry entry :
                    sequentialBatchedExecute(reader, section, manifestReadParallelism)) {
                canceler.write(entry);
            }

            canceler.writeSurvivors(cancelMode, addSorter, deleteSorter);

            List<ManifestFileMeta> addFiles = Collections.emptyList();
            if (!addSorter.isEmpty()) {
                addFiles = addSorter.writeToManifest(manifestFile);
            }

            List<ManifestFileMeta> deleteFiles = Collections.emptyList();
            if (deleteSorter != null && !deleteSorter.isEmpty()) {
                deleteFiles = deleteSorter.writeToManifest(manifestFile);
            }

            return Pair.of(addFiles, deleteFiles);
        }
    }

    static List<ManifestFileMeta> sortAndWriteEntries(
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

    enum CancelMode {
        FULL,
        MINOR
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

    /** Spillable identifier join that cancels ADD/DELETE pairs without keeping a hash map. */
    private static class EntryCanceler implements AutoCloseable {
        private static final int IDENTIFIER_FIELD = 0;
        private static final int KIND_FIELD = 1;
        private static final int ENTRY_FIELD = 2;
        private static final RowType ROW_TYPE =
                DataTypes.ROW(DataTypes.BYTES(), DataTypes.TINYINT(), DataTypes.BYTES());
        private static final int[] KEY_FIELDS = new int[] {IDENTIFIER_FIELD};

        private final ManifestEntrySerializer entrySerializer;
        private final IOManager ioManager;
        private final BinaryExternalSortBuffer sortBuffer;

        private EntryCanceler(Config config) {
            this.entrySerializer = new ManifestEntrySerializer();
            this.ioManager = IOManager.create(System.getProperty("java.io.tmpdir"));
            this.sortBuffer =
                    BinaryExternalSortBuffer.create(
                            ioManager,
                            ROW_TYPE,
                            KEY_FIELDS,
                            config.bufferSize,
                            config.pageSize,
                            config.maxNumFileHandles,
                            config.compression,
                            config.maxDiskSize);
        }

        private void write(ManifestEntry entry) throws Exception {
            GenericRow row = new GenericRow(ROW_TYPE.getFieldCount());
            row.setField(IDENTIFIER_FIELD, identifierBytes(entry.identifier()));
            row.setField(KIND_FIELD, entry.kind().toByteValue());
            row.setField(ENTRY_FIELD, entrySerializer.serializeToBytes(entry));
            sortBuffer.write(row);
        }

        private void writeSurvivors(
                CancelMode cancelMode, EntrySorter addSorter, @Nullable EntrySorter deleteSorter)
                throws Exception {
            if (sortBuffer.isEmpty()) {
                return;
            }

            MutableObjectIterator<BinaryRow> iterator = sortBuffer.sortedIterator();
            BinaryRow reuse = new BinaryRow(ROW_TYPE.getFieldCount());
            BinaryRow row;
            byte[] currentIdentifier = null;
            List<byte[]> addEntries = new ArrayList<>(1);
            List<byte[]> deleteEntries = new ArrayList<>(1);

            while ((row = iterator.next(reuse)) != null) {
                byte[] identifier = row.getBinary(IDENTIFIER_FIELD);
                if (currentIdentifier != null && !Arrays.equals(currentIdentifier, identifier)) {
                    writeSurvivorGroup(
                            cancelMode, addEntries, deleteEntries, addSorter, deleteSorter);
                    addEntries.clear();
                    deleteEntries.clear();
                }

                currentIdentifier = identifier;
                if (row.getByte(KIND_FIELD) == FileKind.ADD.toByteValue()) {
                    addEntries.add(row.getBinary(ENTRY_FIELD));
                } else {
                    deleteEntries.add(row.getBinary(ENTRY_FIELD));
                }
            }

            if (currentIdentifier != null) {
                writeSurvivorGroup(cancelMode, addEntries, deleteEntries, addSorter, deleteSorter);
            }
        }

        private void writeSurvivorGroup(
                CancelMode cancelMode,
                List<byte[]> addEntries,
                List<byte[]> deleteEntries,
                EntrySorter addSorter,
                @Nullable EntrySorter deleteSorter)
                throws Exception {
            if (cancelMode == CancelMode.FULL) {
                if (deleteEntries.isEmpty()) {
                    writeEntries(addEntries, addSorter);
                }
                return;
            }

            if (!addEntries.isEmpty() && !deleteEntries.isEmpty()) {
                if (deleteSorter != null) {
                    for (int i = 1; i < deleteEntries.size(); i++) {
                        writeEntry(deleteEntries.get(i), deleteSorter);
                    }
                }
                return;
            }

            writeEntries(addEntries, addSorter);
            if (deleteSorter != null) {
                writeEntries(deleteEntries, deleteSorter);
            }
        }

        private void writeEntries(List<byte[]> entries, EntrySorter sorter) throws Exception {
            for (byte[] entry : entries) {
                writeEntry(entry, sorter);
            }
        }

        private void writeEntry(byte[] entry, EntrySorter sorter) throws Exception {
            sorter.write(entrySerializer.deserializeFromBytes(entry));
        }

        @Override
        public void close() throws Exception {
            sortBuffer.clear();
            ioManager.close();
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

        @Override
        public void close() throws Exception {
            sortBuffer.clear();
            ioManager.close();
        }
    }

    private static byte[] identifierBytes(FileEntry.Identifier identifier) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        writeString(out, identifier.getClass().getName());
        writeBytes(out, serializeBinaryRow(identifier.partition));
        out.writeInt(identifier.bucket);
        out.writeInt(identifier.level);
        writeString(out, identifier.fileName);
        writeStrings(out, identifier.extraFiles);
        writeBytes(out, identifier.embeddedIndex);
        writeString(out, identifier.externalPath);
        out.flush();
        return bytes.toByteArray();
    }

    private static void writeStrings(DataOutputStream out, @Nullable List<String> values)
            throws IOException {
        if (values == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(values.size());
        for (String value : values) {
            writeString(out, value);
        }
    }

    private static void writeString(DataOutputStream out, @Nullable String value)
            throws IOException {
        if (value == null) {
            out.writeInt(-1);
            return;
        }
        writeBytes(out, value.getBytes(StandardCharsets.UTF_8));
    }

    private static void writeBytes(DataOutputStream out, @Nullable byte[] value)
            throws IOException {
        if (value == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(value.length);
        out.write(value);
    }
}
