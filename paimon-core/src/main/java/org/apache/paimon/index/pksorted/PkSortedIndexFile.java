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

package org.apache.paimon.index.pksorted;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.PositionOutputStream;
import org.apache.paimon.globalindex.GlobalIndexSingleColumnWriter;
import org.apache.paimon.globalindex.GlobalIndexWriter;
import org.apache.paimon.globalindex.GlobalIndexer;
import org.apache.paimon.globalindex.ResultEntry;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFile;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Builds source-backed scalar-index payloads for ordered physical data files. */
public class PkSortedIndexFile extends IndexFile {

    public PkSortedIndexFile(FileIO fileIO, IndexPathFactory pathFactory) {
        super(fileIO, pathFactory);
    }

    public IndexFileMeta build(
            int dataLevel,
            List<PrimaryKeyIndexSourceFile> sourceFiles,
            DataField indexField,
            String indexType,
            Options indexOptions,
            Iterator<Entry> sortedEntries)
            throws IOException {
        List<IndexFileMeta> payloads =
                buildInternal(
                        dataLevel,
                        sourceFiles,
                        indexField,
                        indexType,
                        indexOptions,
                        sortedEntries,
                        true);
        return payloads.get(0);
    }

    List<IndexFileMeta> buildAll(
            int dataLevel,
            List<PrimaryKeyIndexSourceFile> sourceFiles,
            DataField indexField,
            String indexType,
            Options indexOptions,
            Iterator<Entry> entries)
            throws IOException {
        return buildInternal(
                dataLevel, sourceFiles, indexField, indexType, indexOptions, entries, false);
    }

    private List<IndexFileMeta> buildInternal(
            int dataLevel,
            List<PrimaryKeyIndexSourceFile> sourceFiles,
            DataField indexField,
            String indexType,
            Options indexOptions,
            Iterator<Entry> entries,
            boolean requireSinglePayload)
            throws IOException {
        long sourceRowCount = 0;
        for (PrimaryKeyIndexSourceFile sourceFile : sourceFiles) {
            sourceRowCount = Math.addExact(sourceRowCount, sourceFile.rowCount());
        }
        checkArgument(
                sourceRowCount > 0,
                "A source-backed index group must reference at least one source row.");

        TrackingFileWriter fileWriter = new TrackingFileWriter();
        GlobalIndexSingleColumnWriter writer = null;
        boolean success = false;
        try {
            writer = createWriter(indexType, indexField, indexOptions, fileWriter);

            while (entries.hasNext()) {
                Entry entry = entries.next();
                checkArgument(
                        entry.rowId >= 0 && entry.rowId < sourceRowCount,
                        "Row id %s is outside source-backed index group row range [0, %s).",
                        entry.rowId,
                        sourceRowCount);
                writer.write(entry.value, entry.rowId);
            }

            List<ResultEntry> results = writer.finish(sourceRowCount);
            checkArgument(
                    !results.isEmpty(), "Index build must produce at least one payload file.");
            if (requireSinglePayload) {
                checkArgument(
                        results.size() == 1,
                        "Sorted index build must produce exactly one payload file, but produced %s.",
                        results.size());
            }
            byte[] sourceMeta = new PrimaryKeyIndexSourceMeta(dataLevel, sourceFiles).serialize();
            List<IndexFileMeta> payloads = new ArrayList<>(results.size());
            Set<String> resultNames = new HashSet<>();
            long nextRow = 0;
            for (ResultEntry result : results) {
                checkArgument(
                        result.rowCount() > 0,
                        "Index payload %s must cover at least one source row.",
                        result.fileName());
                checkArgument(
                        resultNames.add(result.fileName()),
                        "Index build produced duplicate payload file %s.",
                        result.fileName());
                long rangeEnd = Math.addExact(nextRow, result.rowCount()) - 1;
                checkArgument(
                        rangeEnd < sourceRowCount,
                        "Index payload rows exceed source row count %s.",
                        sourceRowCount);
                Path payloadPath = fileWriter.path(result.fileName());
                payloads.add(
                        new IndexFileMeta(
                                indexType,
                                result.fileName(),
                                fileIO.getFileSize(payloadPath),
                                result.rowCount(),
                                new GlobalIndexMeta(
                                        nextRow,
                                        rangeEnd,
                                        indexField.id(),
                                        null,
                                        result.meta(),
                                        sourceMeta),
                                pathFactory.isExternalPath() ? payloadPath.toString() : null));
                nextRow = rangeEnd + 1;
            }
            checkArgument(
                    nextRow == sourceRowCount,
                    "Index payload row count %s does not match source row count %s.",
                    nextRow,
                    sourceRowCount);
            checkArgument(
                    resultNames.equals(fileWriter.createdFileNames()),
                    "Index build payload results do not match allocated files.");
            success = true;
            return payloads;
        } finally {
            if (writer instanceof AutoCloseable) {
                IOUtils.closeQuietly((AutoCloseable) writer);
            }
            if (!success) {
                fileWriter.deleteCreatedFiles();
            }
        }
    }

    protected GlobalIndexSingleColumnWriter createWriter(
            String indexType,
            DataField indexField,
            Options indexOptions,
            GlobalIndexFileWriter fileWriter)
            throws IOException {
        GlobalIndexer indexer = GlobalIndexer.create(indexType, indexField, indexOptions);
        GlobalIndexWriter writer = indexer.createWriter(fileWriter);
        checkArgument(
                writer instanceof GlobalIndexSingleColumnWriter,
                "Index algorithm %s does not create a single-column writer.",
                indexType);
        return (GlobalIndexSingleColumnWriter) writer;
    }

    /** One index value and its zero-based source-row ordinal. */
    public static final class Entry {

        @Nullable private final Object value;
        private final long rowId;

        public Entry(@Nullable Object value, long rowId) {
            this.value = value;
            this.rowId = rowId;
        }

        @Nullable
        public Object value() {
            return value;
        }

        public long rowId() {
            return rowId;
        }
    }

    private final class TrackingFileWriter implements GlobalIndexFileWriter {

        private final Map<String, Path> createdFiles = new LinkedHashMap<>();

        @Override
        public String newFileName(String prefix) {
            Path path = pathFactory.newPath();
            createdFiles.put(path.getName(), path);
            return path.getName();
        }

        @Override
        public PositionOutputStream newOutputStream(String fileName) throws IOException {
            return fileIO.newOutputStream(path(fileName), false);
        }

        private Path path(String fileName) {
            Path path = createdFiles.get(fileName);
            checkArgument(path != null, "Sorted payload file %s was not allocated.", fileName);
            return path;
        }

        private Set<String> createdFileNames() {
            return new HashSet<>(createdFiles.keySet());
        }

        private void deleteCreatedFiles() {
            for (Path path : createdFiles.values()) {
                fileIO.deleteQuietly(path);
            }
        }
    }
}
