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
import org.apache.paimon.globalindex.sorted.SortedIndexOptions;
import org.apache.paimon.globalindex.sorted.SortedSingleColumnIndexWriter;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFile;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.IndexPathFactory;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Builds source-backed BTree or Bitmap payloads for one physical data file. */
public class PkSortedIndexFile extends IndexFile {

    public PkSortedIndexFile(FileIO fileIO, IndexPathFactory pathFactory) {
        super(fileIO, pathFactory);
    }

    public List<IndexFileMeta> build(
            PrimaryKeyIndexSourceFile sourceFile,
            DataField indexField,
            String indexType,
            Options indexOptions,
            Iterator<Entry> sortedEntries)
            throws IOException {
        checkArgument(
                sourceFile.rowCount() > 0,
                "A sorted index group must reference at least one source row.");

        TrackingFileWriter fileWriter = new TrackingFileWriter();
        boolean success = false;
        try {
            long recordsPerRange =
                    indexOptions.get(SortedIndexOptions.SORTED_INDEX_RECORDS_PER_RANGE);
            SortedSingleColumnIndexWriter writer =
                    new SortedSingleColumnIndexWriter(
                            recordsPerRange,
                            () -> createWriter(indexType, indexField, indexOptions, fileWriter));

            long writtenRows = 0;
            while (sortedEntries.hasNext()) {
                Entry entry = sortedEntries.next();
                checkArgument(
                        entry.localRowId >= 0 && entry.localRowId < sourceFile.rowCount(),
                        "Local row id %s is outside source file %s row range [0, %s).",
                        entry.localRowId,
                        sourceFile.fileName(),
                        sourceFile.rowCount());
                writer.write(entry.value, entry.localRowId);
                writtenRows++;
            }
            checkArgument(
                    writtenRows == sourceFile.rowCount(),
                    "Sorted index input row count %s does not match source file %s row count %s.",
                    writtenRows,
                    sourceFile.fileName(),
                    sourceFile.rowCount());

            List<List<ResultEntry>> resultGroups = writer.finish();
            List<IndexFileMeta> payloads = new ArrayList<>();
            long payloadRows = 0;
            byte[] sourceMeta = new PrimaryKeyIndexSourceMeta(sourceFile).serialize();
            for (List<ResultEntry> resultGroup : resultGroups) {
                for (ResultEntry result : resultGroup) {
                    payloadRows = Math.addExact(payloadRows, result.rowCount());
                    Path payloadPath = fileWriter.path(result.fileName());
                    payloads.add(
                            new IndexFileMeta(
                                    indexType,
                                    result.fileName(),
                                    fileIO.getFileSize(payloadPath),
                                    result.rowCount(),
                                    new GlobalIndexMeta(
                                            0,
                                            sourceFile.rowCount() - 1,
                                            indexField.id(),
                                            null,
                                            result.meta(),
                                            sourceMeta),
                                    pathFactory.isExternalPath() ? payloadPath.toString() : null));
                }
            }
            checkArgument(
                    payloadRows == sourceFile.rowCount(),
                    "Sorted payload row count %s does not match source file %s row count %s.",
                    payloadRows,
                    sourceFile.fileName(),
                    sourceFile.rowCount());
            success = true;
            return Collections.unmodifiableList(payloads);
        } finally {
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

    /** One sorted scalar value and its zero-based position in the source data file. */
    public static final class Entry {

        @Nullable private final Object value;
        private final long localRowId;

        public Entry(@Nullable Object value, long localRowId) {
            this.value = value;
            this.localRowId = localRowId;
        }

        @Nullable
        public Object value() {
            return value;
        }

        public long localRowId() {
            return localRowId;
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

        private void deleteCreatedFiles() {
            for (Path path : createdFiles.values()) {
                fileIO.deleteQuietly(path);
            }
        }
    }
}
