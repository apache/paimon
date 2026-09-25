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

import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Builds source-backed indexes whose writers require values in physical source-row order. */
public class PkSequentialIndexBuilder {

    private final PkSortedIndexBuilder.ReaderFactory readerFactory;
    private final PkSortedIndexFile indexFile;
    private final DataField indexField;
    private final String indexType;
    private final Options options;

    public PkSequentialIndexBuilder(
            PkSortedDataFileReader.Factory readerFactory,
            PkSortedIndexFile indexFile,
            DataField indexField,
            String indexType,
            Options options) {
        this(readerFactory::create, indexFile, indexField, indexType, options);
    }

    PkSequentialIndexBuilder(
            PkSortedIndexBuilder.ReaderFactory readerFactory,
            PkSortedIndexFile indexFile,
            DataField indexField,
            String indexType,
            Options options) {
        this.readerFactory = readerFactory;
        this.indexFile = indexFile;
        this.indexField = indexField;
        this.indexType = indexType;
        this.options = options;
    }

    public IndexFileMeta build(List<DataFileMeta> dataFiles) throws IOException {
        checkArgument(!dataFiles.isEmpty(), "A sequential index build requires source files.");
        List<DataFileMeta> orderedDataFiles = new ArrayList<>(dataFiles);
        orderedDataFiles.sort(Comparator.comparing(DataFileMeta::fileName));
        int dataLevel = orderedDataFiles.get(0).level();
        checkArgument(dataLevel > 0, "A sequential index build requires a positive data level.");

        List<PrimaryKeyIndexSourceFile> sourceFiles = new ArrayList<>();
        for (DataFileMeta dataFile : orderedDataFiles) {
            checkArgument(
                    dataFile.level() == dataLevel,
                    "A sequential index build cannot mix data levels %s and %s.",
                    dataLevel,
                    dataFile.level());
            sourceFiles.add(
                    new PrimaryKeyIndexSourceFile(dataFile.fileName(), dataFile.rowCount()));
        }

        try (SourceEntryIterator entries = new SourceEntryIterator(orderedDataFiles)) {
            try {
                return indexFile.build(
                        dataLevel, sourceFiles, indexField, indexType, options, entries);
            } catch (UncheckedIOException e) {
                throw e.getCause();
            }
        }
    }

    private final class SourceEntryIterator
            implements Iterator<PkSortedIndexFile.Entry>, AutoCloseable {

        private final List<DataFileMeta> dataFiles;

        private int fileIndex;
        private long sourceOffset;
        private long expectedPosition;
        @Nullable private PkSortedIndexBuilder.Reader currentReader;
        @Nullable private PkSortedIndexFile.Entry next;
        private boolean prepared;
        private boolean finished;

        private SourceEntryIterator(List<DataFileMeta> dataFiles) {
            this.dataFiles = dataFiles;
        }

        @Override
        public boolean hasNext() {
            prepare();
            return next != null;
        }

        @Override
        public PkSortedIndexFile.Entry next() {
            prepare();
            if (next == null) {
                throw new NoSuchElementException();
            }
            PkSortedIndexFile.Entry result = next;
            next = null;
            prepared = false;
            return result;
        }

        private void prepare() {
            if (prepared || finished) {
                return;
            }
            try {
                while (fileIndex < dataFiles.size()) {
                    DataFileMeta dataFile = dataFiles.get(fileIndex);
                    if (currentReader == null) {
                        currentReader = readerFactory.create(dataFile);
                        checkArgument(
                                currentReader.rowCount() == dataFile.rowCount(),
                                "Sequential reader row count %s does not match data file %s row count %s.",
                                currentReader.rowCount(),
                                dataFile.fileName(),
                                dataFile.rowCount());
                    }
                    PkSortedDataFileReader.Entry entry = currentReader.readNext();
                    if (entry != null) {
                        checkArgument(
                                entry.rowPosition() == expectedPosition,
                                "Sequential reader for data file %s returned row position %s, expected %s.",
                                dataFile.fileName(),
                                entry.rowPosition(),
                                expectedPosition);
                        next =
                                new PkSortedIndexFile.Entry(
                                        entry.value(),
                                        Math.addExact(sourceOffset, entry.rowPosition()));
                        expectedPosition++;
                        prepared = true;
                        return;
                    }

                    checkArgument(
                            expectedPosition == dataFile.rowCount(),
                            "Sequential reader returned %s rows for data file %s, expected %s.",
                            expectedPosition,
                            dataFile.fileName(),
                            dataFile.rowCount());
                    currentReader.close();
                    currentReader = null;
                    sourceOffset = Math.addExact(sourceOffset, dataFile.rowCount());
                    expectedPosition = 0;
                    fileIndex++;
                }
                finished = true;
                prepared = true;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void close() {
            IOUtils.closeQuietly(currentReader);
            currentReader = null;
        }
    }
}
