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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.index.pk.PrimaryKeyIndexSourceFile;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.options.Options;
import org.apache.paimon.sort.BinaryExternalSortBuffer;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.IOUtils;
import org.apache.paimon.utils.MutableObjectIteratorAdapter;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Sorts physical data files and builds their source-backed scalar index payloads. */
public class PkSortedIndexBuilder {

    private static final int ROW_ID_FIELD_ID = Integer.MAX_VALUE;

    private final ReaderFactory readerFactory;
    private final PkSortedIndexFile indexFile;
    private final DataField indexField;
    private final String indexType;
    private final Options options;
    @Nullable private final IOManager ioManager;

    public PkSortedIndexBuilder(
            PkSortedDataFileReader.Factory readerFactory,
            PkSortedIndexFile indexFile,
            DataField indexField,
            String indexType,
            Options options,
            @Nullable IOManager ioManager) {
        this(readerFactory::create, indexFile, indexField, indexType, options, ioManager);
    }

    PkSortedIndexBuilder(
            ReaderFactory readerFactory,
            PkSortedIndexFile indexFile,
            DataField indexField,
            String indexType,
            Options options,
            @Nullable IOManager ioManager) {
        this.readerFactory = readerFactory;
        this.indexFile = indexFile;
        this.indexField = indexField;
        this.indexType = indexType;
        this.options = options;
        this.ioManager = ioManager;
    }

    public List<org.apache.paimon.index.IndexFileMeta> build(DataFileMeta dataFile)
            throws IOException {
        return build(Collections.singletonList(dataFile));
    }

    public List<org.apache.paimon.index.IndexFileMeta> build(List<DataFileMeta> dataFiles)
            throws IOException {
        checkArgument(!dataFiles.isEmpty(), "A sorted index build requires source files.");
        List<DataFileMeta> orderedDataFiles = new ArrayList<>(dataFiles);
        orderedDataFiles.sort(Comparator.comparing(DataFileMeta::fileName));

        IOManager actualIOManager = ioManager;
        boolean ownsIOManager = false;
        if (actualIOManager == null) {
            actualIOManager = createTemporaryIOManager();
            ownsIOManager = true;
        }

        BinaryExternalSortBuffer sortBuffer = null;
        try {
            CoreOptions coreOptions = new CoreOptions(options);
            RowType sortRowType =
                    RowType.of(
                            indexField,
                            new DataField(
                                    ROW_ID_FIELD_ID, "_ROW_ID", DataTypes.BIGINT().notNull()));
            sortBuffer =
                    BinaryExternalSortBuffer.create(
                            actualIOManager,
                            sortRowType,
                            new int[] {0},
                            coreOptions.writeBufferSize(),
                            coreOptions.pageSize(),
                            coreOptions.localSortMaxNumFileHandles(),
                            coreOptions.spillCompressOptions(),
                            coreOptions.writeBufferSpillDiskSize());

            List<PrimaryKeyIndexSourceFile> sourceFiles = new ArrayList<>();
            long sourceOffset = 0;
            for (DataFileMeta dataFile : orderedDataFiles) {
                sourceFiles.add(
                        new PrimaryKeyIndexSourceFile(dataFile.fileName(), dataFile.rowCount()));
                try (Reader reader = readerFactory.create(dataFile)) {
                    checkArgument(
                            reader.rowCount() == dataFile.rowCount(),
                            "Sorted reader row count %s does not match data file %s row count %s.",
                            reader.rowCount(),
                            dataFile.fileName(),
                            dataFile.rowCount());
                    PkSortedDataFileReader.Entry entry;
                    while ((entry = reader.readNext()) != null) {
                        checkArgument(
                                entry.rowPosition() >= 0
                                        && entry.rowPosition() < dataFile.rowCount(),
                                "Row position %s is outside data file %s row range [0, %s).",
                                entry.rowPosition(),
                                dataFile.fileName(),
                                dataFile.rowCount());
                        sortBuffer.write(
                                GenericRow.of(
                                        entry.value(),
                                        Math.addExact(sourceOffset, entry.rowPosition())));
                    }
                }
                sourceOffset = Math.addExact(sourceOffset, dataFile.rowCount());
            }

            Iterator<InternalRow> sortedRows =
                    new MutableObjectIteratorAdapter<>(
                            sortBuffer.sortedIterator(),
                            new BinaryRow(sortRowType.getFieldCount()));
            InternalRow.FieldGetter valueGetter =
                    InternalRow.createFieldGetter(indexField.type(), 0);
            Iterator<PkSortedIndexFile.Entry> sortedEntries =
                    new Iterator<PkSortedIndexFile.Entry>() {
                        @Override
                        public boolean hasNext() {
                            return sortedRows.hasNext();
                        }

                        @Override
                        public PkSortedIndexFile.Entry next() {
                            InternalRow row = sortedRows.next();
                            return new PkSortedIndexFile.Entry(
                                    valueGetter.getFieldOrNull(row), row.getLong(1));
                        }
                    };
            return indexFile.build(sourceFiles, indexField, indexType, options, sortedEntries);
        } finally {
            if (sortBuffer != null) {
                sortBuffer.clear();
            }
            if (ownsIOManager) {
                IOUtils.closeQuietly(actualIOManager);
            }
        }
    }

    protected IOManager createTemporaryIOManager() {
        return IOManager.create(System.getProperty("java.io.tmpdir"));
    }

    /** Physical source reader used by the sorter. */
    interface Reader extends Closeable {

        long rowCount();

        @Nullable
        PkSortedDataFileReader.Entry readNext();
    }

    @FunctionalInterface
    interface ReaderFactory {

        Reader create(DataFileMeta dataFile) throws IOException;
    }
}
