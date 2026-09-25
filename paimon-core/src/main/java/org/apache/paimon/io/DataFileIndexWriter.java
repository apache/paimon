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

package org.apache.paimon.io;

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fileindex.FileIndexCommon;
import org.apache.paimon.fileindex.FileIndexFormat;
import org.apache.paimon.fileindex.FileIndexOptions;
import org.apache.paimon.fileindex.FileIndexWriter;
import org.apache.paimon.fileindex.FileIndexer;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Index file writer for a data file. */
public final class DataFileIndexWriter implements Closeable {

    public static final FileIndexResult EMPTY_RESULT = FileIndexResult.of(null, null);

    private final FileIO fileIO;

    private final Path path;

    // if the filter size greater than fileIndexInManifestThreshold, we put it in file
    private final int inManifestThreshold;
    private final int formatVersion;

    // index type, column name -> index maintainer
    private final Map<String, Map<String, IndexMaintainer>> indexMaintainers = new HashMap<>();

    private String resultFileName;

    private byte[] embeddedIndexBytes;

    public DataFileIndexWriter(
            FileIO fileIO,
            Path path,
            RowType rowType,
            FileIndexOptions fileIndexOptions,
            @Nullable Map<String, String> colNameMapping) {
        this.fileIO = fileIO;
        this.path = path;
        List<DataField> fields = rowType.getFields();
        Map<String, DataField> map = new HashMap<>();
        Map<String, Integer> index = new HashMap<>();
        fields.forEach(
                dataField -> {
                    map.put(dataField.name(), dataField);
                    index.put(dataField.name(), rowType.getFieldIndex(dataField.name()));
                });
        for (Map.Entry<FileIndexOptions.Column, Map<String, Options>> entry :
                fileIndexOptions.entrySet()) {
            FileIndexOptions.Column entryColumn = entry.getKey();
            String colName = entryColumn.getColumnName();
            if (colNameMapping != null) {
                colName = colNameMapping.getOrDefault(colName, null);
                if (colName == null) {
                    continue;
                }
            }

            String columnName = colName;
            DataField field = map.get(columnName);
            if (field == null) {
                throw new IllegalArgumentException(columnName + " does not exist in column fields");
            }

            for (Map.Entry<String, Options> typeEntry : entry.getValue().entrySet()) {
                String indexType = typeEntry.getKey();
                Map<String, IndexMaintainer> column2maintainers =
                        indexMaintainers.computeIfAbsent(indexType, k -> new HashMap<>());
                IndexMaintainer maintainer = column2maintainers.get(columnName);
                if (entryColumn.isNestedColumn()) {
                    if (field.type().getTypeRoot() != DataTypeRoot.MAP) {
                        throw new IllegalArgumentException(
                                "Column "
                                        + columnName
                                        + " is nested column, but is not map type. Only should map type yet.");
                    }
                    MapFileIndexMaintainer mapMaintainer = (MapFileIndexMaintainer) maintainer;
                    if (mapMaintainer == null) {
                        MapType mapType = (MapType) field.type();
                        mapMaintainer =
                                new MapFileIndexMaintainer(
                                        columnName,
                                        indexType,
                                        mapType.getKeyType(),
                                        mapType.getValueType(),
                                        fileIndexOptions.getMapTopLevelOptions(
                                                columnName, typeEntry.getKey()),
                                        index.get(columnName));
                        column2maintainers.put(columnName, mapMaintainer);
                    }
                    mapMaintainer.add(entryColumn.getNestedColumnName(), typeEntry.getValue());
                } else {
                    if (maintainer == null) {
                        maintainer =
                                new FileIndexMaintainer(
                                        columnName,
                                        indexType,
                                        FileIndexer.create(
                                                        indexType,
                                                        field.type(),
                                                        typeEntry.getValue())
                                                .createWriter(),
                                        InternalRow.createFieldGetter(
                                                field.type(), index.get(columnName)));
                        column2maintainers.put(columnName, maintainer);
                    }
                }
            }
        }
        this.inManifestThreshold = fileIndexOptions.fileIndexInManifestThreshold();
        this.formatVersion = fileIndexOptions.formatVersion();
    }

    public void write(InternalRow row) {
        indexMaintainers
                .values()
                .forEach(
                        column2maintainers ->
                                column2maintainers.values().forEach(index -> index.write(row)));
    }

    @Override
    public void close() throws IOException {
        SpillableIndexOutputStream output =
                new SpillableIndexOutputStream(fileIO, path, inManifestThreshold);
        try {
            try (FileIndexFormat.Writer writer =
                    FileIndexFormat.createWriter(output, formatVersion)) {
                forEachIndex(
                        (column, type, indexWriter) ->
                                writer.writeIndex(
                                        column,
                                        type,
                                        indexWriter == null ? null : indexWriter::writeTo));
                writer.finish();
            }
            if (output.spilled()) {
                resultFileName = path.getName();
            } else {
                embeddedIndexBytes = output.embeddedBytes();
            }
        } catch (IOException | RuntimeException e) {
            try {
                output.abort();
            } catch (IOException cleanupFailure) {
                e.addSuppressed(cleanupFailure);
            }
            throw e;
        }
    }

    /** Visits the index writers without serializing their payloads. */
    public void forEachIndex(IndexConsumer consumer) throws IOException {
        Map<String, Map<String, FileIndexWriter>> entries = new HashMap<>();
        for (Map<String, IndexMaintainer> columnMaintainers : indexMaintainers.values()) {
            for (IndexMaintainer maintainer : columnMaintainers.values()) {
                maintainer.forEachIndex(
                        (column, type, writer) ->
                                entries.computeIfAbsent(column, ignored -> new HashMap<>())
                                        .put(type, writer));
            }
        }
        for (Map.Entry<String, Map<String, FileIndexWriter>> column : entries.entrySet()) {
            for (Map.Entry<String, FileIndexWriter> index : column.getValue().entrySet()) {
                consumer.accept(column.getKey(), index.getKey(), index.getValue());
            }
        }
    }

    /** Consumes the writer for one column and index type. */
    @FunctionalInterface
    public interface IndexConsumer {
        void accept(String columnName, String indexType, @Nullable FileIndexWriter writer)
                throws IOException;
    }

    public FileIndexResult result() {
        return FileIndexResult.of(embeddedIndexBytes, resultFileName);
    }

    @Nullable
    public static DataFileIndexWriter create(
            FileIO fileIO, Path path, RowType rowType, FileIndexOptions fileIndexOptions) {
        return create(fileIO, path, rowType, fileIndexOptions, null);
    }

    @Nullable
    public static DataFileIndexWriter create(
            FileIO fileIO,
            Path path,
            RowType rowType,
            FileIndexOptions fileIndexOptions,
            @Nullable Map<String, String> colNameMapping) {
        return fileIndexOptions.isEmpty()
                ? null
                : new DataFileIndexWriter(fileIO, path, rowType, fileIndexOptions, colNameMapping);
    }

    /** File index result. */
    public interface FileIndexResult {

        @Nullable
        byte[] embeddedIndexBytes();

        @Nullable
        String independentIndexFile();

        static FileIndexResult of(byte[] embeddedIndexBytes, String resultFileName) {
            return new FileIndexResult() {

                @Override
                public byte[] embeddedIndexBytes() {
                    return embeddedIndexBytes;
                }

                @Override
                public String independentIndexFile() {
                    return resultFileName;
                }
            };
        }
    }

    interface IndexMaintainer {

        void write(InternalRow row);

        void forEachIndex(IndexConsumer consumer) throws IOException;
    }

    /** One index maintainer for one column. */
    private static class FileIndexMaintainer implements IndexMaintainer {

        private final String columnName;
        private final String indexType;
        private final FileIndexWriter fileIndexWriter;
        private final InternalRow.FieldGetter getter;

        public FileIndexMaintainer(
                String columnName,
                String indexType,
                FileIndexWriter fileIndexWriter,
                InternalRow.FieldGetter getter) {
            this.columnName = columnName;
            this.indexType = indexType;
            this.fileIndexWriter = fileIndexWriter;
            this.getter = getter;
        }

        public void write(InternalRow row) {
            fileIndexWriter.writeRecord(getter.getFieldOrNull(row));
        }

        public void forEachIndex(IndexConsumer consumer) throws IOException {
            consumer.accept(columnName, indexType, fileIndexWriter);
        }
    }

    /** File index writer for map data type. */
    private static class MapFileIndexMaintainer implements IndexMaintainer {

        private final String columnName;
        private final String indexType;
        private final Options options;
        private final DataType valueType;
        private final Map<String, org.apache.paimon.fileindex.FileIndexWriter> indexWritersMap;
        private final InternalArray.ElementGetter valueElementGetter;
        private final int position;

        public MapFileIndexMaintainer(
                String columnName,
                String indexType,
                DataType keyType,
                DataType valueType,
                Options options,
                int position) {
            this.columnName = columnName;
            this.indexType = indexType;
            this.valueType = valueType;
            this.options = options;
            this.position = position;
            this.indexWritersMap = new HashMap<>();
            this.valueElementGetter = InternalArray.createElementGetter(valueType);

            DataTypeRoot rootType = keyType.getTypeRoot();
            if (rootType != DataTypeRoot.CHAR && rootType != DataTypeRoot.VARCHAR) {
                throw new IllegalArgumentException(
                        "Only support map data type with key field of CHAR、VARCHAR、STRING.");
            }
        }

        public void write(InternalRow row) {
            if (row.isNullAt(position)) {
                indexWritersMap.values().forEach(write -> write.writeRecord(null));
                return;
            }
            InternalMap internalMap = row.getMap(position);
            InternalArray keyArray = internalMap.keyArray();
            InternalArray valueArray = internalMap.valueArray();

            Set<String> writtenKeys = new HashSet<>();
            for (int i = 0; i < keyArray.size(); i++) {
                String key = keyArray.getString(i).toString();
                org.apache.paimon.fileindex.FileIndexWriter writer =
                        indexWritersMap.getOrDefault(key, null);
                if (writer != null) {
                    writtenKeys.add(key);
                    writer.writeRecord(valueElementGetter.getElementOrNull(valueArray, i));
                }
            }

            for (Map.Entry<String, FileIndexWriter> writerEntry : indexWritersMap.entrySet()) {
                if (!writtenKeys.contains(writerEntry.getKey())) {
                    writerEntry.getValue().writeRecord(null);
                }
            }
        }

        public void add(String nestedKey, Options nestedOptions) {
            indexWritersMap.put(
                    nestedKey,
                    FileIndexer.create(
                                    indexType,
                                    valueType,
                                    new Options(options.toMap(), nestedOptions.toMap()))
                            .createWriter());
        }

        public void forEachIndex(IndexConsumer consumer) throws IOException {
            for (Map.Entry<String, FileIndexWriter> entry : indexWritersMap.entrySet()) {
                FileIndexWriter writer = entry.getValue();
                consumer.accept(
                        FileIndexCommon.toMapKey(columnName, entry.getKey()),
                        indexType,
                        writer.empty() ? null : writer);
            }
        }
    }
}
