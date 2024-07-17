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

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Index file writer for a data file. */
public final class DataFileIndexWriter implements Closeable {

    public static final FileIndexResult EMPTY_RESULT = FileIndexResult.of(null, null);

    private final FileIO fileIO;

    private final Path path;

    // if the filter size greater than fileIndexInManifestThreshold, we put it in file
    private final long inManifestThreshold;

    private final Map<String, IndexMaintainer> indexMaintainers = new HashMap<>();

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
                IndexMaintainer maintainer = indexMaintainers.get(columnName);
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
                        indexMaintainers.put(columnName, mapMaintainer);
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
                        indexMaintainers.put(columnName, maintainer);
                    }
                }
            }
        }
        this.inManifestThreshold = fileIndexOptions.fileIndexInManifestThreshold();
    }

    public void write(InternalRow row) {
        indexMaintainers
                .values()
                .forEach(mapFileIndexMaintainer -> mapFileIndexMaintainer.write(row));
    }

    @Override
    public void close() throws IOException {
        Map<String, Map<String, byte[]>> indexMaps = serializeMaintainers();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (FileIndexFormat.Writer writer = FileIndexFormat.createWriter(out)) {
            writer.writeColumnIndexes(indexMaps);
        }

        if (out.size() > inManifestThreshold) {
            try (OutputStream outputStream = fileIO.newOutputStream(path, true)) {
                outputStream.write(out.toByteArray());
            }
            resultFileName = path.getName();
        } else {
            embeddedIndexBytes = out.toByteArray();
        }
    }

    public Map<String, Map<String, byte[]>> serializeMaintainers() {
        Map<String, Map<String, byte[]>> indexMaps = new HashMap<>();
        for (IndexMaintainer indexMaintainer : indexMaintainers.values()) {
            Map<String, byte[]> mapBytes = indexMaintainer.serializedBytes();
            for (Map.Entry<String, byte[]> entry : mapBytes.entrySet()) {
                indexMaps
                        .computeIfAbsent(entry.getKey(), k -> new HashMap<>())
                        .put(indexMaintainer.getIndexType(), entry.getValue());
            }
        }
        return indexMaps;
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

        String getIndexType();

        Map<String, byte[]> serializedBytes();
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

        public String getIndexType() {
            return indexType;
        }

        public Map<String, byte[]> serializedBytes() {
            return Collections.singletonMap(columnName, fileIndexWriter.serializedBytes());
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
                return;
            }
            InternalMap internalMap = row.getMap(position);
            InternalArray keyArray = internalMap.keyArray();
            InternalArray valueArray = internalMap.valueArray();

            for (int i = 0; i < keyArray.size(); i++) {
                String key = keyArray.getString(i).toString();
                org.apache.paimon.fileindex.FileIndexWriter writer =
                        indexWritersMap.getOrDefault(key, null);
                if (writer != null) {
                    writer.writeRecord(valueElementGetter.getElementOrNull(valueArray, i));
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

        public String getIndexType() {
            return indexType;
        }

        public Map<String, byte[]> serializedBytes() {
            Map<String, byte[]> result = new HashMap<>();
            indexWritersMap.forEach(
                    (k, v) -> {
                        if (!v.empty()) {
                            result.put(
                                    FileIndexCommon.toMapKey(columnName, k), v.serializedBytes());
                        } else {
                            result.put(FileIndexCommon.toMapKey(columnName, k), null);
                        }
                    });
            return result;
        }
    }
}
