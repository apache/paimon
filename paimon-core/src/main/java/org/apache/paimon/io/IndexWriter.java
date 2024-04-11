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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fileindex.FileIndexFormat;
import org.apache.paimon.fileindex.FileIndexer;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Index file writer. */
public final class IndexWriter {

    private static final Pair<BinaryRow, List<String>> EMPTY_RESULT =
            Pair.of(BinaryRow.EMPTY_ROW, Collections.emptyList());

    private final FileIO fileIO;

    private final DataFilePathFactory pathFactory;

    // if the filter size greater than indexSizeInMeta, we put it in file
    private final long indexSizeInMeta;

    private final List<IndexMaintainer> indexMaintainers = new ArrayList<>();

    private final List<String> resultFileNames = new ArrayList<>();

    private BinaryRow resultRow = BinaryRow.EMPTY_ROW;

    public IndexWriter(
            FileIO fileIO,
            DataFilePathFactory pathFactory,
            RowType rowType,
            Map<String, Map<String, Options>> fileIndexes,
            long indexSizeInMeta) {
        this.fileIO = fileIO;
        this.pathFactory = pathFactory;
        this.indexSizeInMeta = indexSizeInMeta;
        List<DataField> fields = rowType.getFields();
        Map<String, DataField> map = new HashMap<>();
        Map<String, Integer> index = new HashMap<>();
        fields.forEach(
                dataField -> {
                    map.put(dataField.name(), dataField);
                    index.put(dataField.name(), rowType.getFieldIndex(dataField.name()));
                });
        for (Map.Entry<String, Map<String, Options>> entry : fileIndexes.entrySet()) {
            String columnName = entry.getKey();
            DataField field = map.get(columnName);
            if (field == null) {
                throw new IllegalArgumentException(columnName + " does not exist in column fields");
            }
            for (Map.Entry<String, Options> typeEntry : entry.getValue().entrySet()) {
                String indexType = typeEntry.getKey();
                indexMaintainers.add(
                        new IndexMaintainer(
                                columnName,
                                indexType,
                                FileIndexer.create(indexType, field.type(), typeEntry.getValue())
                                        .createWriter(),
                                InternalRow.createFieldGetter(
                                        field.type(), index.get(columnName))));
            }
        }
    }

    public void write(InternalRow row) {
        indexMaintainers.forEach(indexMaintainer -> indexMaintainer.write(row));
    }

    public void close() throws IOException {
        if (indexMaintainers.isEmpty()) {
            return;
        }

        Map<String, Map<String, byte[]>> indexMaps = new HashMap<>();

        for (IndexMaintainer indexMaintainer : indexMaintainers) {
            indexMaps
                    .computeIfAbsent(indexMaintainer.getColumnName(), k -> new HashMap<>())
                    .put(indexMaintainer.getIndexType(), indexMaintainer.serializedBytes());
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (FileIndexFormat.Writer writer = FileIndexFormat.createWriter(baos)) {
            writer.writeColumnIndexes(indexMaps);
        }

        if (baos.size() > indexSizeInMeta || resultRow != BinaryRow.EMPTY_ROW) {
            Path path = pathFactory.newIndexPath();
            try (OutputStream outputStream = fileIO.newOutputStream(path, false)) {
                outputStream.write(baos.toByteArray());
            }
            resultFileNames.add(path.getName());
        } else {
            resultRow = new BinaryRow(1);
            BinaryRowWriter binaryRowWriter = new BinaryRowWriter(resultRow);
            binaryRowWriter.writeBinary(0, baos.toByteArray());
            binaryRowWriter.complete();
        }
    }

    public Pair<BinaryRow, List<String>> result() {
        return indexMaintainers.isEmpty() ? EMPTY_RESULT : Pair.of(resultRow, resultFileNames);
    }
}
