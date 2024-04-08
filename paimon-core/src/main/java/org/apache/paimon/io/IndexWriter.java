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

import org.apache.paimon.CoreOptions;
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
import org.apache.paimon.utils.StringUtils;

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

    private BinaryRow resultRow;
    private List<String> resultFileNames;

    public IndexWriter(
            FileIO fileIO,
            DataFilePathFactory pathFactory,
            RowType rowType,
            CoreOptions coreOptions) {
        this.fileIO = fileIO;
        this.pathFactory = pathFactory;
        this.indexSizeInMeta = coreOptions.indexSizeInMeta();
        List<String> indexColumns = coreOptions.indexColumns();
        List<DataField> fields = rowType.getFields();
        Map<String, DataField> map = new HashMap<>();
        Map<String, Integer> index = new HashMap<>();
        fields.forEach(
                dataField -> {
                    map.put(dataField.name(), dataField);
                    index.put(dataField.name(), rowType.getFieldIndex(dataField.name()));
                });
        for (String columnExpr : indexColumns) {
            String[] expr = columnExpr.trim().split(":");

            if (expr.length != 3) {
                throw new IllegalArgumentException(
                        "index.column should be in pattern: <columnName>:<indexType>:{op1=value1, op2=value2} , but "
                                + columnExpr
                                + " does not meet the request.");
            }

            String columnName = expr[0].trim();
            String indexType = expr[1].trim();
            String opString = expr[2].trim();

            String[] ops = opString.substring(1, opString.length() - 1).split(",");
            Options options = new Options();
            for (String op : ops) {
                if (!StringUtils.isBlank(op)) {
                    String[] kv = op.split("=");
                    if (kv.length != 2) {
                        throw new IllegalArgumentException(
                                "Options in index.columns should be in pattern key=value, but "
                                        + op
                                        + " does not meet the request.");
                    }
                    options.set(kv[0].trim(), kv[1].trim());
                }
            }

            DataField field = map.get(columnName);
            if (field == null) {
                throw new IllegalArgumentException(columnExpr + " does not exist in column fields");
            }
            indexMaintainers.add(
                    new IndexMaintainer(
                            columnName,
                            indexType,
                            FileIndexer.create(indexType, field.type(), options).createWriter(),
                            InternalRow.createFieldGetter(field.type(), index.get(columnName))));
        }

        this.resultRow = BinaryRow.EMPTY_ROW;
        this.resultFileNames = new ArrayList<>();
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
                    .computeIfAbsent(indexMaintainer.getIndexType(), k -> new HashMap<>())
                    .put(indexMaintainer.getColumnName(), indexMaintainer.serializedBytes());
        }

        for (Map.Entry<String, Map<String, byte[]>> entry : indexMaps.entrySet()) {

            String indexType = entry.getKey();
            Map<String, byte[]> indexMap = entry.getValue();

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (FileIndexFormat.Writer writer = FileIndexFormat.createWriter(baos)) {
                writer.writeColumnIndex(indexType, indexMap);
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
    }

    public Pair<BinaryRow, List<String>> result() {
        return indexMaintainers.isEmpty() ? EMPTY_RESULT : Pair.of(resultRow, resultFileNames);
    }
}
