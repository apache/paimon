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
import org.apache.paimon.filter.FilterInterface;
import org.apache.paimon.filter.IndexMaintainer;
import org.apache.paimon.filter.InternalRowToBytesVisitor;
import org.apache.paimon.filter.PredicateFilterUtil;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Index file writer. */
public final class IndexWriter {

    private static final Pair<BinaryRow, String> EMPTY_RESULT = Pair.of(BinaryRow.EMPTY_ROW, null);

    private final FileIO fileIO;

    private final DataFilePathFactory pathFactory;

    private final String indexType;

    // if the filter size greater than indexSizeInMeta, we put it in file
    private final long indexSizeInMeta;

    private final List<IndexMaintainer> indexMaintainers = new ArrayList<>();

    private BinaryRow resultRow;
    private String resultFileName;

    public IndexWriter(
            FileIO fileIO,
            DataFilePathFactory pathFactory,
            RowType rowType,
            List<String> indexColumns,
            String indexType,
            long indexSizeInMeta) {
        this.fileIO = fileIO;
        this.pathFactory = pathFactory;
        this.indexType = indexType;
        this.indexSizeInMeta = indexSizeInMeta;
        ArrayList<String> columns = new ArrayList<>(indexColumns);
        List<DataField> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            DataField field = fields.get(i);
            if (columns.contains(field.name())) {
                indexMaintainers.add(
                        new IndexMaintainer(
                                field.name(),
                                i,
                                FilterInterface.getFilter(indexType),
                                field.type().accept(InternalRowToBytesVisitor.INSTANCE)));
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
        byte[] serializedBytes = getSerializedBytes();

        if (serializedBytes.length > indexSizeInMeta) {
            resultRow = BinaryRow.EMPTY_ROW;
            Path path = pathFactory.newIndexPath();
            try (OutputStream outputStream = fileIO.newOutputStream(path, false)) {
                outputStream.write(serializedBytes);
            }
            resultFileName = path.getName();
        } else {
            resultRow = new BinaryRow(1);
            BinaryRowWriter binaryRowWriter = new BinaryRowWriter(resultRow);
            binaryRowWriter.writeBinary(0, serializedBytes);
            binaryRowWriter.complete();
        }
    }

    public Pair<BinaryRow, String> result() {
        return indexMaintainers.isEmpty() ? EMPTY_RESULT : Pair.of(resultRow, resultFileName);
    }

    public byte[] getSerializedBytes() {
        Map<String, byte[]> indexMap = new HashMap<>();

        for (IndexMaintainer indexMaintainer : indexMaintainers) {
            indexMap.put(indexMaintainer.getColumnName(), indexMaintainer.serializedBytes());
        }

        return PredicateFilterUtil.serializeIndexMap(indexType, indexMap);
    }
}
