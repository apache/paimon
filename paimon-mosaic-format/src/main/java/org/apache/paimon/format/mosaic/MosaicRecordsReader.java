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

package org.apache.paimon.format.mosaic;

import org.apache.paimon.arrow.reader.ArrowBatchReader;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.mosaic.ColumnStatistics;
import org.apache.paimon.mosaic.InputFile;
import org.apache.paimon.mosaic.MosaicReader;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static org.apache.paimon.format.mosaic.MosaicObjects.convertStatsValue;

/** File reader for Mosaic format. */
public class MosaicRecordsReader implements FileRecordReader<InternalRow> {

    private final MosaicReader reader;
    private final ArrowBatchReader arrowBatchReader;
    private final int[] columnIndices;
    private final Path filePath;
    private final BufferAllocator allocator;
    private final int numRowGroups;
    private final RowType dataSchemaRowType;
    private final Schema fileSchema;
    @Nullable private final List<Predicate> predicates;

    private int currentRowGroup;
    private long returnedPosition = -1;
    private VectorSchemaRoot currentVsr;

    public MosaicRecordsReader(
            InputFile inputFile,
            long fileSize,
            RowType dataSchemaRowType,
            RowType projectedRowType,
            @Nullable List<Predicate> predicates,
            Path filePath) {
        this.filePath = filePath;
        this.dataSchemaRowType = dataSchemaRowType;
        this.predicates = predicates;
        this.allocator = new RootAllocator();

        try {
            this.reader = MosaicReader.open(inputFile, fileSize, allocator);
        } catch (Exception e) {
            allocator.close();
            throw e;
        }

        this.fileSchema = reader.getSchema();
        this.columnIndices = computeColumnIndices(fileSchema, projectedRowType);
        this.numRowGroups = reader.numRowGroups();
        this.currentRowGroup = 0;
        this.arrowBatchReader = new ArrowBatchReader(projectedRowType, true);
    }

    @Nullable
    @Override
    public FileRecordIterator<InternalRow> readBatch() throws IOException {
        while (currentRowGroup < numRowGroups) {
            int numRows = reader.rowGroupNumRows(currentRowGroup);
            if (!matchesRowGroup(currentRowGroup, numRows)) {
                returnedPosition += numRows;
                currentRowGroup++;
                continue;
            }

            releaseCurrentVsr();

            VectorSchemaRoot vsr;
            if (columnIndices != null) {
                vsr = reader.readRowGroup(currentRowGroup, columnIndices, allocator);
            } else {
                vsr = reader.readRowGroup(currentRowGroup, allocator);
            }
            currentRowGroup++;
            this.currentVsr = vsr;

            Iterator<InternalRow> rows = arrowBatchReader.readBatch(vsr).iterator();

            return new FileRecordIterator<InternalRow>() {
                @Override
                public long returnedPosition() {
                    return returnedPosition;
                }

                @Override
                public Path filePath() {
                    return filePath;
                }

                @Nullable
                @Override
                public InternalRow next() {
                    if (rows.hasNext()) {
                        returnedPosition++;
                        return rows.next();
                    }
                    return null;
                }

                @Override
                public void releaseBatch() {
                    releaseCurrentVsr();
                }
            };
        }
        return null;
    }

    private boolean matchesRowGroup(int rowGroupIndex, long rowCount) {
        if (predicates == null || predicates.isEmpty()) {
            return true;
        }

        List<ColumnStatistics> statsList = reader.getRowGroupStatistics(rowGroupIndex);
        if (statsList.isEmpty()) {
            return true;
        }

        int fieldCount = dataSchemaRowType.getFieldCount();
        GenericRow minValues = new GenericRow(fieldCount);
        GenericRow maxValues = new GenericRow(fieldCount);
        long[] nullCounts = new long[fieldCount];

        for (ColumnStatistics stats : statsList) {
            int fileColIdx = stats.getColumnIndex();
            if (fileColIdx < 0 || fileColIdx >= fileSchema.getFields().size()) {
                continue;
            }
            String colName = fileSchema.getFields().get(fileColIdx).getName();
            int schemaIdx = findFieldIndex(dataSchemaRowType, colName);
            if (schemaIdx < 0) {
                continue;
            }

            nullCounts[schemaIdx] = stats.getNullCount();
            if (stats.hasMinMax()) {
                DataType dataType = dataSchemaRowType.getFields().get(schemaIdx).type();
                Object min = convertStatsValue(stats.getMin(), dataType);
                Object max = convertStatsValue(stats.getMax(), dataType);
                minValues.setField(schemaIdx, min);
                maxValues.setField(schemaIdx, max);
            }
        }

        for (Predicate predicate : predicates) {
            if (!predicate.test(rowCount, minValues, maxValues, new GenericArray(nullCounts))) {
                return false;
            }
        }
        return true;
    }

    private static int findFieldIndex(RowType rowType, String name) {
        List<DataField> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).name().equals(name)) {
                return i;
            }
        }
        return -1;
    }

    private void releaseCurrentVsr() {
        if (currentVsr != null) {
            currentVsr.close();
            currentVsr = null;
        }
    }

    @Override
    public void close() throws IOException {
        releaseCurrentVsr();
        reader.close();
        allocator.close();
    }

    @Nullable
    private static int[] computeColumnIndices(Schema fileSchema, RowType projectedRowType) {
        List<Field> fileFields = fileSchema.getFields();
        if (fileFields.size() == projectedRowType.getFieldCount()) {
            return null;
        }

        int[] indices = new int[projectedRowType.getFieldCount()];
        for (int i = 0; i < projectedRowType.getFieldCount(); i++) {
            String name = projectedRowType.getFields().get(i).name();
            int pos = findArrowFieldIndex(fileFields, name);
            if (pos < 0) {
                throw new IllegalArgumentException(
                        "Projected field '" + name + "' not found in Mosaic file schema");
            }
            indices[i] = pos;
        }
        return indices;
    }

    private static int findArrowFieldIndex(List<Field> fields, String name) {
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).getName().equals(name)) {
                return i;
            }
        }
        return -1;
    }
}
