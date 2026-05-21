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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.format.mosaic.MosaicObjects.convertStatsValue;

/** File reader for Mosaic format. */
public class MosaicRecordsReader implements FileRecordReader<InternalRow> {

    private final MosaicInputFileAdapter inputFileAdapter;
    private final MosaicReader reader;
    private final ArrowBatchReader arrowBatchReader;
    private final Path filePath;
    private final BufferAllocator allocator;
    private final int numRowGroups;
    private final RowType dataSchemaRowType;
    @Nullable private final List<Predicate> predicates;

    private int currentRowGroup;
    private long returnedPosition = -1;
    private VectorSchemaRoot currentVsr;

    public MosaicRecordsReader(
            MosaicInputFileAdapter inputFileAdapter,
            long fileSize,
            RowType dataSchemaRowType,
            RowType projectedRowType,
            @Nullable List<Predicate> predicates,
            Path filePath) {
        this.filePath = filePath;
        this.inputFileAdapter = inputFileAdapter;
        this.dataSchemaRowType = dataSchemaRowType;
        this.predicates = predicates;
        this.allocator = new RootAllocator();

        try {
            this.reader = MosaicReader.open(inputFileAdapter, fileSize, allocator);
        } catch (Exception e) {
            allocator.close();
            throw e;
        }

        Schema fileSchema = reader.getSchema();
        Set<String> fileColumnNames = new HashSet<>();
        for (Field field : fileSchema.getFields()) {
            fileColumnNames.add(field.getName());
        }
        List<String> projectedNames = projectedRowType.getFieldNames();
        List<String> existingColumns = new ArrayList<>();
        for (String name : projectedNames) {
            if (fileColumnNames.contains(name)) {
                existingColumns.add(name);
            }
        }
        if (!existingColumns.isEmpty()) {
            reader.project(existingColumns.toArray(new String[0]));
        }

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

            VectorSchemaRoot vsr = reader.readRowGroup(currentRowGroup, allocator);
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

        Map<String, ColumnStatistics> statsMap = reader.getRowGroupStatistics(rowGroupIndex);
        if (statsMap.isEmpty()) {
            return true;
        }

        int fieldCount = dataSchemaRowType.getFieldCount();
        GenericRow minValues = new GenericRow(fieldCount);
        GenericRow maxValues = new GenericRow(fieldCount);
        long[] nullCounts = new long[fieldCount];

        List<DataField> fields = dataSchemaRowType.getFields();
        for (int i = 0; i < fieldCount; i++) {
            String colName = fields.get(i).name();
            ColumnStatistics stats = statsMap.get(colName);
            if (stats == null) {
                continue;
            }

            nullCounts[i] = stats.getNullCount();
            if (stats.hasMinMax()) {
                DataType dataType = fields.get(i).type();
                Object min = convertStatsValue(stats.getMin(), dataType);
                Object max = convertStatsValue(stats.getMax(), dataType);
                minValues.setField(i, min);
                maxValues.setField(i, max);
            }
        }

        for (Predicate predicate : predicates) {
            if (!predicate.test(rowCount, minValues, maxValues, new GenericArray(nullCounts))) {
                return false;
            }
        }
        return true;
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
        inputFileAdapter.close();
    }
}
