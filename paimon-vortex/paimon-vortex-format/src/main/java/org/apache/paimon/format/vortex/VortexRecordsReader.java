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

package org.apache.paimon.format.vortex;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.arrow.reader.ArrowBatchReader;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.LongIterator;
import org.apache.paimon.utils.ProjectedRow;

import dev.vortex.api.DataSource;
import dev.vortex.api.Expression;
import dev.vortex.api.ImmutableScanOptions;
import dev.vortex.api.Partition;
import dev.vortex.api.Scan;
import dev.vortex.api.ScanOptions;
import dev.vortex.api.Session;
import dev.vortex.arrow.ArrowAllocation;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** File reader for Vortex format. */
public class VortexRecordsReader implements FileRecordReader<InternalRow> {

    private final ArrowBatchReader arrowBatchReader;
    private final Path filePath;
    private final BufferAllocator allocator;
    private final Session session;
    private final DataSource dataSource;
    private final Scan scan;
    private final LongIterator positionIterator;
    @Nullable private final int[] physicalFieldMapping;
    private ArrowReader currentArrowReader;
    private Partition currentPartition;
    private long returnedPosition = -1;

    public VortexRecordsReader(
            Path path,
            RowType dataSchemaRowType,
            RowType projectedRowType,
            @Nullable long[] rowIndices,
            @Nullable Expression predicate,
            Map<String, String> storageOptions) {
        this.filePath = path;
        RowType physicalReadRowType = physicalReadRowType(dataSchemaRowType, projectedRowType);
        this.physicalFieldMapping = physicalFieldMapping(physicalReadRowType, projectedRowType);
        this.allocator =
                ArrowAllocation.rootAllocator()
                        .newChildAllocator("vortex-reader", 0, Long.MAX_VALUE);

        try {
            this.session = Session.create();
            try {
                this.dataSource = DataSource.open(session, path.toUri().toString(), storageOptions);
                try {
                    ImmutableScanOptions.Builder scanBuilder = ImmutableScanOptions.builder();

                    java.util.List<String> columns = physicalReadRowType.getFieldNames();
                    scanBuilder.projection(
                            Expression.select(columns.toArray(new String[0]), Expression.root()));

                    if (rowIndices != null) {
                        scanBuilder.selectionIndices(rowIndices);
                        scanBuilder.selectionMode(ScanOptions.SelectionMode.INCLUDE);
                    }
                    if (predicate != null) {
                        scanBuilder.filter(predicate);
                    }
                    this.scan = dataSource.scan(scanBuilder.build());
                } catch (Exception e) {
                    dataSource.close();
                    throw e;
                }
            } catch (Exception e) {
                session.close();
                throw e;
            }
        } catch (Exception e) {
            allocator.close();
            throw new RuntimeException(e);
        }

        this.arrowBatchReader = new ArrowBatchReader(physicalReadRowType, true);
        long totalRowCount = dataSource.rowCount();
        this.positionIterator =
                rowIndices != null
                        ? LongIterator.fromArray(rowIndices)
                        : LongIterator.fromRange(0, totalRowCount > 0 ? totalRowCount : 0);
    }

    @Nullable
    @Override
    public FileRecordIterator<InternalRow> readBatch() throws IOException {
        // Try to load the next batch from the current ArrowReader
        if (currentArrowReader != null && currentArrowReader.loadNextBatch()) {
            return toBatchIterator(currentArrowReader.getVectorSchemaRoot());
        }

        // Close current reader and move to the next partition
        closeCurrentArrowReader();

        while (scan.hasNext()) {
            closeCurrentPartition();
            currentPartition = scan.next();
            currentArrowReader = currentPartition.scanArrow(allocator);
            if (currentArrowReader.loadNextBatch()) {
                return toBatchIterator(currentArrowReader.getVectorSchemaRoot());
            }
            closeCurrentArrowReader();
        }

        return null;
    }

    private FileRecordIterator<InternalRow> toBatchIterator(VectorSchemaRoot vsr) {
        Iterator<InternalRow> rows = arrowBatchReader.readBatch(vsr).iterator();
        ProjectedRow projectedRow =
                physicalFieldMapping == null ? null : ProjectedRow.from(physicalFieldMapping);

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
                if (!rows.hasNext()) {
                    return null;
                }
                returnedPosition = positionIterator.next();
                InternalRow row = rows.next();
                return projectedRow == null ? row : projectedRow.replaceRow(row);
            }

            @Override
            public void releaseBatch() {}
        };
    }

    private void closeCurrentArrowReader() {
        if (currentArrowReader != null) {
            try {
                currentArrowReader.close();
            } catch (IOException e) {
                // ignore
            }
            currentArrowReader = null;
        }
    }

    private void closeCurrentPartition() {
        if (currentPartition != null) {
            currentPartition.close();
            currentPartition = null;
        }
    }

    @Override
    public void close() {
        closeCurrentArrowReader();
        closeCurrentPartition();
        scan.close();
        dataSource.close();
        session.close();
        allocator.close();
    }

    @VisibleForTesting
    static RowType physicalReadRowType(RowType dataSchemaRowType, RowType projectedRowType) {
        if (!hasRowTrackingField(projectedRowType)) {
            return projectedRowType;
        }

        List<DataField> fields = new ArrayList<>();
        Set<Integer> selectedFieldIds = new HashSet<>();
        Set<String> selectedFieldNames = new HashSet<>();
        for (DataField projectedField : projectedRowType.getFields()) {
            if (isRowTrackingField(projectedField)) {
                continue;
            }

            DataField physicalField = physicalDataField(dataSchemaRowType, projectedField);
            if (physicalField == null
                    || selectedFieldIds.contains(physicalField.id())
                    || selectedFieldNames.contains(physicalField.name())) {
                continue;
            }
            fields.add(physicalField);
            selectedFieldIds.add(physicalField.id());
            selectedFieldNames.add(physicalField.name());
        }
        return new RowType(fields);
    }

    @Nullable
    @VisibleForTesting
    static int[] physicalFieldMapping(RowType physicalReadRowType, RowType projectedRowType) {
        if (!hasRowTrackingField(projectedRowType)) {
            return null;
        }

        int[] mapping = new int[projectedRowType.getFieldCount()];
        for (int i = 0; i < projectedRowType.getFieldCount(); i++) {
            DataField field = projectedRowType.getFields().get(i);
            if (isRowTrackingField(field)) {
                mapping[i] = -1;
            } else {
                if (physicalReadRowType.containsField(field.id())) {
                    mapping[i] = physicalReadRowType.getFieldIndexByFieldId(field.id());
                } else {
                    mapping[i] = physicalReadRowType.getFieldIndex(field.name());
                    if (mapping[i] < 0) {
                        throw new RuntimeException(
                                String.format(
                                        "Cannot find physical field for projected field '%s' with id %s.",
                                        field.name(), field.id()));
                    }
                }
            }
        }
        return mapping;
    }

    @Nullable
    private static DataField physicalDataField(
            RowType dataSchemaRowType, DataField projectedField) {
        if (dataSchemaRowType.containsField(projectedField.id())) {
            return dataSchemaRowType.getField(projectedField.id());
        }
        if (dataSchemaRowType.containsField(projectedField.name())) {
            return dataSchemaRowType.getField(projectedField.name());
        }
        return null;
    }

    private static boolean hasRowTrackingField(RowType rowType) {
        for (DataField field : rowType.getFields()) {
            if (isRowTrackingField(field)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isRowTrackingField(DataField field) {
        return SpecialFields.ROW_ID.name().equals(field.name())
                || SpecialFields.SEQUENCE_NUMBER.name().equals(field.name());
    }
}
