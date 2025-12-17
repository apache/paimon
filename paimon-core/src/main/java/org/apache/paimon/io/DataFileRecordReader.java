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

import org.apache.paimon.PartitionSettedRow;
import org.apache.paimon.casting.CastFieldGetter;
import org.apache.paimon.casting.CastedRow;
import org.apache.paimon.casting.FallbackMappingRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.PartitionInfo;
import org.apache.paimon.data.columnar.ColumnarRowIterator;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileUtils;
import org.apache.paimon.utils.ProjectedRow;
import org.apache.paimon.utils.RoaringBitmap32;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/** Reads {@link InternalRow} from data files. */
public class DataFileRecordReader implements FileRecordReader<InternalRow> {

    private final RowType tableRowType;
    private final FileRecordReader<InternalRow> reader;
    @Nullable private final int[] indexMapping;
    @Nullable private final PartitionInfo partitionInfo;
    @Nullable private final CastFieldGetter[] castMapping;
    private final boolean rowTrackingEnabled;
    @Nullable private final Long firstRowId;
    private final long maxSequenceNumber;
    private final Map<String, Integer> systemFields;
    @Nullable private final RoaringBitmap32 selection;

    public DataFileRecordReader(
            RowType tableRowType,
            FormatReaderFactory readerFactory,
            FormatReaderFactory.Context context,
            @Nullable int[] indexMapping,
            @Nullable CastFieldGetter[] castMapping,
            @Nullable PartitionInfo partitionInfo,
            boolean rowTrackingEnabled,
            @Nullable Long firstRowId,
            long maxSequenceNumber,
            Map<String, Integer> systemFields)
            throws IOException {
        this(
                tableRowType,
                createReader(readerFactory, context),
                indexMapping,
                castMapping,
                partitionInfo,
                rowTrackingEnabled,
                firstRowId,
                maxSequenceNumber,
                systemFields,
                context.selection());
    }

    public DataFileRecordReader(
            RowType tableRowType,
            FileRecordReader<InternalRow> reader,
            @Nullable int[] indexMapping,
            @Nullable CastFieldGetter[] castMapping,
            @Nullable PartitionInfo partitionInfo,
            boolean rowTrackingEnabled,
            @Nullable Long firstRowId,
            long maxSequenceNumber,
            Map<String, Integer> systemFields,
            @Nullable RoaringBitmap32 selection) {
        this.tableRowType = tableRowType;
        this.reader = reader;
        this.indexMapping = indexMapping;
        this.partitionInfo = partitionInfo;
        this.castMapping = castMapping;
        this.rowTrackingEnabled = rowTrackingEnabled;
        this.firstRowId = firstRowId;
        this.maxSequenceNumber = maxSequenceNumber;
        this.systemFields = systemFields;
        this.selection = selection;
    }

    private static FileRecordReader<InternalRow> createReader(
            FormatReaderFactory readerFactory, FormatReaderFactory.Context context)
            throws IOException {
        try {
            return readerFactory.createReader(context);
        } catch (Exception e) {
            FileUtils.checkExists(context.fileIO(), context.filePath());
            throw e;
        }
    }

    @Nullable
    @Override
    public FileRecordIterator<InternalRow> readBatch() throws IOException {
        FileRecordIterator<InternalRow> iterator = reader.readBatch();
        if (iterator == null) {
            return null;
        }

        if (iterator instanceof ColumnarRowIterator) {
            iterator = ((ColumnarRowIterator) iterator).mapping(partitionInfo, indexMapping);
            if (rowTrackingEnabled) {
                iterator =
                        ((ColumnarRowIterator) iterator)
                                .assignRowTracking(firstRowId, maxSequenceNumber, systemFields);
            }
        } else {
            if (partitionInfo != null) {
                final PartitionSettedRow partitionSettedRow =
                        PartitionSettedRow.from(partitionInfo);
                iterator = iterator.transform(partitionSettedRow::replaceRow);
            }

            if (indexMapping != null) {
                final ProjectedRow projectedRow = ProjectedRow.from(indexMapping);
                iterator = iterator.transform(projectedRow::replaceRow);
            }

            if (rowTrackingEnabled && !systemFields.isEmpty()) {
                GenericRow trackingRow = new GenericRow(2);

                int[] fallbackToTrackingMappings = new int[tableRowType.getFieldCount()];
                Arrays.fill(fallbackToTrackingMappings, -1);

                if (systemFields.containsKey(SpecialFields.ROW_ID.name())) {
                    fallbackToTrackingMappings[systemFields.get(SpecialFields.ROW_ID.name())] = 0;
                }
                if (systemFields.containsKey(SpecialFields.SEQUENCE_NUMBER.name())) {
                    fallbackToTrackingMappings[
                                    systemFields.get(SpecialFields.SEQUENCE_NUMBER.name())] =
                            1;
                }

                FallbackMappingRow fallbackMappingRow =
                        new FallbackMappingRow(fallbackToTrackingMappings);
                final FileRecordIterator<InternalRow> iteratorInner = iterator;
                iterator =
                        iterator.transform(
                                row -> {
                                    if (firstRowId != null) {
                                        trackingRow.setField(
                                                0, iteratorInner.returnedPosition() + firstRowId);
                                    }
                                    trackingRow.setField(1, maxSequenceNumber);
                                    return fallbackMappingRow.replace(row, trackingRow);
                                });
            }
        }

        if (castMapping != null) {
            final CastedRow castedRow = CastedRow.from(castMapping);
            iterator = iterator.transform(castedRow::replaceRow);
        }

        if (selection != null) {
            iterator = iterator.selection(selection);
        }

        return iterator;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
