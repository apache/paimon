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
    private final boolean rowLineageEnabled;
    @Nullable private final Long firstRowId;
    private final long snapshotId;
    private final Map<String, Integer> metaColumnIndex;

    public DataFileRecordReader(
            RowType tableRowType,
            FormatReaderFactory readerFactory,
            FormatReaderFactory.Context context,
            @Nullable int[] indexMapping,
            @Nullable CastFieldGetter[] castMapping,
            @Nullable PartitionInfo partitionInfo,
            boolean rowLineageEnabled,
            @Nullable Long firstRowId,
            long snapshotId,
            Map<String, Integer> metaColumnIndex)
            throws IOException {
        this.tableRowType = tableRowType;
        try {
            this.reader = readerFactory.createReader(context);
        } catch (Exception e) {
            FileUtils.checkExists(context.fileIO(), context.filePath());
            throw e;
        }
        this.indexMapping = indexMapping;
        this.partitionInfo = partitionInfo;
        this.castMapping = castMapping;
        this.rowLineageEnabled = rowLineageEnabled;
        this.firstRowId = firstRowId;
        this.snapshotId = snapshotId;
        this.metaColumnIndex = metaColumnIndex;
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
            if (rowLineageEnabled) {
                iterator =
                        ((ColumnarRowIterator) iterator)
                                .assignRowLineage(firstRowId, snapshotId, metaColumnIndex);
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

            if (rowLineageEnabled && !metaColumnIndex.isEmpty()) {
                GenericRow genericRow = new GenericRow(metaColumnIndex.size());

                int[] fallbackToMetaRowLineageMappings = new int[tableRowType.getFieldCount()];
                Arrays.fill(fallbackToMetaRowLineageMappings, -1);

                if (metaColumnIndex.containsKey(SpecialFields.ROW_ID.name())) {
                    fallbackToMetaRowLineageMappings[
                                    metaColumnIndex.get(SpecialFields.ROW_ID.name())] =
                            0;
                }
                if (metaColumnIndex.containsKey(SpecialFields.SEQUENCE_NUMBER.name())) {
                    fallbackToMetaRowLineageMappings[
                                    metaColumnIndex.get(SpecialFields.SEQUENCE_NUMBER.name())] =
                            1;
                }

                FallbackMappingRow fallbackMappingRow =
                        new FallbackMappingRow(fallbackToMetaRowLineageMappings);
                final FileRecordIterator<InternalRow> iteratorInner = iterator;
                iterator =
                        iterator.transform(
                                row -> {
                                    genericRow.setField(
                                            0, iteratorInner.returnedPosition() + firstRowId);
                                    genericRow.setField(1, snapshotId);
                                    return fallbackMappingRow.replace(row, genericRow);
                                });
            }
        }

        if (castMapping != null) {
            final CastedRow castedRow = CastedRow.from(castMapping);
            iterator = iterator.transform(castedRow::replaceRow);
        }

        return iterator;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
