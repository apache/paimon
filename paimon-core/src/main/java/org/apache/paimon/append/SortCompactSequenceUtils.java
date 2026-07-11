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

package org.apache.paimon.append;

import org.apache.paimon.KeyValue;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.table.SpecialFields;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.OffsetRow;

import java.util.ArrayList;
import java.util.List;

/** Utilities for preserving key-value sequence numbers during sort compact. */
public class SortCompactSequenceUtils {

    private SortCompactSequenceUtils() {}

    /**
     * Row type with {@link SpecialFields#SEQUENCE_NUMBER} prepended for sort compact read/write.
     */
    public static RowType rowTypeWithKeyValueSequenceNumber(RowType rowType) {
        List<DataField> fields = new ArrayList<>(rowType.getFieldCount() + 1);
        fields.add(SpecialFields.SEQUENCE_NUMBER);
        fields.addAll(rowType.getFields());
        return new RowType(fields);
    }

    public static boolean needsSequencePreservation(RowType logicalRowType, InternalRow row) {
        return sequenceNumber(row, logicalRowType.getFieldCount()) != KeyValue.UNKNOWN_SEQUENCE;
    }

    public static long sequenceNumber(InternalRow row, int logicalFieldCount) {
        if (row instanceof JoinedRow) {
            JoinedRow joinedRow = (JoinedRow) row;
            return joinedRow.row1().getLong(0);
        }
        if (row.getFieldCount() == logicalFieldCount + 1) {
            return row.getLong(0);
        }
        return KeyValue.UNKNOWN_SEQUENCE;
    }

    public static InternalRow valueRow(InternalRow row, int logicalFieldCount) {
        if (row instanceof JoinedRow) {
            return ((JoinedRow) row).row2();
        }
        if (row.getFieldCount() == logicalFieldCount + 1) {
            return new OffsetRow(logicalFieldCount, 1).replace(row);
        }
        return row;
    }

    public static TableWriteImpl.RecordExtractor<KeyValue> sequencePreservingExtractor(
            RowType logicalRowType, KeyValue reuse) {
        int logicalFieldCount = logicalRowType.getFieldCount();
        return (record, rowKind) -> {
            InternalRow row = record.row();
            long sequenceNumber = sequenceNumber(row, logicalFieldCount);
            InternalRow valueRow = valueRow(row, logicalFieldCount);
            return reuse.replace(record.primaryKey(), sequenceNumber, rowKind, valueRow);
        };
    }

    public static DataFileMeta asCompactOutput(
            DataFileMeta file, long minSequenceNumber, long maxSequenceNumber) {
        return file.assignSequenceNumber(minSequenceNumber, maxSequenceNumber)
                .assignFileSource(FileSource.COMPACT);
    }

    public static long minSequenceNumber(List<DataFileMeta> files) {
        return files.stream()
                .mapToLong(DataFileMeta::minSequenceNumber)
                .min()
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "Cannot get min sequence number from compact before files."));
    }

    public static long maxSequenceNumber(List<DataFileMeta> files) {
        return files.stream()
                .mapToLong(DataFileMeta::maxSequenceNumber)
                .max()
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "Cannot get max sequence number from compact before files."));
    }
}
