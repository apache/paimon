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

package org.apache.paimon.index;

import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.DeletionFileKey;
import org.apache.paimon.utils.ObjectSerializer;
import org.apache.paimon.utils.VersionedObjectSerializer;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.paimon.data.BinaryString.fromString;
import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;
import static org.apache.paimon.utils.Preconditions.checkState;

/** A {@link VersionedObjectSerializer} for {@link IndexFileMeta}. */
public class IndexFileMetaSerializer extends ObjectSerializer<IndexFileMeta> {

    public IndexFileMetaSerializer() {
        super(IndexFileMeta.SCHEMA);
    }

    @Override
    public InternalRow toRow(IndexFileMeta record) {
        GlobalIndexMeta globalIndexMeta = record.globalIndexMeta();
        InternalRow globalIndexRow =
                globalIndexMeta == null
                        ? null
                        : GenericRow.of(
                                globalIndexMeta.rowRangeStart(),
                                globalIndexMeta.rowRangeEnd(),
                                globalIndexMeta.indexFieldId(),
                                globalIndexMeta.extraFieldIds() == null
                                        ? null
                                        : new GenericArray(globalIndexMeta.extraFieldIds()),
                                globalIndexMeta.indexMeta());
        LinkedHashMap<DeletionFileKey, DeletionVectorMeta> dvRanges = record.dvRanges();

        return GenericRow.of(
                fromString(record.indexType()),
                fromString(record.fileName()),
                record.fileSize(),
                record.rowCount(),
                metasToRowArrayData(dvRanges, DeletionFileKey.Type.FILE_NAME),
                fromString(record.externalPath()),
                globalIndexRow,
                metasToRowArrayData(dvRanges, DeletionFileKey.Type.ROW_RANGE));
    }

    @Override
    public IndexFileMeta fromRow(InternalRow row) {
        GlobalIndexMeta globalIndexMeta = null;
        if (!row.isNullAt(6)) {
            InternalRow globalIndexRow = row.getRow(6, 5);
            Long rowRangeStart = globalIndexRow.getLong(0);
            Long rowRangeEnd = globalIndexRow.getLong(1);
            Integer indexFieldId = globalIndexRow.getInt(2);
            int[] extralFields =
                    globalIndexRow.isNullAt(3) ? null : globalIndexRow.getArray(3).toIntArray();
            byte[] indexMeta = globalIndexRow.isNullAt(4) ? null : globalIndexRow.getBinary(4);
            globalIndexMeta =
                    new GlobalIndexMeta(
                            rowRangeStart, rowRangeEnd, indexFieldId, extralFields, indexMeta);
        }
        return new IndexFileMeta(
                row.getString(0).toString(),
                row.getString(1).toString(),
                row.getLong(2),
                row.getLong(3),
                readDvRanges(row),
                row.isNullAt(5) ? null : row.getString(5).toString(),
                globalIndexMeta);
    }

    // ----------------------- Write methods -------------------------------

    /**
     * Serialize the dvMetas to an GenericArray. Note that we set an invalid marker row to
     * fileNameDv field if current rowRangeDv is not empty. This fast-fail path can prevent older
     * sdk silently reading deleted records for data evolution tables.
     */
    public static InternalArray metasToRowArrayData(
            Map<DeletionFileKey, DeletionVectorMeta> dvMetas, DeletionFileKey.Type type) {
        if (dvMetas == null || dvMetas.isEmpty()) {
            return null;
        }

        List<GenericRow> rows;

        if (DeletionFileKey.checkType(dvMetas.keySet()) != type) {
            if (type == DeletionFileKey.Type.FILE_NAME) {
                // If dvRanges are row-range keyed, set a legacy marker row
                rows = Collections.singletonList(DeletionVectorMeta.newLegacyMarkerRow());
            } else {
                return null;
            }
        } else {
            rows =
                    dvMetas.values().stream()
                            .map(DeletionVectorMeta::toRow)
                            .collect(Collectors.toList());
        }

        return new GenericArray(rows.toArray(new GenericRow[0]));
    }

    // ------------------------ Read methods -------------------------------

    public static LinkedHashMap<DeletionFileKey, DeletionVectorMeta> rowArrayDataToFileNameDvMetas(
            InternalArray arrayData) {
        return rowArrayDataToDvMetas(
                arrayData,
                DeletionFileKey.Type.FILE_NAME,
                DeletionVectorMeta.SCHEMA.getFieldCount());
    }

    public static LinkedHashMap<DeletionFileKey, DeletionVectorMeta>
            rowArrayDataToRowIdRangeDvMetas(InternalArray arrayData) {
        return rowArrayDataToDvMetas(
                arrayData,
                DeletionFileKey.Type.ROW_RANGE,
                DeletionVectorMeta.ROW_ID_RANGE_SCHEMA.getFieldCount());
    }

    private static LinkedHashMap<DeletionFileKey, DeletionVectorMeta> rowArrayDataToDvMetas(
            InternalArray arrayData, DeletionFileKey.Type keyType, int fieldCount) {
        LinkedHashMap<DeletionFileKey, DeletionVectorMeta> dvMetas =
                new LinkedHashMap<>(arrayData.size());
        for (int i = 0; i < arrayData.size(); i++) {
            if (arrayData.isNullAt(i)) {
                continue;
            }
            DeletionVectorMeta dvMeta =
                    DeletionVectorMeta.fromRow(keyType, arrayData.getRow(i, fieldCount));
            dvMetas.put(dvMeta.key(), dvMeta);
        }
        return dvMetas;
    }

    private static LinkedHashMap<DeletionFileKey, DeletionVectorMeta> readDvRanges(
            InternalRow row) {
        InternalArray fileNameDvRanges = row.isNullAt(4) ? null : row.getArray(4);
        InternalArray rowRangeDvRanges =
                row.getFieldCount() > 7 && !row.isNullAt(7) ? row.getArray(7) : null;
        return readDvRanges(
                row.getString(0).toString(), row.getLong(3), fileNameDvRanges, rowRangeDvRanges);
    }

    public static LinkedHashMap<DeletionFileKey, DeletionVectorMeta> readDvRanges(
            String indexType,
            long rowCount,
            @Nullable InternalArray fileNameDvRanges,
            @Nullable InternalArray rowRangeDvRanges) {
        boolean hasFileNameDvRanges =
                fileNameDvRanges != null && !DeletionVectorMeta.isLegacyMarker(fileNameDvRanges);
        boolean hasRowRangeDvRanges = rowRangeDvRanges != null;
        checkState(
                !(hasFileNameDvRanges && hasRowRangeDvRanges),
                "File-name deletion vector ranges and row-range deletion vector ranges should not"
                        + " be both non-null.");
        if (hasFileNameDvRanges) {
            return rowArrayDataToFileNameDvMetas(fileNameDvRanges);
        } else if (hasRowRangeDvRanges) {
            return rowArrayDataToRowIdRangeDvMetas(rowRangeDvRanges);
        }

        if (!DELETION_VECTORS_INDEX.equals(indexType)) {
            return null;
        }

        checkState(
                rowCount == 0,
                "Invalid state, all null dvRanges with non-zero row count: " + rowCount);

        return new LinkedHashMap<>();
    }
}
