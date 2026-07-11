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

import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Schema for global index. */
public class GlobalIndexMeta {

    public static final RowType SCHEMA =
            new RowType(
                    true,
                    Arrays.asList(
                            new DataField(0, "_ROW_RANGE_START", new BigIntType(false)),
                            new DataField(1, "_ROW_RANGE_END", new BigIntType(false)),
                            new DataField(2, "_INDEX_FIELD_ID", new IntType(false)),
                            new DataField(
                                    3, "_EXTRA_FIELD_IDS", DataTypes.ARRAY(new IntType(false))),
                            new DataField(4, "_INDEX_META", DataTypes.BYTES()),
                            new DataField(5, "_SOURCE_META", DataTypes.BYTES())));

    private final long rowRangeStart;
    private final long rowRangeEnd;
    private final int indexFieldId;
    @Nullable private final int[] extraFieldIds;
    @Nullable private final byte[] indexMeta;
    @Nullable private final byte[] sourceMeta;

    public GlobalIndexMeta(
            long rowRangeStart,
            long rowRangeEnd,
            int indexFieldId,
            @Nullable int[] extraFieldIds,
            @Nullable byte[] indexMeta) {
        this(rowRangeStart, rowRangeEnd, indexFieldId, extraFieldIds, indexMeta, null);
    }

    public GlobalIndexMeta(
            long rowRangeStart,
            long rowRangeEnd,
            int indexFieldId,
            @Nullable int[] extraFieldIds,
            @Nullable byte[] indexMeta,
            @Nullable byte[] sourceMeta) {
        this.rowRangeStart = rowRangeStart;
        this.rowRangeEnd = rowRangeEnd;
        this.indexFieldId = indexFieldId;
        this.extraFieldIds = extraFieldIds;
        this.indexMeta = indexMeta;
        this.sourceMeta = sourceMeta;
    }

    public long rowRangeStart() {
        return rowRangeStart;
    }

    public long rowRangeEnd() {
        return rowRangeEnd;
    }

    public Range rowRange() {
        return new Range(rowRangeStart, rowRangeEnd);
    }

    public int indexFieldId() {
        return indexFieldId;
    }

    @Nullable
    public int[] extraFieldIds() {
        return extraFieldIds;
    }

    /** Metadata produced and consumed by the global-index implementation. */
    @Nullable
    public byte[] indexMeta() {
        return indexMeta;
    }

    /** Metadata describing how index row ids map to their source data. */
    @Nullable
    public byte[] sourceMeta() {
        return sourceMeta;
    }

    /** All indexed field ids in order: the primary {@link #indexFieldId} followed by the rest. */
    public List<Integer> getIndexedFieldIds() {
        List<Integer> ids = new ArrayList<>();
        ids.add(indexFieldId);
        if (extraFieldIds != null) {
            for (int id : extraFieldIds) {
                ids.add(id);
            }
        }
        return ids;
    }

    public List<DataField> getIndexedFields(RowType rowType) {
        List<DataField> fields = new ArrayList<>();
        for (int id : getIndexedFieldIds()) {
            fields.add(rowType.getField(id));
        }
        return fields;
    }

    /** The primary index column. */
    public DataField getIndexField(RowType rowType) {
        return rowType.getField(indexFieldId);
    }

    /** The extra columns beyond the primary one; empty for a single-column index. */
    public List<DataField> getExtraFields(RowType rowType) {
        List<DataField> fields = new ArrayList<>();
        if (extraFieldIds != null) {
            for (int id : extraFieldIds) {
                fields.add(rowType.getField(id));
            }
        }
        return fields;
    }

    public List<String> getIndexedFieldNames(RowType rowType) {
        List<String> names = new ArrayList<>();
        for (int id : getIndexedFieldIds()) {
            names.add(rowType.getField(id).name());
        }
        return names;
    }
}
