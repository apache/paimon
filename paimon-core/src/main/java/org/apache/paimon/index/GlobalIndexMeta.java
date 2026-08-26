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
import java.util.Objects;

/** Schema for global index. */
public class GlobalIndexMeta {

    public static final String ROW_RANGE_START = "_ROW_RANGE_START";
    public static final String ROW_RANGE_END = "_ROW_RANGE_END";
    public static final String INDEX_FIELD_ID = "_INDEX_FIELD_ID";
    public static final String EXTRA_FIELD_IDS = "_EXTRA_FIELD_IDS";
    public static final String INDEX_META = "_INDEX_META";
    public static final String SOURCE_META = "_SOURCE_META";
    public static final String BUILD_SCHEMA_ID = "_BUILD_SCHEMA_ID";

    public static final RowType SCHEMA =
            new RowType(
                    true,
                    Arrays.asList(
                            new DataField(0, ROW_RANGE_START, new BigIntType(false)),
                            new DataField(1, ROW_RANGE_END, new BigIntType(false)),
                            new DataField(2, INDEX_FIELD_ID, new IntType(false)),
                            new DataField(3, EXTRA_FIELD_IDS, DataTypes.ARRAY(new IntType(false))),
                            new DataField(4, INDEX_META, DataTypes.BYTES()),
                            new DataField(5, SOURCE_META, DataTypes.BYTES()),
                            new DataField(6, BUILD_SCHEMA_ID, new BigIntType())));

    private final long rowRangeStart;
    private final long rowRangeEnd;
    private final int indexFieldId;
    @Nullable private final int[] extraFieldIds;
    @Nullable private final byte[] indexMeta;
    @Nullable private final byte[] sourceMeta;
    @Nullable private final Long buildSchemaId;

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
        this(rowRangeStart, rowRangeEnd, indexFieldId, extraFieldIds, indexMeta, sourceMeta, null);
    }

    public GlobalIndexMeta(
            long rowRangeStart,
            long rowRangeEnd,
            int indexFieldId,
            @Nullable int[] extraFieldIds,
            @Nullable byte[] indexMeta,
            @Nullable byte[] sourceMeta,
            @Nullable Long buildSchemaId) {
        this.rowRangeStart = rowRangeStart;
        this.rowRangeEnd = rowRangeEnd;
        this.indexFieldId = indexFieldId;
        this.extraFieldIds = extraFieldIds;
        this.indexMeta = indexMeta;
        this.sourceMeta = sourceMeta;
        this.buildSchemaId = buildSchemaId;
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

    /** Schema used to build this global index. */
    @Nullable
    public Long buildSchemaId() {
        return buildSchemaId;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GlobalIndexMeta that = (GlobalIndexMeta) o;
        return rowRangeStart == that.rowRangeStart
                && rowRangeEnd == that.rowRangeEnd
                && indexFieldId == that.indexFieldId
                && Arrays.equals(extraFieldIds, that.extraFieldIds)
                && Arrays.equals(indexMeta, that.indexMeta)
                && Arrays.equals(sourceMeta, that.sourceMeta)
                && Objects.equals(buildSchemaId, that.buildSchemaId);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(rowRangeStart, rowRangeEnd, indexFieldId, buildSchemaId);
        result = 31 * result + Arrays.hashCode(extraFieldIds);
        result = 31 * result + Arrays.hashCode(indexMeta);
        result = 31 * result + Arrays.hashCode(sourceMeta);
        return result;
    }
}
