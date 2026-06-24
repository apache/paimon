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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.DeletionFileKey;
import org.apache.paimon.deletionvectors.FileNameKey;
import org.apache.paimon.deletionvectors.RowIdRangeKey;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Range;

import javax.annotation.Nullable;

import java.util.Objects;

import static org.apache.paimon.data.BinaryString.fromString;
import static org.apache.paimon.utils.SerializationUtils.newStringType;

/** Metadata of deletion vector. */
public class DeletionVectorMeta {

    public static final RowType SCHEMA =
            RowType.of(
                    new DataField(0, "f0", newStringType(true)),
                    new DataField(1, "f1", new IntType(false)),
                    new DataField(2, "f2", new IntType(false)),
                    new DataField(3, "_CARDINALITY", new BigIntType(true)));

    public static final RowType ROW_ID_RANGE_SCHEMA =
            RowType.of(
                    new DataField(0, "_ROW_ID_START", new BigIntType(false)),
                    new DataField(1, "_ROW_ID_END", new BigIntType(false)),
                    new DataField(2, "_OFFSET", new IntType(false)),
                    new DataField(3, "_LENGTH", new IntType(false)),
                    new DataField(4, "_CARDINALITY", new BigIntType(true)));

    private final DeletionFileKey key;
    private final int offset;
    private final int length;
    @Nullable private final Long cardinality;

    public DeletionVectorMeta(
            String dataFileName, int start, int length, @Nullable Long cardinality) {
        this(DeletionFileKey.ofFileName(dataFileName), start, length, cardinality);
    }

    public DeletionVectorMeta(
            DeletionFileKey key, int start, int length, @Nullable Long cardinality) {
        this.key = key;
        this.offset = start;
        this.length = length;
        this.cardinality = cardinality;
    }

    public GenericRow toRow() {
        switch (key.type()) {
            case FILE_NAME:
                String fileName = ((FileNameKey) key).fileName();
                return GenericRow.of(fromString(fileName), offset, length, cardinality);
            case ROW_RANGE:
                Range range = ((RowIdRangeKey) key).range();
                return GenericRow.of(range.from, range.to, offset, length, cardinality);
            default:
                throw new UnsupportedOperationException("Unsupported key type: " + key.type());
        }
    }

    public static GenericRow newLegacyMarkerRow() {
        return GenericRow.of(null, 0, 0, null);
    }

    public static boolean isLegacyMarker(InternalArray metas) {
        if (metas != null && metas.size() == 1) {
            InternalRow row = metas.getRow(0, SCHEMA.getFieldCount());
            return row.isNullAt(0);
        }
        return false;
    }

    public static DeletionVectorMeta fromRow(DeletionFileKey.Type keyType, InternalRow row) {
        switch (keyType) {
            case FILE_NAME:
                DeletionFileKey fileNameKey =
                        DeletionFileKey.ofFileName(row.getString(0).toString());
                return new DeletionVectorMeta(
                        fileNameKey,
                        row.getInt(1),
                        row.getInt(2),
                        row.isNullAt(3) ? null : row.getLong(3));
            case ROW_RANGE:
                DeletionFileKey rowRangeKey =
                        DeletionFileKey.ofRange(new Range(row.getLong(0), row.getLong(1)));
                return new DeletionVectorMeta(
                        rowRangeKey,
                        row.getInt(2),
                        row.getInt(3),
                        row.isNullAt(4) ? null : row.getLong(4));
            default:
                throw new UnsupportedOperationException("Unsupported key type: " + keyType);
        }
    }

    public DeletionFileKey key() {
        return key;
    }

    public String dataFileName() {
        if (key instanceof FileNameKey) {
            return ((FileNameKey) key).fileName();
        }
        throw new IllegalStateException("Deletion vector key is not file-name based: " + key);
    }

    public int offset() {
        return offset;
    }

    public int length() {
        return length;
    }

    @Nullable
    public Long cardinality() {
        return cardinality;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DeletionVectorMeta that = (DeletionVectorMeta) o;
        return offset == that.offset
                && length == that.length
                && Objects.equals(key, that.key)
                && Objects.equals(cardinality, that.cardinality);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, offset, length, cardinality);
    }

    @Override
    public String toString() {
        return "DeletionVectorMeta{"
                + "key="
                + key
                + ", offset="
                + offset
                + ", length="
                + length
                + ", cardinality="
                + cardinality
                + '}';
    }
}
