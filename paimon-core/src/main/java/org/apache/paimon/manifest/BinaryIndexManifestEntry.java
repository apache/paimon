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

package org.apache.paimon.manifest;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.Arrays;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Reusable binary view of a projected index manifest entry. */
public final class BinaryIndexManifestEntry {

    private static final IndexManifestEntrySerializer FULL_SERIALIZER =
            new IndexManifestEntrySerializer();

    public static final Projection FULL_PROJECTION =
            Projection.create(IndexManifestEntry.MANIFEST_ROW_TYPE);
    public static final Projection GLOBAL_INDEX_PROJECTION = createGlobalIndexProjection();

    private final Projection projection;
    private @Nullable InternalRow row;

    private BinaryIndexManifestEntry(Projection projection) {
        this.projection = projection;
    }

    private static Projection createGlobalIndexProjection() {
        RowType manifestType = IndexManifestEntry.MANIFEST_ROW_TYPE;
        return Projection.create(
                new RowType(
                        false,
                        Arrays.asList(
                                manifestType.getField(IndexManifestEntry.KIND),
                                manifestType.getField(IndexManifestEntry.PARTITION),
                                manifestType.getField(IndexManifestEntry.BUCKET),
                                manifestType.getField(IndexManifestEntry.INDEX_TYPE),
                                manifestType
                                        .getField(IndexManifestEntry.GLOBAL_INDEX)
                                        .newType(
                                                GlobalIndexMeta.SCHEMA.project(
                                                        GlobalIndexMeta.ROW_RANGE_START,
                                                        GlobalIndexMeta.ROW_RANGE_END,
                                                        GlobalIndexMeta.INDEX_FIELD_ID,
                                                        GlobalIndexMeta.EXTRA_FIELD_IDS)))));
    }

    BinaryIndexManifestEntry replace(InternalRow row) {
        checkArgument(row != null, "Index manifest row cannot be null.");
        checkArgument(
                row.getFieldCount() == projection.projectedType.getFieldCount(),
                "Index manifest row field count %s does not match projected field count %s.",
                row.getFieldCount(),
                projection.projectedType.getFieldCount());
        this.row = row;
        return this;
    }

    void clear() {
        row = null;
    }

    public boolean isAdd() {
        return current().getByte(requiredPosition(projection.kindPosition, IndexManifestEntry.KIND))
                == FileKind.ADD.toByteValue();
    }

    public boolean isDelete() {
        return current().getByte(requiredPosition(projection.kindPosition, IndexManifestEntry.KIND))
                == FileKind.DELETE.toByteValue();
    }

    public byte[] partitionBytes() {
        byte[] partition =
                current()
                        .getBinary(
                                requiredPosition(
                                        projection.partitionPosition,
                                        IndexManifestEntry.PARTITION));
        checkState(partition != null, "Serialized index manifest partition cannot be null.");
        return partition;
    }

    public int bucket() {
        return current()
                .getInt(requiredPosition(projection.bucketPosition, IndexManifestEntry.BUCKET));
    }

    public BinaryString indexType() {
        BinaryString indexType =
                current()
                        .getString(
                                requiredPosition(
                                        projection.indexTypePosition,
                                        IndexManifestEntry.INDEX_TYPE));
        checkState(indexType != null, "Index type cannot be null.");
        return indexType;
    }

    public boolean hasGlobalIndexMeta() {
        return !current()
                .isNullAt(
                        requiredPosition(
                                projection.globalIndexPosition, IndexManifestEntry.GLOBAL_INDEX));
    }

    public long rowRangeStart() {
        return globalIndex()
                .getLong(
                        requiredPosition(
                                projection.rowRangeStartPosition, GlobalIndexMeta.ROW_RANGE_START));
    }

    public long rowRangeEnd() {
        return globalIndex()
                .getLong(
                        requiredPosition(
                                projection.rowRangeEndPosition, GlobalIndexMeta.ROW_RANGE_END));
    }

    public int indexFieldId() {
        return globalIndex()
                .getInt(
                        requiredPosition(
                                projection.indexFieldIdPosition, GlobalIndexMeta.INDEX_FIELD_ID));
    }

    public boolean hasExtraFields() {
        int position =
                requiredPosition(projection.extraFieldIdsPosition, GlobalIndexMeta.EXTRA_FIELD_IDS);
        InternalRow global = globalIndex();
        return !global.isNullAt(position) && global.getArray(position).size() > 0;
    }

    /** Copies this reusable view into a complete, independently owned manifest entry. */
    public IndexManifestEntry copy() {
        checkState(
                projection.projectedType.equals(IndexManifestEntry.MANIFEST_ROW_TYPE),
                "A complete index manifest projection is required to copy an entry.");
        return FULL_SERIALIZER.fromRow(current());
    }

    private InternalRow globalIndex() {
        InternalRow global =
                current()
                        .getRow(
                                requiredPosition(
                                        projection.globalIndexPosition,
                                        IndexManifestEntry.GLOBAL_INDEX),
                                projection.projectedGlobalIndexFieldCount);
        checkState(global != null, "Global index metadata is not present.");
        return global;
    }

    private InternalRow current() {
        checkState(row != null, "Binary index manifest entry is not backed by a row.");
        return row;
    }

    private static int requiredPosition(int position, String fieldName) {
        if (position < 0) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The selected binary index manifest projection does not contain %s.",
                            fieldName));
        }
        return position;
    }

    /** Projected index manifest schema together with its bound binary field layout. */
    public static final class Projection {

        private final RowType projectedType;
        private final int kindPosition;
        private final int partitionPosition;
        private final int bucketPosition;
        private final int indexTypePosition;
        private final int globalIndexPosition;
        private final int projectedGlobalIndexFieldCount;
        private final int rowRangeStartPosition;
        private final int rowRangeEndPosition;
        private final int indexFieldIdPosition;
        private final int extraFieldIdsPosition;

        private Projection(
                RowType projectedType,
                int kindPosition,
                int partitionPosition,
                int bucketPosition,
                int indexTypePosition,
                int globalIndexPosition,
                int projectedGlobalIndexFieldCount,
                int rowRangeStartPosition,
                int rowRangeEndPosition,
                int indexFieldIdPosition,
                int extraFieldIdsPosition) {
            this.projectedType = projectedType;
            this.kindPosition = kindPosition;
            this.partitionPosition = partitionPosition;
            this.bucketPosition = bucketPosition;
            this.indexTypePosition = indexTypePosition;
            this.globalIndexPosition = globalIndexPosition;
            this.projectedGlobalIndexFieldCount = projectedGlobalIndexFieldCount;
            this.rowRangeStartPosition = rowRangeStartPosition;
            this.rowRangeEndPosition = rowRangeEndPosition;
            this.indexFieldIdPosition = indexFieldIdPosition;
            this.extraFieldIdsPosition = extraFieldIdsPosition;
        }

        public static Projection create(RowType projectedType) {
            checkArgument(projectedType != null, "Projected index manifest type cannot be null.");
            validateProjection(projectedType);

            int globalIndexPosition = projectedType.getFieldIndex(IndexManifestEntry.GLOBAL_INDEX);
            int projectedGlobalIndexFieldCount = 0;
            int rowRangeStartPosition = -1;
            int rowRangeEndPosition = -1;
            int indexFieldIdPosition = -1;
            int extraFieldIdsPosition = -1;
            if (globalIndexPosition >= 0) {
                RowType globalIndexType =
                        (RowType) projectedType.getFields().get(globalIndexPosition).type();
                projectedGlobalIndexFieldCount = globalIndexType.getFieldCount();
                rowRangeStartPosition =
                        globalIndexType.getFieldIndex(GlobalIndexMeta.ROW_RANGE_START);
                rowRangeEndPosition = globalIndexType.getFieldIndex(GlobalIndexMeta.ROW_RANGE_END);
                indexFieldIdPosition =
                        globalIndexType.getFieldIndex(GlobalIndexMeta.INDEX_FIELD_ID);
                extraFieldIdsPosition =
                        globalIndexType.getFieldIndex(GlobalIndexMeta.EXTRA_FIELD_IDS);
            }

            return new Projection(
                    projectedType,
                    projectedType.getFieldIndex(IndexManifestEntry.KIND),
                    projectedType.getFieldIndex(IndexManifestEntry.PARTITION),
                    projectedType.getFieldIndex(IndexManifestEntry.BUCKET),
                    projectedType.getFieldIndex(IndexManifestEntry.INDEX_TYPE),
                    globalIndexPosition,
                    projectedGlobalIndexFieldCount,
                    rowRangeStartPosition,
                    rowRangeEndPosition,
                    indexFieldIdPosition,
                    extraFieldIdsPosition);
        }

        private static void validateProjection(RowType projectedType) {
            for (DataField projectedField : projectedType.getFields()) {
                checkArgument(
                        IndexManifestEntry.MANIFEST_ROW_TYPE.containsField(projectedField.id()),
                        "Unknown projected index manifest field '%s' (id %s).",
                        projectedField.name(),
                        projectedField.id());
                DataField manifestField =
                        IndexManifestEntry.MANIFEST_ROW_TYPE.getField(projectedField.id());
                checkArgument(
                        projectedField.isPrunedFrom(manifestField),
                        "Projected index manifest field '%s' does not match %s.",
                        projectedField.name(),
                        manifestField);
            }
        }

        RowType projectedType() {
            return projectedType;
        }

        public BinaryIndexManifestEntry createEntry() {
            return new BinaryIndexManifestEntry(this);
        }
    }
}
