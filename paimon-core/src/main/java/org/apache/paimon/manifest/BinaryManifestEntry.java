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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.io.BinaryDataFileMeta;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.VersionedObjectSerializer;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;
import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;

/**
 * Reusable binary view of a projected manifest entry.
 *
 * <p>This class is intended for streaming manifest algorithms which only need a subset of {@link
 * ManifestEntry}. Unlike {@link PojoManifestEntry}, it does not deserialize the nested {@code
 * _FILE} row into a POJO. The view is mutable and only valid while its backing {@link InternalRow}
 * is valid; callers must not retain it across reader iterations or batches.
 */
public final class BinaryManifestEntry implements ManifestEntry {

    private static final RowType MANIFEST_TYPE =
            VersionedObjectSerializer.versionType(ManifestEntry.SCHEMA);

    private final Projection projection;
    private final @Nullable BinaryDataFileMeta file;
    private @Nullable InternalRow row;

    private BinaryManifestEntry(Projection projection) {
        this.projection = projection;
        this.file =
                projection.fileProjection == null
                        ? null
                        : projection.fileProjection.createDataFile();
    }

    /** Replaces the backing row and returns this reusable view. */
    public BinaryManifestEntry replace(InternalRow row) {
        checkArgument(row != null, "Manifest row cannot be null.");
        if (projection.filePosition >= 0) {
            InternalRow fileRow =
                    row.getRow(projection.filePosition, projection.projectedFileFieldCount);
            checkState(fileRow != null, "Manifest data file metadata cannot be null.");
            file.replace(fileRow);
        }
        this.row = row;
        return this;
    }

    /** Drops references to the current row before its reader batch is released. */
    public void clear() {
        row = null;
        if (file != null) {
            file.clear();
        }
    }

    public boolean isAdd() {
        return row.getByte(requiredOuterPosition(projection.kindPosition, "_KIND"))
                == FileKind.ADD.toByteValue();
    }

    public boolean isDelete() {
        return row.getByte(requiredOuterPosition(projection.kindPosition, "_KIND"))
                == FileKind.DELETE.toByteValue();
    }

    @Override
    public FileKind kind() {
        return FileKind.fromByteValue(
                row.getByte(requiredOuterPosition(projection.kindPosition, "_KIND")));
    }

    public byte[] partitionBytes() {
        byte[] partition =
                row.getBinary(requiredOuterPosition(projection.partitionPosition, "_PARTITION"));
        checkState(partition != null, "Serialized manifest partition cannot be null.");
        return partition;
    }

    @Override
    public BinaryRow partition() {
        return deserializeBinaryRow(partitionBytes());
    }

    @Override
    public int bucket() {
        return row.getInt(requiredOuterPosition(projection.bucketPosition, "_BUCKET"));
    }

    @Override
    public int totalBuckets() {
        return row.getInt(requiredOuterPosition(projection.totalBucketsPosition, "_TOTAL_BUCKETS"));
    }

    @Override
    public String fileName() {
        return file().fileName();
    }

    @Override
    public long rowCount() {
        return file().rowCount();
    }

    @Override
    public int level() {
        return file().level();
    }

    @Nullable
    @Override
    public String externalPath() {
        return file().externalPath().orElse(null);
    }

    @Nullable
    @Override
    public Long firstRowId() {
        return file().firstRowId();
    }

    @Override
    public List<String> extraFiles() {
        return file().extraFiles();
    }

    @Override
    public Identifier identifier() {
        BinaryDataFileMeta file = file();
        return new Identifier(
                partition(),
                bucket(),
                file.level(),
                file.fileName(),
                file.extraFiles(),
                file.embeddedIndex(),
                file.externalPath().orElse(null));
    }

    @Override
    public BinaryRow minKey() {
        return file().minKey();
    }

    @Override
    public BinaryRow maxKey() {
        return file().maxKey();
    }

    @Override
    public BinaryDataFileMeta file() {
        checkState(row != null, "Binary manifest entry is not backed by a row.");
        if (file == null) {
            throw unsupported("_FILE");
        }
        return file;
    }

    @Override
    public ManifestEntry copyWithoutStats() {
        throw unsupportedOperation("copyWithoutStats()");
    }

    @Override
    public ManifestEntry assignSequenceNumber(long minSequenceNumber, long maxSequenceNumber) {
        throw unsupportedOperation("assignSequenceNumber(long, long)");
    }

    @Override
    public ManifestEntry assignFirstRowId(long firstRowId) {
        throw unsupportedOperation("assignFirstRowId(long)");
    }

    @Override
    public ManifestEntry upgrade(int newLevel) {
        throw unsupportedOperation("upgrade(int)");
    }

    private static int requiredOuterPosition(int position, String fieldName) {
        if (position < 0) {
            throw unsupported(fieldName);
        }
        return position;
    }

    private static UnsupportedOperationException unsupported(String field) {
        return new UnsupportedOperationException(
                String.format(
                        "The selected binary manifest projection does not contain %s.", field));
    }

    private static UnsupportedOperationException unsupportedOperation(String operation) {
        return new UnsupportedOperationException(
                String.format("Binary manifest entry does not support %s.", operation));
    }

    /**
     * Projected manifest schema together with its bound binary field layout.
     *
     * <p>The projected type may contain any subset and ordering of the versioned {@link
     * ManifestEntry#SCHEMA}. Unsupported {@link FileEntry} accessors fail explicitly when their
     * fields were not projected.
     */
    public static final class Projection {

        private final FormatReaderFactory readerFactory;
        private final int kindPosition;
        private final int partitionPosition;
        private final int bucketPosition;
        private final int totalBucketsPosition;
        private final int filePosition;
        private final int projectedFileFieldCount;
        private final @Nullable BinaryDataFileMeta.Projection fileProjection;

        private Projection(
                FormatReaderFactory readerFactory,
                int kindPosition,
                int partitionPosition,
                int bucketPosition,
                int totalBucketsPosition,
                int filePosition,
                int projectedFileFieldCount,
                @Nullable BinaryDataFileMeta.Projection fileProjection) {
            this.readerFactory = readerFactory;
            this.kindPosition = kindPosition;
            this.partitionPosition = partitionPosition;
            this.bucketPosition = bucketPosition;
            this.totalBucketsPosition = totalBucketsPosition;
            this.filePosition = filePosition;
            this.projectedFileFieldCount = projectedFileFieldCount;
            this.fileProjection = fileProjection;
        }

        public static Projection create(FileFormat format, RowType projectedType) {
            checkArgument(format != null, "Manifest format cannot be null.");
            checkArgument(projectedType != null, "Projected manifest type cannot be null.");
            validateProjection(projectedType);

            int filePosition = projectedType.getFieldIndex("_FILE");
            int projectedFileFieldCount = 0;
            BinaryDataFileMeta.Projection fileProjection = null;
            if (filePosition >= 0) {
                RowType projectedFileType =
                        (RowType) projectedType.getFields().get(filePosition).type();
                projectedFileFieldCount = projectedFileType.getFieldCount();
                fileProjection = BinaryDataFileMeta.Projection.create(projectedFileType);
            }

            return new Projection(
                    format.createReaderFactory(
                            MANIFEST_TYPE, projectedType, Collections.emptyList()),
                    projectedType.getFieldIndex("_KIND"),
                    projectedType.getFieldIndex("_PARTITION"),
                    projectedType.getFieldIndex("_BUCKET"),
                    projectedType.getFieldIndex("_TOTAL_BUCKETS"),
                    filePosition,
                    projectedFileFieldCount,
                    fileProjection);
        }

        private static void validateProjection(RowType projectedType) {
            for (DataField projectedField : projectedType.getFields()) {
                checkArgument(
                        MANIFEST_TYPE.containsField(projectedField.id()),
                        "Unknown projected manifest field '%s' (id %s).",
                        projectedField.name(),
                        projectedField.id());
                DataField manifestField = MANIFEST_TYPE.getField(projectedField.id());
                checkArgument(
                        projectedField.isPrunedFrom(manifestField),
                        "Projected manifest field '%s' does not match %s.",
                        projectedField.name(),
                        manifestField);
            }
        }

        public FormatReaderFactory readerFactory() {
            return readerFactory;
        }

        public BinaryManifestEntry createEntry() {
            return new BinaryManifestEntry(this);
        }
    }
}
