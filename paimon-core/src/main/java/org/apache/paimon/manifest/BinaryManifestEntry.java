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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.BinaryDataFileMeta;
import org.apache.paimon.memory.MemorySegment;
import org.apache.paimon.memory.MemorySegmentUtils;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.VersionedObjectSerializer;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

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
    private static final Projection FULL_PROJECTION = Projection.create(MANIFEST_TYPE);

    private final Projection projection;
    private final @Nullable BinaryDataFileMeta file;
    private final ReusablePartition partitionView = new ReusablePartition();
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
        if (row.getFieldCount() != projection.projectedType.getFieldCount()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Manifest row field count %s does not match projected field count %s.",
                            row.getFieldCount(), projection.projectedType.getFieldCount()));
        }
        if (projection.filePosition >= 0) {
            InternalRow fileRow =
                    row.getRow(projection.filePosition, projection.projectedFileFieldCount);
            checkState(fileRow != null, "Manifest data file metadata cannot be null.");
            file.replace(fileRow);
        }
        this.row = row;
        this.partitionView.reset();
        return this;
    }

    /** Returns the backing row when this entry uses the complete versioned manifest schema. */
    public InternalRow fullRow() {
        checkState(
                projection.fullProjection,
                "The selected binary manifest projection is not the complete manifest schema.");
        checkState(row != null, "Binary manifest entry is not backed by a row.");
        return row;
    }

    /** Returns the reusable projection for the complete versioned manifest schema. */
    public static Projection fullProjection() {
        return FULL_PROJECTION;
    }

    /** Drops references to the current row before its reader batch is released. */
    public void clear() {
        row = null;
        partitionView.reset();
        if (file != null) {
            file.clear();
        }
    }

    public boolean isAdd() {
        return row.getByte(requiredOuterPosition(projection.kindPosition, ManifestEntry.KIND))
                == FileKind.ADD.toByteValue();
    }

    public boolean isDelete() {
        return row.getByte(requiredOuterPosition(projection.kindPosition, ManifestEntry.KIND))
                == FileKind.DELETE.toByteValue();
    }

    @Override
    public FileKind kind() {
        return FileKind.fromByteValue(
                row.getByte(requiredOuterPosition(projection.kindPosition, ManifestEntry.KIND)));
    }

    public byte[] partitionBytes() {
        return partitionView.getBytes(
                row, requiredOuterPosition(projection.partitionPosition, ManifestEntry.PARTITION));
    }

    @Override
    public BinaryRow partition() {
        return partitionView.getRow(
                row, requiredOuterPosition(projection.partitionPosition, ManifestEntry.PARTITION));
    }

    private static final class ReusablePartition {

        private final MemorySegment[] segments = new MemorySegment[1];
        private @Nullable byte[] bytes;
        private @Nullable BinaryRow row;

        private byte[] getBytes(InternalRow entryRow, int position) {
            if (bytes == null) {
                bytes = entryRow.getBinary(position);
                checkState(bytes != null, "Serialized manifest partition cannot be null.");
            }
            return bytes;
        }

        private BinaryRow getRow(InternalRow entryRow, int position) {
            if (segments[0] == null) {
                byte[] bytes = getBytes(entryRow, position);
                checkState(
                        bytes.length >= Integer.BYTES,
                        "Serialized manifest partition is too short.");
                int arity =
                        ((bytes[0] & 0xff) << 24)
                                | ((bytes[1] & 0xff) << 16)
                                | ((bytes[2] & 0xff) << 8)
                                | (bytes[3] & 0xff);
                if (row == null || row.getFieldCount() != arity) {
                    row = new BinaryRow(arity);
                }
                segments[0] = MemorySegment.wrap(bytes);
                row.pointTo(segments, Integer.BYTES, bytes.length - Integer.BYTES);
            }
            return row;
        }

        private void reset() {
            bytes = null;
            segments[0] = null;
        }
    }

    @Override
    public int bucket() {
        return row.getInt(requiredOuterPosition(projection.bucketPosition, ManifestEntry.BUCKET));
    }

    @Override
    public int totalBuckets() {
        return row.getInt(
                requiredOuterPosition(
                        projection.totalBucketsPosition, ManifestEntry.TOTAL_BUCKETS));
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
            throw unsupported(ManifestEntry.FILE);
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
     * Reusable byte encoding of a binary manifest entry's identity fields.
     *
     * <p>The encoded identifier is the prefix of {@link #bytes()} ending at {@link #length()}. It
     * is valid until the next call to {@link #replace(BinaryManifestEntry)} or {@link #release()}
     * and must not be modified by callers.
     */
    public static final class ReusableIdentifier {

        private byte[] bytes = new byte[256];
        private int length;

        public ReusableIdentifier replace(BinaryManifestEntry entry) {
            checkArgument(entry != null, "Binary manifest entry cannot be null.");
            length = 0;
            return appendEntryFields(entry);
        }

        /** Replaces this encoding with the entry's partition and identity fields. */
        public ReusableIdentifier replaceWithPartition(BinaryManifestEntry entry) {
            checkArgument(entry != null, "Binary manifest entry cannot be null.");
            length = 0;
            putBytes(entry.partitionBytes());
            return appendEntryFields(entry);
        }

        private ReusableIdentifier appendEntryFields(BinaryManifestEntry entry) {
            putInt(entry.bucket());
            BinaryDataFileMeta file = entry.file();
            putInt(file.level());
            putString(file.fileNameBinary());

            int extraFileCount = file.extraFileCount();
            putInt(extraFileCount);
            for (int i = 0; i < extraFileCount; i++) {
                putString(file.extraFile(i));
            }

            if (!file.hasEmbeddedIndex()) {
                putInt(-1);
            } else {
                putBytes(file.embeddedIndex());
            }
            if (!file.hasExternalPath()) {
                putInt(-1);
            } else {
                putString(file.externalPathBinary());
            }
            return this;
        }

        public byte[] bytes() {
            return bytes;
        }

        public int length() {
            return length;
        }

        public void release() {
            bytes = new byte[0];
            length = 0;
        }

        private void putString(BinaryString value) {
            checkState(value != null, "Manifest string field cannot be null.");
            int valueLength = value.getSizeInBytes();
            putInt(valueLength);
            ensureCapacity(valueLength);
            MemorySegmentUtils.copyToBytes(
                    value.getSegments(), value.getOffset(), bytes, length, valueLength);
            length += valueLength;
        }

        private void putBytes(byte[] value) {
            checkState(value != null, "Manifest binary field cannot be null.");
            putInt(value.length);
            ensureCapacity(value.length);
            System.arraycopy(value, 0, bytes, length, value.length);
            length += value.length;
        }

        private void putInt(int value) {
            ensureCapacity(Integer.BYTES);
            bytes[length++] = (byte) (value >>> 24);
            bytes[length++] = (byte) (value >>> 16);
            bytes[length++] = (byte) (value >>> 8);
            bytes[length++] = (byte) value;
        }

        private void ensureCapacity(int additional) {
            int required = Math.addExact(length, additional);
            if (required <= bytes.length) {
                return;
            }
            int grown = Math.max(required, bytes.length + (bytes.length >>> 1));
            bytes = Arrays.copyOf(bytes, grown);
        }
    }

    /**
     * Projected manifest schema together with its bound binary field layout.
     *
     * <p>The projected type may contain any subset and ordering of the versioned {@link
     * ManifestEntry#SCHEMA}. Unsupported {@link FileEntry} accessors fail explicitly when their
     * fields were not projected.
     */
    public static final class Projection {

        private final RowType projectedType;
        private final int kindPosition;
        private final int partitionPosition;
        private final int bucketPosition;
        private final int totalBucketsPosition;
        private final int filePosition;
        private final int projectedFileFieldCount;
        private final @Nullable BinaryDataFileMeta.Projection fileProjection;
        private final boolean fullProjection;

        private Projection(
                RowType projectedType,
                int kindPosition,
                int partitionPosition,
                int bucketPosition,
                int totalBucketsPosition,
                int filePosition,
                int projectedFileFieldCount,
                @Nullable BinaryDataFileMeta.Projection fileProjection,
                boolean fullProjection) {
            this.projectedType = projectedType;
            this.kindPosition = kindPosition;
            this.partitionPosition = partitionPosition;
            this.bucketPosition = bucketPosition;
            this.totalBucketsPosition = totalBucketsPosition;
            this.filePosition = filePosition;
            this.projectedFileFieldCount = projectedFileFieldCount;
            this.fileProjection = fileProjection;
            this.fullProjection = fullProjection;
        }

        public static Projection create(RowType projectedType) {
            checkArgument(projectedType != null, "Projected manifest type cannot be null.");
            validateProjection(projectedType);

            int filePosition = projectedType.getFieldIndex(ManifestEntry.FILE);
            int projectedFileFieldCount = 0;
            BinaryDataFileMeta.Projection fileProjection = null;
            if (filePosition >= 0) {
                RowType projectedFileType =
                        (RowType) projectedType.getFields().get(filePosition).type();
                projectedFileFieldCount = projectedFileType.getFieldCount();
                fileProjection = BinaryDataFileMeta.Projection.create(projectedFileType);
            }

            return new Projection(
                    projectedType,
                    projectedType.getFieldIndex(ManifestEntry.KIND),
                    projectedType.getFieldIndex(ManifestEntry.PARTITION),
                    projectedType.getFieldIndex(ManifestEntry.BUCKET),
                    projectedType.getFieldIndex(ManifestEntry.TOTAL_BUCKETS),
                    filePosition,
                    projectedFileFieldCount,
                    fileProjection,
                    projectedType.equals(MANIFEST_TYPE));
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

        RowType projectedType() {
            return projectedType;
        }

        public BinaryManifestEntry createEntry() {
            return new BinaryManifestEntry(this);
        }
    }
}
