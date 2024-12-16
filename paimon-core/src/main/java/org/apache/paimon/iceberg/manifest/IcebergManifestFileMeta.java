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

package org.apache.paimon.iceberg.manifest;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Metadata of an Iceberg manifest file.
 *
 * <p>See <a href="https://iceberg.apache.org/spec/#manifest-lists">Iceberg spec</a>.
 */
public class IcebergManifestFileMeta {

    /** Content type stored in a manifest file. */
    public enum Content {
        DATA(0),
        DELETES(1);

        private final int id;

        Content(int id) {
            this.id = id;
        }

        public int id() {
            return id;
        }

        public static Content fromId(int id) {
            switch (id) {
                case 0:
                    return DATA;
                case 1:
                    return DELETES;
            }
            throw new IllegalArgumentException("Unknown manifest content: " + id);
        }
    }

    private final String manifestPath;
    private final long manifestLength;
    private final int partitionSpecId;
    private final Content content;
    private final long sequenceNumber;
    private final long minSequenceNumber;
    private final long addedSnapshotId;
    private final int addedFilesCount;
    private final int existingFilesCount;
    private final int deletedFilesCount;
    private final long addedRowsCount;
    private final long existingRowsCount;
    private final long deletedRowsCount;
    private final List<IcebergPartitionSummary> partitions;

    public IcebergManifestFileMeta(
            String manifestPath,
            long manifestLength,
            int partitionSpecId,
            Content content,
            long sequenceNumber,
            long minSequenceNumber,
            long addedSnapshotId,
            int addedFilesCount,
            int existingFilesCount,
            int deletedFilesCount,
            long addedRowsCount,
            long existingRowsCount,
            long deletedRowsCount,
            List<IcebergPartitionSummary> partitions) {
        this.manifestPath = manifestPath;
        this.manifestLength = manifestLength;
        this.partitionSpecId = partitionSpecId;
        this.content = content;
        this.sequenceNumber = sequenceNumber;
        this.minSequenceNumber = minSequenceNumber;
        this.addedSnapshotId = addedSnapshotId;
        this.addedFilesCount = addedFilesCount;
        this.existingFilesCount = existingFilesCount;
        this.deletedFilesCount = deletedFilesCount;
        this.addedRowsCount = addedRowsCount;
        this.existingRowsCount = existingRowsCount;
        this.deletedRowsCount = deletedRowsCount;
        this.partitions = partitions;
    }

    public String manifestPath() {
        return manifestPath;
    }

    public long manifestLength() {
        return manifestLength;
    }

    public int partitionSpecId() {
        return partitionSpecId;
    }

    public Content content() {
        return content;
    }

    public long sequenceNumber() {
        return sequenceNumber;
    }

    public long minSequenceNumber() {
        return minSequenceNumber;
    }

    public long addedSnapshotId() {
        return addedSnapshotId;
    }

    public int addedFilesCount() {
        return addedFilesCount;
    }

    public int existingFilesCount() {
        return existingFilesCount;
    }

    public int deletedFilesCount() {
        return deletedFilesCount;
    }

    public long addedRowsCount() {
        return addedRowsCount;
    }

    public long existingRowsCount() {
        return existingRowsCount;
    }

    public long deletedRowsCount() {
        return deletedRowsCount;
    }

    public long liveRowsCount() {
        return addedRowsCount + existingRowsCount;
    }

    public List<IcebergPartitionSummary> partitions() {
        return partitions;
    }

    public static RowType schema(boolean legacyVersion) {
        return legacyVersion ? schemaForIceberg1_4() : schemaForIcebergNew();
    }

    private static RowType schemaForIcebergNew() {
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(500, "manifest_path", DataTypes.STRING().notNull()));
        fields.add(new DataField(501, "manifest_length", DataTypes.BIGINT().notNull()));
        fields.add(new DataField(502, "partition_spec_id", DataTypes.INT().notNull()));
        fields.add(new DataField(517, "content", DataTypes.INT().notNull()));
        fields.add(new DataField(515, "sequence_number", DataTypes.BIGINT().notNull()));
        fields.add(new DataField(516, "min_sequence_number", DataTypes.BIGINT().notNull()));
        fields.add(new DataField(503, "added_snapshot_id", DataTypes.BIGINT()));
        fields.add(new DataField(504, "added_files_count", DataTypes.INT().notNull()));
        fields.add(new DataField(505, "existing_files_count", DataTypes.INT().notNull()));
        fields.add(new DataField(506, "deleted_files_count", DataTypes.INT().notNull()));
        fields.add(new DataField(512, "added_rows_count", DataTypes.BIGINT().notNull()));
        fields.add(new DataField(513, "existing_rows_count", DataTypes.BIGINT().notNull()));
        fields.add(new DataField(514, "deleted_rows_count", DataTypes.BIGINT().notNull()));
        fields.add(
                new DataField(
                        508, "partitions", DataTypes.ARRAY(IcebergPartitionSummary.schema())));
        return new RowType(false, fields);
    }

    private static RowType schemaForIceberg1_4() {
        // see https://github.com/apache/iceberg/pull/5338
        // some reader still want old schema, for example, AWS athena
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(500, "manifest_path", DataTypes.STRING().notNull()));
        fields.add(new DataField(501, "manifest_length", DataTypes.BIGINT().notNull()));
        fields.add(new DataField(502, "partition_spec_id", DataTypes.INT().notNull()));
        fields.add(new DataField(517, "content", DataTypes.INT().notNull()));
        fields.add(new DataField(515, "sequence_number", DataTypes.BIGINT().notNull()));
        fields.add(new DataField(516, "min_sequence_number", DataTypes.BIGINT().notNull()));
        fields.add(new DataField(503, "added_snapshot_id", DataTypes.BIGINT()));
        fields.add(new DataField(504, "added_data_files_count", DataTypes.INT().notNull()));
        fields.add(new DataField(505, "existing_data_files_count", DataTypes.INT().notNull()));
        fields.add(new DataField(506, "deleted_data_files_count", DataTypes.INT().notNull()));
        fields.add(new DataField(512, "added_rows_count", DataTypes.BIGINT().notNull()));
        fields.add(new DataField(513, "existing_rows_count", DataTypes.BIGINT().notNull()));
        fields.add(new DataField(514, "deleted_rows_count", DataTypes.BIGINT().notNull()));
        fields.add(
                new DataField(
                        508, "partitions", DataTypes.ARRAY(IcebergPartitionSummary.schema())));
        return new RowType(false, fields);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IcebergManifestFileMeta that = (IcebergManifestFileMeta) o;
        return Objects.equals(manifestPath, that.manifestPath)
                && manifestLength == that.manifestLength
                && partitionSpecId == that.partitionSpecId
                && content == that.content
                && sequenceNumber == that.sequenceNumber
                && minSequenceNumber == that.minSequenceNumber
                && addedSnapshotId == that.addedSnapshotId
                && addedFilesCount == that.addedFilesCount
                && existingFilesCount == that.existingFilesCount
                && deletedFilesCount == that.deletedFilesCount
                && addedRowsCount == that.addedRowsCount
                && existingRowsCount == that.existingRowsCount
                && deletedRowsCount == that.deletedRowsCount
                && Objects.equals(partitions, that.partitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                manifestPath,
                manifestLength,
                partitionSpecId,
                content,
                sequenceNumber,
                minSequenceNumber,
                addedSnapshotId,
                addedFilesCount,
                existingFilesCount,
                deletedFilesCount,
                addedRowsCount,
                existingRowsCount,
                deletedRowsCount,
                partitions);
    }
}
