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
 * Entry of an Iceberg manifest file.
 *
 * <p>See <a href="https://iceberg.apache.org/spec/#manifests">Iceberg spec</a>.
 */
public class IcebergManifestEntry {

    /** See Iceberg <code>manifest_entry</code> struct <code>status</code> field. */
    public enum Status {
        EXISTING(0),
        ADDED(1),
        DELETED(2);

        private final int id;

        Status(int id) {
            this.id = id;
        }

        public int id() {
            return id;
        }

        public static Status fromId(int id) {
            switch (id) {
                case 0:
                    return EXISTING;
                case 1:
                    return ADDED;
                case 2:
                    return DELETED;
            }
            throw new IllegalArgumentException("Unknown manifest content: " + id);
        }
    }

    private final Status status;
    private final long snapshotId;
    // sequenceNumber indicates when the records in the data files are written. It might be smaller
    // than fileSequenceNumber.
    // For example, when a file with sequenceNumber 3 and another file with sequenceNumber 5 are
    // compacted into one file during snapshot 6, the compacted file will have sequenceNumber =
    // max(3, 5) = 5, and fileSequenceNumber = 6.
    private final long sequenceNumber;
    private final long fileSequenceNumber;
    private final IcebergDataFileMeta dataFile;

    public IcebergManifestEntry(
            Status status,
            long snapshotId,
            long sequenceNumber,
            long fileSequenceNumber,
            IcebergDataFileMeta dataFile) {
        this.status = status;
        this.snapshotId = snapshotId;
        this.sequenceNumber = sequenceNumber;
        this.fileSequenceNumber = fileSequenceNumber;
        this.dataFile = dataFile;
    }

    public Status status() {
        return status;
    }

    public boolean isLive() {
        return status == Status.ADDED || status == Status.EXISTING;
    }

    public long snapshotId() {
        return snapshotId;
    }

    public long sequenceNumber() {
        return sequenceNumber;
    }

    public long fileSequenceNumber() {
        return fileSequenceNumber;
    }

    public IcebergDataFileMeta file() {
        return dataFile;
    }

    public static RowType schema(RowType partitionType) {
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(0, "status", DataTypes.INT().notNull()));
        fields.add(new DataField(1, "snapshot_id", DataTypes.BIGINT()));
        fields.add(new DataField(3, "sequence_number", DataTypes.BIGINT()));
        fields.add(new DataField(4, "file_sequence_number", DataTypes.BIGINT()));
        fields.add(
                new DataField(2, "data_file", IcebergDataFileMeta.schema(partitionType).notNull()));
        return new RowType(fields);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IcebergManifestEntry that = (IcebergManifestEntry) o;
        return status == that.status && Objects.equals(dataFile, that.dataFile);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, dataFile);
    }
}
