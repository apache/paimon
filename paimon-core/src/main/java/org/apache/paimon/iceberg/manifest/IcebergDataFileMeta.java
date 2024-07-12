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

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Iceberg data file meta.
 *
 * <p>See <a href="https://iceberg.apache.org/spec/#manifests">Iceberg spec</a>.
 */
public class IcebergDataFileMeta {

    /** See Iceberg <code>data_file</code> struct <code>content</code> field. */
    public enum Content {
        DATA(0),
        POSITION_DELETES(1),
        EQUALITY_DELETES(2);

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
                    return POSITION_DELETES;
                case 2:
                    return EQUALITY_DELETES;
            }
            throw new IllegalArgumentException("Unknown manifest content: " + id);
        }
    }

    private final Content content;
    private final String filePath;
    private final String fileFormat;
    private final BinaryRow partition;
    private final long recordCount;
    private final long fileSizeInBytes;

    public IcebergDataFileMeta(
            Content content,
            String filePath,
            String fileFormat,
            BinaryRow partition,
            long recordCount,
            long fileSizeInBytes) {
        this.content = content;
        this.filePath = filePath;
        this.fileFormat = fileFormat;
        this.partition = partition;
        this.recordCount = recordCount;
        this.fileSizeInBytes = fileSizeInBytes;
    }

    public Content content() {
        return content;
    }

    public String filePath() {
        return filePath;
    }

    public String fileFormat() {
        return fileFormat;
    }

    public BinaryRow partition() {
        return partition;
    }

    public long recordCount() {
        return recordCount;
    }

    public long fileSizeInBytes() {
        return fileSizeInBytes;
    }

    public static RowType schema(RowType partitionType) {
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(134, "content", DataTypes.INT().notNull()));
        fields.add(new DataField(100, "file_path", DataTypes.STRING().notNull()));
        fields.add(new DataField(101, "file_format", DataTypes.STRING().notNull()));
        fields.add(new DataField(102, "partition", partitionType));
        fields.add(new DataField(103, "record_count", DataTypes.BIGINT().notNull()));
        fields.add(new DataField(104, "file_size_in_bytes", DataTypes.BIGINT().notNull()));
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
        IcebergDataFileMeta that = (IcebergDataFileMeta) o;
        return content == that.content
                && recordCount == that.recordCount
                && fileSizeInBytes == that.fileSizeInBytes
                && Objects.equals(filePath, that.filePath)
                && Objects.equals(fileFormat, that.fileFormat)
                && Objects.equals(partition, that.partition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(content, filePath, fileFormat, partition, recordCount, fileSizeInBytes);
    }
}
