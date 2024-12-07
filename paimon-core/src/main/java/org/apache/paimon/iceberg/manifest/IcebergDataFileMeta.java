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
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.iceberg.metadata.IcebergDataField;
import org.apache.paimon.iceberg.metadata.IcebergSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    private final InternalMap nullValueCounts;
    private final InternalMap lowerBounds;
    private final InternalMap upperBounds;

    IcebergDataFileMeta(
            Content content,
            String filePath,
            String fileFormat,
            BinaryRow partition,
            long recordCount,
            long fileSizeInBytes,
            InternalMap nullValueCounts,
            InternalMap lowerBounds,
            InternalMap upperBounds) {
        this.content = content;
        this.filePath = filePath;
        this.fileFormat = fileFormat;
        this.partition = partition;
        this.recordCount = recordCount;
        this.fileSizeInBytes = fileSizeInBytes;
        this.nullValueCounts = nullValueCounts;
        this.lowerBounds = lowerBounds;
        this.upperBounds = upperBounds;
    }

    public static IcebergDataFileMeta create(
            Content content,
            String filePath,
            String fileFormat,
            BinaryRow partition,
            long recordCount,
            long fileSizeInBytes,
            IcebergSchema icebergSchema,
            SimpleStats stats,
            @Nullable List<String> statsColumns) {
        int numFields = icebergSchema.fields().size();
        Map<String, Integer> indexMap = new HashMap<>();
        if (statsColumns == null) {
            for (int i = 0; i < numFields; i++) {
                indexMap.put(icebergSchema.fields().get(i).name(), i);
            }
        } else {
            for (int i = 0; i < statsColumns.size(); i++) {
                indexMap.put(statsColumns.get(i), i);
            }
        }

        Map<Integer, Long> nullValueCounts = new HashMap<>();
        Map<Integer, byte[]> lowerBounds = new HashMap<>();
        Map<Integer, byte[]> upperBounds = new HashMap<>();

        for (int i = 0; i < numFields; i++) {
            IcebergDataField field = icebergSchema.fields().get(i);
            if (!indexMap.containsKey(field.name())) {
                continue;
            }

            int idx = indexMap.get(field.name());
            nullValueCounts.put(field.id(), stats.nullCounts().getLong(idx));

            InternalRow.FieldGetter fieldGetter =
                    InternalRow.createFieldGetter(field.dataType(), idx);
            Object minValue = fieldGetter.getFieldOrNull(stats.minValues());
            Object maxValue = fieldGetter.getFieldOrNull(stats.maxValues());
            if (minValue != null && maxValue != null) {
                lowerBounds.put(
                        field.id(),
                        IcebergConversions.toByteBuffer(field.dataType(), minValue).array());
                upperBounds.put(
                        field.id(),
                        IcebergConversions.toByteBuffer(field.dataType(), maxValue).array());
            }
        }

        return new IcebergDataFileMeta(
                content,
                filePath,
                fileFormat,
                partition,
                recordCount,
                fileSizeInBytes,
                new GenericMap(nullValueCounts),
                new GenericMap(lowerBounds),
                new GenericMap(upperBounds));
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

    public InternalMap nullValueCounts() {
        return nullValueCounts;
    }

    public InternalMap lowerBounds() {
        return lowerBounds;
    }

    public InternalMap upperBounds() {
        return upperBounds;
    }

    public static RowType schema(RowType partitionType) {
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(134, "content", DataTypes.INT().notNull()));
        fields.add(new DataField(100, "file_path", DataTypes.STRING().notNull()));
        fields.add(new DataField(101, "file_format", DataTypes.STRING().notNull()));
        fields.add(new DataField(102, "partition", partitionType));
        fields.add(new DataField(103, "record_count", DataTypes.BIGINT().notNull()));
        fields.add(new DataField(104, "file_size_in_bytes", DataTypes.BIGINT().notNull()));
        fields.add(
                new DataField(
                        110,
                        "null_value_counts",
                        DataTypes.MAP(DataTypes.INT().notNull(), DataTypes.BIGINT().notNull())));
        fields.add(
                new DataField(
                        125,
                        "lower_bounds",
                        DataTypes.MAP(DataTypes.INT().notNull(), DataTypes.BYTES().notNull())));
        fields.add(
                new DataField(
                        128,
                        "upper_bounds",
                        DataTypes.MAP(DataTypes.INT().notNull(), DataTypes.BYTES().notNull())));
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
        IcebergDataFileMeta that = (IcebergDataFileMeta) o;
        return content == that.content
                && recordCount == that.recordCount
                && fileSizeInBytes == that.fileSizeInBytes
                && Objects.equals(filePath, that.filePath)
                && Objects.equals(fileFormat, that.fileFormat)
                && Objects.equals(partition, that.partition)
                && Objects.equals(nullValueCounts, that.nullValueCounts)
                && Objects.equals(lowerBounds, that.lowerBounds)
                && Objects.equals(upperBounds, that.upperBounds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                content,
                filePath,
                fileFormat,
                partition,
                recordCount,
                fileSizeInBytes,
                nullValueCounts,
                lowerBounds,
                upperBounds);
    }
}
