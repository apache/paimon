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

package org.apache.paimon.table.format;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.paimon.table.BucketMode.NO_NEED_BUCKET;

/** {@link FormatDataSplit} for format table. */
public class FormatDataSplit extends DataSplit {

    private final FileIO fileIO;
    private final Path filePath;
    private final long offset;
    private final long length;
    private final RowType rowType;
    private final Predicate predicate;
    private final int[] projection;
    private final Map<String, String> partitionSpec;
    private final long modificationTime;
    public List<DataFileMeta> dataFiles;

    public FormatDataSplit(
            FileIO fileIO,
            Path filePath,
            long offset,
            long length,
            RowType rowType,
            @Nullable Predicate predicate,
            @Nullable int[] projection) {
        this(
                fileIO,
                filePath,
                offset,
                length,
                rowType,
                predicate,
                projection,
                Collections.emptyMap(),
                System.currentTimeMillis());
    }

    public FormatDataSplit(
            FileIO fileIO,
            Path filePath,
            long offset,
            long length,
            RowType rowType,
            @Nullable Predicate predicate,
            @Nullable int[] projection,
            Map<String, String> partitionSpec,
            long modificationTime) {
        this.fileIO = fileIO;
        this.filePath = filePath;
        this.offset = offset;
        this.length = length;
        this.rowType = rowType;
        this.predicate = predicate;
        this.projection = projection;
        this.partitionSpec = partitionSpec;
        this.modificationTime = modificationTime;
        this.dataFiles = new ArrayList<>();
        dataFiles.add(
                new DataFileMeta(
                        filePath.getName(),
                        length,
                        length,
                        null,
                        null,
                        null,
                        null,
                        0L,
                        0L,
                        0L,
                        0,
                        Collections.emptyList(),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null));
    }

    @Override
    public BinaryRow partition() {
        return new BinaryRow(0);
    }

    @Override
    public int bucket() {
        return NO_NEED_BUCKET;
    }

    public FileIO fileIO() {
        return fileIO;
    }

    public Path path() {
        return filePath;
    }

    public long offset() {
        return offset;
    }

    public long length() {
        return length;
    }

    public RowType rowType() {
        return rowType;
    }

    public List<DataFileMeta> dataFiles() {
        return dataFiles;
    }

    @Nullable
    public Predicate predicate() {
        return predicate;
    }

    @Nullable
    public int[] projection() {
        return projection;
    }

    public Map<String, String> partitionSpec() {
        return partitionSpec;
    }

    @Override
    public long rowCount() {
        return -1;
    }

    @Override
    public Optional<List<RawFile>> convertToRawFiles() {
        return Optional.empty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FormatDataSplit that = (FormatDataSplit) o;
        return offset == that.offset
                && length == that.length
                && modificationTime == that.modificationTime
                && Objects.equals(filePath, that.filePath)
                && Objects.equals(rowType, that.rowType)
                && Objects.equals(predicate, that.predicate)
                && Arrays.equals(projection, that.projection)
                && Objects.equals(partitionSpec, that.partitionSpec);
    }

    @Override
    public int hashCode() {
        int result =
                Objects.hash(
                        filePath,
                        offset,
                        length,
                        rowType,
                        predicate,
                        partitionSpec,
                        modificationTime);
        result = 31 * result + Arrays.hashCode(projection);
        return result;
    }

    //    public BinaryRow generatePartition(Map<String, Object> partitionMap) {
    //        return new BinaryRow(0);
    //                if (partitionKeys.isEmpty()) {
    //                    // Non-partitioned table returns empty BinaryRow
    //                    return new BinaryRow(0);
    //                }
    //
    //                // 1. Create GenericRow from partition map
    //                GenericRow genericRow = new GenericRow(partitionKeys.size());
    //
    //                for (int i = 0; i < partitionKeys.size(); i++) {
    //                    String partitionKey = partitionKeys.get(i);
    //                    Object value = partitionMap.get(partitionKey);
    //
    //                    // Convert and validate partition value
    //                    Object convertedValue = convertPartitionValue(value,
    //         partitionType.getTypeAt(i));
    //                    genericRow.setField(i, convertedValue);
    //                }
    //
    //                // 2. Serialize GenericRow to BinaryRow
    //                return serializer.toBinaryRow(genericRow);
    //    }

    @Override
    public String toString() {
        return "FormatDataSplit{"
                + "filePath="
                + filePath
                + ", offset="
                + offset
                + ", length="
                + length
                + ", rowType="
                + rowType
                + ", predicate="
                + predicate
                + ", projection="
                + Arrays.toString(projection)
                + ", partitionSpec="
                + partitionSpec
                + ", modificationTime="
                + modificationTime
                + '}';
    }
}
