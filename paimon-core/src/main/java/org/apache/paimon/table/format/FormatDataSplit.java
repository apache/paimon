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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** {@link FormatDataSplit} for format table. */
public class FormatDataSplit implements Split {

    private final Path filePath;
    private final long offset;
    private final long length;
    private final RowType rowType;
    private final Predicate predicate;
    private final int[] projection;
    private final Map<String, String> partitionSpec;
    private final long modificationTime;

    public FormatDataSplit(
            FileIO fileIO,
            Path filePath,
            long offset,
            long length,
            RowType rowType,
            Map<String, String> partitionSpec,
            long modificationTime,
            @Nullable Predicate predicate,
            @Nullable int[] projection) {
        this.filePath = filePath;
        this.offset = offset;
        this.length = length;
        this.rowType = rowType;
        this.predicate = predicate;
        this.projection = projection;
        this.partitionSpec = partitionSpec;
        this.modificationTime = modificationTime;
    }

    public Path filePath() {
        return this.filePath;
    }

    public Path dataPath() {
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

    @Nullable
    public Predicate predicate() {
        return predicate;
    }

    @Nullable
    public int[] projection() {
        return projection;
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

    //    public void serialize(DataOutputView out) throws IOException {
    //        out.writeLong(MAGIC);
    //        out.writeInt(VERSION);
    //        out.writeLong(snapshotId);
    //        out.writeBoolean(true);
    //    }
    //
    //    public static FormatDataSplit deserialize(DataInputView in) throws IOException {
    //        long magic = in.readLong();
    //        int version = magic == MAGIC ? in.readInt() : 1;
    //        // version 1 does not write magic number in, so the first long is snapshot id.
    //        long snapshotId = version == 1 ? magic : in.readLong();
    //        String bucketPath = in.readUTF();
    //        boolean rawConvertible = in.readBoolean();
    //
    //        FormatDataSplit split = new FormatDataSplit(null, null, 0, 0, null, null, 0, null,
    // null);
    //        return split;
    //    }

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
