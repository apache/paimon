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
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.utils.Filter;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static org.apache.paimon.utils.SerializationUtils.newBytesType;

/** Entry of a manifest file, representing an addition / deletion of a data file. */
public class ManifestEntry implements FileEntry {

    private final FileKind kind;
    // for tables without partition this field should be a row with 0 columns (not null)
    private final BinaryRow partition;
    private final int bucket;
    private final int totalBuckets;
    private final DataFileMeta file;

    public ManifestEntry(
            FileKind kind, BinaryRow partition, int bucket, int totalBuckets, DataFileMeta file) {
        this.kind = kind;
        this.partition = partition;
        this.bucket = bucket;
        this.totalBuckets = totalBuckets;
        this.file = file;
    }

    @Override
    public FileKind kind() {
        return kind;
    }

    @Override
    public BinaryRow partition() {
        return partition;
    }

    @Override
    public int bucket() {
        return bucket;
    }

    @Override
    public int level() {
        return file.level();
    }

    @Override
    public String fileName() {
        return file.fileName();
    }

    @Override
    public BinaryRow minKey() {
        return file.minKey();
    }

    @Override
    public BinaryRow maxKey() {
        return file.maxKey();
    }

    public int totalBuckets() {
        return totalBuckets;
    }

    public DataFileMeta file() {
        return file;
    }

    @Override
    public Identifier identifier() {
        return new Identifier(partition, bucket, file.level(), file.fileName());
    }

    public static RowType schema() {
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(0, "_KIND", new TinyIntType(false)));
        fields.add(new DataField(1, "_PARTITION", newBytesType(false)));
        fields.add(new DataField(2, "_BUCKET", new IntType(false)));
        fields.add(new DataField(3, "_TOTAL_BUCKETS", new IntType(false)));
        fields.add(new DataField(4, "_FILE", DataFileMeta.schema()));
        return new RowType(false, fields);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ManifestEntry)) {
            return false;
        }
        ManifestEntry that = (ManifestEntry) o;
        return Objects.equals(kind, that.kind)
                && Objects.equals(partition, that.partition)
                && bucket == that.bucket
                && totalBuckets == that.totalBuckets
                && Objects.equals(file, that.file);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, partition, bucket, totalBuckets, file);
    }

    @Override
    public String toString() {
        return String.format("{%s, %s, %d, %d, %s}", kind, partition, bucket, totalBuckets, file);
    }

    /**
     * According to the {@link ManifestCacheFilter}, entry that needs to be cached will be retained,
     * so the entry that will not be accessed in the future will not be cached.
     *
     * <p>Implemented to {@link InternalRow} is for performance (No deserialization).
     */
    public static Filter<InternalRow> createCacheRowFilter(
            @Nullable ManifestCacheFilter manifestCacheFilter, int numOfBuckets) {
        if (manifestCacheFilter == null) {
            return Filter.alwaysTrue();
        }

        Function<InternalRow, BinaryRow> partitionGetter =
                ManifestEntrySerializer.partitionGetter();
        Function<InternalRow, Integer> bucketGetter = ManifestEntrySerializer.bucketGetter();
        Function<InternalRow, Integer> totalBucketGetter =
                ManifestEntrySerializer.totalBucketGetter();
        return row -> {
            if (numOfBuckets != totalBucketGetter.apply(row)) {
                return true;
            }

            return manifestCacheFilter.test(partitionGetter.apply(row), bucketGetter.apply(row));
        };
    }

    /**
     * Read the corresponding entries based on the current required partition and bucket.
     *
     * <p>Implemented to {@link InternalRow} is for performance (No deserialization).
     */
    public static Filter<InternalRow> createEntryRowFilter(
            @Nullable PartitionPredicate partitionFilter,
            @Nullable Filter<Integer> bucketFilter,
            @Nullable Filter<String> fileNameFilter,
            int numOfBuckets) {
        Function<InternalRow, BinaryRow> partitionGetter =
                ManifestEntrySerializer.partitionGetter();
        Function<InternalRow, Integer> bucketGetter = ManifestEntrySerializer.bucketGetter();
        Function<InternalRow, Integer> totalBucketGetter =
                ManifestEntrySerializer.totalBucketGetter();
        Function<InternalRow, String> fileNameGetter = ManifestEntrySerializer.fileNameGetter();
        return row -> {
            if ((partitionFilter != null && !partitionFilter.test(partitionGetter.apply(row)))) {
                return false;
            }

            if (bucketFilter != null
                    && numOfBuckets == totalBucketGetter.apply(row)
                    && !bucketFilter.test(bucketGetter.apply(row))) {
                return false;
            }

            if (fileNameFilter != null && !fileNameFilter.test((fileNameGetter.apply(row)))) {
                return false;
            }

            return true;
        };
    }
}
