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

package org.apache.paimon.table;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.bucket.BucketFunction;
import org.apache.paimon.codegen.CodeGenUtils;
import org.apache.paimon.codegen.Projection;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.postpone.PostponeBucketFileStoreWrite;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.CoreOptions.COMMIT_STRICT_MODE_LAST_SAFE_SNAPSHOT;
import static org.apache.paimon.CoreOptions.WRITE_ONLY;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Utils for postpone table. */
public class PostponeUtils {

    /**
     * Groups each partition's postpone files by writer and creation time. Equal creation times keep
     * scan order; different writers have no relative order.
     */
    public static List<DataSplit> groupPostponeFiles(List<DataSplit> splits) {
        Map<Pair<BinaryRow, String>, List<PostponeFile>> filesByWriter = new LinkedHashMap<>();
        for (DataSplit split : splits) {
            if (split.isStreaming() || split.bucket() != BucketMode.POSTPONE_BUCKET) {
                throw new IllegalArgumentException(
                        "Postpone file grouping requires batch splits from the postpone bucket.");
            }

            List<DataFileMeta> dataFiles = split.dataFiles();
            List<DeletionFile> deletionFiles = split.deletionFiles().orElse(null);
            for (int fileOrder = 0; fileOrder < dataFiles.size(); fileOrder++) {
                DataFileMeta file = dataFiles.get(fileOrder);
                Pair<BinaryRow, String> writer =
                        Pair.of(
                                split.partition(),
                                PostponeBucketFileStoreWrite.getWriterPrefix(file.fileName()));
                filesByWriter
                        .computeIfAbsent(writer, ignored -> new ArrayList<>())
                        .add(
                                new PostponeFile(
                                        split,
                                        file,
                                        deletionFiles == null ? null : deletionFiles.get(fileOrder),
                                        deletionFiles != null));
            }
        }

        List<DataSplit> result = new ArrayList<>(filesByWriter.size());
        for (List<PostponeFile> writerFiles : filesByWriter.values()) {
            writerFiles.sort(Comparator.comparingLong(file -> file.file.creationTimeEpochMillis()));

            PostponeFile first = writerFiles.get(0);
            List<DataFileMeta> dataFiles = new ArrayList<>(writerFiles.size());
            List<DeletionFile> deletionFiles = new ArrayList<>(writerFiles.size());
            boolean hasDeletionFiles = false;
            boolean rawConvertible = true;
            for (PostponeFile file : writerFiles) {
                dataFiles.add(file.file);
                deletionFiles.add(file.deletionFile);
                hasDeletionFiles |= file.deletionFilesPresent;
                rawConvertible &= file.split.rawConvertible();
            }

            DataSplit split = first.split;
            DataSplit.Builder builder =
                    DataSplit.builder()
                            .withSnapshot(split.snapshotId())
                            .withPartition(split.partition())
                            .withBucket(split.bucket())
                            .withBucketPath(split.bucketPath())
                            .withTotalBuckets(split.totalBuckets())
                            .withDataFiles(dataFiles)
                            .isStreaming(false)
                            .rawConvertible(rawConvertible);
            if (hasDeletionFiles) {
                builder.withDataDeletionFiles(deletionFiles);
            }
            result.add(builder.build());
        }
        return result;
    }

    public static PostponeBucketAssigner createPostponeBucketAssigner(
            FileStoreTable table, long snapshotId, int defaultParallelism) {
        return createPostponeBucketAssigner(table, snapshotId, defaultParallelism, null);
    }

    private static PostponeBucketAssigner createPostponeBucketAssigner(
            FileStoreTable table,
            long snapshotId,
            int defaultParallelism,
            @Nullable PartitionPredicate partitionFilter) {
        Map<BinaryRow, Integer> knownNumBuckets =
                getKnownNumBuckets(table, snapshotId, partitionFilter);
        Map<BinaryRow, Long> postponeRowCounts =
                !table.coreOptions().postponeTargetRowNumPerBucket().isPresent()
                        ? Collections.emptyMap()
                        : getPostponeRowCounts(table, snapshotId, partitionFilter);
        return createPostponeBucketAssigner(
                table, knownNumBuckets, postponeRowCounts, defaultParallelism);
    }

    private static PostponeBucketAssigner createPostponeBucketAssigner(
            FileStoreTable table,
            Map<BinaryRow, Integer> knownNumBuckets,
            Map<BinaryRow, Long> postponeRowCounts,
            int defaultParallelism) {
        Long targetRowNumPerBucket =
                table.coreOptions().postponeTargetRowNumPerBucket().orElse(null);
        int defaultBucketNum =
                table.coreOptions()
                                .toConfiguration()
                                .contains(CoreOptions.POSTPONE_DEFAULT_BUCKET_NUM)
                        ? table.coreOptions().postponeDefaultBucketNum()
                        : defaultParallelism;
        return new PostponeBucketAssigner(
                knownNumBuckets, targetRowNumPerBucket, postponeRowCounts, defaultBucketNum);
    }

    public static PostponeBucketRouter createPostponeBucketRouter(
            FileStoreTable table,
            long snapshotId,
            int defaultParallelism,
            @Nullable PartitionPredicate partitionFilter) {
        return createPostponeBucketRouter(
                table,
                createPostponeBucketAssigner(
                        table, snapshotId, defaultParallelism, partitionFilter));
    }

    private static PostponeBucketRouter createPostponeBucketRouter(
            FileStoreTable table, PostponeBucketAssigner bucketAssigner) {
        List<String> trimmedPrimaryKeys = table.schema().trimmedPrimaryKeys();
        int[] bucketKeyMapping =
                table.schema().bucketKeys().stream()
                        .mapToInt(trimmedPrimaryKeys::indexOf)
                        .toArray();
        for (int index : bucketKeyMapping) {
            if (index < 0) {
                throw new IllegalArgumentException(
                        "Postpone bucket key must be part of the trimmed primary key.");
            }
        }
        RowType keyType =
                new RowType(
                        PrimaryKeyTableUtils.PrimaryKeyFieldsExtractor.EXTRACTOR.keyFields(
                                table.schema()));
        return new PostponeBucketRouter(
                bucketAssigner,
                keyType,
                table.schema().logicalBucketKeyType(),
                bucketKeyMapping,
                table.coreOptions().bucketFunctionType());
    }

    public static int computeBucketNumByRowCount(long rowCount, long targetRowNumPerBucket) {
        if (targetRowNumPerBucket <= 0) {
            throw new IllegalArgumentException(
                    "Option 'postpone.target-row-num-per-bucket' must be greater than 0.");
        }

        long bucketNum = rowCount <= 0 ? 1 : (rowCount - 1) / targetRowNumPerBucket + 1;
        if (bucketNum > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "Computed postpone bucket number "
                            + bucketNum
                            + " exceeds the maximum integer value (Integer.MAX_VALUE = "
                            + Integer.MAX_VALUE
                            + "). Consider increasing 'postpone.target-row-num-per-bucket' "
                            + "to reduce the bucket count.");
        }
        return (int) bucketNum;
    }

    public static int determineBucketNum(
            BinaryRow partition,
            Map<BinaryRow, Integer> knownNumBuckets,
            Optional<Long> targetRowNumPerBucket,
            Map<BinaryRow, Long> postponeRowCounts,
            int defaultBucketNum) {
        return determineBucketNum(
                partition,
                knownNumBuckets,
                targetRowNumPerBucket.orElse(null),
                postponeRowCounts,
                defaultBucketNum);
    }

    public static int determineBucketNum(
            BinaryRow partition,
            Map<BinaryRow, Integer> knownNumBuckets,
            @Nullable Long targetRowNumPerBucket,
            Map<BinaryRow, Long> postponeRowCounts,
            int defaultBucketNum) {
        Integer knownBucketNum = knownNumBuckets.get(partition);
        if (knownBucketNum != null) {
            return knownBucketNum;
        } else if (targetRowNumPerBucket != null) {
            return computeBucketNumByRowCount(
                    postponeRowCounts.getOrDefault(partition, 0L), targetRowNumPerBucket);
        } else {
            return defaultBucketNum;
        }
    }

    public static Map<BinaryRow, Integer> getKnownNumBuckets(FileStoreTable table) {
        return getKnownNumBuckets(
                table.store().newScan().onlyReadRealBuckets().readSimpleEntries());
    }

    public static Map<BinaryRow, Integer> getKnownNumBuckets(
            FileStoreTable table, long snapshotId) {
        return getKnownNumBuckets(table, snapshotId, null);
    }

    static Map<BinaryRow, Integer> getKnownNumBuckets(
            FileStoreTable table, long snapshotId, @Nullable PartitionPredicate partitionFilter) {
        FileStoreScan scan = table.store().newScan().withSnapshot(snapshotId).onlyReadRealBuckets();
        if (partitionFilter != null) {
            scan.withPartitionFilter(partitionFilter);
        }
        return getKnownNumBuckets(scan.readSimpleEntries());
    }

    private static Map<BinaryRow, Integer> getKnownNumBuckets(
            List<SimpleFileEntry> simpleFileEntries) {
        Map<BinaryRow, Integer> knownNumBuckets = new HashMap<>();
        for (SimpleFileEntry entry : simpleFileEntries) {
            if (entry.totalBuckets() >= 0) {
                Integer oldTotalBuckets =
                        knownNumBuckets.put(entry.partition(), entry.totalBuckets());
                if (oldTotalBuckets != null && oldTotalBuckets != entry.totalBuckets()) {
                    throw new IllegalStateException(
                            "Partition "
                                    + entry.partition()
                                    + " has different totalBuckets "
                                    + oldTotalBuckets
                                    + " and "
                                    + entry.totalBuckets());
                }
            }
        }
        return knownNumBuckets;
    }

    /** Returns real buckets containing active Level-0 files in the specified snapshot. */
    public static List<CompactBucket> getLevel0Buckets(FileStoreTable table, long snapshotId) {
        List<SimpleFileEntry> entries =
                table.store()
                        .newScan()
                        .withSnapshot(snapshotId)
                        .onlyReadRealBuckets()
                        .readSimpleEntries();
        Set<CompactBucket> buckets = new LinkedHashSet<>();
        for (SimpleFileEntry entry : entries) {
            if (entry.bucket() >= 0 && entry.totalBuckets() > 0 && entry.level() == 0) {
                buckets.add(
                        new CompactBucket(entry.partition(), entry.bucket(), entry.totalBuckets()));
            }
        }
        return new ArrayList<>(buckets);
    }

    /** Returns row counts of current active files in the postpone bucket. */
    public static Map<BinaryRow, Long> getPostponeRowCounts(FileStoreTable table) {
        return getPostponeRowCounts(
                table.newSnapshotReader()
                        .withBucket(BucketMode.POSTPONE_BUCKET)
                        .readFileIterator());
    }

    /** Returns row counts of active postpone files in the specified snapshot. */
    public static Map<BinaryRow, Long> getPostponeRowCounts(FileStoreTable table, long snapshotId) {
        return getPostponeRowCounts(table, snapshotId, null);
    }

    static Map<BinaryRow, Long> getPostponeRowCounts(
            FileStoreTable table, long snapshotId, @Nullable PartitionPredicate partitionFilter) {
        SnapshotReader reader =
                table.newSnapshotReader()
                        .withSnapshot(snapshotId)
                        .withBucket(BucketMode.POSTPONE_BUCKET);
        if (partitionFilter != null) {
            reader.withPartitionFilter(partitionFilter);
        }
        return getPostponeRowCounts(reader.readFileIterator());
    }

    private static Map<BinaryRow, Long> getPostponeRowCounts(Iterator<ManifestEntry> iterator) {
        Map<BinaryRow, Long> rowCounts = new HashMap<>();
        while (iterator.hasNext()) {
            ManifestEntry entry = iterator.next();
            rowCounts.merge(entry.partition(), entry.file().rowCount(), Long::sum);
        }
        return rowCounts;
    }

    public static FileStoreTable tableForFixBucketWrite(FileStoreTable table) {
        Map<String, String> batchWriteOptions = new HashMap<>();
        batchWriteOptions.put(WRITE_ONLY.key(), "true");
        // It's just used to create merge tree writer for writing files to fixed bucket.
        // The real bucket number is determined at runtime.
        batchWriteOptions.put(BUCKET.key(), "1");
        return table.copy(batchWriteOptions);
    }

    public static FileStoreTable tableForPostponeCompact(
            FileStoreTable table, int numBuckets, long snapshotId) {
        Map<String, String> compactOptions = new HashMap<>();
        compactOptions.put(BUCKET.key(), String.valueOf(numBuckets));
        compactOptions.put(WRITE_ONLY.key(), "false");
        compactOptions.put(COMMIT_STRICT_MODE_LAST_SAFE_SNAPSHOT.key(), String.valueOf(snapshotId));
        return table.copy(compactOptions);
    }

    public static FileStoreTable tableForCommit(FileStoreTable table) {
        return table.copy(
                Collections.singletonMap(BUCKET.key(), String.valueOf(BucketMode.POSTPONE_BUCKET)));
    }

    /** Snapshot-bound bucket-count assignment. */
    public static final class PostponeBucketAssigner implements Serializable {

        private static final long serialVersionUID = 1L;

        private final Map<BinaryRow, Integer> knownNumBuckets;
        @Nullable private final Long targetRowNumPerBucket;
        private final Map<BinaryRow, Long> postponeRowCounts;
        private final int defaultBucketNum;

        private PostponeBucketAssigner(
                Map<BinaryRow, Integer> knownNumBuckets,
                @Nullable Long targetRowNumPerBucket,
                Map<BinaryRow, Long> postponeRowCounts,
                int defaultBucketNum) {
            this.knownNumBuckets = knownNumBuckets;
            this.targetRowNumPerBucket = targetRowNumPerBucket;
            this.postponeRowCounts = postponeRowCounts;
            this.defaultBucketNum = defaultBucketNum;
        }

        public int assign(BinaryRow partition) {
            return determineBucketNum(
                    partition,
                    knownNumBuckets,
                    targetRowNumPerBucket,
                    postponeRowCounts,
                    defaultBucketNum);
        }

        private PostponeBucketAssigner withDefaultBucketNum(int newDefaultBucketNum) {
            return new PostponeBucketAssigner(
                    knownNumBuckets, targetRowNumPerBucket, postponeRowCounts, newDefaultBucketNum);
        }
    }

    /** Snapshot-bound routing metadata for postpone records. */
    public static final class PostponeBucketRouter implements Serializable {

        private static final long serialVersionUID = 1L;

        private final PostponeBucketAssigner bucketAssigner;
        private final RowType keyType;
        private final RowType bucketKeyType;
        private final int[] bucketKeyMapping;
        private final CoreOptions.BucketFunctionType bucketFunctionType;
        @Nullable private transient Projection bucketKeyProjection;
        @Nullable private transient BucketFunction bucketFunction;

        private PostponeBucketRouter(
                PostponeBucketAssigner bucketAssigner,
                RowType keyType,
                RowType bucketKeyType,
                int[] bucketKeyMapping,
                CoreOptions.BucketFunctionType bucketFunctionType) {
            this.bucketAssigner = bucketAssigner;
            this.keyType = keyType;
            this.bucketKeyType = bucketKeyType;
            this.bucketKeyMapping = bucketKeyMapping;
            this.bucketFunctionType = bucketFunctionType;
        }

        public int bucket(BinaryRow partition, InternalRow trimmedPrimaryKey) {
            if (bucketKeyProjection == null) {
                bucketKeyProjection = CodeGenUtils.newProjection(keyType, bucketKeyMapping);
            }
            if (bucketFunction == null) {
                bucketFunction = BucketFunction.create(bucketFunctionType, bucketKeyType);
            }
            return bucketFunction.bucket(
                    bucketKeyProjection.apply(trimmedPrimaryKey), numBuckets(partition));
        }

        public int numBuckets(BinaryRow partition) {
            return bucketAssigner.assign(partition);
        }

        public PostponeBucketRouter withDefaultBucketNum(int newDefaultBucketNum) {
            checkArgument(
                    newDefaultBucketNum > 0, "Default postpone bucket number must be positive.");
            return new PostponeBucketRouter(
                    bucketAssigner.withDefaultBucketNum(newDefaultBucketNum),
                    keyType,
                    bucketKeyType,
                    bucketKeyMapping,
                    bucketFunctionType);
        }
    }

    private static final class PostponeFile {

        private final DataSplit split;
        private final DataFileMeta file;
        @Nullable private final DeletionFile deletionFile;
        private final boolean deletionFilesPresent;

        private PostponeFile(
                DataSplit split,
                DataFileMeta file,
                @Nullable DeletionFile deletionFile,
                boolean deletionFilesPresent) {
            this.split = split;
            this.file = file;
            this.deletionFile = deletionFile;
            this.deletionFilesPresent = deletionFilesPresent;
        }
    }

    /** A real bucket which requires background compaction. */
    public static final class CompactBucket implements Serializable {

        private static final long serialVersionUID = 1L;

        private final BinaryRow partition;
        private final int bucket;
        private final int totalBuckets;

        public CompactBucket(BinaryRow partition, int bucket, int totalBuckets) {
            this.partition = partition.copy();
            this.bucket = bucket;
            this.totalBuckets = totalBuckets;
        }

        public BinaryRow partition() {
            return partition;
        }

        public int bucket() {
            return bucket;
        }

        public int totalBuckets() {
            return totalBuckets;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof CompactBucket)) {
                return false;
            }
            CompactBucket that = (CompactBucket) o;
            return bucket == that.bucket
                    && totalBuckets == that.totalBuckets
                    && Objects.equals(partition, that.partition);
        }

        @Override
        public int hashCode() {
            return Objects.hash(partition, bucket, totalBuckets);
        }
    }
}
