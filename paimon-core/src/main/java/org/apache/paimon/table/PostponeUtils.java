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
import java.math.BigInteger;
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

    public static PostponeBucketNumResolver createPostponeBucketNumResolver(
            FileStoreTable table, long snapshotId) {
        return loadPostponeBucketNumResolver(table, snapshotId, null);
    }

    private static PostponeBucketNumResolver loadPostponeBucketNumResolver(
            FileStoreTable table, long snapshotId, @Nullable List<BinaryRow> postponePartitions) {
        CoreOptions options = table.coreOptions();
        Map<BinaryRow, Integer> numBucketsByPartition =
                postponePartitions == null
                        ? getKnownNumBuckets(table, snapshotId)
                        : getKnownNumBuckets(table, snapshotId, postponePartitions);
        Integer configuredDefaultBucketNum = options.postponeDefaultBucketNum().orElse(null);
        if (configuredDefaultBucketNum == null) {
            Optional<Long> targetRowNumPerBucket = options.postponeTargetRowNumPerBucket();
            if (targetRowNumPerBucket.isPresent()) {
                checkArgument(
                        targetRowNumPerBucket.get() > 0,
                        "Option '%s' must be greater than 0.",
                        CoreOptions.POSTPONE_TARGET_ROW_NUM_PER_BUCKET.key());
                addEstimatedBucketNums(
                        numBucketsByPartition,
                        getPostponeRowCounts(
                                postponeFileIterator(table, snapshotId, postponePartitions)),
                        targetRowNumPerBucket.get(),
                        CoreOptions.POSTPONE_TARGET_ROW_NUM_PER_BUCKET.key());
            } else {
                long targetSizePerBucket = options.postponeTargetSizePerBucket();
                checkArgument(
                        targetSizePerBucket > 0,
                        "Option '%s' must be greater than 0.",
                        CoreOptions.POSTPONE_TARGET_SIZE_PER_BUCKET.key());
                addEstimatedBucketNums(
                        numBucketsByPartition,
                        getPostponeFileSizes(
                                postponeFileIterator(table, snapshotId, postponePartitions)),
                        targetSizePerBucket,
                        CoreOptions.POSTPONE_TARGET_SIZE_PER_BUCKET.key());
            }
        }
        return new PostponeBucketNumResolver(numBucketsByPartition, configuredDefaultBucketNum);
    }

    /** Creates snapshot-bound routing metadata for partitions containing postpone files. */
    public static PostponeBucketRouter createPostponeBucketRouter(
            FileStoreTable table, long snapshotId, List<BinaryRow> postponePartitions) {
        return newPostponeBucketRouter(
                table, loadPostponeBucketNumResolver(table, snapshotId, postponePartitions));
    }

    /** Creates routing metadata from bucket numbers decided by an execution engine. */
    public static PostponeBucketRouter createPostponeBucketRouter(
            FileStoreTable table, Map<BinaryRow, Integer> numBucketsByPartition) {
        Map<BinaryRow, Integer> copied = new HashMap<>();
        for (Map.Entry<BinaryRow, Integer> entry : numBucketsByPartition.entrySet()) {
            checkArgument(
                    entry.getValue() != null && entry.getValue() > 0,
                    "Postpone bucket number must be positive.");
            copied.put(entry.getKey().copy(), entry.getValue());
        }
        return newPostponeBucketRouter(table, new PostponeBucketNumResolver(copied, null));
    }

    private static PostponeBucketRouter newPostponeBucketRouter(
            FileStoreTable table, PostponeBucketNumResolver bucketNumResolver) {
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
                bucketNumResolver,
                keyType,
                table.schema().logicalBucketKeyType(),
                bucketKeyMapping,
                table.coreOptions().bucketFunctionType());
    }

    public static int computeBucketNumByRowCount(long rowCount, long targetRowNumPerBucket) {
        return computeBucketNum(
                rowCount,
                targetRowNumPerBucket,
                CoreOptions.POSTPONE_TARGET_ROW_NUM_PER_BUCKET.key());
    }

    private static int computeBucketNum(
            long value, long targetValuePerBucket, String targetOptionKey) {
        checkArgument(
                targetValuePerBucket > 0, "Option '%s' must be greater than 0.", targetOptionKey);
        long bucketNum = value <= 0 ? 1 : (value - 1) / targetValuePerBucket + 1;
        if (bucketNum > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "Computed postpone bucket number "
                            + bucketNum
                            + " exceeds the maximum integer value (Integer.MAX_VALUE = "
                            + Integer.MAX_VALUE
                            + "). Consider increasing '"
                            + targetOptionKey
                            + "' "
                            + "to reduce the bucket count.");
        }
        return (int) bucketNum;
    }

    private static void addEstimatedBucketNums(
            Map<BinaryRow, Integer> numBucketsByPartition,
            Map<BinaryRow, Long> valuesByPartition,
            long targetValuePerBucket,
            String targetOptionKey) {
        for (Map.Entry<BinaryRow, Long> entry : valuesByPartition.entrySet()) {
            if (!numBucketsByPartition.containsKey(entry.getKey())) {
                numBucketsByPartition.put(
                        entry.getKey(),
                        computeBucketNum(entry.getValue(), targetValuePerBucket, targetOptionKey));
            }
        }
    }

    static int determineBucketNum(
            BinaryRow partition,
            Map<BinaryRow, Integer> numBucketsByPartition,
            @Nullable Integer configuredDefaultBucketNum) {
        Integer numBuckets = numBucketsByPartition.get(partition);
        if (numBuckets != null) {
            return numBuckets;
        } else if (configuredDefaultBucketNum != null) {
            return configuredDefaultBucketNum;
        } else {
            throw new IllegalArgumentException(
                    "Missing postpone bucket number for partition " + partition + ".");
        }
    }

    /**
     * Decides the target bucket number from an exactly measured staged batch.
     *
     * <p>Previously committed postpone files are intentionally excluded. An existing layout is
     * retained while the staged batch's required bucket number stays within the configured rescale
     * load factor. This avoids paying for a layout rewrite for ordinary size differences while
     * protecting a large batch from being funneled into too few writers.
     */
    public static FixedBucketDecision decideFixedBucketNum(
            long stagedRowCount,
            long stagedFileSize,
            @Nullable Integer existingBucketNum,
            CoreOptions options) {
        checkArgument(stagedRowCount >= 0, "Staged row count cannot be negative.");
        checkArgument(stagedFileSize >= 0, "Staged file size cannot be negative.");
        checkArgument(
                existingBucketNum == null || existingBucketNum > 0,
                "Existing bucket number must be positive.");

        Optional<Integer> configuredDefaultBucketNum = options.postponeDefaultBucketNum();
        if (existingBucketNum == null && configuredDefaultBucketNum.isPresent()) {
            return new FixedBucketDecision(configuredDefaultBucketNum.get(), false);
        }

        int maxBucketNum = options.postponeBatchWriteFixedBucketMaxParallelism();
        checkArgument(
                maxBucketNum > 0,
                "Option '%s' must be greater than 0.",
                CoreOptions.POSTPONE_BATCH_WRITE_FIXED_BUCKET_MAX_PARALLELISM.key());
        int rescaleLoadFactor = options.postponeBatchWriteFixedBucketRescaleLoadFactor();
        checkArgument(
                rescaleLoadFactor > 0,
                "Option '%s' must be greater than 0.",
                CoreOptions.POSTPONE_BATCH_WRITE_FIXED_BUCKET_RESCALE_LOAD_FACTOR.key());

        BigInteger requiredBucketNum;
        if (options.postponeTargetRowNumPerBucket().isPresent()) {
            long targetRowCount = options.postponeTargetRowNumPerBucket().get();
            checkArgument(
                    targetRowCount > 0,
                    "Option '%s' must be greater than 0.",
                    CoreOptions.POSTPONE_TARGET_ROW_NUM_PER_BUCKET.key());
            requiredBucketNum =
                    ceilDiv(BigInteger.valueOf(stagedRowCount), BigInteger.valueOf(targetRowCount));
        } else {
            long targetSize = options.postponeTargetSizePerBucket();
            checkArgument(
                    targetSize > 0,
                    "Option '%s' must be greater than 0.",
                    CoreOptions.POSTPONE_TARGET_SIZE_PER_BUCKET.key());
            requiredBucketNum =
                    ceilDiv(BigInteger.valueOf(stagedFileSize), BigInteger.valueOf(targetSize));
        }

        requiredBucketNum = requiredBucketNum.max(BigInteger.ONE);
        int suggestedBucketNum = roundUpToPowerOfTwo(requiredBucketNum, maxBucketNum);
        boolean requiresRescale =
                existingBucketNum != null
                        && requiredBucketNum.compareTo(
                                        BigInteger.valueOf(existingBucketNum)
                                                .multiply(BigInteger.valueOf(rescaleLoadFactor)))
                                > 0
                        && suggestedBucketNum > existingBucketNum;
        int targetBucketNum;
        if (existingBucketNum == null) {
            targetBucketNum = suggestedBucketNum;
        } else if (requiresRescale) {
            targetBucketNum = suggestedBucketNum;
        } else {
            targetBucketNum = existingBucketNum;
        }
        return new FixedBucketDecision(targetBucketNum, requiresRescale);
    }

    private static BigInteger ceilDiv(BigInteger value, BigInteger divisor) {
        checkArgument(divisor.signum() > 0, "Bucket target must be positive.");
        if (value.signum() <= 0) {
            return BigInteger.ZERO;
        }
        return value.subtract(BigInteger.ONE).divide(divisor).add(BigInteger.ONE);
    }

    private static int roundUpToPowerOfTwo(BigInteger value, int upperBound) {
        int cappedValue = value.min(BigInteger.valueOf(upperBound)).intValueExact();
        if (cappedValue <= 1) {
            return 1;
        }

        long roundedValue = (long) Integer.highestOneBit(cappedValue - 1) << 1;
        return (int) Math.min(roundedValue, upperBound);
    }

    public static Map<BinaryRow, Integer> getKnownNumBuckets(FileStoreTable table) {
        return getKnownNumBuckets(
                table.store().newScan().onlyReadRealBuckets().readSimpleEntries());
    }

    public static Map<BinaryRow, Integer> getKnownNumBuckets(
            FileStoreTable table, long snapshotId) {
        return getKnownNumBuckets(table, snapshotId, (PartitionPredicate) null);
    }

    /** Returns known real-bucket counts only for the specified partitions. */
    public static Map<BinaryRow, Integer> getKnownNumBuckets(
            FileStoreTable table, long snapshotId, List<BinaryRow> partitions) {
        if (partitions.isEmpty()) {
            return Collections.emptyMap();
        }
        return getKnownNumBuckets(
                table.store()
                        .newScan()
                        .withSnapshot(snapshotId)
                        .withPartitionFilter(partitions)
                        .onlyReadRealBuckets()
                        .readSimpleEntries());
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

    private static Iterator<ManifestEntry> postponeFileIterator(
            FileStoreTable table, long snapshotId, @Nullable List<BinaryRow> postponePartitions) {
        SnapshotReader reader =
                table.newSnapshotReader()
                        .withSnapshot(snapshotId)
                        .withBucket(BucketMode.POSTPONE_BUCKET);
        if (postponePartitions != null) {
            reader.withPartitionFilter(postponePartitions);
        }
        return reader.readFileIterator();
    }

    static Map<BinaryRow, Long> getPostponeFileSizes(
            FileStoreTable table, long snapshotId, @Nullable PartitionPredicate partitionFilter) {
        SnapshotReader reader =
                table.newSnapshotReader()
                        .withSnapshot(snapshotId)
                        .withBucket(BucketMode.POSTPONE_BUCKET);
        if (partitionFilter != null) {
            reader.withPartitionFilter(partitionFilter);
        }

        return getPostponeFileSizes(reader.readFileIterator());
    }

    private static Map<BinaryRow, Long> getPostponeFileSizes(Iterator<ManifestEntry> iterator) {
        Map<BinaryRow, Long> fileSizes = new HashMap<>();
        while (iterator.hasNext()) {
            ManifestEntry entry = iterator.next();
            fileSizes.merge(
                    entry.partition(),
                    entry.file().fileSize(),
                    (left, right) -> Math.addExact(left, right));
        }
        return fileSizes;
    }

    public static FileStoreTable tableForPostponeRewrite(
            FileStoreTable table, int numBuckets, long snapshotId) {
        Map<String, String> rewriteOptions = new HashMap<>();
        rewriteOptions.put(BUCKET.key(), String.valueOf(numBuckets));
        rewriteOptions.put(WRITE_ONLY.key(), "false");
        rewriteOptions.put(COMMIT_STRICT_MODE_LAST_SAFE_SNAPSHOT.key(), String.valueOf(snapshotId));
        return table.copy(rewriteOptions);
    }

    /** Resolves the snapshot-bound bucket count of a partition. */
    public static final class PostponeBucketNumResolver implements Serializable {

        private static final long serialVersionUID = 1L;

        private final Map<BinaryRow, Integer> numBucketsByPartition;
        @Nullable private final Integer configuredDefaultBucketNum;

        private PostponeBucketNumResolver(
                Map<BinaryRow, Integer> numBucketsByPartition,
                @Nullable Integer configuredDefaultBucketNum) {
            this.numBucketsByPartition = numBucketsByPartition;
            this.configuredDefaultBucketNum = configuredDefaultBucketNum;
        }

        public int numBuckets(BinaryRow partition) {
            return determineBucketNum(partition, numBucketsByPartition, configuredDefaultBucketNum);
        }
    }

    /** Bucket decision for an exactly measured staged batch. */
    public static final class FixedBucketDecision {

        private final int targetBucketNum;
        private final boolean requiresRescale;

        private FixedBucketDecision(int targetBucketNum, boolean requiresRescale) {
            this.targetBucketNum = targetBucketNum;
            this.requiresRescale = requiresRescale;
        }

        public int targetBucketNum() {
            return targetBucketNum;
        }

        public boolean requiresRescale() {
            return requiresRescale;
        }
    }

    /** Snapshot-bound routing metadata for postpone records. */
    public static final class PostponeBucketRouter implements Serializable {

        private static final long serialVersionUID = 1L;

        private final PostponeBucketNumResolver bucketNumResolver;
        private final RowType keyType;
        private final RowType bucketKeyType;
        private final int[] bucketKeyMapping;
        private final CoreOptions.BucketFunctionType bucketFunctionType;
        @Nullable private transient Projection bucketKeyProjection;
        @Nullable private transient BucketFunction bucketFunction;

        private PostponeBucketRouter(
                PostponeBucketNumResolver bucketNumResolver,
                RowType keyType,
                RowType bucketKeyType,
                int[] bucketKeyMapping,
                CoreOptions.BucketFunctionType bucketFunctionType) {
            this.bucketNumResolver = bucketNumResolver;
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
            return bucketNumResolver.numBuckets(partition);
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
