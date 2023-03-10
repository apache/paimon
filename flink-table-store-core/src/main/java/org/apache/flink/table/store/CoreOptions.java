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

package org.apache.flink.table.store;

import org.apache.flink.table.store.annotation.Documentation.ExcludeFromDocumentation;
import org.apache.flink.table.store.annotation.Documentation.Immutable;
import org.apache.flink.table.store.file.WriteMode;
import org.apache.flink.table.store.format.FileFormat;
import org.apache.flink.table.store.fs.Path;
import org.apache.flink.table.store.options.ConfigOption;
import org.apache.flink.table.store.options.MemorySize;
import org.apache.flink.table.store.options.Options;
import org.apache.flink.table.store.options.description.DescribedEnum;
import org.apache.flink.table.store.options.description.Description;
import org.apache.flink.table.store.options.description.InlineElement;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.store.options.ConfigOptions.key;
import static org.apache.flink.table.store.options.description.TextElement.text;

/** Core options for table store. */
public class CoreOptions implements Serializable {

    public static final ConfigOption<Integer> BUCKET =
            key("bucket")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Bucket number for file store.");

    @Immutable
    public static final ConfigOption<String> BUCKET_KEY =
            key("bucket-key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Specify the table store distribution policy. Data is assigned"
                                                    + " to each bucket according to the hash value of bucket-key.")
                                    .linebreak()
                                    .text("If you specify multiple fields, delimiter is ','.")
                                    .linebreak()
                                    .text(
                                            "If not specified, the primary key will be used; "
                                                    + "if there is no primary key, the full row will be used.")
                                    .build());

    @ExcludeFromDocumentation("Internal use only")
    public static final ConfigOption<String> PATH =
            key("path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The file path of this table in the filesystem.");

    public static final ConfigOption<String> FILE_FORMAT =
            key("file.format")
                    .stringType()
                    .defaultValue("orc")
                    .withDescription("Specify the message format of data files.");

    public static final ConfigOption<String> ORC_BLOOM_FILTER_COLUMNS =
            key("orc.bloom.filter.columns")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "A comma-separated list of columns for which to create a bloon filter when writing.");

    public static final ConfigOption<Double> ORC_BLOOM_FILTER_FPP =
            key("orc.bloom.filter.fpp")
                    .doubleType()
                    .defaultValue(0.05)
                    .withDescription(
                            "Define the default false positive probability for bloom filters.");

    public static final ConfigOption<Map<String, String>> FILE_COMPRESSION_PER_LEVEL =
            key("file.compression.per.level")
                    .mapType()
                    .defaultValue(new HashMap<>())
                    .withDescription(
                            "Define different compression policies for different level, you can add the conf like this:"
                                    + " 'file.compression.per.level' = '0:lz4,1:zlib', for orc file format, the compression value "
                                    + "could be NONE, ZLIB, SNAPPY, LZO, LZ4, for parquet file format, the compression value could be "
                                    + "UNCOMPRESSED, SNAPPY, GZIP, LZO, BROTLI, LZ4, ZSTD.");

    public static final ConfigOption<String> MANIFEST_FORMAT =
            key("manifest.format")
                    .stringType()
                    .defaultValue("avro")
                    .withDescription("Specify the message format of manifest files.");

    public static final ConfigOption<MemorySize> MANIFEST_TARGET_FILE_SIZE =
            key("manifest.target-file-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(8))
                    .withDescription("Suggested file size of a manifest file.");

    public static final ConfigOption<Integer> MANIFEST_MERGE_MIN_COUNT =
            key("manifest.merge-min-count")
                    .intType()
                    .defaultValue(30)
                    .withDescription(
                            "To avoid frequent manifest merges, this parameter specifies the minimum number "
                                    + "of ManifestFileMeta to merge.");

    public static final ConfigOption<String> PARTITION_DEFAULT_NAME =
            key("partition.default-name")
                    .stringType()
                    .defaultValue("__DEFAULT_PARTITION__")
                    .withDescription(
                            "The default partition name in case the dynamic partition"
                                    + " column value is null/empty string.");

    public static final ConfigOption<Integer> SNAPSHOT_NUM_RETAINED_MIN =
            key("snapshot.num-retained.min")
                    .intType()
                    .defaultValue(10)
                    .withDescription("The minimum number of completed snapshots to retain.");

    public static final ConfigOption<Integer> SNAPSHOT_NUM_RETAINED_MAX =
            key("snapshot.num-retained.max")
                    .intType()
                    .defaultValue(Integer.MAX_VALUE)
                    .withDescription("The maximum number of completed snapshots to retain.");

    public static final ConfigOption<Duration> SNAPSHOT_TIME_RETAINED =
            key("snapshot.time-retained")
                    .durationType()
                    .defaultValue(Duration.ofHours(1))
                    .withDescription("The maximum time of completed snapshots to retain.");

    public static final ConfigOption<Duration> CONTINUOUS_DISCOVERY_INTERVAL =
            key("continuous.discovery-interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription("The discovery interval of continuous reading.");

    @Immutable
    public static final ConfigOption<MergeEngine> MERGE_ENGINE =
            key("merge-engine")
                    .enumType(MergeEngine.class)
                    .defaultValue(MergeEngine.DEDUPLICATE)
                    .withDescription("Specify the merge engine for table with primary key.");

    public static final ConfigOption<Boolean> PARTIAL_UPDATE_IGNORE_DELETE =
            key("partial-update.ignore-delete")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to ignore delete records in partial-update mode.");

    @Immutable
    public static final ConfigOption<WriteMode> WRITE_MODE =
            key("write-mode")
                    .enumType(WriteMode.class)
                    .defaultValue(WriteMode.CHANGE_LOG)
                    .withDescription("Specify the write mode for table.");

    public static final ConfigOption<Boolean> WRITE_ONLY =
            key("write-only")
                    .booleanType()
                    .defaultValue(false)
                    .withDeprecatedKeys("write.compaction-skip")
                    .withDescription(
                            "If set to true, compactions and snapshot expiration will be skipped. "
                                    + "This option is used along with dedicated compact jobs.");

    public static final ConfigOption<MemorySize> SOURCE_SPLIT_TARGET_SIZE =
            key("source.split.target-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(128))
                    .withDescription("Target size of a source split when scanning a bucket.");

    public static final ConfigOption<MemorySize> SOURCE_SPLIT_OPEN_FILE_COST =
            key("source.split.open-file-cost")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(4))
                    .withDescription(
                            "Open file cost of a source file. It is used to avoid reading"
                                    + " too many files with a source split, which can be very slow.");

    public static final ConfigOption<MemorySize> WRITE_BUFFER_SIZE =
            key("write-buffer-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("256 mb"))
                    .withDescription(
                            "Amount of data to build up in memory before converting to a sorted on-disk file.");

    public static final ConfigOption<Boolean> WRITE_BUFFER_SPILLABLE =
            key("write-buffer-spillable")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Whether the write buffer can be spillable. Enabled by default when using object storage.");

    public static final ConfigOption<Integer> LOCAL_SORT_MAX_NUM_FILE_HANDLES =
            key("local-sort.max-num-file-handles")
                    .intType()
                    .defaultValue(128)
                    .withDescription(
                            "The maximal fan-in for external merge sort. It limits the number of file handles. "
                                    + "If it is too small, may cause intermediate merging. But if it is too large, "
                                    + "it will cause too many files opened at the same time, consume memory and lead to random reading.");

    public static final ConfigOption<MemorySize> PAGE_SIZE =
            key("page-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("64 kb"))
                    .withDescription("Memory page size.");

    public static final ConfigOption<MemorySize> TARGET_FILE_SIZE =
            key("target-file-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(128))
                    .withDescription("Target size of a file.");

    public static final ConfigOption<Integer> NUM_SORTED_RUNS_COMPACTION_TRIGGER =
            key("num-sorted-run.compaction-trigger")
                    .intType()
                    .defaultValue(5)
                    .withDescription(
                            "The sorted run number to trigger compaction. Includes level0 files (one file one sorted run) and "
                                    + "high-level runs (one level one sorted run).");

    public static final ConfigOption<Integer> NUM_SORTED_RUNS_STOP_TRIGGER =
            key("num-sorted-run.stop-trigger")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The number of sorted runs that trigger the stopping of writes,"
                                    + " the default value is 'num-sorted-run.compaction-trigger' + 1.");

    public static final ConfigOption<Integer> NUM_LEVELS =
            key("num-levels")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Total level number, for example, there are 3 levels, including 0,1,2 levels.");

    public static final ConfigOption<Boolean> COMMIT_FORCE_COMPACT =
            key("commit.force-compact")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to force a compaction before commit.");

    public static final ConfigOption<Integer> COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT =
            key("compaction.max-size-amplification-percent")
                    .intType()
                    .defaultValue(200)
                    .withDescription(
                            "The size amplification is defined as the amount (in percentage) of additional storage "
                                    + "needed to store a single byte of data in the merge tree for changelog mode table.");

    public static final ConfigOption<Integer> COMPACTION_SIZE_RATIO =
            key("compaction.size-ratio")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "Percentage flexibility while comparing sorted run size for changelog mode table. If the candidate sorted run(s) "
                                    + "size is 1% smaller than the next sorted run's size, then include next sorted run "
                                    + "into this candidate set.");

    public static final ConfigOption<Integer> COMPACTION_MIN_FILE_NUM =
            key("compaction.min.file-num")
                    .intType()
                    .defaultValue(5)
                    .withDescription(
                            "For file set [f_0,...,f_N], the minimum file number which satisfies "
                                    + "sum(size(f_i)) >= targetFileSize to trigger a compaction for "
                                    + "append-only table. This value avoids almost-full-file to be compacted, "
                                    + "which is not cost-effective.");

    public static final ConfigOption<Integer> COMPACTION_MAX_FILE_NUM =
            key("compaction.early-max.file-num")
                    .intType()
                    .defaultValue(50)
                    .withDescription(
                            "For file set [f_0,...,f_N], the maximum file number to trigger a compaction "
                                    + "for append-only table, even if sum(size(f_i)) < targetFileSize. This value "
                                    + "avoids pending too much small files, which slows down the performance.");

    public static final ConfigOption<Integer> COMPACTION_MAX_SORTED_RUN_NUM =
            key("compaction.max-sorted-run-num")
                    .intType()
                    .defaultValue(Integer.MAX_VALUE)
                    .withDescription(
                            "The maximum sorted run number to pick for compaction. "
                                    + "This value avoids merging too much sorted runs at the same time during compaction, "
                                    + "which may lead to OutOfMemoryError.");

    public static final ConfigOption<ChangelogProducer> CHANGELOG_PRODUCER =
            key("changelog-producer")
                    .enumType(ChangelogProducer.class)
                    .defaultValue(ChangelogProducer.NONE)
                    .withDescription(
                            "Whether to double write to a changelog file. "
                                    + "This changelog file keeps the details of data changes, "
                                    + "it can be read directly during stream reads.");

    @Immutable
    public static final ConfigOption<String> SEQUENCE_FIELD =
            key("sequence.field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The field that generates the sequence number for primary key table,"
                                    + " the sequence number determines which data is the most recent.");

    public static final ConfigOption<StartupMode> SCAN_MODE =
            key("scan.mode")
                    .enumType(StartupMode.class)
                    .defaultValue(StartupMode.DEFAULT)
                    .withDeprecatedKeys("log.scan")
                    .withDescription("Specify the scanning behavior of the source.");

    public static final ConfigOption<Long> SCAN_TIMESTAMP_MILLIS =
            key("scan.timestamp-millis")
                    .longType()
                    .noDefaultValue()
                    .withDeprecatedKeys("log.scan.timestamp-millis")
                    .withDescription(
                            "Optional timestamp used in case of \"from-timestamp\" scan mode.");

    public static final ConfigOption<Long> SCAN_SNAPSHOT_ID =
            key("scan.snapshot-id")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional snapshot id used in case of \"from-snapshot\" scan mode");

    public static final ConfigOption<Long> SCAN_BOUNDED_WATERMARK =
            key("scan.bounded.watermark")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "End condition \"watermark\" for bounded streaming mode. Stream"
                                    + " reading will end when a larger watermark snapshot is encountered.");

    public static final ConfigOption<LogConsistency> LOG_CONSISTENCY =
            key("log.consistency")
                    .enumType(LogConsistency.class)
                    .defaultValue(LogConsistency.TRANSACTIONAL)
                    .withDescription("Specify the log consistency mode for table.");

    public static final ConfigOption<LogChangelogMode> LOG_CHANGELOG_MODE =
            key("log.changelog-mode")
                    .enumType(LogChangelogMode.class)
                    .defaultValue(LogChangelogMode.AUTO)
                    .withDescription("Specify the log changelog mode for table.");

    public static final ConfigOption<Boolean> LOG_SCAN_REMOVE_NORMALIZE =
            key("log.scan.remove-normalize")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to force the removal of the normalize node when streaming read."
                                    + " Note: This is dangerous and is likely to cause data errors if downstream"
                                    + " is used to calculate aggregation and the input is not complete changelog.");

    public static final ConfigOption<String> LOG_KEY_FORMAT =
            key("log.key.format")
                    .stringType()
                    .defaultValue("json")
                    .withDescription(
                            "Specify the key message format of log system with primary key.");

    public static final ConfigOption<String> LOG_FORMAT =
            key("log.format")
                    .stringType()
                    .defaultValue("debezium-json")
                    .withDescription("Specify the message format of log system.");

    public static final ConfigOption<Boolean> AUTO_CREATE =
            key("auto-create")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to create underlying storage when reading and writing the table.");

    public static final ConfigOption<Boolean> STREAMING_READ_OVERWRITE =
            key("streaming-read-overwrite")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to read the changes from overwrite in streaming mode.");

    public static final ConfigOption<Duration> PARTITION_EXPIRATION_TIME =
            key("partition.expiration-time")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "The expiration interval of a partition. A partition will be expired if"
                                    + " it‘s lifetime is over this value. Partition time is extracted from"
                                    + " the partition value.");

    public static final ConfigOption<Duration> PARTITION_EXPIRATION_CHECK_INTERVAL =
            key("partition.expiration-check-interval")
                    .durationType()
                    .defaultValue(Duration.ofHours(1))
                    .withDescription("The check interval of partition expiration.");

    public static final ConfigOption<String> PARTITION_TIMESTAMP_FORMATTER =
            key("partition.timestamp-formatter")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The formatter to format timestamp from string. It can be used"
                                                    + " with 'partition.timestamp-pattern' to create a formatter"
                                                    + " using the specified value.")
                                    .list(
                                            text(
                                                    "Default formatter is 'yyyy-MM-dd HH:mm:ss' and 'yyyy-MM-dd'."),
                                            text(
                                                    "Supports multiple partition fields like '$year-$month-$day $hour:00:00'."),
                                            text(
                                                    "The timestamp-formatter is compatible with Java's DateTimeFormatter."))
                                    .build());

    public static final ConfigOption<String> PARTITION_TIMESTAMP_PATTERN =
            key("partition.timestamp-pattern")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "You can specify a pattern to get a timestamp from partitions. "
                                                    + "The formatter pattern is defined by 'partition.timestamp-formatter'.")
                                    .list(
                                            text("By default, read from the first field."),
                                            text(
                                                    "If the timestamp in the partition is a single field called 'dt', you can use '$dt'."),
                                            text(
                                                    "If it is spread across multiple fields for year, month, day, and hour,"
                                                            + " you can use '$year-$month-$day $hour:00:00'."),
                                            text(
                                                    "If the timestamp is in fields dt and hour, you can use '$dt "
                                                            + "$hour:00:00'."))
                                    .build());

    public static final ConfigOption<Boolean> SCAN_PLAN_SORT_PARTITION =
            key("scan.plan-sort-partition")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Whether to sort plan files by partition fields, this allows you to read"
                                                    + " according to the partition order, even if your partition writes are out of order.")
                                    .linebreak()
                                    .text(
                                            "It is recommended that you use this for streaming read of the 'append-only' table."
                                                    + " By default, streaming read will read the full snapshot first. In order to"
                                                    + " avoid the disorder reading for partitions, you can open this option.")
                                    .build());

    @Immutable
    public static final ConfigOption<String> PRIMARY_KEY =
            key("primary-key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Define primary key by table options, cannot define primary key on DDL and table options at the same time.");

    @Immutable
    public static final ConfigOption<String> PARTITION =
            key("partition")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Define partition by table options, cannot define partition on DDL and table options at the same time.");

    private final Options options;

    public CoreOptions(Map<String, String> options) {
        this(Options.fromMap(options));
    }

    public CoreOptions(Options options) {
        this.options = options;
    }

    public Options toConfiguration() {
        return options;
    }

    public Map<String, String> toMap() {
        return options.toMap();
    }

    public int bucket() {
        return options.get(BUCKET);
    }

    public Path path() {
        return path(options.toMap());
    }

    public static Path path(Map<String, String> options) {
        return new Path(options.get(PATH.key()));
    }

    public static Path path(Options options) {
        return new Path(options.get(PATH));
    }

    public FileFormat fileFormat() {
        return FileFormat.fromTableOptions(options, FILE_FORMAT);
    }

    public FileFormat manifestFormat() {
        return FileFormat.fromTableOptions(options, MANIFEST_FORMAT);
    }

    public MemorySize manifestTargetSize() {
        return options.get(MANIFEST_TARGET_FILE_SIZE);
    }

    public String partitionDefaultName() {
        return options.get(PARTITION_DEFAULT_NAME);
    }

    public Map<Integer, String> fileCompressionPerLevel() {
        Map<String, String> levelCompressions = options.get(FILE_COMPRESSION_PER_LEVEL);
        return levelCompressions.entrySet().stream()
                .collect(Collectors.toMap(e -> Integer.valueOf(e.getKey()), Map.Entry::getValue));
    }

    public int snapshotNumRetainMin() {
        return options.get(SNAPSHOT_NUM_RETAINED_MIN);
    }

    public int snapshotNumRetainMax() {
        return options.get(SNAPSHOT_NUM_RETAINED_MAX);
    }

    public Duration snapshotTimeRetain() {
        return options.get(SNAPSHOT_TIME_RETAINED);
    }

    public int manifestMergeMinCount() {
        return options.get(MANIFEST_MERGE_MIN_COUNT);
    }

    public MergeEngine mergeEngine() {
        return options.get(MERGE_ENGINE);
    }

    public long splitTargetSize() {
        return options.get(SOURCE_SPLIT_TARGET_SIZE).getBytes();
    }

    public long splitOpenFileCost() {
        return options.get(SOURCE_SPLIT_OPEN_FILE_COST).getBytes();
    }

    public long writeBufferSize() {
        return options.get(WRITE_BUFFER_SIZE).getBytes();
    }

    public boolean writeBufferSpillable(boolean usingObjectStore) {
        return options.getOptional(WRITE_BUFFER_SPILLABLE).orElse(usingObjectStore);
    }

    public Duration continuousDiscoveryInterval() {
        return options.get(CONTINUOUS_DISCOVERY_INTERVAL);
    }

    public int localSortMaxNumFileHandles() {
        return options.get(LOCAL_SORT_MAX_NUM_FILE_HANDLES);
    }

    public int pageSize() {
        return (int) options.get(PAGE_SIZE).getBytes();
    }

    public long targetFileSize() {
        return options.get(TARGET_FILE_SIZE).getBytes();
    }

    public int numSortedRunCompactionTrigger() {
        return options.get(NUM_SORTED_RUNS_COMPACTION_TRIGGER);
    }

    public int numSortedRunStopTrigger() {
        Integer stopTrigger = options.get(NUM_SORTED_RUNS_STOP_TRIGGER);
        if (stopTrigger == null) {
            stopTrigger = numSortedRunCompactionTrigger() + 1;
        }
        return Math.max(numSortedRunCompactionTrigger(), stopTrigger);
    }

    public int numLevels() {
        // By default, this ensures that the compaction does not fall to level 0, but at least to
        // level 1
        Integer numLevels = options.get(NUM_LEVELS);
        int expectedRuns =
                maxSortedRunNum() == Integer.MAX_VALUE
                        ? numSortedRunCompactionTrigger()
                        : numSortedRunStopTrigger();
        numLevels = numLevels == null ? expectedRuns + 1 : numLevels;
        return numLevels;
    }

    public boolean commitForceCompact() {
        return options.get(COMMIT_FORCE_COMPACT);
    }

    public int maxSizeAmplificationPercent() {
        return options.get(COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT);
    }

    public int sortedRunSizeRatio() {
        return options.get(COMPACTION_SIZE_RATIO);
    }

    public int compactionMinFileNum() {
        return options.get(COMPACTION_MIN_FILE_NUM);
    }

    public int compactionMaxFileNum() {
        return options.get(COMPACTION_MAX_FILE_NUM);
    }

    public int maxSortedRunNum() {
        return options.get(COMPACTION_MAX_SORTED_RUN_NUM);
    }

    public ChangelogProducer changelogProducer() {
        return options.get(CHANGELOG_PRODUCER);
    }

    public boolean scanPlanSortPartition() {
        return options.get(SCAN_PLAN_SORT_PARTITION);
    }

    public StartupMode startupMode() {
        return startupMode(options);
    }

    public static StartupMode startupMode(Options options) {
        StartupMode mode = options.get(SCAN_MODE);
        if (mode == StartupMode.DEFAULT) {
            if (options.getOptional(SCAN_TIMESTAMP_MILLIS).isPresent()) {
                return StartupMode.FROM_TIMESTAMP;
            } else if (options.getOptional(SCAN_SNAPSHOT_ID).isPresent()) {
                return StartupMode.FROM_SNAPSHOT;
            } else {
                return StartupMode.LATEST_FULL;
            }
        } else if (mode == StartupMode.FULL) {
            return StartupMode.LATEST_FULL;
        } else {
            return mode;
        }
    }

    public Long scanTimestampMills() {
        return options.get(SCAN_TIMESTAMP_MILLIS);
    }

    public Long scanBoundedWatermark() {
        return options.get(SCAN_BOUNDED_WATERMARK);
    }

    public Long scanSnapshotId() {
        return options.get(SCAN_SNAPSHOT_ID);
    }

    public Optional<String> sequenceField() {
        return options.getOptional(SEQUENCE_FIELD);
    }

    public WriteMode writeMode() {
        return options.get(WRITE_MODE);
    }

    public boolean writeOnly() {
        return options.get(WRITE_ONLY);
    }

    public boolean streamingReadOverwrite() {
        return options.get(STREAMING_READ_OVERWRITE);
    }

    public Duration partitionExpireTime() {
        return options.get(PARTITION_EXPIRATION_TIME);
    }

    public Duration partitionExpireCheckInterval() {
        return options.get(PARTITION_EXPIRATION_CHECK_INTERVAL);
    }

    public String partitionTimestampFormatter() {
        return options.get(PARTITION_TIMESTAMP_FORMATTER);
    }

    public String partitionTimestampPattern() {
        return options.get(PARTITION_TIMESTAMP_PATTERN);
    }

    /** Specifies the merge engine for table with primary key. */
    public enum MergeEngine implements DescribedEnum {
        DEDUPLICATE("deduplicate", "De-duplicate and keep the last row."),

        PARTIAL_UPDATE("partial-update", "Partial update non-null fields."),

        AGGREGATE("aggregation", "Aggregate fields with same primary key.");

        private final String value;
        private final String description;

        MergeEngine(String value, String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return text(description);
        }
    }

    /** Specifies the startup mode for log consumer. */
    public enum StartupMode implements DescribedEnum {
        DEFAULT(
                "default",
                "Determines actual startup mode according to other table properties. "
                        + "If \"scan.timestamp-millis\" is set the actual startup mode will be \"from-timestamp\", "
                        + "and if \"scan.snapshot-id\" is set the actual startup mode will be \"from-snapshot\". "
                        + "Otherwise the actual startup mode will be \"latest-full\"."),

        LATEST_FULL(
                "latest-full",
                "For streaming sources, produces the latest snapshot on the table upon first startup, "
                        + "and continue to read the latest changes. "
                        + "For batch sources, just produce the latest snapshot but does not read new changes."),

        FULL("full", "Deprecated. Same as \"latest-full\"."),

        LATEST(
                "latest",
                "For streaming sources, continuously reads latest changes "
                        + "without producing a snapshot at the beginning. "
                        + "For batch sources, behaves the same as the \"latest-full\" startup mode."),

        COMPACTED_FULL(
                "compacted-full",
                "For streaming sources, produces a snapshot after the latest compaction on the table "
                        + "upon first startup, and continue to read the latest changes. "
                        + "For batch sources, just produce a snapshot after the latest compaction "
                        + "but does not read new changes."),

        FROM_TIMESTAMP(
                "from-timestamp",
                "For streaming sources, continuously reads changes "
                        + "starting from timestamp specified by \"scan.timestamp-millis\", "
                        + "without producing a snapshot at the beginning. "
                        + "For batch sources, produces a snapshot at timestamp specified by \"scan.timestamp-millis\" "
                        + "but does not read new changes."),

        FROM_SNAPSHOT(
                "from-snapshot",
                "For streaming sources, continuously reads changes "
                        + "starting from snapshot specified by \"scan.snapshot-id\", "
                        + "without producing a snapshot at the beginning. For batch sources, "
                        + "produces a snapshot specified by \"scan.snapshot-id\" but does not read new changes.");

        private final String value;
        private final String description;

        StartupMode(String value, String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return text(description);
        }
    }

    /** Specifies the log consistency mode for table. */
    public enum LogConsistency implements DescribedEnum {
        TRANSACTIONAL(
                "transactional",
                "Only the data after the checkpoint can be seen by readers, the latency depends on checkpoint interval."),

        EVENTUAL(
                "eventual",
                "Immediate data visibility, you may see some intermediate states, "
                        + "but eventually the right results will be produced, only works for table with primary key.");

        private final String value;
        private final String description;

        LogConsistency(String value, String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return text(description);
        }
    }

    /** Specifies the log changelog mode for table. */
    public enum LogChangelogMode implements DescribedEnum {
        AUTO("auto", "Upsert for table with primary key, all for table without primary key."),

        ALL("all", "The log system stores all changes including UPDATE_BEFORE."),

        UPSERT(
                "upsert",
                "The log system does not store the UPDATE_BEFORE changes, the log consumed job"
                        + " will automatically add the normalized node, relying on the state"
                        + " to generate the required update_before.");

        private final String value;
        private final String description;

        LogChangelogMode(String value, String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return text(description);
        }
    }

    /** Specifies the changelog producer for table. */
    public enum ChangelogProducer implements DescribedEnum {
        NONE("none", "No changelog file."),

        INPUT(
                "input",
                "Double write to a changelog file when flushing memory table, the changelog is from input."),

        FULL_COMPACTION("full-compaction", "Generate changelog files with each full compaction.");

        private final String value;
        private final String description;

        ChangelogProducer(String value, String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return text(description);
        }
    }

    /**
     * Set the default values of the {@link CoreOptions} via the given {@link Options}.
     *
     * @param options the options to set default values
     */
    public static void setDefaultValues(Options options) {
        if (options.contains(SCAN_TIMESTAMP_MILLIS) && !options.contains(SCAN_MODE)) {
            options.set(SCAN_MODE, StartupMode.FROM_TIMESTAMP);
        }
    }

    public static List<ConfigOption<?>> getOptions() {
        final Field[] fields = CoreOptions.class.getFields();
        final List<ConfigOption<?>> list = new ArrayList<>(fields.length);
        for (Field field : fields) {
            if (ConfigOption.class.isAssignableFrom(field.getType())) {
                try {
                    list.add((ConfigOption<?>) field.get(CoreOptions.class));
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return list;
    }

    public static Set<String> getImmutableOptionKeys() {
        final Field[] fields = CoreOptions.class.getFields();
        final Set<String> immutableKeys = new HashSet<>(fields.length);
        for (Field field : fields) {
            if (ConfigOption.class.isAssignableFrom(field.getType())
                    && field.getAnnotation(Immutable.class) != null) {
                try {
                    immutableKeys.add(((ConfigOption<?>) field.get(CoreOptions.class)).key());
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return immutableKeys;
    }
}
