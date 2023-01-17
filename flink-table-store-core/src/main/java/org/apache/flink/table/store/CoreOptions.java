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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.InlineElement;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.store.file.WriteMode;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.format.FileFormat;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.TextElement.text;
import static org.apache.flink.table.store.file.WriteMode.APPEND_ONLY;
import static org.apache.flink.table.store.file.schema.TableSchema.KEY_FIELD_PREFIX;
import static org.apache.flink.table.store.file.schema.TableSchema.SYSTEM_FIELD_NAMES;
import static org.apache.flink.util.Preconditions.checkState;

/** Core options for table store. */
public class CoreOptions implements Serializable {

    public static final ConfigOption<Integer> BUCKET =
            ConfigOptions.key("bucket")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Bucket number for file store.");

    @Immutable
    public static final ConfigOption<String> BUCKET_KEY =
            ConfigOptions.key("bucket-key")
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

    @Internal
    @Documentation.ExcludeFromDocumentation("Internal use only")
    public static final ConfigOption<String> PATH =
            ConfigOptions.key("path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The file path of this table in the filesystem.");

    public static final ConfigOption<String> FILE_FORMAT =
            ConfigOptions.key("file.format")
                    .stringType()
                    .defaultValue("orc")
                    .withDescription("Specify the message format of data files.");

    public static final ConfigOption<String> MANIFEST_FORMAT =
            ConfigOptions.key("manifest.format")
                    .stringType()
                    .defaultValue("avro")
                    .withDescription("Specify the message format of manifest files.");

    public static final ConfigOption<MemorySize> MANIFEST_TARGET_FILE_SIZE =
            ConfigOptions.key("manifest.target-file-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(8))
                    .withDescription("Suggested file size of a manifest file.");

    public static final ConfigOption<Integer> MANIFEST_MERGE_MIN_COUNT =
            ConfigOptions.key("manifest.merge-min-count")
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
            ConfigOptions.key("snapshot.num-retained.min")
                    .intType()
                    .defaultValue(10)
                    .withDescription("The minimum number of completed snapshots to retain.");

    public static final ConfigOption<Integer> SNAPSHOT_NUM_RETAINED_MAX =
            ConfigOptions.key("snapshot.num-retained.max")
                    .intType()
                    .defaultValue(Integer.MAX_VALUE)
                    .withDescription("The maximum number of completed snapshots to retain.");

    public static final ConfigOption<Duration> SNAPSHOT_TIME_RETAINED =
            ConfigOptions.key("snapshot.time-retained")
                    .durationType()
                    .defaultValue(Duration.ofHours(1))
                    .withDescription("The maximum time of completed snapshots to retain.");

    public static final ConfigOption<Duration> CONTINUOUS_DISCOVERY_INTERVAL =
            ConfigOptions.key("continuous.discovery-interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription("The discovery interval of continuous reading.");

    @Immutable
    public static final ConfigOption<MergeEngine> MERGE_ENGINE =
            ConfigOptions.key("merge-engine")
                    .enumType(MergeEngine.class)
                    .defaultValue(MergeEngine.DEDUPLICATE)
                    .withDescription("Specify the merge engine for table with primary key.");

    public static final ConfigOption<Boolean> PARTIAL_UPDATE_IGNORE_DELETE =
            ConfigOptions.key("partial-update.ignore-delete")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to ignore delete records in partial-update mode.");

    @Immutable
    public static final ConfigOption<WriteMode> WRITE_MODE =
            ConfigOptions.key("write-mode")
                    .enumType(WriteMode.class)
                    .defaultValue(WriteMode.CHANGE_LOG)
                    .withDescription("Specify the write mode for table.");

    public static final ConfigOption<Boolean> WRITE_ONLY =
            ConfigOptions.key("write-only")
                    .booleanType()
                    .defaultValue(false)
                    .withDeprecatedKeys("write.compaction-skip")
                    .withDescription(
                            "If set to true, compactions and snapshot expiration will be skipped. "
                                    + "This option is used along with dedicated compact jobs.");

    public static final ConfigOption<MemorySize> SOURCE_SPLIT_TARGET_SIZE =
            ConfigOptions.key("source.split.target-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(128))
                    .withDescription("Target size of a source split when scanning a bucket.");

    public static final ConfigOption<MemorySize> SOURCE_SPLIT_OPEN_FILE_COST =
            ConfigOptions.key("source.split.open-file-cost")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(4))
                    .withDescription(
                            "Open file cost of a source file. It is used to avoid reading"
                                    + " too many files with a source split, which can be very slow.");

    public static final ConfigOption<MemorySize> WRITE_BUFFER_SIZE =
            ConfigOptions.key("write-buffer-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("256 mb"))
                    .withDescription(
                            "Amount of data to build up in memory before converting to a sorted on-disk file.");

    public static final ConfigOption<Boolean> WRITE_BUFFER_SPILLABLE =
            ConfigOptions.key("write-buffer-spillable")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Whether the write buffer can be spillable. Enabled by default when using object storage.");

    public static final ConfigOption<Integer> LOCAL_SORT_MAX_NUM_FILE_HANDLES =
            ConfigOptions.key("local-sort.max-num-file-handles")
                    .intType()
                    .defaultValue(128)
                    .withDescription(
                            "The maximal fan-in for external merge sort. It limits the number of file handles. "
                                    + "If it is too small, may cause intermediate merging. But if it is too large, "
                                    + "it will cause too many files opened at the same time, consume memory and lead to random reading.");

    public static final ConfigOption<MemorySize> PAGE_SIZE =
            ConfigOptions.key("page-size")
                    .memoryType()
                    .defaultValue(MemorySize.parse("64 kb"))
                    .withDescription("Memory page size.");

    public static final ConfigOption<MemorySize> TARGET_FILE_SIZE =
            ConfigOptions.key("target-file-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(128))
                    .withDescription("Target size of a file.");

    public static final ConfigOption<Integer> NUM_SORTED_RUNS_COMPACTION_TRIGGER =
            ConfigOptions.key("num-sorted-run.compaction-trigger")
                    .intType()
                    .defaultValue(5)
                    .withDescription(
                            "The sorted run number to trigger compaction. Includes level0 files (one file one sorted run) and "
                                    + "high-level runs (one level one sorted run).");

    public static final ConfigOption<Integer> NUM_SORTED_RUNS_STOP_TRIGGER =
            ConfigOptions.key("num-sorted-run.stop-trigger")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "The number of sorted runs that trigger the stopping of writes,"
                                    + " the default value is 'num-sorted-run.compaction-trigger' + 1.");

    public static final ConfigOption<Integer> NUM_LEVELS =
            ConfigOptions.key("num-levels")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Total level number, for example, there are 3 levels, including 0,1,2 levels.");

    public static final ConfigOption<Boolean> COMMIT_FORCE_COMPACT =
            ConfigOptions.key("commit.force-compact")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to force a compaction before commit.");

    public static final ConfigOption<Integer> COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT =
            ConfigOptions.key("compaction.max-size-amplification-percent")
                    .intType()
                    .defaultValue(200)
                    .withDescription(
                            "The size amplification is defined as the amount (in percentage) of additional storage "
                                    + "needed to store a single byte of data in the merge tree for changelog mode table.");

    public static final ConfigOption<Integer> COMPACTION_SIZE_RATIO =
            ConfigOptions.key("compaction.size-ratio")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "Percentage flexibility while comparing sorted run size for changelog mode table. If the candidate sorted run(s) "
                                    + "size is 1% smaller than the next sorted run's size, then include next sorted run "
                                    + "into this candidate set.");

    public static final ConfigOption<Integer> COMPACTION_MIN_FILE_NUM =
            ConfigOptions.key("compaction.min.file-num")
                    .intType()
                    .defaultValue(5)
                    .withDescription(
                            "For file set [f_0,...,f_N], the minimum file number which satisfies "
                                    + "sum(size(f_i)) >= targetFileSize to trigger a compaction for "
                                    + "append-only table. This value avoids almost-full-file to be compacted, "
                                    + "which is not cost-effective.");

    public static final ConfigOption<Integer> COMPACTION_MAX_FILE_NUM =
            ConfigOptions.key("compaction.early-max.file-num")
                    .intType()
                    .defaultValue(50)
                    .withDescription(
                            "For file set [f_0,...,f_N], the maximum file number to trigger a compaction "
                                    + "for append-only table, even if sum(size(f_i)) < targetFileSize. This value "
                                    + "avoids pending too much small files, which slows down the performance.");

    public static final ConfigOption<Integer> COMPACTION_MAX_SORTED_RUN_NUM =
            ConfigOptions.key("compaction.max-sorted-run-num")
                    .intType()
                    .defaultValue(Integer.MAX_VALUE)
                    .withDescription(
                            "The maximum sorted run number to pick for compaction. "
                                    + "This value avoids merging too much sorted runs at the same time during compaction, "
                                    + "which may lead to OutOfMemoryError.");

    public static final ConfigOption<ChangelogProducer> CHANGELOG_PRODUCER =
            ConfigOptions.key("changelog-producer")
                    .enumType(ChangelogProducer.class)
                    .defaultValue(ChangelogProducer.NONE)
                    .withDescription(
                            "Whether to double write to a changelog file. "
                                    + "This changelog file keeps the details of data changes, "
                                    + "it can be read directly during stream reads.");

    public static final ConfigOption<Duration> CHANGELOG_PRODUCER_FULL_COMPACTION_TRIGGER_INTERVAL =
            ConfigOptions.key("changelog-producer.compaction-interval")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(30))
                    .withDescription(
                            "When "
                                    + CHANGELOG_PRODUCER.key()
                                    + " is set to "
                                    + ChangelogProducer.FULL_COMPACTION.name()
                                    + ", full compaction will be constantly triggered after this interval.");

    @Immutable
    public static final ConfigOption<String> SEQUENCE_FIELD =
            ConfigOptions.key("sequence.field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The field that generates the sequence number for primary key table,"
                                    + " the sequence number determines which data is the most recent.");

    public static final ConfigOption<StartupMode> SCAN_MODE =
            ConfigOptions.key("scan.mode")
                    .enumType(StartupMode.class)
                    .defaultValue(StartupMode.DEFAULT)
                    .withDeprecatedKeys("log.scan")
                    .withDescription("Specify the scanning behavior of the source.");

    public static final ConfigOption<Long> SCAN_TIMESTAMP_MILLIS =
            ConfigOptions.key("scan.timestamp-millis")
                    .longType()
                    .noDefaultValue()
                    .withDeprecatedKeys("log.scan.timestamp-millis")
                    .withDescription(
                            "Optional timestamp used in case of \"from-timestamp\" scan mode.");

    public static final ConfigOption<Long> SCAN_SNAPSHOT_ID =
            ConfigOptions.key("scan.snapshot-id")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional snapshot id used in case of \"from-snapshot\" scan mode");

    public static final ConfigOption<Duration> LOG_RETENTION =
            ConfigOptions.key("log.retention")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "It means how long changes log will be kept. The default value is from the log system cluster.");

    public static final ConfigOption<LogConsistency> LOG_CONSISTENCY =
            ConfigOptions.key("log.consistency")
                    .enumType(LogConsistency.class)
                    .defaultValue(LogConsistency.TRANSACTIONAL)
                    .withDescription("Specify the log consistency mode for table.");

    public static final ConfigOption<LogChangelogMode> LOG_CHANGELOG_MODE =
            ConfigOptions.key("log.changelog-mode")
                    .enumType(LogChangelogMode.class)
                    .defaultValue(LogChangelogMode.AUTO)
                    .withDescription("Specify the log changelog mode for table.");

    public static final ConfigOption<Boolean> LOG_SCAN_REMOVE_NORMALIZE =
            ConfigOptions.key("log.scan.remove-normalize")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to force the removal of the normalize node when streaming read."
                                    + " Note: This is dangerous and is likely to cause data errors if downstream"
                                    + " is used to calculate aggregation and the input is not complete changelog.");

    public static final ConfigOption<String> LOG_KEY_FORMAT =
            ConfigOptions.key("log.key.format")
                    .stringType()
                    .defaultValue("json")
                    .withDescription(
                            "Specify the key message format of log system with primary key.");

    public static final ConfigOption<String> LOG_FORMAT =
            ConfigOptions.key("log.format")
                    .stringType()
                    .defaultValue("debezium-json")
                    .withDescription("Specify the message format of log system.");

    public static final ConfigOption<Boolean> AUTO_CREATE =
            ConfigOptions.key("auto-create")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to create underlying storage when reading and writing the table.");

    private final Configuration options;

    public CoreOptions(Map<String, String> options) {
        this(Configuration.fromMap(options));
    }

    public CoreOptions(Configuration options) {
        this.options = options;
    }

    public Configuration toConfiguration() {
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

    public static Path path(Configuration options) {
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

    public StartupMode startupMode() {
        return startupMode(options);
    }

    public static StartupMode startupMode(ReadableConfig options) {
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

    public Long scanSnapshotId() {
        return options.get(SCAN_SNAPSHOT_ID);
    }

    public Duration changelogProducerFullCompactionTriggerInterval() {
        return options.get(CHANGELOG_PRODUCER_FULL_COMPACTION_TRIGGER_INTERVAL);
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
     * Set the default values of the {@link CoreOptions} via the given {@link Configuration}.
     *
     * @param options the options to set default values
     */
    public static void setDefaultValues(Configuration options) {
        if (options.contains(SCAN_TIMESTAMP_MILLIS) && !options.contains(SCAN_MODE)) {
            options.set(SCAN_MODE, StartupMode.FROM_TIMESTAMP);
        }
    }

    /**
     * Validate the {@link TableSchema} and {@link CoreOptions}.
     *
     * <p>TODO validate all items in schema and all keys in options.
     *
     * @param schema the schema to be validated
     */
    public static void validateTableSchema(TableSchema schema) {
        CoreOptions options = new CoreOptions(schema.options());
        if (options.startupMode() == StartupMode.FROM_TIMESTAMP) {
            checkOptionExistInMode(options, SCAN_TIMESTAMP_MILLIS, StartupMode.FROM_TIMESTAMP);
            checkOptionsConflict(options, SCAN_SNAPSHOT_ID, SCAN_TIMESTAMP_MILLIS);
        } else if (options.startupMode() == StartupMode.FROM_SNAPSHOT) {
            checkOptionExistInMode(options, SCAN_SNAPSHOT_ID, StartupMode.FROM_SNAPSHOT);
            checkOptionsConflict(options, SCAN_TIMESTAMP_MILLIS, SCAN_SNAPSHOT_ID);
        } else {
            checkOptionNotExistInMode(options, SCAN_TIMESTAMP_MILLIS, options.startupMode());
            checkOptionNotExistInMode(options, SCAN_SNAPSHOT_ID, options.startupMode());
        }

        Preconditions.checkArgument(
                options.snapshotNumRetainMin() > 0,
                SNAPSHOT_NUM_RETAINED_MIN.key() + " should be at least 1");
        Preconditions.checkArgument(
                options.snapshotNumRetainMin() <= options.snapshotNumRetainMax(),
                SNAPSHOT_NUM_RETAINED_MIN.key()
                        + " should not be larger than "
                        + SNAPSHOT_NUM_RETAINED_MAX.key());

        // Only changelog tables with primary keys support full compaction
        if (options.changelogProducer() == ChangelogProducer.FULL_COMPACTION
                && options.writeMode() == WriteMode.CHANGE_LOG
                && schema.primaryKeys().isEmpty()) {
            throw new UnsupportedOperationException(
                    "Changelog table with full compaction must have primary keys");
        }

        // Check column names in schema
        schema.fieldNames()
                .forEach(
                        f -> {
                            checkState(
                                    !SYSTEM_FIELD_NAMES.contains(f),
                                    String.format(
                                            "Field name[%s] in schema cannot be exist in [%s]",
                                            f, SYSTEM_FIELD_NAMES.toString()));
                            checkState(
                                    !f.startsWith(KEY_FIELD_PREFIX),
                                    String.format(
                                            "Field name[%s] in schema cannot start with [%s]",
                                            f, KEY_FIELD_PREFIX));
                        });

        // Cannot define any primary key in an append-only table.
        if (!schema.primaryKeys().isEmpty() && Objects.equals(APPEND_ONLY, options.writeMode())) {
            throw new RuntimeException(
                    "Cannot define any primary key in an append-only table. Set 'write-mode'='change-log' if "
                            + "still want to keep the primary key definition.");
        }
    }

    private static void checkOptionExistInMode(
            CoreOptions options, ConfigOption<?> option, StartupMode startupMode) {
        Preconditions.checkArgument(
                options.options.contains(option),
                String.format(
                        "%s can not be null when you use %s for %s",
                        option.key(), startupMode, SCAN_MODE.key()));
    }

    private static void checkOptionNotExistInMode(
            CoreOptions options, ConfigOption<?> option, StartupMode startupMode) {
        Preconditions.checkArgument(
                !options.options.contains(option),
                String.format(
                        "%s must be null when you use %s for %s",
                        option.key(), startupMode, SCAN_MODE.key()));
    }

    private static void checkOptionsConflict(
            CoreOptions options, ConfigOption<?> illegalOption, ConfigOption<?> legalOption) {
        Preconditions.checkArgument(
                !options.options.contains(illegalOption),
                String.format(
                        "%s must be null when you set %s", illegalOption.key(), legalOption.key()));
    }

    @Internal
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

    @Internal
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

    /** Annotation used on {@link ConfigOption} fields to exclude it from schema change. */
    @Internal
    @Target(ElementType.FIELD)
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Immutable {}
}
