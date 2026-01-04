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

package org.apache.paimon.flink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.annotation.Documentation.ExcludeFromDocumentation;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.description.DescribedEnum;
import org.apache.paimon.options.description.InlineElement;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.options.ConfigOptions.key;
import static org.apache.paimon.options.description.TextElement.text;

/** Options for flink connector. */
public class FlinkConnectorOptions {

    public static final String NONE = "none";

    public static final String TABLE_DYNAMIC_OPTION_PREFIX = "paimon.";

    public static final int MIN_CLUSTERING_SAMPLE_FACTOR = 20;

    public static final ConfigOption<Integer> SINK_PARALLELISM =
            ConfigOptions.key("sink.parallelism")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines a custom parallelism for the sink. "
                                    + "By default, if this option is not defined, the planner will derive the parallelism "
                                    + "for each statement individually by also considering the global configuration.");

    public static final ConfigOption<Integer> SCAN_PARALLELISM =
            ConfigOptions.key("scan.parallelism")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Define a custom parallelism for the scan source. "
                                    + "By default, if this option is not defined, the planner will derive the parallelism "
                                    + "for each statement individually by also considering the global configuration. "
                                    + "If user enable the scan.infer-parallelism, the planner will derive the parallelism by inferred parallelism.");

    public static final ConfigOption<Integer> UNAWARE_BUCKET_COMPACTION_PARALLELISM =
            ConfigOptions.key("unaware-bucket.compaction.parallelism")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines a custom parallelism for the unaware-bucket table compaction job. "
                                    + "By default, if this option is not defined, the planner will derive the parallelism "
                                    + "for each statement individually by also considering the global configuration.");

    public static final ConfigOption<Boolean> INFER_SCAN_PARALLELISM =
            ConfigOptions.key("scan.infer-parallelism")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "If it is false, parallelism of source are set by global parallelism."
                                    + " Otherwise, source parallelism is inferred from splits number (batch mode) or bucket number(streaming mode).");

    public static final ConfigOption<Integer> INFER_SCAN_MAX_PARALLELISM =
            ConfigOptions.key("scan.infer-parallelism.max")
                    .intType()
                    .defaultValue(1024)
                    .withDescription(
                            "If scan.infer-parallelism is true, limit the parallelism of source through this option.");

    @Deprecated
    @ExcludeFromDocumentation("Deprecated")
    public static final ConfigOption<Duration> CHANGELOG_PRODUCER_FULL_COMPACTION_TRIGGER_INTERVAL =
            key("changelog-producer.compaction-interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(0))
                    .withDescription(
                            "When "
                                    + CoreOptions.CHANGELOG_PRODUCER.key()
                                    + " is set to "
                                    + ChangelogProducer.FULL_COMPACTION.name()
                                    + ", full compaction will be constantly triggered after this interval.");

    public static final ConfigOption<WatermarkEmitStrategy> SCAN_WATERMARK_EMIT_STRATEGY =
            key("scan.watermark.emit.strategy")
                    .enumType(WatermarkEmitStrategy.class)
                    .defaultValue(WatermarkEmitStrategy.ON_EVENT)
                    .withDescription("Emit strategy for watermark generation.");

    public static final ConfigOption<String> SCAN_WATERMARK_ALIGNMENT_GROUP =
            key("scan.watermark.alignment.group")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("A group of sources to align watermarks.");

    public static final ConfigOption<Duration> SCAN_WATERMARK_IDLE_TIMEOUT =
            key("scan.watermark.idle-timeout")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "If no records flow in a partition of a stream for that amount of time, then"
                                    + " that partition is considered \"idle\" and will not hold back the progress of"
                                    + " watermarks in downstream operators.");

    public static final ConfigOption<Duration> SCAN_WATERMARK_ALIGNMENT_MAX_DRIFT =
            key("scan.watermark.alignment.max-drift")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "Maximal drift to align watermarks, "
                                    + "before we pause consuming from the source/task/partition.");

    public static final ConfigOption<Duration> SCAN_WATERMARK_ALIGNMENT_UPDATE_INTERVAL =
            key("scan.watermark.alignment.update-interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription(
                            "How often tasks should notify coordinator about the current watermark "
                                    + "and how often the coordinator should announce the maximal aligned watermark.");

    public static final ConfigOption<Integer> SCAN_SPLIT_ENUMERATOR_BATCH_SIZE =
            key("scan.split-enumerator.batch-size")
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            "How many splits should assign to subtask per batch in StaticFileStoreSplitEnumerator "
                                    + "to avoid exceed `akka.framesize` limit.");

    public static final ConfigOption<Integer> SCAN_MAX_SNAPSHOT_COUNT =
            key("scan.max-snapshot.count")
                    .intType()
                    .defaultValue(-1)
                    .withDescription(
                            "The max snapshot count to scan per checkpoint. Not limited when it's negative.");

    public static final ConfigOption<SplitAssignMode> SCAN_SPLIT_ENUMERATOR_ASSIGN_MODE =
            key("scan.split-enumerator.mode")
                    .enumType(SplitAssignMode.class)
                    .defaultValue(SplitAssignMode.FAIR)
                    .withDescription(
                            "The mode used by StaticFileStoreSplitEnumerator to assign splits.");

    /* Sink writer allocate segments from managed memory. */
    public static final ConfigOption<Boolean> SINK_USE_MANAGED_MEMORY =
            ConfigOptions.key("sink.use-managed-memory-allocator")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If true, flink sink will use managed memory for merge tree; otherwise, "
                                    + "it will create an independent memory allocator.");

    public static final ConfigOption<Boolean> SCAN_REMOVE_NORMALIZE =
            key("scan.remove-normalize")
                    .booleanType()
                    .defaultValue(false)
                    .withFallbackKeys("log.scan.remove-normalize")
                    .withDescription(
                            "Whether to force the removal of the normalize node when streaming read."
                                    + " Note: This is dangerous and is likely to cause data errors if downstream"
                                    + " is used to calculate aggregation and the input is not complete changelog.");

    public static final ConfigOption<Boolean> READ_SHUFFLE_BUCKET_WITH_PARTITION =
            key("read.shuffle-bucket-with-partition")
                    .booleanType()
                    .defaultValue(true)
                    .withFallbackKeys("streaming-read.shuffle-bucket-with-partition")
                    .withDescription("Whether shuffle by partition and bucket when read.");

    /**
     * Weight of writer buffer in managed memory, Flink will compute the memory size for writer
     * according to the weight, the actual memory used depends on the running environment.
     */
    public static final ConfigOption<MemorySize> SINK_MANAGED_WRITER_BUFFER_MEMORY =
            ConfigOptions.key("sink.managed.writer-buffer-memory")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(256))
                    .withDescription(
                            "Weight of writer buffer in managed memory, Flink will compute the memory size "
                                    + "for writer according to the weight, the actual memory used depends on the running environment.");

    public static final ConfigOption<MemorySize> SINK_CROSS_PARTITION_MANAGED_MEMORY =
            ConfigOptions.key("sink.cross-partition.managed-memory")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(256))
                    .withDescription(
                            "Weight of managed memory for RocksDB in cross-partition update, Flink will compute the memory size "
                                    + "according to the weight, the actual memory used depends on the running environment.");

    public static final ConfigOption<Boolean> SOURCE_CHECKPOINT_ALIGN_ENABLED =
            ConfigOptions.key("source.checkpoint-align.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to align the flink checkpoint with the snapshot of the paimon table, If true, a checkpoint will only be made if a snapshot is consumed.");

    public static final ConfigOption<Duration> SOURCE_CHECKPOINT_ALIGN_TIMEOUT =
            ConfigOptions.key("source.checkpoint-align.timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription(
                            "If the new snapshot has not been generated when the checkpoint starts to trigger, the enumerator will block the checkpoint and wait for the new snapshot. Set the maximum waiting time to avoid infinite waiting, if timeout, the checkpoint will fail. Note that it should be set smaller than the checkpoint timeout.");

    public static final ConfigOption<Boolean> LOOKUP_ASYNC =
            ConfigOptions.key("lookup.async")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to enable async lookup join.");

    public static final ConfigOption<Integer> LOOKUP_BOOTSTRAP_PARALLELISM =
            ConfigOptions.key("lookup.bootstrap-parallelism")
                    .intType()
                    .defaultValue(4)
                    .withDescription(
                            "The parallelism for bootstrap in a single task for lookup join.");

    public static final ConfigOption<Integer> LOOKUP_ASYNC_THREAD_NUMBER =
            ConfigOptions.key("lookup.async-thread-number")
                    .intType()
                    .defaultValue(16)
                    .withDescription("The thread number for lookup async.");

    public static final ConfigOption<LookupCacheMode> LOOKUP_CACHE_MODE =
            ConfigOptions.key("lookup.cache")
                    .enumType(LookupCacheMode.class)
                    .defaultValue(LookupCacheMode.AUTO)
                    .withDescription("The cache mode of lookup join.");

    public static final ConfigOption<String> SCAN_PARTITIONS =
            ConfigOptions.key("scan.partitions")
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys("lookup.dynamic-partition")
                    .withDescription(
                            "Specify the partitions to scan. "
                                    + "Partitions should be given in the form of key1=value1,key2=value2. "
                                    + "Partition keys not specified will be filled with the value of "
                                    + CoreOptions.PARTITION_DEFAULT_NAME.key()
                                    + ". Multiple partitions should be separated by semicolon (;). "
                                    + "This option can support normal source tables and lookup join tables. "
                                    + "There are two special values max_pt() and max_two_pt() are also supported "
                                    + "to specify the (two) partition(s) with the largest partition value. For "
                                    + "lookup source, the max partition(s) will be periodically refreshed; for "
                                    + "normal source, the max partition(s) will be determined before job running "
                                    + "without refreshing even for streaming jobs.");

    public static final ConfigOption<Duration> LOOKUP_DYNAMIC_PARTITION_REFRESH_INTERVAL =
            ConfigOptions.key("lookup.dynamic-partition.refresh-interval")
                    .durationType()
                    .defaultValue(Duration.ofHours(1))
                    .withDescription(
                            "Specific dynamic partition refresh interval for lookup, "
                                    + "scan all partitions and obtain corresponding partition.");

    public static final ConfigOption<Boolean> LOOKUP_REFRESH_ASYNC =
            ConfigOptions.key("lookup.refresh.async")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to refresh lookup table in an async thread.");

    public static final ConfigOption<Integer> LOOKUP_REFRESH_ASYNC_PENDING_SNAPSHOT_COUNT =
            ConfigOptions.key("lookup.refresh.async.pending-snapshot-count")
                    .intType()
                    .defaultValue(5)
                    .withDescription(
                            "If the pending snapshot count exceeds the threshold, lookup operator will refresh the table in sync.");

    public static final ConfigOption<String> LOOKUP_REFRESH_TIME_PERIODS_BLACKLIST =
            ConfigOptions.key("lookup.refresh.time-periods-blacklist")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The blacklist contains several time periods. During these time periods, the lookup table's "
                                    + "cache refreshing is forbidden. Blacklist format is start1->end1,start2->end2,... , "
                                    + "and the time format is yyyy-MM-dd HH:mm. Only used when lookup table is FULL cache mode.");

    public static final ConfigOption<Boolean> SINK_AUTO_TAG_FOR_SAVEPOINT =
            ConfigOptions.key("sink.savepoint.auto-tag")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If true, a tag will be automatically created for the snapshot created by flink savepoint.");

    public static final ConfigOption<Double> SINK_WRITER_CPU =
            ConfigOptions.key("sink.writer-cpu")
                    .doubleType()
                    .defaultValue(1.0)
                    .withDescription("Sink writer cpu to control cpu cores of writer.");

    public static final ConfigOption<MemorySize> SINK_WRITER_MEMORY =
            ConfigOptions.key("sink.writer-memory")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription("Sink writer memory to control heap memory of writer.");

    public static final ConfigOption<Double> SINK_COMMITTER_CPU =
            ConfigOptions.key("sink.committer-cpu")
                    .doubleType()
                    .defaultValue(1.0)
                    .withDescription(
                            "Sink committer cpu to control cpu cores of global committer.");

    public static final ConfigOption<MemorySize> SINK_COMMITTER_MEMORY =
            ConfigOptions.key("sink.committer-memory")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            "Sink committer memory to control heap memory of global committer.");

    public static final ConfigOption<Boolean> SINK_COMMITTER_OPERATOR_CHAINING =
            ConfigOptions.key("sink.committer-operator-chaining")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Allow sink committer and writer operator to be chained together");

    public static final ConfigOption<PartitionMarkDoneActionMode> PARTITION_MARK_DONE_MODE =
            key("partition.mark-done-action.mode")
                    .enumType(PartitionMarkDoneActionMode.class)
                    .defaultValue(PartitionMarkDoneActionMode.PROCESS_TIME)
                    .withDescription("How to trigger partition mark done action.");

    public static final ConfigOption<Duration> PARTITION_IDLE_TIME_TO_DONE =
            key("partition.idle-time-to-done")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "Set a time duration when a partition has no new data after this time duration, "
                                    + "mark the done status to indicate that the data is ready.");

    public static final ConfigOption<Duration> PARTITION_TIME_INTERVAL =
            key("partition.time-interval")
                    .durationType()
                    .noDefaultValue()
                    .withDescription(
                            "You can specify time interval for partition, for example, "
                                    + "daily partition is '1 d', hourly partition is '1 h'.");

    public static final ConfigOption<Boolean> PARTITION_MARK_DONE_RECOVER_FROM_STATE =
            key("partition.mark-done.recover-from-state")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether trigger partition mark done when recover from state.");

    public static final ConfigOption<Boolean> CLUSTERING_SORT_IN_CLUSTER =
            key("sink.clustering.sort-in-cluster")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Indicates whether to further sort data belonged to each sink task after range partitioning.");

    public static final ConfigOption<Integer> CLUSTERING_SAMPLE_FACTOR =
            key("sink.clustering.sample-factor")
                    .intType()
                    .defaultValue(100)
                    .withDescription(
                            "Specifies the sample factor. Let S represent the total number of samples, F represent the sample factor, "
                                    + "and P represent the sink parallelism, then S=F×P. The minimum allowed sample factor is 20.");

    public static final ConfigOption<Long> END_INPUT_WATERMARK =
            key("end-input.watermark")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional endInput watermark used in case of batch mode or bounded stream.");

    public static final ConfigOption<Boolean> PRECOMMIT_COMPACT =
            key("precommit-compact")
                    .booleanType()
                    .defaultValue(false)
                    .withFallbackKeys("changelog.precommit-compact")
                    .withDescription(
                            "If true, it will add a compact coordinator and worker operator after the writer operator,"
                                    + "in order to compact several changelog files (for primary key tables) "
                                    + "or newly created data files (for unaware bucket tables) "
                                    + "from the same partition into large ones, "
                                    + "which can decrease the number of small files.");

    public static final ConfigOption<Integer> CHANGELOG_PRECOMMIT_COMPACT_THREAD_NUM =
            key("changelog.precommit-compact.thread-num")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Maximum number of threads to copy bytes from small changelog files. "
                                    + "By default is the number of processors available to the Java virtual machine.");

    @ExcludeFromDocumentation("Most users won't need to adjust this config")
    public static final ConfigOption<MemorySize> CHANGELOG_PRECOMMIT_COMPACT_BUFFER_SIZE =
            key("changelog.precommit-compact.buffer-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(128))
                    .withDescription(
                            "The buffer size for copying bytes from small changelog files. "
                                    + "The default value is 128 MB.");

    public static final ConfigOption<String> SOURCE_OPERATOR_UID_SUFFIX =
            key("source.operator-uid.suffix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Set the uid suffix for the source operators. After setting, the uid format is "
                                    + "${UID_PREFIX}_${TABLE_NAME}_${USER_UID_SUFFIX}. If the uid suffix is not set, flink will "
                                    + "automatically generate the operator uid, which may be incompatible when the topology changes.");

    public static final ConfigOption<String> SINK_OPERATOR_UID_SUFFIX =
            key("sink.operator-uid.suffix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Set the uid suffix for the writer, dynamic bucket assigner and committer operators. The uid format is "
                                    + "${UID_PREFIX}_${TABLE_NAME}_${USER_UID_SUFFIX}. If the uid suffix is not set, flink will "
                                    + "automatically generate the operator uid, which may be incompatible when the topology changes.");

    public static final ConfigOption<Boolean> SCAN_BOUNDED =
            key("scan.bounded")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "Bounded mode for Paimon consumer. "
                                    + "By default, Paimon automatically selects bounded mode based on the mode of the Flink job.");

    public static final ConfigOption<Integer> POSTPONE_DEFAULT_BUCKET_NUM =
            key("postpone.default-bucket-num")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "Bucket number for the partitions compacted for the first time in postpone bucket tables.");

    public static final ConfigOption<Boolean> SCAN_DEDICATED_SPLIT_GENERATION =
            key("scan.dedicated-split-generation")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If true, the split generation process would be performed during runtime on a Flink task, instead of on the JobManager during initialization phase.");

    public static final ConfigOption<String> COMMIT_CUSTOM_LISTENERS =
            key("commit.custom-listeners")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            "Commit listener will be called after a successful commit. This option list custom commit "
                                    + "listener identifiers separated by comma.");

    public static final ConfigOption<Boolean> SINK_WRITER_COORDINATOR_ENABLED =
            key("sink.writer-coordinator.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Enable sink writer coordinator to plan data files in Job Manager.");

    public static final ConfigOption<MemorySize> SINK_WRITER_COORDINATOR_CACHE_MEMORY =
            key("sink.writer-coordinator.cache-memory")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(2048))
                    .withDescription(
                            "Controls the cache memory of writer coordinator to cache manifest files in Job Manager.");

    public static final ConfigOption<MemorySize> SINK_WRITER_COORDINATOR_PAGE_SIZE =
            key("sink.writer-coordinator.page-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofKibiBytes(32))
                    .withDescription(
                            "Controls the page size for one RPC request of writer coordinator.");

    public static final ConfigOption<Boolean> FILESYSTEM_JOB_LEVEL_SETTINGS_ENABLED =
            key("filesystem.job-level-settings.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Enable pass job level filesystem settings to table file IO.");

    public static final ConfigOption<String> SINK_WRITER_REFRESH_DETECTORS =
            key("sink.writer-refresh-detectors")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The option groups which are expected to be refreshed when streaming writing, "
                                    + "multiple option group separated by commas. "
                                    + "Now only 'external-paths' is supported.");

    public static List<ConfigOption<?>> getOptions() {
        final Field[] fields = FlinkConnectorOptions.class.getFields();
        final List<ConfigOption<?>> list = new ArrayList<>(fields.length);
        for (Field field : fields) {
            if (ConfigOption.class.isAssignableFrom(field.getType())) {
                try {
                    list.add((ConfigOption<?>) field.get(FlinkConnectorOptions.class));
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return list;
    }

    public static String generateCustomUid(
            String uidPrefix, String tableName, String userDefinedSuffix) {
        return String.format("%s_%s_%s", uidPrefix, tableName, userDefinedSuffix);
    }

    /** The mode of lookup cache. */
    public enum LookupCacheMode {
        /** Auto mode, try to use partial mode. */
        AUTO,

        /** Use full caching mode. */
        FULL,

        /** Use in-memory caching mode. */
        MEMORY
    }

    /** Watermark emit strategy for scan. */
    public enum WatermarkEmitStrategy implements DescribedEnum {
        ON_PERIODIC(
                "on-periodic",
                "Emit watermark periodically, interval is controlled by Flink 'pipeline.auto-watermark-interval'."),

        ON_EVENT("on-event", "Emit watermark per record.");

        private final String value;
        private final String description;

        WatermarkEmitStrategy(String value, String description) {
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
     * Split assign mode for {@link org.apache.paimon.flink.source.StaticFileStoreSplitEnumerator}.
     */
    public enum SplitAssignMode implements DescribedEnum {
        FAIR(
                "fair",
                "Distribute splits evenly when batch reading to prevent a few tasks from reading all."),
        PREEMPTIVE(
                "preemptive",
                "Distribute splits preemptively according to the consumption speed of the task.");

        private final String value;
        private final String description;

        SplitAssignMode(String value, String description) {
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

    /** The mode for partition mark done. */
    public enum PartitionMarkDoneActionMode implements DescribedEnum {
        PROCESS_TIME(
                "process-time",
                "Based on the time of the machine, mark the partition done once the processing time passes period time plus delay."),
        WATERMARK(
                "watermark",
                "Based on the watermark of the input, mark the partition done once the watermark passes period time plus delay.");

        private final String value;
        private final String description;

        PartitionMarkDoneActionMode(String value, String description) {
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
}
