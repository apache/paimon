package org.apache.paimon.flink.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.source.operator.BatchMonitorFunction;
import org.apache.paimon.flink.source.operator.StreamMonitorFunction;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.system.BucketsTable2;
import org.apache.paimon.types.RowType;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** this is a doc. */
public class MonitorCompactorSourceBuilder {
    private final String databaseName;
    private final List<FileStoreTable> tables;

    private boolean isContinuous = false;
    private StreamExecutionEnvironment env;
    @Nullable private List<Map<String, String>> specifiedPartitions = null;

    public MonitorCompactorSourceBuilder(String databaseName, List<FileStoreTable> tables) {
        this.databaseName = databaseName;
        this.tables = tables;
    }

    public MonitorCompactorSourceBuilder withContinuousMode(boolean isContinuous) {
        this.isContinuous = isContinuous;
        return this;
    }

    public MonitorCompactorSourceBuilder withEnv(StreamExecutionEnvironment env) {
        this.env = env;
        return this;
    }

    public MonitorCompactorSourceBuilder withPartition(Map<String, String> partition) {
        return withPartitions(Collections.singletonList(partition));
    }

    public MonitorCompactorSourceBuilder withPartitions(List<Map<String, String>> partitions) {
        this.specifiedPartitions = partitions;
        return this;
    }

    // 传进来catalog+database
    public DataStream<RowData> build() {
        if (env == null) {
            throw new IllegalArgumentException("StreamExecutionEnvironment should not be null.");
        }

        List<BucketsTable2> bucketsTables =
                tables.stream()
                        .map(
                                fileStoreTable ->
                                        new BucketsTable2(
                                                fileStoreTable,
                                                isContinuous,
                                                databaseName,
                                                fileStoreTable.name()))
                        .collect(Collectors.toList());
        Predicate partitionPredicate = null;
        //        if (specifiedPartitions != null) {
        //            // This predicate is based on the row type of the original table, not bucket
        // table.
        //            // Because TableScan in BucketsTable is the same with FileStoreTable,
        //            // and partition filter is done by scan.
        //            partitionPredicate =
        //                    PredicateBuilder.or(
        //                            specifiedPartitions.stream()
        //                                    .map(p -> PredicateBuilder.partition(p,
        // table.rowType()))
        //                                    .toArray(Predicate[]::new));
        //        }
        RowType produceType = bucketsTables.get(0).rowType();
        if (isContinuous) {
            for (BucketsTable2 bucketsTable : bucketsTables) {
                bucketsTable = bucketsTable.copy(streamingCompactOptions());
            }
            return StreamMonitorFunction.buildSource(
                    env,
                    "source",
                    InternalTypeInfo.of(LogicalTypeConversion.toLogicalType(produceType)),
                    bucketsTables.get(0).newReadBuilder().withFilter(partitionPredicate),
                    10,
                    false);
        } else {
            for (BucketsTable2 bucketsTable : bucketsTables) {
                bucketsTable = bucketsTable.copy(batchCompactOptions());
            }
            return BatchMonitorFunction.buildSource(
                    env,
                    "source",
                    InternalTypeInfo.of(LogicalTypeConversion.toLogicalType(produceType)),
                    bucketsTables.stream()
                            .map(
                                    bucketsTable ->
                                            bucketsTable
                                                    .newReadBuilder()
                                                    .withFilter(partitionPredicate))
                            .collect(Collectors.toList()),
                    10,
                    false);
        }
    }

    private Map<String, String> streamingCompactOptions() {
        // set 'streaming-compact' and remove 'scan.bounded.watermark'
        return new HashMap<String, String>() {
            {
                put(
                        CoreOptions.STREAMING_COMPACT.key(),
                        CoreOptions.StreamingCompactionType.NORMAL.getValue());
                put(CoreOptions.SCAN_BOUNDED_WATERMARK.key(), null);
            }
        };
    }

    private Map<String, String> batchCompactOptions() {
        // batch compactor source will compact all current files
        return new HashMap<String, String>() {
            {
                put(CoreOptions.SCAN_TIMESTAMP_MILLIS.key(), null);
                put(CoreOptions.SCAN_SNAPSHOT_ID.key(), null);
                put(CoreOptions.SCAN_MODE.key(), CoreOptions.StartupMode.LATEST_FULL.toString());
            }
        };
    }
}
