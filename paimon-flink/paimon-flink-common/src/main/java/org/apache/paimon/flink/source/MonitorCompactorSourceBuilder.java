package org.apache.paimon.flink.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.flink.LogicalTypeConversion;
import org.apache.paimon.flink.source.operator.BatchMonitorFunction;
import org.apache.paimon.flink.source.operator.StreamMonitorFunction;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.system.BucketsTable;
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

/** this is a doc. */
public class MonitorCompactorSourceBuilder {
    private final String tableIdentifier;
    private final FileStoreTable table;

    private boolean isContinuous = false;
    private StreamExecutionEnvironment env;
    @Nullable private List<Map<String, String>> specifiedPartitions = null;

    public MonitorCompactorSourceBuilder(String tableIdentifier, FileStoreTable table) {
        this.tableIdentifier = tableIdentifier;
        this.table = table;
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

        BucketsTable bucketsTable = new BucketsTable(table, isContinuous);
        Predicate partitionPredicate = null;
        if (specifiedPartitions != null) {
            // This predicate is based on the row type of the original table, not bucket table.
            // Because TableScan in BucketsTable is the same with FileStoreTable,
            // and partition filter is done by scan.
            partitionPredicate =
                    PredicateBuilder.or(
                            specifiedPartitions.stream()
                                    .map(p -> PredicateBuilder.partition(p, table.rowType()))
                                    .toArray(Predicate[]::new));
        }
        RowType produceType = bucketsTable.rowType();
        if (isContinuous) {
            bucketsTable = bucketsTable.copy(streamingCompactOptions());
            return StreamMonitorFunction.buildSource(
                    env,
                    "source",
                    InternalTypeInfo.of(LogicalTypeConversion.toLogicalType(produceType)),
                    bucketsTable.newReadBuilder().withFilter(partitionPredicate),
                    10,
                    false);
        } else {
            bucketsTable = bucketsTable.copy(batchCompactOptions());
            return BatchMonitorFunction.buildSource(
                    env,
                    "source",
                    InternalTypeInfo.of(LogicalTypeConversion.toLogicalType(produceType)),
                    bucketsTable.newReadBuilder().withFilter(partitionPredicate),
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
