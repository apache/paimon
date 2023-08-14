package org.apache.paimon.flink.action;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.flink.compact.UnawareBucketCompactionTopoBuilder;
import org.apache.paimon.flink.sink.CompactorSinkBuilder;
import org.apache.paimon.flink.source.MonitorCompactorSourceBuilder;
import org.apache.paimon.flink.utils.StreamExecutionEnvironmentUtils;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/** this is a doc. */
public class MonitorCompactAction extends TableActionBase {
    private List<Map<String, String>> partitions;

    public MonitorCompactAction(String warehouse, String database, String tableName) {
        this(warehouse, database, tableName, Collections.emptyMap());
    }

    public MonitorCompactAction(
            String warehouse,
            String database,
            String tableName,
            Map<String, String> catalogConfig) {
        super(warehouse, database, tableName, catalogConfig);
        if (!(table instanceof FileStoreTable)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Only FileStoreTable supports compact action. The table type is '%s'.",
                            table.getClass().getName()));
        }
        table = table.copy(Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "false"));
    }

    // ------------------------------------------------------------------------
    //  Java API
    // ------------------------------------------------------------------------

    public MonitorCompactAction withPartitions(List<Map<String, String>> partitions) {
        this.partitions = partitions;
        return this;
    }

    public void build(StreamExecutionEnvironment env) {
        ReadableConfig conf = StreamExecutionEnvironmentUtils.getConfiguration(env);
        boolean isStreaming =
                conf.get(ExecutionOptions.RUNTIME_MODE) == RuntimeExecutionMode.STREAMING;
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        switch (fileStoreTable.bucketMode()) {
            case UNAWARE:
                {
                    buildForUnawareBucketCompaction(
                            env, (AppendOnlyFileStoreTable) table, isStreaming);
                    break;
                }
            case FIXED:
            case DYNAMIC:
            default:
                {
                    buildForTraditionalCompaction(env, fileStoreTable, isStreaming);
                }
        }
    }

    private void buildForTraditionalCompaction(
            StreamExecutionEnvironment env, FileStoreTable table, boolean isStreaming) {
        DataStream<RowData> source =
                new MonitorCompactorSourceBuilder(identifier.getFullName(), table)
                        .withPartitions(partitions)
                        .withContinuousMode(isStreaming)
                        .withEnv(env)
                        .build();
        CompactorSinkBuilder sinkBuilder = new CompactorSinkBuilder(table);
        sinkBuilder.withInput(source).build();
    }

    private void buildForUnawareBucketCompaction(
            StreamExecutionEnvironment env, AppendOnlyFileStoreTable table, boolean isStreaming) {
        UnawareBucketCompactionTopoBuilder unawareBucketCompactionTopoBuilder =
                new UnawareBucketCompactionTopoBuilder(env, identifier.getFullName(), table);

        unawareBucketCompactionTopoBuilder.withPartitions(partitions);
        unawareBucketCompactionTopoBuilder.withContinuousMode(isStreaming);
        unawareBucketCompactionTopoBuilder.build();
    }

    @Override
    public void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        build(env);
        execute(env, "Compact job");
    }
}
