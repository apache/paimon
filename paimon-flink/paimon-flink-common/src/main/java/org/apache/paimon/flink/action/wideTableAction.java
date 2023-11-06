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

package org.apache.paimon.flink.action;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Map;

/** Delete tag action for Flink. */
public class wideTableAction extends TableActionBase {

    private final String tagName;

    public wideTableAction(
            String warehouse,
            String databaseName,
            String tableName,
            Map<String, String> catalogConfig,
            String tagName) {
        super(warehouse, databaseName, tableName, catalogConfig);
        this.tagName = tagName;
    }

    @Override
    public void run() throws Exception {
        ParamUitl.checkRequiredArgument(params, JobOptions.PRE_NAME);
        ParamUitl.checkRequiredArgument(params, SourceOptions.PRE_NAME);
        ParamUitl.checkRequiredArgument(params, DimensionOptions.PRE_NAME);
        ParamUitl.checkRequiredArgument(params, SqlInfoOptions.PRE_NAME);
        ParamUitl.checkRequiredArgument(params, SinkOptions.PRE_NAME);

        Configuration jobConfig =
                Configuration.fromMap(ParamUitl.optionalConfigMap(params, JobOptions.PRE_NAME));
        int parallel = jobConfig.getInteger(JobOptions.PARALLEL);
        int windowSize = jobConfig.getInteger(JobOptions.WINDOW_SIZE);
        int interval = jobConfig.getInteger(JobOptions.CHECKPOINT_INTERVAL);

        Configuration sourceConfig =
                Configuration.fromMap(ParamUitl.optionalConfigMap(params, SourceOptions.PRE_NAME));
        // 解析source
        Map<String, Configuration> sourceConfigMap = ParamUitl.buildSourceConfigMap(sourceConfig);
        String dbTables = sourceConfig.getString(SourceOptions.INCLUDING_TABLES);

        int fetchSize = sourceConfig.getInteger(MySqlSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE);
        String startupMode = sourceConfig.getString(MySqlSourceOptions.SCAN_STARTUP_MODE);

        StartupOptions startupOptions = StartupOptions.latest();
        if (Constant.INITIAL.equalsIgnoreCase(startupMode)) {
            startupOptions = StartupOptions.initial();
        } else if (Constant.EARLIEST_OFFSET.equalsIgnoreCase(startupMode)) {
            startupOptions = StartupOptions.earliest();
        } else if (Constant.LATEST_OFFSET.equalsIgnoreCase(startupMode)) {
            startupOptions = StartupOptions.latest();
        } else if (Constant.TIMESTAMP.equalsIgnoreCase(startupMode)) {
            startupOptions =
                    StartupOptions.timestamp(
                            sourceConfig.get(MySqlSourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS));
        }

        Configuration indexConf =
                Configuration.fromMap(
                        ParamUitl.optionalConfigMap(params, DimensionOptions.PRE_NAME));
        Configuration sqlConf =
                Configuration.fromMap(ParamUitl.optionalConfigMap(params, SqlInfoOptions.PRE_NAME));
        // 解析sql配置
        ParamUitl.praseSql(sqlConf);
        Configuration sinkConfig =
                Configuration.fromMap(ParamUitl.optionalConfigMap(params, SinkOptions.PRE_NAME));
        String selectFields = sqlConf.getString(SqlInfoOptions.SELECT_FIELDS);

        // 生成SinkCreateTableFlinkSql
        String sinkCreateTableFlinkSql =
                SinkTable.buildSinkCreateTableFlinkSql(selectFields, sinkConfig);

        Configuration conf = new Configuration();
        conf.set(JobOptions.PARALLEL, parallel);
        conf.set(SourceOptions.INCLUDING_TABLES, dbTables);
        conf.addAll(sqlConf);
        conf.addAll(indexConf);
        Map<String, DimensionTable> tablesMap = DimensionTableHelper.buildTableMap(conf);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallel);
        env.getConfig().setGlobalJobParameters(conf);

        // 设置 30s 的 checkpoint 间隔
        env.enableCheckpointing(interval);

        // 多source 合并
        DataStream<Message> messageDataStream =
                ParamUitl.buildSourceDataStream(
                        parallel, sourceConfigMap, fetchSize, startupOptions, env);
        DataStream<ChangeLog> changelog =
                messageDataStream
                        // 开启窗口计算逻辑
                        .keyBy(t -> t.getHash_pk())
                        .window(TumblingProcessingTimeWindows.of(Time.milliseconds(windowSize)))
                        .reduce(new DimensionWindowReduceFunction())
                        // end
                        .map(new MessageMysqlSinkMapFunction())
                        .flatMap(new MessageMysqlFlatMapFunction());

        System.out.println(env.getExecutionPlan());
        // 2.创建表的执行环境
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // source table
        tenv.registerDataStream(Constant.CHANGELOG_TABLENAME, changelog);

        // sink table
        tenv.executeSql(sinkCreateTableFlinkSql);

        // dim table
        for (Map.Entry<String, DimensionTable> item : tablesMap.entrySet()) {
            String createTempTableSql = item.getValue().getCreate_table_flink_sql();
            // System.out.println(createTempTableSql);
            tenv.executeSql(createTempTableSql);
        }

        // insert sql
        String insert_flink_sql = conf.getString(SqlInfoOptions.INSERT_FLINK_SQL);

        tenv.executeSql(insert_flink_sql);
    }
}
