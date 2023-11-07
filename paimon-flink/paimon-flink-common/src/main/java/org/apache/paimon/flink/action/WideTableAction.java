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

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Map;

/** Delete tag action for Flink. */
public class WideTableAction extends TableActionBase {

    public WideTableAction(
            String warehouse,
            String database,
            String table,
            Map<String, String> catalogConfig,
            Map<String, String> sourceConfig) {
        super(warehouse, database, table, catalogConfig);
    }

    @Override
    public void build() throws Exception {
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

    // ------------------------------------------------------------------------
    //  Flink run methods
    // ------------------------------------------------------------------------

    @Override
    public void run() throws Exception {
        build();
        execute(env, "Wide Table Sync");
    }
}
