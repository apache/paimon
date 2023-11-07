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

package org.apache.paimon.flink.widetable;

import org.apache.paimon.flink.action.ActionBase;
import org.apache.paimon.flink.action.TableActionBase;
import org.apache.paimon.flink.widetable.bean.ChangeLog;
import org.apache.paimon.flink.widetable.bean.DimensionTable;
import org.apache.paimon.flink.widetable.map.MessageMysqlFlatMapFunction;
import org.apache.paimon.flink.widetable.map.MessageMysqlSinkMapFunction;
import org.apache.paimon.flink.widetable.msg.Message;
import org.apache.paimon.flink.widetable.reduce.DimensionWindowReduceFunction;
import org.apache.paimon.flink.widetable.utils.ParamUitl;
import org.apache.paimon.flink.widetable.utils.options.SqlInfoOptions;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.HashMap;
import java.util.Map;

/** Delete tag action for Flink. */
public class WideTableAction extends ActionBase {

    private final String database;
    private final String table;
    private Map<String, String> sourceConfing = new HashMap<>();
    private Map<String, String> sinkConfing = new HashMap<>();
    private Map<String, String> dimConfing = new HashMap<>();

    public WideTableAction(
            String warehouse, String database, String table, Map<String, String> catalogConfig) {
        super(warehouse,catalogConfig);
        this.database = database;
        this.table = table;
    }

    public WideTableAction withSourceConfig(Map<String, String> sourceConfing) {
        this.sourceConfing = sourceConfing;
        return this;
    }

    public WideTableAction withSinkConfig(Map<String, String> sinkConfing) {
        this.sinkConfing = sinkConfing;
        return this;
    }

    public WideTableAction withdimConfing(Map<String, String> dimConfing) {
        this.dimConfing = dimConfing;
        return this;
    }

    @Override
    public void build() throws Exception {
        // 多source 合并
        DataStream<Message> messageDataStream =
                ParamUitl.buildSourceDataStream(1, null, 1000, null, env);

        DataStream<ChangeLog> changelog =
                messageDataStream
                        // 开启窗口计算逻辑
                        .keyBy(t -> t.getHash_pk())
                        .window(TumblingProcessingTimeWindows.of(Time.milliseconds(10000)))
                        .reduce(new DimensionWindowReduceFunction())
                        // end
                        .map(new MessageMysqlSinkMapFunction())
                        .flatMap(new MessageMysqlFlatMapFunction());

        System.out.println(env.getExecutionPlan());

        // sink table
        batchTEnv.executeSql(sinkCreateTableFlinkSql);

        // dim table
        for (Map.Entry<String, DimensionTable> item : tablesMap.entrySet()) {
            String createTempTableSql = item.getValue().getCreate_table_flink_sql();
            // System.out.println(createTempTableSql);
            batchTEnv.executeSql(createTempTableSql);
        }

        // insert sql
        String insert_flink_sql = conf.getString(SqlInfoOptions.INSERT_FLINK_SQL);

        batchTEnv.executeSql(insert_flink_sql);
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
