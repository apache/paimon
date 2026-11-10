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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.flink.utils.StreamExecutionEnvironmentUtils;
import org.apache.paimon.globalindex.EsIndexSearch;
import org.apache.paimon.globalindex.EsIndexSearch.Hit;
import org.apache.paimon.globalindex.EsIndexSearch.Shard;
import org.apache.paimon.globalindex.EsIndexSearch.TopK;
import org.apache.paimon.globalindex.EsSearchQuery;
import org.apache.paimon.utils.InstantiationUtil;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.util.CloseableIterator;

import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.CoreOptions.GLOBAL_INDEX_THREAD_NUM;

/** Bounded ES search: TaskManagers query index shards, then the client merges/fetches one page. */
public class PaimonEsSearchProcedure extends ProcedureBase {
    public static final String IDENTIFIER = "paimon_es_search";

    @ProcedureHint(argument = @ArgumentHint(name = "request", type = @DataTypeHint("STRING")))
    public String[] call(ProcedureContext context, String envelope) throws Exception {
        EsIndexSearch.Execution execution = EsIndexSearch.prepare(catalog, envelope);
        StreamExecutionEnvironment env = context.getExecutionEnvironment();
        int configured =
                execution.plan.table.coreOptions().toConfiguration().get(GLOBAL_INDEX_THREAD_NUM);
        int workers =
                Math.max(
                        1,
                        Math.min(
                                execution.plan.shards.size(),
                                Math.min(
                                        configured,
                                        env.getParallelism() > 0
                                                ? env.getParallelism()
                                                : configured)));
        List<List<Shard>> groups = new ArrayList<>();
        for (int i = 0; i < workers; i++) {
            groups.add(new ArrayList<>());
        }
        for (int i = 0; i < execution.plan.shards.size(); i++) {
            groups.get(i % workers).add(execution.plan.shards.get(i));
        }
        List<byte[]> tasks = new ArrayList<>();
        for (List<Shard> group : groups) {
            tasks.add(InstantiationUtil.serializeObject(group));
        }
        EsSearchQuery query = execution.query;
        DataStream<byte[]> results =
                StreamExecutionEnvironmentUtils.fromData(
                                env, tasks, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO)
                        .name("ES search shards")
                        .setParallelism(1)
                        .rebalance()
                        .map(new SearchShards(query))
                        .returns(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO)
                        .name("Paimon ES index search")
                        .setParallelism(workers);
        TopK top = new TopK(query.limit());
        try (CloseableIterator<byte[]> iterator = results.executeAndCollect("Paimon ES search")) {
            while (iterator.hasNext()) {
                List<Hit> hits =
                        InstantiationUtil.deserializeObject(
                                iterator.next(), Thread.currentThread().getContextClassLoader());
                top.addAll(hits);
            }
        }
        return execution.finish(top.hits()).jsonRecords();
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    private static class SearchShards implements MapFunction<byte[], byte[]> {
        private static final long serialVersionUID = 1L;
        private final EsSearchQuery query;

        private SearchShards(EsSearchQuery query) {
            this.query = query;
        }

        @Override
        public byte[] map(byte[] value) throws Exception {
            List<Shard> shards =
                    InstantiationUtil.deserializeObject(
                            value, Thread.currentThread().getContextClassLoader());
            TopK top = new TopK(query.limit());
            for (Shard shard : shards) {
                top.addAll(shard.search(query));
            }
            return InstantiationUtil.serializeObject(top.hits());
        }
    }
}
