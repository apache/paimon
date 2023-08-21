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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.sink.CompactorSinkBuilderForDb;
import org.apache.paimon.flink.source.MonitorCompactorSourceBuilder;
import org.apache.paimon.flink.utils.StreamExecutionEnvironmentUtils;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.CloseableIterator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;

/** this is a doc. */
public class CompactActionForDb extends ActionBase {

    private String warehouse;
    private String database;
    private BucketMode bucketMode;

    private Options options;
    private CoreOptions coreOptions;

    public CompactActionForDb(
            String warehouse,
            String database,
            BucketMode bucketMode,
            Options options,
            CoreOptions coreOptions) {
        this(warehouse, database, Collections.emptyMap(), bucketMode, options, coreOptions);
    }

    public CompactActionForDb(
            String warehouse,
            String database,
            Map<String, String> catalogConfig,
            BucketMode bucketMode,
            Options options,
            CoreOptions coreOptions) {
        super(warehouse, catalogConfig);
        this.database = database;
        this.bucketMode = bucketMode;
        this.options = options;
        this.coreOptions = coreOptions;
        //        table = table.copy(Collections.singletonMap(CoreOptions.WRITE_ONLY.key(),
        // "false"));
    }

    // ------------------------------------------------------------------------
    //  Java API
    // ------------------------------------------------------------------------

    public void build(StreamExecutionEnvironment env) {
        ReadableConfig conf = StreamExecutionEnvironmentUtils.getConfiguration(env);
        boolean isStreaming =
                conf.get(ExecutionOptions.RUNTIME_MODE) == RuntimeExecutionMode.STREAMING;

        switch (bucketMode) {
            case UNAWARE:
                {
                    //                    buildForUnawareBucketCompaction(
                    //                            env, (AppendOnlyFileStoreTable) table,
                    // isStreaming);
                    break;
                }
            case FIXED:
            case DYNAMIC:
            default:
                {
                    buildForTraditionalCompaction(env, isStreaming);
                }
        }
    }

    private void buildForTraditionalCompaction(
            StreamExecutionEnvironment env, boolean isStreaming) {
        List<FileStoreTable> tables = new ArrayList<>();
        try {
            for (String tableName : catalog.listTables(database)) {
                FileStoreTable table =
                        (FileStoreTable) catalog.getTable(new Identifier(database, tableName));
                tables.add(table);
            }
        } catch (Catalog.DatabaseNotExistException | Catalog.TableNotExistException e) {
            e.printStackTrace();
        }
        MonitorCompactorSourceBuilder sourceBuilder =
                new MonitorCompactorSourceBuilder(database, tables);
        CompactorSinkBuilderForDb sinkBuilder =
                new CompactorSinkBuilderForDb(catalogLoader(), bucketMode, options, coreOptions);

        DataStream<RowData> source =
                sourceBuilder.withEnv(env).withContinuousMode(isStreaming).build();

        sinkBuilder.withInput(source).build();
    }

    //    private void buildForUnawareBucketCompaction(
    //            StreamExecutionEnvironment env, AppendOnlyFileStoreTable table, boolean
    // isStreaming) {
    //        UnawareBucketCompactionTopoBuilder unawareBucketCompactionTopoBuilder =
    //                new UnawareBucketCompactionTopoBuilder(env, identifier.getFullName(), table);
    //
    //        unawareBucketCompactionTopoBuilder.withPartitions(partitions);
    //        unawareBucketCompactionTopoBuilder.withContinuousMode(isStreaming);
    //        unawareBucketCompactionTopoBuilder.build();
    //    }

    @Override
    public void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        build(env);
        execute(env, "Compact job");
    }
}
