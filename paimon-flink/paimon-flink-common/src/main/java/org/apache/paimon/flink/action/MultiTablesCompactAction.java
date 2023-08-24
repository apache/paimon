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

import static org.apache.paimon.utils.SerializationUtils.deserializeBinaryRow;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.flink.sink.MultiTablesCompactorSinkBuilder;
import org.apache.paimon.flink.source.MultiTablesCompactorSourceBuilder;
import org.apache.paimon.flink.utils.StreamExecutionEnvironmentUtils;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.options.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

/** this is a doc. */
public class MultiTablesCompactAction extends ActionBase {
    private static final Logger LOG = LoggerFactory.getLogger(MultiTablesCompactAction.class);

    private final Pattern includingPattern;
    private final Pattern excludingPattern;
    private final Pattern databasePattern;

    private Options compactOptions;

    public MultiTablesCompactAction(
            String warehouse,
            @Nullable String includingDatabases,
            @Nullable String includingTables,
            @Nullable String excludingTables,
            Map<String, String> catalogConfig,
            Map<String, String> compactOptions) {
        super(warehouse, catalogConfig);
        this.databasePattern =
                Pattern.compile(includingDatabases == null ? ".*" : includingDatabases);
        this.includingPattern = Pattern.compile(includingTables == null ? ".*" : includingTables);
        this.excludingPattern = excludingTables == null ? null : Pattern.compile(excludingTables);
        compactOptions.put(CoreOptions.WRITE_ONLY.key(), "false");
        this.compactOptions = new Options(compactOptions);
    }

    // ------------------------------------------------------------------------
    //  Java API
    // ------------------------------------------------------------------------

    public void build(StreamExecutionEnvironment env) {
        ReadableConfig conf = StreamExecutionEnvironmentUtils.getConfiguration(env);
        boolean isStreaming =
                conf.get(ExecutionOptions.RUNTIME_MODE) == RuntimeExecutionMode.STREAMING;
        // Currently, multi-tables compaction do not support tables which bucketmode is UNWARE.
        buildForTraditionalCompaction(env, isStreaming);
    }

    private void buildForTraditionalCompaction(
            StreamExecutionEnvironment env, boolean isStreaming) {

        MultiTablesCompactorSourceBuilder sourceBuilder =
                new MultiTablesCompactorSourceBuilder(
                        catalogLoader(),
                        databasePattern,
                        includingPattern,
                        excludingPattern,
                        compactOptions.get(CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL).toMillis());

        MultiTablesCompactorSinkBuilder sinkBuilder =
                new MultiTablesCompactorSinkBuilder(catalogLoader(), compactOptions);

        DataStream<RowData> source =
                sourceBuilder.withEnv(env).withContinuousMode(isStreaming).build();

        sinkBuilder.withInput(source).build();
    }

    private String toString(RowData rowData) {
        int numFiles;
        DataFileMetaSerializer dataFileMetaSerializer = new DataFileMetaSerializer();
        try {
            numFiles = dataFileMetaSerializer.deserializeList(rowData.getBinary(3)).size();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        BinaryRow partition = deserializeBinaryRow(rowData.getBinary(1));

        return String.format(
                "%s %d|%s|%d|%d|%d|%s|%s",
                rowData.getRowKind().shortString(),
                rowData.getLong(0),
                partition.getString(0),
                partition.getInt(1),
                rowData.getInt(2),
                numFiles,
                rowData.getString(4),
                rowData.getString(5));
    }

    @Override
    public void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        build(env);
        execute(env, "Multi-tables compact job");
    }
}
