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

package org.apache.paimon.flink.sink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.flink.source.FlinkSourceBuilder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.CoreOptions.DEFAULT_PARALLELISM;
import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.CoreOptions.BUCKET_KEY;
import static org.apache.paimon.CoreOptions.FILE_FORMAT;
import static org.apache.paimon.CoreOptions.PATH;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_COMMITTER_OPERATOR_CHAINING;
import static org.apache.paimon.flink.LogicalTypeConversion.toDataType;
import static org.apache.paimon.utils.FailingFileIO.retryArtificialException;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for Flink sink's dynamic parallelism inference ability. */
@ExtendWith(ParameterizedTestExtension.class)
public class ParallelismConfiguredITCase {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelismConfiguredITCase.class);

    private static final RowType TABLE_TYPE =
            new RowType(
                    Arrays.asList(
                            new RowType.RowField("_k", new IntType()),
                            new RowType.RowField("p", new VarCharType(10)),
                            new RowType.RowField("v", new IntType())));

    private static final DataType INPUT_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("_k", DataTypes.INT()),
                    DataTypes.FIELD("p", DataTypes.STRING()),
                    DataTypes.FIELD("v", DataTypes.INT()));

    private static final int NUM_KEYS = 100;

    @TempDir private static java.nio.file.Path temporaryFolder;

    private final boolean isBatch;
    private final boolean hasPrimaryKey;
    private final int numBucket;

    public ParallelismConfiguredITCase(boolean isBatch, boolean hasPrimaryKey, int numBucket) {
        this.isBatch = isBatch;
        this.hasPrimaryKey = hasPrimaryKey;
        this.numBucket = numBucket;
    }

    @Parameters(name = "isBatch={0}, hasPrimaryKey={1}, numBucket={2}")
    public static List<Object[]> getVarSeg() {
        List<Boolean> isBatchList = Arrays.asList(true, false);
        List<Boolean> hasPrimaryKeyList = Arrays.asList(true, false);
        List<Integer> numBucketList = Arrays.asList(-1, 1, 8);
        List<Object[]> result = new ArrayList<>();
        for (Boolean isBatch : isBatchList) {
            for (Boolean hasPrimaryKey : hasPrimaryKeyList) {
                for (Integer numBucket : numBucketList) {
                    result.add(new Object[] {isBatch, hasPrimaryKey, numBucket});
                }
            }
        }

        return result;
    }

    @TestTemplate
    public void testParallelismConfigurable() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(BUCKET.key(), Integer.toString(numBucket));
        int[] primaryKey = hasPrimaryKey ? new int[] {0, 1} : new int[] {};
        if (primaryKey.length == 0 && numBucket > 0) {
            options.put(BUCKET_KEY.key(), "_k");
        }
        options.put(SINK_COMMITTER_OPERATOR_CHAINING.key(), "false");
        String tempDirPath = new File(temporaryFolder.toFile(), UUID.randomUUID() + "/").toString();
        FileStoreTable table = buildFileStoreTable(tempDirPath, new int[] {1}, primaryKey, options);

        Configuration configuration = new Configuration();
        configuration.set(
                ExecutionOptions.RUNTIME_MODE,
                isBatch ? RuntimeExecutionMode.BATCH : RuntimeExecutionMode.STREAMING);

        configuration.set(DEFAULT_PARALLELISM, 1);
        configuration.set(BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_MIN_PARALLELISM, 1);
        configuration.set(BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_MAX_PARALLELISM, 1);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        buildJob(env, table, 1);
        verifyJobGraph(env, table);
        env.execute();
        verifyResult(table, 1);

        LOG.info("restart job with parallelism 3");

        configuration.set(DEFAULT_PARALLELISM, 3);
        configuration.set(BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_MIN_PARALLELISM, 3);
        configuration.set(BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_MAX_PARALLELISM, 3);
        env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        buildJob(env, table, 2);
        verifyJobGraph(env, table);
        env.execute();
        verifyResult(table, 2);

        LOG.info("restart job with parallelism 5");

        configuration.set(DEFAULT_PARALLELISM, 5);
        configuration.set(BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_MIN_PARALLELISM, 5);
        configuration.set(BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_MAX_PARALLELISM, 5);
        env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        buildJob(env, table, 3);
        verifyJobGraph(env, table);
        env.execute();
        verifyResult(table, 3);
    }

    private static FileStoreTable buildFileStoreTable(
            String temporaryPath,
            int[] partitions,
            int[] primaryKey,
            Map<String, String> optionsMap)
            throws Exception {
        Options options = new Options();
        options.set(PATH, temporaryPath);
        options.set(FILE_FORMAT, CoreOptions.FILE_FORMAT_AVRO);
        for (Map.Entry<String, String> entry : optionsMap.entrySet()) {
            options.set(entry.getKey(), entry.getValue());
        }
        Path tablePath = new CoreOptions(options.toMap()).path();
        Schema schema =
                new Schema(
                        toDataType(TABLE_TYPE).getFields(),
                        Arrays.stream(partitions)
                                .mapToObj(i -> TABLE_TYPE.getFieldNames().get(i))
                                .collect(Collectors.toList()),
                        Arrays.stream(primaryKey)
                                .mapToObj(i -> TABLE_TYPE.getFieldNames().get(i))
                                .collect(Collectors.toList()),
                        options.toMap(),
                        "");
        return retryArtificialException(
                () -> {
                    new SchemaManager(LocalFileIO.create(), tablePath).createTable(schema);
                    return FileStoreTableFactory.create(LocalFileIO.create(), options);
                });
    }

    private void buildJob(StreamExecutionEnvironment env, FileStoreTable table, int round) {
        DataStream<Row> source =
                env.fromSequence((long) (round - 1) * NUM_KEYS, (long) round * NUM_KEYS - 1)
                        .map(
                                x ->
                                        Row.of(
                                                x.intValue() % NUM_KEYS,
                                                String.valueOf(x % NUM_KEYS),
                                                x.intValue()));
        source.getTransformation().setParallelism(source.getParallelism(), false);
        new FlinkSinkBuilder(table).forRow(source, INPUT_TYPE).build();
    }

    private void verifyJobGraph(StreamExecutionEnvironment env, FileStoreTable table) {
        for (JobVertex jobVertex : env.getStreamGraph(false).getJobGraph().getVertices()) {
            // The following operators should be forced to have single parallelism, and they should
            // not be chained with upstream operators to avoid affecting their ability on
            // auto-parallelism-inference in AQE.
            if (jobVertex.getName().startsWith("Global Committer")
                    || jobVertex.getName().startsWith("end: Writer")
                    || jobVertex.getName().startsWith("Compact Coordinator")) {
                assertThat(jobVertex.isParallelismConfigured())
                        .withFailMessage("Vertex %s should have parallelism configured", jobVertex)
                        .isTrue();
                assertThat(jobVertex.getParallelism())
                        .withFailMessage("Vertex %s should have parallelism 1", jobVertex)
                        .isOne();
                continue;
            }

            // Dynamic Bucket mode operators does not support parallelismConfigured.
            if (BucketMode.HASH_DYNAMIC.equals(table.bucketMode())
                    && isBatch
                    && (jobVertex.getName().contains("Writer")
                            || jobVertex.getName().contains("dynamic-bucket-assigner"))) {
                assertThat(jobVertex.isParallelismConfigured())
                        .withFailMessage("Vertex %s should have parallelism configured", jobVertex)
                        .isTrue();
                continue;
            }

            assertThat(jobVertex.isParallelismConfigured())
                    .withFailMessage("Vertex %s should not have parallelism configured", jobVertex)
                    .isFalse();
        }
    }

    private void verifyResult(FileStoreTable table, int round) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Row> source =
                new FlinkSourceBuilder(table).env(env).sourceBounded(true).buildForRow();
        List<Row> results = new ArrayList<>();
        try (CloseableIterator<Row> iterator = source.executeAndCollect()) {
            while (iterator.hasNext()) {
                results.add(iterator.next());
            }
        }
        if (hasPrimaryKey) {
            assertThat(results).hasSize(NUM_KEYS);
            results.sort(Comparator.comparingInt(x -> x.getFieldAs(0)));
            for (int i = 0; i < NUM_KEYS; i++) {
                Row result = results.get(i);
                assertThat(result.getField(0)).isEqualTo(i);
                assertThat(result.getField(1)).isEqualTo(Integer.toString(i));
                assertThat((int) result.getFieldAs(2)).isEqualTo((round - 1) * NUM_KEYS + i);
            }
        } else {
            assertThat(results).hasSize(NUM_KEYS * round);
            results.sort(Comparator.comparingInt(x -> x.getFieldAs(2)));
            for (int i = 0; i < NUM_KEYS * round; i++) {
                assertThat(results.get(i))
                        .isEqualTo(Row.of(i % NUM_KEYS, String.valueOf(i % NUM_KEYS), i));
            }
        }
    }
}
