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
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
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
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.flink.configuration.CoreOptions.DEFAULT_PARALLELISM;
import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.CoreOptions.FILE_FORMAT;
import static org.apache.paimon.CoreOptions.PATH;
import static org.apache.paimon.flink.LogicalTypeConversion.toDataType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for {@code sink.striped-bucket-write.*} on fixed-bucket tables. */
public class StripedBucketWriteSinkITCase {

    @TempDir private static java.nio.file.Path temporaryFolder;

    private static final RowType TABLE_TYPE =
            new RowType(
                    Arrays.asList(
                            new RowType.RowField("k", new IntType()),
                            new RowType.RowField("v", new IntType())));

    private static final DataType INPUT_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("k", DataTypes.INT()), DataTypes.FIELD("v", DataTypes.INT()));

    private static final RowType INDEX_TABLE_TYPE =
            new RowType(
                    Arrays.asList(
                            new RowType.RowField("id", new IntType()),
                            new RowType.RowField(
                                    "content", new VarCharType(VarCharType.MAX_LENGTH))));

    private static final DataType INDEX_INPUT_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("id", DataTypes.INT()),
                    DataTypes.FIELD("content", DataTypes.STRING()));

    private static final int NUM_KEYS = 100;
    private static final int NUM_BUCKETS = 2;

    @Test
    public void testAutoStripesExpandWriterParallelism() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("sink.striped-bucket-write.enabled", "true");
        options.put("write-only", "true");
        options.put("sink.parallelism", "4");
        // sink parallelism 4 > bucket 2, striping should keep all 4 writers instead of capping to 2
        assertThat(writerParallelism(options, 4)).isEqualTo(4);
    }

    @Test
    public void testExplicitWritersPerBucketDerivesParallelism() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("sink.striped-bucket-write.enabled", "true");
        options.put("sink.striped-bucket-write.writers-per-bucket", "3");
        options.put("write-only", "true");
        // explicit writers-per-bucket=3 -> writer parallelism is 3 * bucket = 6, regardless of env
        // parallelism
        assertThat(writerParallelism(options, 1)).isEqualTo(6);
    }

    @Test
    public void testMatchParallelismStripes() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("sink.striped-bucket-write.enabled", "true");
        options.put("sink.striped-bucket-write.writers-per-bucket", "-1");
        options.put("write-only", "true");
        options.put("sink.parallelism", "4");
        // K = parallelism, every writer writes every bucket, parallelism stays 4
        assertThat(writerParallelism(options, 4)).isEqualTo(4);
    }

    @Test
    public void testDisabledKeepsBucketCap() throws Exception {
        // no striped-write options and no sink.parallelism -> writer parallelism caps to bucket
        // count
        assertThat(writerParallelism(new HashMap<>(), 4)).isEqualTo(NUM_BUCKETS);
    }

    @Test
    public void testExplicitOneWriterPerBucket() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("sink.striped-bucket-write.enabled", "true");
        options.put("sink.striped-bucket-write.writers-per-bucket", "1");
        options.put("write-only", "true");
        // a single writer per bucket still uses the striped computer (degenerate routing);
        // writer parallelism is 1 * bucket
        assertThat(writerParallelism(options, 4)).isEqualTo(NUM_BUCKETS);
    }

    @Test
    public void testWritersPerBucketIgnoredWhenDisabled() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("sink.striped-bucket-write.writers-per-bucket", "3");
        // enabled is the sole switch: without it writers-per-bucket is ignored, no write-only is
        // required, and the default path caps writer parallelism to the bucket count
        assertThat(writerParallelism(options, 4)).isEqualTo(NUM_BUCKETS);
    }

    @Test
    public void testWritersPerBucketZeroRejected() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("sink.striped-bucket-write.enabled", "true");
        options.put("write-only", "true");
        options.put("sink.striped-bucket-write.writers-per-bucket", "0");
        FileStoreTable table = buildTable(options);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(batchConfiguration(4));
        assertThatThrownBy(() -> buildJob(env, table, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("writers-per-bucket");
    }

    @Test
    public void testStripedWriteRequiresWriteOnly() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("sink.striped-bucket-write.enabled", "true");
        // write-only defaults to false; striped write must reject inline compaction by multiple
        // writers
        FileStoreTable table = buildTable(options);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(batchConfiguration(4));
        assertThatThrownBy(() -> buildJob(env, table, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("write-only");
    }

    @Test
    public void testStripedWriteRejectsDeletionVectors() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("sink.striped-bucket-write.enabled", "true");
        options.put("write-only", "true");
        options.put("deletion-vectors.enabled", "true");
        FileStoreTable table = buildTable(options);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(batchConfiguration(4));
        assertThatThrownBy(() -> buildJob(env, table, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("deletion-vectors");
    }

    @Test
    public void testStripedWriteAppendFixedBucket() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("sink.striped-bucket-write.enabled", "true");
        options.put("write-only", "true");
        options.put("sink.parallelism", "4");
        options.put("bucket-key", "k");
        // striping an append table breaks the append order contract, so it requires unordered
        options.put("bucket-append-ordered", "false");
        // append-only fixed-bucket table: writer parallelism is not capped to the bucket count
        assertThat(writerParallelism(options, 4, Collections.emptyList(), NUM_BUCKETS))
                .isEqualTo(4);

        // records round-robin across the writers of each bucket; all appended rows are retained
        FileStoreTable table = buildTable(options, Collections.emptyList(), NUM_BUCKETS);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(batchConfiguration(4));
        buildJob(env, table, 1);
        env.execute();
        assertThat(readResults(table)).hasSize(NUM_KEYS);
    }

    @Test
    public void testStripedWriteRejectsOrderedAppend() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("sink.striped-bucket-write.enabled", "true");
        options.put("write-only", "true");
        options.put("bucket-key", "k");
        // bucket-append-ordered defaults to true; striping successive rows across writers would
        // break the ordered append read contract, so the sink must reject it
        FileStoreTable table = buildTable(options, Collections.emptyList(), NUM_BUCKETS);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(batchConfiguration(4));
        assertThatThrownBy(() -> buildJob(env, table, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bucket-append-ordered");
    }

    @Test
    public void testStripedWriteRejectsPrimaryKeyIndex() throws Exception {
        // a full-text index on a FIRST_ROW table is valid without deletion vectors, so the
        // primary-key index guard (not the deletion-vectors guard) must reject striped write:
        // each writer would publish a conflicting index for the same bucket
        Map<String, String> options = new HashMap<>();
        options.put("sink.striped-bucket-write.enabled", "true");
        options.put("write-only", "true");
        options.put("merge-engine", "first-row");
        options.put("pk-full-text.index.columns", "content");
        FileStoreTable table =
                buildTableWithType(options, INDEX_TABLE_TYPE, Collections.singletonList("id"), 1);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(batchConfiguration(4));
        DataStream<Row> source =
                env.fromSequence(1, 3).map(x -> Row.of(x.intValue(), "content-" + x));
        source.getTransformation().setParallelism(source.getParallelism(), false);
        FlinkSinkBuilder builder =
                new FlinkSinkBuilder(table).forRow(source, INDEX_INPUT_TYPE).parallelism(4);
        assertThatThrownBy(() -> builder.build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("primary-key indexes");
    }

    @Test
    public void testStripedWriteIgnoredForDynamicBucket() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("sink.striped-bucket-write.enabled", "true");
        // dynamic-bucket mode never reads the striped-write option: no write-only needed, builds
        // normally
        FileStoreTable table = buildTable(options, Collections.singletonList("k"), -1);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(batchConfiguration(4));
        assertThatCode(() -> buildJob(env, table, 1)).doesNotThrowAnyException();
    }

    @Test
    public void testStripedWriteIgnoredForUnawareBucket() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("sink.striped-bucket-write.enabled", "true");
        // unaware-bucket append mode never reads the striped-write option: builds normally
        FileStoreTable table = buildTable(options, Collections.emptyList(), -1);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(batchConfiguration(4));
        assertThatCode(() -> buildJob(env, table, 1)).doesNotThrowAnyException();
    }

    @Test
    public void testStripedWriteReadCorrectness() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("sink.striped-bucket-write.enabled", "true");
        options.put("write-only", "true");
        options.put("sink.parallelism", "4");
        FileStoreTable table = buildTable(options);

        // two rounds of upserts on the same keys; the second round must win after merge-on-read
        for (int round = 1; round <= 2; round++) {
            StreamExecutionEnvironment env =
                    StreamExecutionEnvironment.getExecutionEnvironment(batchConfiguration(4));
            buildJob(env, table, round);
            env.execute();
        }

        List<Row> results = readResults(table);
        assertThat(results).hasSize(NUM_KEYS);
        results.sort(Comparator.comparingInt(x -> x.getFieldAs(0)));
        for (int i = 0; i < NUM_KEYS; i++) {
            Row result = results.get(i);
            assertThat(result.getField(0)).isEqualTo(i);
            assertThat((int) result.getFieldAs(1)).isEqualTo(NUM_KEYS + i);
        }
    }

    private int writerParallelism(Map<String, String> tableOptions, int envParallelism)
            throws Exception {
        return writerParallelism(
                tableOptions, envParallelism, Collections.singletonList("k"), NUM_BUCKETS);
    }

    private int writerParallelism(
            Map<String, String> tableOptions,
            int envParallelism,
            List<String> primaryKeys,
            int bucket)
            throws Exception {
        FileStoreTable table = buildTable(tableOptions, primaryKeys, bucket);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(
                        batchConfiguration(envParallelism));
        buildJob(env, table, 1);
        for (JobVertex vertex : env.getStreamGraph(false).getJobGraph().getVertices()) {
            if (vertex.getName().startsWith("Writer")) {
                return vertex.getParallelism();
            }
        }
        throw new AssertionError("Writer vertex not found");
    }

    private Configuration batchConfiguration(int parallelism) {
        Configuration configuration = new Configuration();
        configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        configuration.set(DEFAULT_PARALLELISM, parallelism);
        configuration.set(
                BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_MIN_PARALLELISM, parallelism);
        configuration.set(
                BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_MAX_PARALLELISM, parallelism);
        return configuration;
    }

    private FileStoreTable buildTable(Map<String, String> tableOptions) throws Exception {
        return buildTable(tableOptions, Collections.singletonList("k"), NUM_BUCKETS);
    }

    private FileStoreTable buildTable(
            Map<String, String> tableOptions, List<String> primaryKeys, int bucket)
            throws Exception {
        return buildTableWithType(tableOptions, TABLE_TYPE, primaryKeys, bucket);
    }

    private FileStoreTable buildTableWithType(
            Map<String, String> tableOptions, RowType rowType, List<String> primaryKeys, int bucket)
            throws Exception {
        Options options = new Options();
        String path = new File(temporaryFolder.toFile(), UUID.randomUUID() + "/").toString();
        options.set(PATH, path);
        options.set(FILE_FORMAT, CoreOptions.FILE_FORMAT_AVRO);
        options.set(BUCKET, bucket);
        for (Map.Entry<String, String> entry : tableOptions.entrySet()) {
            options.set(entry.getKey(), entry.getValue());
        }
        Path tablePath = new CoreOptions(options.toMap()).path();
        Schema schema =
                new Schema(
                        toDataType(rowType).getFields(),
                        Collections.emptyList(),
                        primaryKeys,
                        options.toMap(),
                        "");
        new FileSystemSchemaManager(LocalFileIO.create(), tablePath).createTable(schema);
        return FileStoreTableFactory.create(LocalFileIO.create(), options);
    }

    private void buildJob(StreamExecutionEnvironment env, FileStoreTable table, int round) {
        DataStream<Row> source =
                env.fromSequence((long) (round - 1) * NUM_KEYS, (long) round * NUM_KEYS - 1)
                        .map(x -> Row.of(x.intValue() % NUM_KEYS, x.intValue()));
        source.getTransformation().setParallelism(source.getParallelism(), false);
        FlinkSinkBuilder builder = new FlinkSinkBuilder(table).forRow(source, INPUT_TYPE);
        // mirror FlinkTableSinkBase, which feeds sink.parallelism into the builder
        String sinkParallelism = table.options().get("sink.parallelism");
        if (sinkParallelism != null) {
            builder.parallelism(Integer.parseInt(sinkParallelism));
        }
        builder.build();
    }

    private List<Row> readResults(FileStoreTable table) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Row> source =
                new FlinkSourceBuilder(table).env(env).sourceBounded(true).buildForRow();
        List<Row> results = new ArrayList<>();
        try (CloseableIterator<Row> iterator = source.executeAndCollect()) {
            while (iterator.hasNext()) {
                results.add(iterator.next());
            }
        }
        return results;
    }
}
