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
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_OPERATOR_UID_COVER_ALL_OPERATORS;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_OPERATOR_UID_SUFFIX;
import static org.apache.paimon.flink.LogicalTypeConversion.toLogicalType;
import static org.apache.paimon.flink.OperatorUidGraphs.TEST_UID_PREFIX;
import static org.apache.paimon.flink.OperatorUidGraphs.operatorsWithoutUid;
import static org.apache.paimon.flink.OperatorUidGraphs.paimonOperatorIdsByUid;
import static org.apache.paimon.flink.OperatorUidGraphs.paimonUidsByOperatorName;
import static org.apache.paimon.flink.OperatorUidGraphs.uidsByOperatorName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

/**
 * Asserts that with {@code sink.operator-uid.cover-all-operators} every operator a streaming write
 * adds carries a uid, one shape per site that adds operators.
 */
class OperatorUidSuffixTest {

    private static final String UID_SUFFIX = "test-uid";

    /** Every table is called this, so a uid does not vary with the shape that built it. */
    private static final String TABLE_NAME = "tbl";

    private static final RowType TABLE_TYPE =
            RowType.of(
                    new DataType[] {DataTypes.INT(), DataTypes.BIGINT(), DataTypes.STRING()},
                    new String[] {"k", "v", "pt"});

    @TempDir static Path warehouse;

    static List<Shape> shapes() {
        return Arrays.asList(
                // AppendTableSink.doWrite, the streaming compaction pair
                shape("append").reaches("Compact Coordinator: tbl", "Compact Worker: tbl"),
                // FlinkSinkBuilder.forRow, a Row to RowData conversion on the caller's stream
                shape("append-for-row").forRow().reaches("Map"),
                // AppendTableSink.doWrite, the pre-commit compaction pair
                shape("append-precommit-compact")
                        .option(FlinkConnectorOptions.PRECOMMIT_COMPACT.key(), "true")
                        .reaches(
                                "New Files Compact Coordinator: tbl",
                                "New Files Compact Worker: tbl"),
                // FlinkSinkBuilder.applyDynamicPartitionShuffle
                shape("append-partition-dynamic")
                        .partitioned()
                        .option(
                                CoreOptions.PARTITION_SINK_STRATEGY.key(),
                                CoreOptions.PartitionSinkStrategy.PARTITION_DYNAMIC.name())
                        .reaches("Collect Statistics: tbl", "Strip Statistics"),
                // FlinkSink.doCoordinatorCommit: 'end' follows the writer, with no global
                // committer. Both commit routes name that node the same, so the route is
                // identified by the committer it does not build.
                shape("append-coordinator-commit")
                        .option(CoreOptions.WRITE_ONLY.key(), "true")
                        .option(FlinkConnectorOptions.SINK_COORDINATOR_COMMIT_ENABLED.key(), "true")
                        .reaches("end: Writer")
                        .avoids("Global Committer : tbl"),
                // FixedBucketSink on an append table, no compaction operators at all
                shape("append-fixed").bucket(2).reaches("Writer : tbl", "Global Committer : tbl"),
                shape("pk-fixed")
                        .primaryKey()
                        .bucket(2)
                        .reaches("Writer : tbl", "Global Committer : tbl"),
                // FlinkSinkBuilder.build, the local merge operator
                shape("pk-fixed-local-merge")
                        .primaryKey()
                        .bucket(2)
                        .option(CoreOptions.LOCAL_MERGE_BUFFER_SIZE.key(), "64 mb")
                        .reaches("local merge"),
                // FlinkSink.doWrite, the changelog compaction trio
                shape("pk-fixed-precommit-compact")
                        .primaryKey()
                        .bucket(2)
                        .option(FlinkConnectorOptions.PRECOMMIT_COMPACT.key(), "true")
                        .reaches(
                                "Changelog Compact Coordinator",
                                "Changelog Compact Worker",
                                "Changelog Sort by Creation Time"),
                shape("pk-dynamic").primaryKey().bucket(-1).reaches("dynamic-bucket-assigner"),
                // GlobalDynamicBucketSink, when the primary key does not cover the partition keys
                shape("pk-cross-partition")
                        .primaryKey()
                        .partitioned()
                        .bucket(-1)
                        .reaches("INDEX_BOOTSTRAP", "cross-partition-bucket-assigner"),
                shape("pk-postpone").primaryKey().bucket(-2).reaches("Writer : tbl"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("shapes")
    void testEveryOperatorCarriesAUid(Shape shape) {
        // auto-generate-uids off, so building the graph is itself Flink's uid assertion
        StreamGraph graph = buildGraph(shape, 0, false);

        List<String> names =
                graph.getStreamNodes().stream()
                        .map(StreamNode::getOperatorName)
                        .collect(Collectors.toList());
        assertThat(names)
                .as("shape %s exists to reach %s", shape, shape.reaches)
                .containsAll(shape.reaches);
        for (String avoided : shape.avoids) {
            assertThat(names)
                    .as("shape %s is the route that does not build '%s'", shape, avoided)
                    .doesNotContain(avoided);
        }
        assertThat(paimonUidsByOperatorName(graph).values())
                .as("shape %s: every uid is built from the table name and the suffix", shape)
                .allMatch(uid -> uid.endsWith("_" + TABLE_NAME + "_" + UID_SUFFIX));
    }

    /**
     * What the uids are for. Flink hashes a uid'd node from the uid alone, so with every node
     * covered the operator ids no longer depend on what sits upstream of the sink.
     */
    @Test
    void testOperatorIdsSurviveAnUpstreamChange() {
        Shape shape = shape("append");
        assertThat(paimonOperatorIdsByUid(buildGraph(shape, 1, true)))
                .isEqualTo(paimonOperatorIdsByUid(buildGraph(shape, 0, true)));
    }

    /** The coverage is opt-in: with the option off the graph is what it was before the option. */
    @Test
    void testTheOptionOffLeavesTodaysUidsOnly() {
        StreamGraph graph = buildGraph(shape("option-off").coverAll(false), 0, true);

        assertThat(paimonUidsByOperatorName(graph))
                .containsOnly(
                        entry("Writer : tbl", "Writer_tbl_test-uid"),
                        entry("Global Committer : tbl", "Global Committer_tbl_test-uid"));
        assertThat(operatorsWithoutUid(graph)).isNotEmpty();
    }

    /** With no suffix there is nothing to build a uid out of, option or not. */
    @Test
    void testTheOptionWithoutASuffixAssignsNothing() {
        assertThat(paimonUidsByOperatorName(buildGraph(shape("no-suffix").suffix(null), 0, true)))
                .isEmpty();
    }

    /**
     * The uids that exist today, pinned literally. A change to {@link
     * FlinkConnectorOptions#generateCustomUid} orphans every checkpoint written by an older Paimon,
     * and nothing else in the repo would notice.
     */
    @Test
    void testTheUidsThatExistTodayAreUnchanged() {
        assertThat(uidsByOperatorName(buildGraph(shape("golden").primaryKey().bucket(-1), 0, true)))
                .containsEntry("Writer : tbl", "Writer_tbl_test-uid")
                .containsEntry("Global Committer : tbl", "Global Committer_tbl_test-uid")
                .containsEntry("dynamic-bucket-assigner", "dynamic-bucket-assigner_tbl_test-uid");

        // write-only renames the writer's operator but not its uid
        assertThat(
                        uidsByOperatorName(
                                buildGraph(
                                        shape("golden-write-only")
                                                .option(CoreOptions.WRITE_ONLY.key(), "true"),
                                        0,
                                        true)))
                .containsEntry("Writer(write-only) : tbl", "Writer_tbl_test-uid");
    }

    /**
     * Every uid of two rich shapes, pinned literally, so a change to any prefix fails a test
     * instead of silently orphaning the checkpoints of every job that set the option.
     */
    @Test
    void testTheNewUidsArePinned() {
        Map<String, String> appendPrecommitCompact = new TreeMap<>();
        appendPrecommitCompact.put("Compact Coordinator: tbl", "Compact Coordinator_tbl_test-uid");
        appendPrecommitCompact.put("Compact Worker: tbl", "Compact Worker_tbl_test-uid");
        appendPrecommitCompact.put("Global Committer : tbl", "Global Committer_tbl_test-uid");
        appendPrecommitCompact.put("Map", "Internal Row Conversion_tbl_test-uid");
        appendPrecommitCompact.put(
                "New Files Compact Coordinator: tbl", "New Files Compact Coordinator_tbl_test-uid");
        appendPrecommitCompact.put(
                "New Files Compact Worker: tbl", "New Files Compact Worker_tbl_test-uid");
        appendPrecommitCompact.put("Writer : tbl", "Writer_tbl_test-uid");
        appendPrecommitCompact.put("end: Writer", "end_tbl_test-uid");
        assertThat(
                        paimonUidsByOperatorName(
                                buildGraph(
                                        shape("pinned-append-precommit-compact")
                                                .option(
                                                        FlinkConnectorOptions.PRECOMMIT_COMPACT
                                                                .key(),
                                                        "true"),
                                        0,
                                        true)))
                .isEqualTo(appendPrecommitCompact);

        Map<String, String> localMerge = new TreeMap<>();
        localMerge.put("Global Committer : tbl", "Global Committer_tbl_test-uid");
        localMerge.put("Map", "Internal Row Conversion_tbl_test-uid");
        localMerge.put("Writer : tbl", "Writer_tbl_test-uid");
        localMerge.put("end: Writer", "end_tbl_test-uid");
        localMerge.put("local merge", "local merge_tbl_test-uid");
        assertThat(
                        paimonUidsByOperatorName(
                                buildGraph(
                                        shape("pinned-pk-fixed-local-merge")
                                                .primaryKey()
                                                .bucket(2)
                                                .option(
                                                        CoreOptions.LOCAL_MERGE_BUFFER_SIZE.key(),
                                                        "64 mb"),
                                        0,
                                        true)))
                .isEqualTo(localMerge);

        Map<String, String> crossPartition = new TreeMap<>();
        crossPartition.put("Global Committer : tbl", "Global Committer_tbl_test-uid");
        crossPartition.put("INDEX_BOOTSTRAP", "INDEX_BOOTSTRAP_tbl_test-uid");
        crossPartition.put("Map", "Internal Row Conversion_tbl_test-uid");
        crossPartition.put("Writer : tbl", "Writer_tbl_test-uid");
        crossPartition.put(
                "cross-partition-bucket-assigner", "cross-partition-bucket-assigner_tbl_test-uid");
        crossPartition.put("end: Writer", "end_tbl_test-uid");
        assertThat(
                        paimonUidsByOperatorName(
                                buildGraph(
                                        shape("pinned-pk-cross-partition")
                                                .primaryKey()
                                                .partitioned()
                                                .bucket(-1),
                                        0,
                                        true)))
                .isEqualTo(crossPartition);
    }

    // ------------------------------------------------------------------------
    //  graph building
    // ------------------------------------------------------------------------

    private static StreamGraph buildGraph(
            Shape shape, int extraUpstreamOperators, boolean autoGenerateUids) {
        Configuration conf = new Configuration();
        conf.set(PipelineOptions.AUTO_GENERATE_UIDS, autoGenerateUids);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        // coordinator commit insists on checkpointing with one concurrent checkpoint
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        FileStoreTable table = shape.createTable();
        org.apache.flink.table.types.logical.RowType logicalType = toLogicalType(table.rowType());
        FlinkSinkBuilder builder = new FlinkSinkBuilder(table);
        if (shape.forRow) {
            org.apache.flink.table.types.DataType dataType = fromLogicalToDataType(logicalType);
            builder.forRow(
                    source(env, ExternalTypeInfo.of(dataType), extraUpstreamOperators), dataType);
        } else {
            TypeInformation<RowData> typeInfo = InternalTypeInfo.of(logicalType);
            builder.forRowData(source(env, typeInfo, extraUpstreamOperators));
        }
        // deliberately not uid'd by the test: the terminal sink is Paimon's operator, and SQL
        // users have no hook to uid it themselves
        builder.build();
        return env.getStreamGraph();
    }

    /** Everything the test adds carries a uid, so an operator without one is Paimon's. */
    private static <T> DataStream<T> source(
            StreamExecutionEnvironment env, TypeInformation<T> typeInfo, int extraOperators) {
        DataStream<T> stream =
                env.fromCollection(Collections.<T>emptyList(), typeInfo)
                        .name("test-source")
                        .uid(TEST_UID_PREFIX + "source");
        for (int i = 0; i < extraOperators; i++) {
            stream =
                    stream.map((MapFunction<T, T>) value -> value)
                            .returns(typeInfo)
                            .name("test-extra-" + i)
                            .uid(TEST_UID_PREFIX + "extra-" + i);
        }
        return stream;
    }

    private static Shape shape(String name) {
        return new Shape(name);
    }

    /** A table definition plus the entry point that feeds the sink. */
    private static final class Shape {

        private final String name;
        private final Map<String, String> options = new LinkedHashMap<>();
        private List<String> reaches = Collections.emptyList();
        private List<String> avoids = Collections.emptyList();
        private List<String> partitionKeys = Collections.emptyList();
        private List<String> primaryKeys = Collections.emptyList();
        private int bucket = -1;
        private boolean forRow;
        private boolean coverAll = true;
        private String uidSuffix = UID_SUFFIX;

        private Shape(String name) {
            this.name = name;
        }

        private Shape reaches(String... operatorNames) {
            this.reaches = Arrays.asList(operatorNames);
            return this;
        }

        /** Names no node may carry, for a route identified by what it does not build. */
        private Shape avoids(String... operatorNames) {
            this.avoids = Arrays.asList(operatorNames);
            return this;
        }

        private Shape partitioned() {
            this.partitionKeys = Collections.singletonList("pt");
            return this;
        }

        private Shape primaryKey() {
            this.primaryKeys = Collections.singletonList("k");
            return this;
        }

        /** {@code -1} is unaware or dynamic, {@code -2} postpone, a positive value fixed. */
        private Shape bucket(int bucket) {
            this.bucket = bucket;
            return this;
        }

        private Shape forRow() {
            this.forRow = true;
            return this;
        }

        private Shape coverAll(boolean coverAll) {
            this.coverAll = coverAll;
            return this;
        }

        private Shape suffix(String uidSuffix) {
            this.uidSuffix = uidSuffix;
            return this;
        }

        private Shape option(String key, String value) {
            this.options.put(key, value);
            return this;
        }

        /** A fresh table per graph, in its own directory so every table is called TABLE_NAME. */
        private FileStoreTable createTable() {
            Options options = Options.fromMap(this.options);
            org.apache.paimon.fs.Path path =
                    new org.apache.paimon.fs.Path(
                            warehouse
                                    .resolve(name + "-" + UUID.randomUUID())
                                    .resolve(TABLE_NAME)
                                    .toString());
            options.set(CoreOptions.PATH, path.toString());
            options.set(CoreOptions.BUCKET, bucket);
            if (bucket > 0 && primaryKeys.isEmpty()) {
                options.set(CoreOptions.BUCKET_KEY, "k");
            }
            if (uidSuffix != null) {
                options.set(SINK_OPERATOR_UID_SUFFIX, uidSuffix);
            }
            options.set(SINK_OPERATOR_UID_COVER_ALL_OPERATORS, coverAll);
            try {
                new FileSystemSchemaManager(LocalFileIO.create(), path)
                        .createTable(
                                new Schema(
                                        TABLE_TYPE.getFields(),
                                        partitionKeys,
                                        primaryKeys,
                                        options.toMap(),
                                        ""));
            } catch (Exception e) {
                throw new AssertionError("shape " + name + " has an illegal table", e);
            }
            return FileStoreTableFactory.create(LocalFileIO.create(), options);
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
