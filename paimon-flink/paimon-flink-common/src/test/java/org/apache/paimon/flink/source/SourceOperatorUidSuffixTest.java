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

package org.apache.paimon.flink.source;

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
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
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

import static org.apache.paimon.flink.FlinkConnectorOptions.SOURCE_OPERATOR_UID_COVER_ALL_OPERATORS;
import static org.apache.paimon.flink.FlinkConnectorOptions.SOURCE_OPERATOR_UID_SUFFIX;
import static org.apache.paimon.flink.OperatorUidGraphs.TEST_UID_PREFIX;
import static org.apache.paimon.flink.OperatorUidGraphs.operatorsWithoutUid;
import static org.apache.paimon.flink.OperatorUidGraphs.paimonOperatorIdsByUid;
import static org.apache.paimon.flink.OperatorUidGraphs.paimonUidsByOperatorName;
import static org.apache.paimon.flink.OperatorUidGraphs.uidsByOperatorName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

/**
 * Asserts that with {@code source.operator-uid.cover-all-operators} every operator a streaming read
 * adds carries a uid, one shape per route {@link FlinkSourceBuilder} can take.
 */
class SourceOperatorUidSuffixTest {

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
                // FlinkSourceBuilder.toDataStream, the one node the suffix reaches on its own
                shape("bounded").bounded().reaches("Source: tbl"),
                shape("unbounded").reaches("Source: tbl"),
                // FlinkSourceBuilder.buildForRow, a RowData to Row conversion
                shape("bounded-for-row").bounded().forRow().reaches("Map"),
                // MonitorSource.buildSource: a monitor and a reader, neither built by toDataStream
                shape("dedicated-split-generation")
                        .bounded()
                        .option(FlinkConnectorOptions.SCAN_DEDICATED_SPLIT_GENERATION.key(), "true")
                        .reaches("Source: tbl-Monitor", "tbl-Reader"),
                // the same route with a watermark strategy, assigned as a separate operator here
                shape("dedicated-split-generation-watermarked")
                        .bounded()
                        .watermarked()
                        .option(FlinkConnectorOptions.SCAN_DEDICATED_SPLIT_GENERATION.key(), "true")
                        .reaches("Timestamps/Watermarks"),
                // the unbounded way into the same MonitorSource route
                shape("consumer-exactly-once")
                        .option(CoreOptions.CONSUMER_ID.key(), "uid-coverage-consumer")
                        .option(CoreOptions.CONSUMER_EXPIRATION_TIME.key(), "1 d")
                        .option(CoreOptions.CONSUMER_CONSISTENCY_MODE.key(), "exactly-once")
                        .reaches("Source: tbl-Monitor", "tbl-Reader"),
                // buildAlignedContinuousFileSource, still toDataStream
                shape("checkpoint-align")
                        .option(FlinkConnectorOptions.SOURCE_CHECKPOINT_ALIGN_ENABLED.key(), "true")
                        .reaches("Source: tbl"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("shapes")
    void testEveryOperatorCarriesAUid(Shape shape) {
        // auto-generate-uids off, so building the graph is itself Flink's uid assertion
        StreamGraph graph = buildGraph(shape, false, false);

        List<String> names =
                graph.getStreamNodes().stream()
                        .map(StreamNode::getOperatorName)
                        .collect(Collectors.toList());
        assertThat(names)
                .as("shape %s exists to reach %s", shape, shape.reaches)
                .containsAll(shape.reaches);
        assertThat(paimonUidsByOperatorName(graph).values())
                .as("shape %s: every uid is built from the table name and the suffix", shape)
                .allMatch(uid -> uid.endsWith("_" + TABLE_NAME + "_" + UID_SUFFIX));
    }

    /**
     * A source has nothing upstream, so the change that would move its ids is another branch added
     * to the same job. Flink hashes a uid-less node partly from how many nodes it hashed before it.
     */
    @Test
    void testOperatorIdsSurviveAnotherSourceInTheJob() {
        Shape shape =
                shape("dedicated-split-generation")
                        .bounded()
                        .option(
                                FlinkConnectorOptions.SCAN_DEDICATED_SPLIT_GENERATION.key(),
                                "true");
        assertThat(paimonOperatorIdsByUid(buildGraph(shape, true, true)))
                .isEqualTo(paimonOperatorIdsByUid(buildGraph(shape, false, true)));
    }

    /** The coverage is opt-in: with the option off the graph is what it was before the option. */
    @Test
    void testTheOptionOffLeavesTodaysUidOnly() {
        StreamGraph graph =
                buildGraph(shape("option-off").bounded().forRow().coverAll(false), false, true);

        assertThat(paimonUidsByOperatorName(graph))
                .containsOnly(entry("Source: tbl", "Source_tbl_test-uid"));
        assertThat(operatorsWithoutUid(graph)).isNotEmpty();
    }

    /** With no suffix there is nothing to build a uid out of, option or not. */
    @Test
    void testTheOptionWithoutASuffixAssignsNothing() {
        assertThat(
                        paimonUidsByOperatorName(
                                buildGraph(shape("no-suffix").bounded().suffix(null), false, true)))
                .isEmpty();
    }

    /** The one source uid that exists today, pinned literally. */
    @Test
    void testTheUidThatExistsTodayIsUnchanged() {
        assertThat(uidsByOperatorName(buildGraph(shape("golden").bounded(), false, true)))
                .containsEntry("Source: tbl", "Source_tbl_test-uid");
    }

    /**
     * Every uid of the richest read shape, pinned literally, so a change to any prefix fails a test
     * instead of silently orphaning the checkpoints of every job that set the option.
     */
    @Test
    void testTheNewUidsArePinned() {
        Map<String, String> expected = new TreeMap<>();
        expected.put("Source: tbl-Monitor", "Monitor_tbl_test-uid");
        expected.put("Timestamps/Watermarks", "Timestamps/Watermarks_tbl_test-uid");
        expected.put("tbl-Reader", "Reader_tbl_test-uid");
        assertThat(
                        paimonUidsByOperatorName(
                                buildGraph(
                                        shape("pinned-dedicated-split-generation-watermarked")
                                                .bounded()
                                                .watermarked()
                                                .option(
                                                        FlinkConnectorOptions
                                                                .SCAN_DEDICATED_SPLIT_GENERATION
                                                                .key(),
                                                        "true"),
                                        false,
                                        true)))
                .isEqualTo(expected);
    }

    // ------------------------------------------------------------------------
    //  graph building
    // ------------------------------------------------------------------------

    private static StreamGraph buildGraph(
            Shape shape, boolean withAnotherSource, boolean autoGenerateUids) {
        Configuration conf = new Configuration();
        conf.set(PipelineOptions.AUTO_GENERATE_UIDS, autoGenerateUids);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        env.setRuntimeMode(
                shape.bounded ? RuntimeExecutionMode.BATCH : RuntimeExecutionMode.STREAMING);
        // the checkpoint-align route insists on checkpointing with one concurrent checkpoint
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        if (withAnotherSource) {
            // an unrelated branch of the same job, entirely the test's own and fully uid'd
            env.fromCollection(Collections.<Long>emptyList(), Types.LONG)
                    .name("test-other-source")
                    .uid(TEST_UID_PREFIX + "other-source")
                    .sinkTo(new DiscardingSink<>())
                    .name("test-other-sink")
                    .uid(TEST_UID_PREFIX + "other-sink");
        }

        FlinkSourceBuilder builder =
                new FlinkSourceBuilder(shape.createTable()).env(env).sourceBounded(shape.bounded);
        if (shape.watermarked) {
            builder.watermarkStrategy(WatermarkStrategy.noWatermarks());
        }
        DataStream<?> stream = shape.forRow ? builder.buildForRow() : builder.build();
        stream.sinkTo(new DiscardingSink<>()).name("test-sink").uid(TEST_UID_PREFIX + "sink");
        return env.getStreamGraph();
    }

    private static Shape shape(String name) {
        return new Shape(name);
    }

    /** A table definition plus the entry point and boundedness of the read. */
    private static final class Shape {

        private final String name;
        private final Map<String, String> options = new LinkedHashMap<>();
        private List<String> reaches = Collections.emptyList();
        private boolean bounded;
        private boolean forRow;
        private boolean watermarked;
        private boolean coverAll = true;
        private String uidSuffix = UID_SUFFIX;

        private Shape(String name) {
            this.name = name;
        }

        private Shape reaches(String... operatorNames) {
            this.reaches = Arrays.asList(operatorNames);
            return this;
        }

        private Shape bounded() {
            this.bounded = true;
            return this;
        }

        private Shape forRow() {
            this.forRow = true;
            return this;
        }

        private Shape watermarked() {
            this.watermarked = true;
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
            if (uidSuffix != null) {
                options.set(SOURCE_OPERATOR_UID_SUFFIX, uidSuffix);
            }
            options.set(SOURCE_OPERATOR_UID_COVER_ALL_OPERATORS, coverAll);
            try {
                new FileSystemSchemaManager(LocalFileIO.create(), path)
                        .createTable(
                                new Schema(
                                        TABLE_TYPE.getFields(),
                                        Collections.emptyList(),
                                        Collections.emptyList(),
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
