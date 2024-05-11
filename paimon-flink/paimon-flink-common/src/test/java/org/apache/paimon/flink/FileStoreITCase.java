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

package org.apache.paimon.flink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.flink.sink.FixedBucketSink;
import org.apache.paimon.flink.sink.FlinkSinkBuilder;
import org.apache.paimon.flink.source.ContinuousFileStoreSource;
import org.apache.paimon.flink.source.FlinkSourceBuilder;
import org.apache.paimon.flink.source.StaticFileStoreSource;
import org.apache.paimon.flink.util.AbstractTestBase;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.utils.BlockingIterator;
import org.apache.paimon.utils.FailingFileIO;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.CoreOptions.BUCKET_KEY;
import static org.apache.paimon.CoreOptions.FILE_FORMAT;
import static org.apache.paimon.CoreOptions.PATH;
import static org.apache.paimon.flink.LogicalTypeConversion.toDataType;
import static org.apache.paimon.utils.FailingFileIO.retryArtificialException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * ITCase for {@link StaticFileStoreSource}, {@link ContinuousFileStoreSource} and {@link
 * FixedBucketSink}.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class FileStoreITCase extends AbstractTestBase {

    public static final RowType TABLE_TYPE =
            new RowType(
                    Arrays.asList(
                            new RowType.RowField("v", new IntType()),
                            new RowType.RowField("p", new VarCharType(10)),
                            // rename key
                            new RowType.RowField("_k", new IntType())));

    public static final ObjectIdentifier IDENTIFIER = ObjectIdentifier.of("catalog", "db", "t");

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static final DataStructureConverter<RowData, Row> CONVERTER =
            (DataStructureConverter)
                    DataStructureConverters.getConverter(
                            TypeConversions.fromLogicalToDataType(TABLE_TYPE));

    private static final int NUM_BUCKET = 3;

    public static final List<RowData> SOURCE_DATA =
            Arrays.asList(
                    wrap(GenericRowData.of(0, StringData.fromString("p1"), 1)),
                    wrap(GenericRowData.of(0, StringData.fromString("p1"), 2)),
                    wrap(GenericRowData.of(0, StringData.fromString("p1"), 1)),
                    wrap(GenericRowData.of(5, StringData.fromString("p1"), 1)),
                    wrap(GenericRowData.of(6, StringData.fromString("p2"), 1)),
                    wrap(GenericRowData.of(3, StringData.fromString("p2"), 5)),
                    wrap(GenericRowData.of(5, StringData.fromString("p2"), 1)));

    private final boolean isBatch;

    private final StreamExecutionEnvironment env;

    public FileStoreITCase(boolean isBatch) {
        this.isBatch = isBatch;
        this.env = isBatch ? buildBatchEnv() : buildStreamEnv();
    }

    @Parameters(name = "isBatch-{0}")
    public static List<Boolean> getVarSeg() {
        return Arrays.asList(true, false);
    }

    private static SerializableRowData wrap(RowData row) {
        return new SerializableRowData(row, InternalSerializers.create(TABLE_TYPE));
    }

    @TestTemplate
    public void testRowSourceSink() throws Exception {
        FileStoreTable table = buildFileStoreTable(new int[] {1}, new int[] {1, 2});

        // write
        DataStreamSource<RowData> source = buildTestSource(env, isBatch);
        DataStream<Row> input =
                source.map(
                                (MapFunction<RowData, Row>)
                                        r ->
                                                Row.of(
                                                        r.getInt(0),
                                                        r.getString(1).toString(),
                                                        r.getInt(2)))
                        .setParallelism(source.getParallelism());
        DataType inputType =
                DataTypes.ROW(
                        DataTypes.FIELD("v", DataTypes.INT()),
                        DataTypes.FIELD("p", DataTypes.STRING()),
                        DataTypes.FIELD("_k", DataTypes.INT()));
        new FlinkSinkBuilder(table).forRow(input, inputType).build();
        env.execute();

        // read
        List<Row> results =
                executeAndCollectRow(
                        new FlinkSourceBuilder(table).env(env).sourceBounded(true).buildForRow());

        // assert
        Row[] expected =
                new Row[] {
                    Row.of(5, "p2", 1), Row.of(3, "p2", 5), Row.of(5, "p1", 1), Row.of(0, "p1", 2)
                };
        assertThat(results).containsExactlyInAnyOrder(expected);
    }

    @TestTemplate
    public void testPartitioned() throws Exception {
        FileStoreTable table = buildFileStoreTable(new int[] {1}, new int[] {1, 2});

        // write
        new FlinkSinkBuilder(table).forRowData(buildTestSource(env, isBatch)).build();
        env.execute();

        // read
        List<Row> results =
                executeAndCollect(
                        new FlinkSourceBuilder(table).sourceBounded(true).env(env).build());

        // assert
        Row[] expected =
                new Row[] {
                    Row.of(5, "p2", 1), Row.of(3, "p2", 5), Row.of(5, "p1", 1), Row.of(0, "p1", 2)
                };
        assertThat(results).containsExactlyInAnyOrder(expected);
    }

    @TestTemplate
    public void testNonPartitioned() throws Exception {
        FileStoreTable table = buildFileStoreTable(new int[0], new int[] {2});

        // write
        new FlinkSinkBuilder(table).forRowData(buildTestSource(env, isBatch)).build();
        env.execute();

        // read
        List<Row> results =
                executeAndCollect(
                        new FlinkSourceBuilder(table).sourceBounded(true).env(env).build());

        // assert
        Row[] expected = new Row[] {Row.of(5, "p2", 1), Row.of(0, "p1", 2), Row.of(3, "p2", 5)};
        assertThat(results).containsExactlyInAnyOrder(expected);
    }

    @TestTemplate
    public void testOverwrite() throws Exception {
        assumeTrue(isBatch);

        FileStoreTable table = buildFileStoreTable(new int[] {1}, new int[] {1, 2});

        // write
        new FlinkSinkBuilder(table).forRowData(buildTestSource(env, isBatch)).build();
        env.execute();

        // overwrite p2
        DataStreamSource<RowData> partialData =
                env.fromCollection(
                        Collections.singletonList(
                                wrap(GenericRowData.of(9, StringData.fromString("p2"), 5))),
                        InternalTypeInfo.of(TABLE_TYPE));
        Map<String, String> overwrite = new HashMap<>();
        overwrite.put("p", "p2");
        new FlinkSinkBuilder(table).forRowData(partialData).overwrite(overwrite).build();
        env.execute();

        // read
        List<Row> results =
                executeAndCollect(
                        new FlinkSourceBuilder(table).sourceBounded(true).env(env).build());

        Row[] expected = new Row[] {Row.of(9, "p2", 5), Row.of(5, "p1", 1), Row.of(0, "p1", 2)};
        assertThat(results).containsExactlyInAnyOrder(expected);

        // test dynamic partition overwrite
        partialData =
                env.fromCollection(
                        Collections.singletonList(
                                wrap(GenericRowData.of(19, StringData.fromString("p2"), 6))),
                        InternalTypeInfo.of(TABLE_TYPE));
        new FlinkSinkBuilder(table).forRowData(partialData).overwrite().build();
        env.execute();

        // read
        results =
                executeAndCollect(
                        new FlinkSourceBuilder(table).sourceBounded(true).env(env).build());
        expected = new Row[] {Row.of(19, "p2", 6), Row.of(5, "p1", 1), Row.of(0, "p1", 2)};
        assertThat(results).containsExactlyInAnyOrder(expected);

        // test static overwrite all
        partialData =
                env.fromCollection(
                        Collections.singletonList(
                                wrap(GenericRowData.of(20, StringData.fromString("p2"), 3))),
                        InternalTypeInfo.of(TABLE_TYPE));
        new FlinkSinkBuilder(
                        table.copy(
                                Collections.singletonMap(
                                        CoreOptions.DYNAMIC_PARTITION_OVERWRITE.key(), "false")))
                .forRowData(partialData)
                .overwrite()
                .build();
        env.execute();

        // read
        results =
                executeAndCollect(
                        new FlinkSourceBuilder(table).sourceBounded(true).env(env).build());
        expected = new Row[] {Row.of(20, "p2", 3)};
        assertThat(results).containsExactlyInAnyOrder(expected);
    }

    @TestTemplate
    public void testPartitionedNonKey() throws Exception {
        FileStoreTable table = buildFileStoreTable(new int[] {1}, new int[0]);

        // write
        new FlinkSinkBuilder(table).forRowData(buildTestSource(env, isBatch)).build();
        env.execute();

        // read
        List<Row> results =
                executeAndCollect(
                        new FlinkSourceBuilder(table).sourceBounded(true).env(env).build());

        // assert
        // in streaming mode, expect origin data X 2 (FiniteTestSource)
        Stream<RowData> expectedStream =
                isBatch
                        ? SOURCE_DATA.stream()
                        : Stream.concat(SOURCE_DATA.stream(), SOURCE_DATA.stream());
        Row[] expected = expectedStream.map(CONVERTER::toExternal).toArray(Row[]::new);
        assertThat(results).containsExactlyInAnyOrder(expected);
    }

    @TestTemplate
    public void testKeyedProjection() throws Exception {
        testProjection(buildFileStoreTable(new int[0], new int[] {2}));
    }

    @TestTemplate
    public void testNonKeyedProjection() throws Exception {
        testProjection(buildFileStoreTable(new int[0], new int[0]));
    }

    private void testProjection(FileStoreTable table) throws Exception {
        // write
        new FlinkSinkBuilder(table).forRowData(buildTestSource(env, isBatch)).build();
        env.execute();

        // read
        Projection projection = Projection.of(new int[] {1, 2});
        @SuppressWarnings({"unchecked", "rawtypes"})
        DataStructureConverter<RowData, Row> converter =
                (DataStructureConverter)
                        DataStructureConverters.getConverter(
                                TypeConversions.fromLogicalToDataType(
                                        projection.project(TABLE_TYPE)));
        List<Row> results =
                executeAndCollect(
                        new FlinkSourceBuilder(table)
                                .sourceBounded(true)
                                .projection(projection.toNestedIndexes())
                                .env(env)
                                .build(),
                        converter);

        // assert
        Row[] expected = new Row[] {Row.of("p2", 1), Row.of("p1", 2), Row.of("p2", 5)};
        if (table.schema().trimmedPrimaryKeys().isEmpty()) {
            // in streaming mode, expect origin data X 2 (FiniteTestSource)
            Stream<RowData> expectedStream =
                    isBatch
                            ? SOURCE_DATA.stream()
                            : Stream.concat(SOURCE_DATA.stream(), SOURCE_DATA.stream());
            expected =
                    expectedStream
                            .map(CONVERTER::toExternal)
                            .map(r -> Row.of(r.getField(1), r.getField(2)))
                            .toArray(Row[]::new);
        }
        assertThat(results).containsExactlyInAnyOrder(expected);
    }

    @TestTemplate
    public void testContinuous() throws Exception {
        innerTestContinuous(buildFileStoreTable(new int[0], new int[] {2}));
    }

    @TestTemplate
    public void testContinuousWithoutPK() throws Exception {
        innerTestContinuous(buildFileStoreTable(new int[0], new int[0]));
    }

    @TestTemplate
    public void testContinuousBounded() throws Exception {
        FileStoreTable table = buildFileStoreTable(new int[0], new int[] {2});
        table =
                table.copy(
                        Collections.singletonMap(CoreOptions.SCAN_BOUNDED_WATERMARK.key(), "1024"));
        DataStream<RowData> source =
                new FlinkSourceBuilder(table).sourceBounded(false).env(env).build();
        Transformation<RowData> transformation = source.getTransformation();
        assertThat(transformation).isInstanceOf(SourceTransformation.class);
        assertThat(((SourceTransformation<?, ?, ?>) transformation).getSource().getBoundedness())
                .isEqualTo(Boundedness.BOUNDED);
    }

    private void innerTestContinuous(FileStoreTable table) throws Exception {
        assumeFalse(isBatch);

        BlockingIterator<RowData, Row> iterator =
                BlockingIterator.of(
                        new FlinkSourceBuilder(table)
                                .sourceBounded(false)
                                .env(env)
                                .build()
                                .executeAndCollect(),
                        CONVERTER::toExternal);
        Thread.sleep(ThreadLocalRandom.current().nextInt(1000));

        sinkAndValidate(
                table,
                Arrays.asList(
                        srcRow(RowKind.INSERT, 1, "p1", 1), srcRow(RowKind.INSERT, 2, "p2", 2)),
                iterator,
                Row.ofKind(RowKind.INSERT, 1, "p1", 1),
                Row.ofKind(RowKind.INSERT, 2, "p2", 2));

        if (table.primaryKeys().size() > 0) {
            // only primary key table can accept delete
            sinkAndValidate(
                    table,
                    Arrays.asList(
                            srcRow(RowKind.DELETE, 1, "p1", 1), srcRow(RowKind.INSERT, 3, "p3", 3)),
                    iterator,
                    Row.ofKind(RowKind.DELETE, 1, "p1", 1),
                    Row.ofKind(RowKind.INSERT, 3, "p3", 3));
        }
    }

    private void sinkAndValidate(
            FileStoreTable table,
            List<RowData> src,
            BlockingIterator<RowData, Row> iterator,
            Row... expected)
            throws Exception {
        if (isBatch) {
            throw new UnsupportedOperationException();
        }
        DataStreamSource<RowData> source =
                env.addSource(new FiniteTestSource<>(src, true), InternalTypeInfo.of(TABLE_TYPE));
        new FlinkSinkBuilder(table).forRowData(source).build();
        env.execute();
        assertThat(iterator.collect(expected.length)).containsExactlyInAnyOrder(expected);
    }

    public FileStoreTable buildFileStoreTable(int[] partitions, int[] primaryKey) throws Exception {
        return buildFileStoreTable(isBatch, getTempDirPath(), partitions, primaryKey);
    }

    private static RowData srcRow(RowKind kind, int v, String p, int k) {
        return wrap(GenericRowData.ofKind(kind, v, StringData.fromString(p), k));
    }

    public StreamExecutionEnvironment buildStreamEnv() {
        return streamExecutionEnvironmentBuilder()
                .streamingMode()
                .checkpointIntervalMs(100)
                .parallelism(2)
                .build();
    }

    public StreamExecutionEnvironment buildBatchEnv() {
        return streamExecutionEnvironmentBuilder().batchMode().parallelism(2).build();
    }

    public static FileStoreTable buildFileStoreTable(
            boolean noFail, String temporaryPath, int[] partitions, int[] primaryKey)
            throws Exception {
        Options options = buildConfiguration(noFail, temporaryPath);
        Path tablePath = new CoreOptions(options.toMap()).path();
        if (primaryKey.length == 0) {
            options.set(BUCKET_KEY, "_k");
        }
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

    public static Options buildConfiguration(boolean noFail, String temporaryPath) {
        Options options = new Options();
        options.set(BUCKET, NUM_BUCKET);
        if (noFail) {
            options.set(PATH, temporaryPath);
        } else {
            String failingName = UUID.randomUUID().toString();
            FailingFileIO.reset(failingName, 3, 100);
            options.set(PATH, FailingFileIO.getFailingPath(failingName, temporaryPath));
        }
        options.set(FILE_FORMAT, CoreOptions.FILE_FORMAT_AVRO);
        return options;
    }

    public static DataStreamSource<RowData> buildTestSource(
            StreamExecutionEnvironment env, boolean isBatch) {
        return isBatch
                ? env.fromCollection(SOURCE_DATA, InternalTypeInfo.of(TABLE_TYPE))
                : env.addSource(
                        new FiniteTestSource<>(SOURCE_DATA, false),
                        InternalTypeInfo.of(TABLE_TYPE));
    }

    public static List<Row> executeAndCollect(DataStream<RowData> source) throws Exception {
        return executeAndCollect(source, CONVERTER);
    }

    public static List<Row> executeAndCollect(
            DataStream<RowData> source, DataStructureConverter<RowData, Row> converter)
            throws Exception {
        CloseableIterator<RowData> iterator = source.executeAndCollect();
        List<Row> results = new ArrayList<>();
        while (iterator.hasNext()) {
            results.add(converter.toExternal(iterator.next()));
        }
        iterator.close();
        return results;
    }

    public static List<Row> executeAndCollectRow(DataStream<Row> source) throws Exception {
        CloseableIterator<Row> iterator = source.executeAndCollect();
        List<Row> results = new ArrayList<>();
        while (iterator.hasNext()) {
            results.add(iterator.next());
        }
        iterator.close();
        return results;
    }
}
