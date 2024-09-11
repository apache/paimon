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
import org.apache.paimon.flink.sink.FlinkSinkBuilder;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.CoreOptions.BUCKET_KEY;
import static org.apache.paimon.CoreOptions.FILE_FORMAT;
import static org.apache.paimon.CoreOptions.PATH;
import static org.apache.paimon.flink.LogicalTypeConversion.toDataType;
import static org.apache.paimon.utils.FailingFileIO.retryArtificialException;

/** ITCase for user define watermark when end input. */
@ExtendWith(ParameterizedTestExtension.class)
public class FlinkEndInputWatermarkITCase extends CatalogITCaseBase {

    private static final RowType TABLE_TYPE =
            new RowType(
                    Arrays.asList(
                            new RowType.RowField("v", new IntType()),
                            new RowType.RowField("p", new VarCharType(10)),
                            // rename key
                            new RowType.RowField("_k", new IntType())));

    private static final List<RowData> SOURCE_DATA =
            Arrays.asList(
                    wrap(GenericRowData.of(0, StringData.fromString("p1"), 1)),
                    wrap(GenericRowData.of(0, StringData.fromString("p1"), 2)),
                    wrap(GenericRowData.of(0, StringData.fromString("p1"), 1)),
                    wrap(GenericRowData.of(5, StringData.fromString("p1"), 1)),
                    wrap(GenericRowData.of(6, StringData.fromString("p2"), 1)),
                    wrap(GenericRowData.of(3, StringData.fromString("p2"), 5)),
                    wrap(GenericRowData.of(5, StringData.fromString("p2"), 1)));

    private static SerializableRowData wrap(RowData row) {
        return new SerializableRowData(row, InternalSerializers.create(TABLE_TYPE));
    }

    private static final long END_INPUT_WATERMARK = 11111;

    private final StreamExecutionEnvironment env;

    public FlinkEndInputWatermarkITCase() {
        this.env = streamExecutionEnvironmentBuilder().batchMode().parallelism(2).build();
    }

    @Parameters(name = "isBatch-{0}")
    public static List<Boolean> getVarSeg() {
        return Arrays.asList(true, false);
    }

    @Override
    protected List<String> ddl() {
        return Collections.singletonList("CREATE TABLE IF NOT EXISTS T (a INT, b INT, c INT)");
    }

    @TestTemplate
    public void testEndInputWatermarkBySQL() throws Exception {
        batchSql(
                "INSERT INTO T /*+ OPTIONS('end-input.watermark'= '%s') */ VALUES (1, 11, 111), (2, 22, 222)",
                String.valueOf(END_INPUT_WATERMARK));

        FileStoreTable table = paimonTable("T");
        Assertions.assertEquals(1, table.snapshotManager().snapshotCount());
        Long waterMark = table.snapshotManager().latestSnapshot().watermark();
        Assertions.assertNotNull(waterMark);
        Assertions.assertEquals(END_INPUT_WATERMARK, waterMark);
    }

    @TestTemplate
    public void testEndInputWatermark() throws Exception {
        FileStoreTable table = buildFileStoreTable(new int[] {1}, new int[] {1, 2});

        // write
        DataStreamSource<RowData> source =
                env.fromCollection(SOURCE_DATA, InternalTypeInfo.of(TABLE_TYPE));
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

        Assertions.assertEquals(1, table.snapshotManager().snapshotCount());
        Long waterMark = table.snapshotManager().latestSnapshot().watermark();
        Assertions.assertNotNull(waterMark);
        Assertions.assertEquals(END_INPUT_WATERMARK, waterMark);
    }

    private FileStoreTable buildFileStoreTable(int[] partitions, int[] primaryKey)
            throws Exception {
        Options options = new Options();
        options.set(BUCKET, 3);
        options.set(PATH, getTempDirPath());
        options.set(FILE_FORMAT, CoreOptions.FILE_FORMAT_AVRO);
        options.set(
                FlinkConnectorOptions.END_INPUT_WATERMARK.key(),
                String.valueOf(END_INPUT_WATERMARK));

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
}
