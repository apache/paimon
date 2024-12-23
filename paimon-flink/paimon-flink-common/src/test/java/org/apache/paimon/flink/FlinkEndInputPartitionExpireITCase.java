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
import org.apache.paimon.Snapshot;
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

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.BUCKET;
import static org.apache.paimon.CoreOptions.BUCKET_KEY;
import static org.apache.paimon.CoreOptions.END_INPUT_CHECK_PARTITION_EXPIRE;
import static org.apache.paimon.CoreOptions.FILE_FORMAT;
import static org.apache.paimon.CoreOptions.PARTITION_EXPIRATION_CHECK_INTERVAL;
import static org.apache.paimon.CoreOptions.PARTITION_EXPIRATION_TIME;
import static org.apache.paimon.CoreOptions.PARTITION_TIMESTAMP_FORMATTER;
import static org.apache.paimon.CoreOptions.TABLE_SCHEMA_PATH;
import static org.apache.paimon.flink.LogicalTypeConversion.toDataType;
import static org.apache.paimon.utils.FailingFileIO.retryArtificialException;

/** ITCase for partition expire when end input. */
@ExtendWith(ParameterizedTestExtension.class)
public class FlinkEndInputPartitionExpireITCase extends CatalogITCaseBase {

    private static final RowType TABLE_TYPE =
            new RowType(
                    Arrays.asList(
                            new RowType.RowField("v", new IntType()),
                            new RowType.RowField("p", new VarCharType(10)),
                            // rename key
                            new RowType.RowField("_k", new IntType())));

    private static final List<RowData> SOURCE_DATA =
            Arrays.asList(
                    wrap(GenericRowData.of(0, StringData.fromString("20240101"), 1)),
                    wrap(GenericRowData.of(0, StringData.fromString("20240101"), 2)),
                    wrap(GenericRowData.of(0, StringData.fromString("20240103"), 1)),
                    wrap(GenericRowData.of(5, StringData.fromString("20240103"), 1)),
                    wrap(GenericRowData.of(6, StringData.fromString("20240105"), 1)));

    private static SerializableRowData wrap(RowData row) {
        return new SerializableRowData(row, InternalSerializers.create(TABLE_TYPE));
    }

    private final StreamExecutionEnvironment env;

    public FlinkEndInputPartitionExpireITCase() {
        this.env = streamExecutionEnvironmentBuilder().batchMode().parallelism(2).build();
    }

    @Parameters(name = "isBatch-{0}")
    public static List<Boolean> getVarSeg() {
        return Arrays.asList(true, false);
    }

    @TestTemplate
    public void testEndInputPartitionExpire() throws Exception {
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

        Assertions.assertEquals(2, table.snapshotManager().snapshotCount());
        Assertions.assertEquals(
                Snapshot.CommitKind.OVERWRITE, table.snapshotManager().snapshot(2).commitKind());
    }

    private FileStoreTable buildFileStoreTable(int[] partitions, int[] primaryKey)
            throws Exception {
        Options options = new Options();
        options.set(BUCKET, 3);
        options.set(TABLE_SCHEMA_PATH, getTempDirPath());
        options.set(FILE_FORMAT, CoreOptions.FILE_FORMAT_AVRO);
        options.set(PARTITION_EXPIRATION_TIME, Duration.ofDays(2));
        options.set(PARTITION_EXPIRATION_CHECK_INTERVAL, Duration.ofHours(1));
        options.set(PARTITION_TIMESTAMP_FORMATTER, "yyyyMMdd");
        options.set(END_INPUT_CHECK_PARTITION_EXPIRE, true);

        Path tablePath = new CoreOptions(options.toMap()).schemaPath();
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
