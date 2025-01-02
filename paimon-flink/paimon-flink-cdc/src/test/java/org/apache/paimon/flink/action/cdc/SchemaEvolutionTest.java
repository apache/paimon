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

package org.apache.paimon.flink.action.cdc;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.sink.cdc.UpdatedDataFieldsProcessFunction;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.SchemaUtils;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.VarCharType;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

/** Used to test schema evolution related logic. */
public class SchemaEvolutionTest extends TableTestBase {

    private static List<List<DataField>> prepareData() {
        List<DataField> upField1 =
                Arrays.asList(
                        new DataField(0, "col_0", new VarCharType(), "test description."),
                        new DataField(1, "col_1", new IntType(), "test description."),
                        new DataField(2, "col_2", new IntType(), "test description."),
                        new DataField(3, "col_3", new VarCharType(), "Someone's desc."),
                        new DataField(4, "col_4", new VarCharType(), "Someone's desc."),
                        new DataField(5, "col_5", new VarCharType(), "Someone's desc."),
                        new DataField(6, "col_6", new DecimalType(), "Someone's desc."),
                        new DataField(7, "col_7", new VarCharType(), "Someone's desc."),
                        new DataField(8, "col_8", new VarCharType(), "Someone's desc."),
                        new DataField(9, "col_9", new VarCharType(), "Someone's desc."),
                        new DataField(10, "col_10", new VarCharType(), "Someone's desc."),
                        new DataField(11, "col_11", new VarCharType(), "Someone's desc."),
                        new DataField(12, "col_12", new DoubleType(), "Someone's desc."),
                        new DataField(13, "col_13", new VarCharType(), "Someone's desc."),
                        new DataField(14, "col_14", new VarCharType(), "Someone's desc."),
                        new DataField(15, "col_15", new VarCharType(), "Someone's desc."),
                        new DataField(16, "col_16", new VarCharType(), "Someone's desc."),
                        new DataField(17, "col_17", new VarCharType(), "Someone's desc."),
                        new DataField(18, "col_18", new VarCharType(), "Someone's desc."),
                        new DataField(19, "col_19", new VarCharType(), "Someone's desc."),
                        new DataField(20, "col_20", new VarCharType(), "Someone's desc."));
        List<DataField> upField2 =
                Arrays.asList(
                        new DataField(0, "col_0", new VarCharType(), "test description."),
                        new DataField(1, "col_1", new BigIntType(), "test description."),
                        new DataField(2, "col_2", new IntType(), "test description."),
                        new DataField(3, "col_3", new VarCharType(), "Someone's desc."),
                        new DataField(4, "col_4", new VarCharType(), "Someone's desc."),
                        new DataField(5, "col_5", new VarCharType(), "Someone's desc."),
                        new DataField(6, "col_6", new DecimalType(), "Someone's desc."),
                        new DataField(7, "col_7", new VarCharType(), "Someone's desc."),
                        new DataField(8, "col_8", new VarCharType(), "Someone's desc."),
                        new DataField(9, "col_9", new VarCharType(), "Someone's desc."),
                        new DataField(10, "col_10", new VarCharType(), "Someone's desc."),
                        new DataField(11, "col_11", new VarCharType(), "Someone's desc."),
                        new DataField(12, "col_12", new DoubleType(), "Someone's desc."),
                        new DataField(13, "col_13", new VarCharType(), "Someone's desc."),
                        new DataField(14, "col_14", new VarCharType(), "Someone's desc."),
                        new DataField(15, "col_15", new VarCharType(), "Someone's desc."),
                        new DataField(16, "col_16", new VarCharType(), "Someone's desc."),
                        new DataField(17, "col_17", new VarCharType(), "Someone's desc."),
                        new DataField(18, "col_18", new VarCharType(), "Someone's desc."),
                        new DataField(19, "col_19", new VarCharType(), "Someone's desc."),
                        new DataField(20, "col_20", new VarCharType(), "Someone's desc."));
        List<DataField> upField3 =
                Arrays.asList(
                        new DataField(0, "col_0", new VarCharType(), "test description."),
                        new DataField(1, "col_1", new BigIntType(), "test description."),
                        new DataField(2, "col_2", new IntType(), "test description 2."),
                        new DataField(3, "col_3", new VarCharType(), "Someone's desc."),
                        new DataField(4, "col_4", new VarCharType(), "Someone's desc."),
                        new DataField(5, "col_5", new VarCharType(), "Someone's desc."),
                        new DataField(6, "col_6", new DecimalType(), "Someone's desc."),
                        new DataField(7, "col_7", new VarCharType(), "Someone's desc."),
                        new DataField(8, "col_8", new VarCharType(), "Someone's desc."),
                        new DataField(9, "col_9", new VarCharType(), "Someone's desc."),
                        new DataField(10, "col_10", new VarCharType(), "Someone's desc."),
                        new DataField(11, "col_11", new VarCharType(), "Someone's desc."),
                        new DataField(12, "col_12", new DoubleType(), "Someone's desc."),
                        new DataField(13, "col_13", new VarCharType(), "Someone's desc."),
                        new DataField(14, "col_14", new VarCharType(), "Someone's desc."),
                        new DataField(15, "col_15", new VarCharType(), "Someone's desc."),
                        new DataField(16, "col_16", new VarCharType(), "Someone's desc."),
                        new DataField(17, "col_17", new VarCharType(), "Someone's desc."),
                        new DataField(18, "col_18", new VarCharType(), "Someone's desc."),
                        new DataField(19, "col_19", new VarCharType(), "Someone's desc."),
                        new DataField(20, "col_20", new VarCharType(), "Someone's desc."));
        List<DataField> upField4 =
                Arrays.asList(
                        new DataField(0, "col_0", new VarCharType(), "test description."),
                        new DataField(1, "col_1", new BigIntType(), "test description."),
                        new DataField(2, "col_2", new IntType(), "test description."),
                        new DataField(3, "col_3_1", new VarCharType(), "Someone's desc."),
                        new DataField(4, "col_4", new VarCharType(), "Someone's desc."),
                        new DataField(5, "col_5", new VarCharType(), "Someone's desc."),
                        new DataField(6, "col_6", new DecimalType(), "Someone's desc."),
                        new DataField(7, "col_7", new VarCharType(), "Someone's desc."),
                        new DataField(8, "col_8", new VarCharType(), "Someone's desc."),
                        new DataField(9, "col_9", new VarCharType(), "Someone's desc."),
                        new DataField(10, "col_10", new VarCharType(), "Someone's desc."),
                        new DataField(11, "col_11", new VarCharType(), "Someone's desc."),
                        new DataField(12, "col_12", new DoubleType(), "Someone's desc."),
                        new DataField(13, "col_13", new VarCharType(), "Someone's desc."),
                        new DataField(14, "col_14", new VarCharType(), "Someone's desc."),
                        new DataField(15, "col_15", new VarCharType(), "Someone's desc."),
                        new DataField(16, "col_16", new VarCharType(), "Someone's desc."),
                        new DataField(17, "col_17", new VarCharType(), "Someone's desc."),
                        new DataField(18, "col_18", new VarCharType(), "Someone's desc."),
                        new DataField(19, "col_19", new VarCharType(), "Someone's desc."),
                        new DataField(20, "col_20", new VarCharType(), "Someone's desc."));
        List<DataField> upField5 =
                Arrays.asList(
                        new DataField(0, "col_0", new VarCharType(), "test description."),
                        new DataField(1, "col_1", new BigIntType(), "test description."),
                        new DataField(2, "col_2_1", new BigIntType(), "test description 2."),
                        new DataField(3, "col_3", new VarCharType(), "Someone's desc."),
                        new DataField(4, "col_4", new VarCharType(), "Someone's desc."),
                        new DataField(5, "col_5", new VarCharType(), "Someone's desc."),
                        new DataField(6, "col_6", new DecimalType(), "Someone's desc."),
                        new DataField(7, "col_7", new VarCharType(), "Someone's desc."),
                        new DataField(8, "col_8", new VarCharType(), "Someone's desc."),
                        new DataField(9, "col_9", new VarCharType(), "Someone's desc."),
                        new DataField(10, "col_10", new VarCharType(), "Someone's desc."),
                        new DataField(11, "col_11", new VarCharType(), "Someone's desc."),
                        new DataField(12, "col_12", new DoubleType(), "Someone's desc."),
                        new DataField(13, "col_13", new VarCharType(), "Someone's desc."),
                        new DataField(14, "col_14", new VarCharType(), "Someone's desc."),
                        new DataField(15, "col_15", new VarCharType(), "Someone's desc."),
                        new DataField(16, "col_16", new VarCharType(), "Someone's desc."),
                        new DataField(17, "col_17", new VarCharType(), "Someone's desc."),
                        new DataField(18, "col_18", new VarCharType(), "Someone's desc."),
                        new DataField(19, "col_19", new VarCharType(), "Someone's desc."),
                        new DataField(20, "col_20", new VarCharType(), "Someone's desc."));
        return Arrays.asList(upField1, upField2, upField3, upField4, upField5);
    }

    private FileStoreTable table;
    private String tableName = "MyTable";

    @BeforeEach
    public void before() throws Exception {
        FileIO fileIO = LocalFileIO.create();
        Path tablePath = new Path(String.format("%s/%s.db/%s", warehouse, database, tableName));
        Schema schema =
                Schema.newBuilder()
                        .column("pk", DataTypes.INT())
                        .column("pt1", DataTypes.INT())
                        .column("pt2", DataTypes.INT())
                        .column("col1", DataTypes.INT())
                        .partitionKeys("pt1", "pt2")
                        .primaryKey("pk", "pt1", "pt2")
                        .option(CoreOptions.CHANGELOG_PRODUCER.key(), "input")
                        .option(CoreOptions.BUCKET.key(), "2")
                        .option(CoreOptions.SEQUENCE_FIELD.key(), "col1")
                        .build();
        TableSchema tableSchema =
                SchemaUtils.forceCommit(new SchemaManager(fileIO, tablePath), schema);
        table = FileStoreTableFactory.create(LocalFileIO.create(), tablePath, tableSchema);
    }

    @Test
    public void testSchemaEvolution() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<List<DataField>> upDataFieldStream = env.fromCollection(prepareData());
        Options options = new Options();
        options.set("warehouse", tempPath.toString());
        final CatalogLoader catalogLoader = () -> FlinkCatalogFactory.createPaimonCatalog(options);
        Identifier identifier = Identifier.create(database, tableName);
        DataStream<Void> schemaChangeProcessFunction =
                upDataFieldStream
                        .process(
                                new UpdatedDataFieldsProcessFunction(
                                        new SchemaManager(table.fileIO(), table.location()),
                                        identifier,
                                        catalogLoader))
                        .name("Schema Evolution");
        schemaChangeProcessFunction.getTransformation().setParallelism(1);
        schemaChangeProcessFunction.getTransformation().setMaxParallelism(1);
        env.execute();
    }
}
