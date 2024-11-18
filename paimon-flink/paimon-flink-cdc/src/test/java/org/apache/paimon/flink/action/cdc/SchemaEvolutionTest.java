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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.sink.cdc.UpdatedDataFieldsProcessFunction;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.*;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Before;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class SchemaEvolutionTest {

    private static List<List<DataField>> prepareData() {
        List<DataField> upField1 =
                Arrays.asList(
                        new DataField(0, "col_0", new VarCharType(), "test description."),
                        new DataField(1, "col_1", new VarCharType(), "test description."),
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
                        new DataField(15, "col_15_1", new VarCharType(), "Someone's desc."),
                        new DataField(16, "col_16", new VarCharType(), "Someone's desc."),
                        new DataField(17, "col_17", new VarCharType(), "Someone's desc."),
                        new DataField(18, "col_18", new VarCharType(), "Someone's desc."),
                        new DataField(19, "col_19", new VarCharType(), "Someone's desc."),
                        new DataField(20, "col_20", new VarCharType(), "Someone's desc."));
        List<DataField> upField3 =
                Arrays.asList(
                        new DataField(0, "col_0", new VarCharType(), "test description."),
                        new DataField(1, "col_1", new VarCharType(), "test description."),
                        new DataField(2, "col_2", new IntType(), "test description."),
                        new DataField(3, "col_3", new VarCharType(), "Someone's desc."),
                        new DataField(4, "col_4", new VarCharType(), "Someone's desc."),
                        new DataField(5, "col_5", new VarCharType(), "Someone's desc."),
                        new DataField(6, "col_6", new DecimalType(), "Someone's desc."),
                        new DataField(7, "col_7", new VarCharType(), "Someone's desc."),
                        new DataField(8, "col_8", new VarCharType(), "Someone's desc."),
                        new DataField(9, "col_9", new VarCharType(), "Someone's desc."),
                        new DataField(10, "col_10", new VarCharType(), "Someone's desc."),
                        new DataField(11, "col_11", new IntType(), "Someone's desc."),
                        new DataField(12, "col_12_2", new DoubleType(), "Someone's desc."),
                        new DataField(13, "col_13", new VarCharType(), "Someone's desc."),
                        new DataField(14, "col_14", new VarCharType(), "Someone's desc."),
                        new DataField(15, "col_15", new VarCharType(), "Someone's desc."),
                        new DataField(16, "col_16", new VarCharType(), "Someone's desc."),
                        new DataField(17, "col_17", new VarCharType(), "Someone's desc."),
                        new DataField(18, "col_18", new VarCharType(), "Someone's desc."),
                        new DataField(19, "col_19_1", new VarCharType(), "Someone's desc."),
                        new DataField(20, "col_20", new VarCharType(), "Someone's desc."));
        List<DataField> upField4 =
                Arrays.asList(
                        new DataField(0, "col_0", new VarCharType(), "test description."),
                        new DataField(1, "col_1", new VarCharType(), "test description."),
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
                        new DataField(12, "col_12", new DoubleType(), "up desc."),
                        new DataField(13, "col_13", new VarCharType(), "Someone's desc."),
                        new DataField(14, "col_14", new VarCharType(), "Someone's desc."),
                        new DataField(15, "col_15", new VarCharType(), "Someone's desc."),
                        new DataField(16, "col_16_1", new VarCharType(), "up desc."),
                        new DataField(17, "col_17", new VarCharType(), "Someone's desc."),
                        new DataField(18, "col_18", new VarCharType(), "Someone's desc."),
                        new DataField(19, "col_19", new VarCharType(), "Someone's desc."),
                        new DataField(20, "col_20", new VarCharType(), "Someone's desc."));
        List<DataField> upField5 =
                Arrays.asList(
                        new DataField(0, "col_0", new VarCharType(), "test description."),
                        new DataField(1, "col_1", new VarCharType(), "test description."),
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
        return Arrays.asList(upField1, upField3, upField3, upField4, upField5);
    }

    private final String warehouse = "/tmp/paimon/";
    private final String database = "test_db";
    private final String tableName = "test_tb";

    @Before
    public void createTable() throws Exception {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.primaryKey("f0", "f1");
        schemaBuilder.partitionKeys("f1");
        schemaBuilder.column("f0", DataTypes.STRING());
        schemaBuilder.column("f1", DataTypes.INT());
        Schema schema = schemaBuilder.build();
        Identifier identifier = Identifier.create(database, tableName);
        try {
            CatalogContext context = CatalogContext.create(new Path(warehouse));
            Catalog catalog = CatalogFactory.createCatalog(context);
            catalog.createDatabase(database, true);
            catalog.createTable(identifier, schema, true);
        } catch (Catalog.TableAlreadyExistException e) {
            // do something
        } catch (Catalog.DatabaseNotExistException e) {
            // do something
        }
    }

    @Test
    public void testSchemaEvolution() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<List<DataField>> upDataFieldStream = env.fromCollection(prepareData());
        Options options = new Options();
        options.set("warehouse", warehouse);
        final Catalog.Loader catalogLoader = () -> FlinkCatalogFactory.createPaimonCatalog(options);
        Identifier identifier = Identifier.create(database, tableName);
        Table table = catalogLoader.load().getTable(identifier);
        FileStoreTable dataTable = (FileStoreTable) table;
        upDataFieldStream
                .process(
                        new UpdatedDataFieldsProcessFunction(
                                new SchemaManager(dataTable.fileIO(), dataTable.location()),
                                identifier,
                                catalogLoader))
                .name("Schema Evolution");
        env.execute();
    }
}
