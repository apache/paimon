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

package org.apache.paimon.hive;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.CatalogUtils;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;
import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.paimon.CoreOptions.METASTORE_PARTITIONED_TABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for testing create managed table ddl. */
public class CreateTableITCase extends HiveTestBase {

    @Test
    public void testCreateExternalTableWithoutPaimonTable() {
        // Create hive external table without paimon table
        String tableName = "without_paimon_table";
        String hiveSql =
                String.join(
                        "\n",
                        Arrays.asList(
                                "CREATE EXTERNAL TABLE " + tableName + " ",
                                "STORED BY '" + PaimonStorageHandler.class.getName() + "'",
                                "LOCATION '" + path + "'"));
        assertThatThrownBy(() -> hiveShell.execute(hiveSql))
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage(
                        "Schema file not found in location file:"
                                + path
                                + ". Please create table first.");
    }

    @Test
    public void testCreateExternalTableWithPaimonTable() throws Exception {
        // Create hive external table with paimon table
        String tableName = "with_paimon_table";

        // Create a paimon table
        Schema schema =
                new Schema(
                        Lists.newArrayList(
                                new DataField(0, "col1", DataTypes.INT(), "first comment"),
                                new DataField(1, "Col2", DataTypes.STRING(), "second comment"),
                                new DataField(2, "COL3", DataTypes.DECIMAL(5, 3), "last comment")),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Maps.newHashMap(),
                        "");
        Identifier identifier = Identifier.create(DATABASE_TEST, tableName);
        Path tablePath = CatalogUtils.newTableLocation(path, identifier);
        new SchemaManager(LocalFileIO.create(), tablePath).createTable(schema);

        // Create hive external table
        String hiveSql =
                String.join(
                        "\n",
                        Arrays.asList(
                                "CREATE EXTERNAL TABLE " + tableName + " ",
                                "STORED BY '" + PaimonStorageHandler.class.getName() + "'",
                                "LOCATION '" + tablePath.toUri().toString() + "'"));
        assertThatCode(() -> hiveShell.execute(hiveSql)).doesNotThrowAnyException();

        List<String> result = hiveShell.executeQuery("SHOW CREATE TABLE " + tableName);
        assertThat(result)
                .containsAnyOf(
                        "CREATE EXTERNAL TABLE `with_paimon_table`(",
                        "  `col1` int COMMENT 'first comment', ",
                        "  `col2` string COMMENT 'second comment', ",
                        "  `col3` decimal(5,3) COMMENT 'last comment')",
                        "ROW FORMAT SERDE ",
                        "  'org.apache.paimon.hive.PaimonSerDe' ",
                        "STORED BY ",
                        "  'org.apache.paimon.hive.PaimonStorageHandler' ");
    }

    @Test
    public void testCallCreateTableToCreatHiveExternalTable() throws Exception {
        // Create hive external table with paimon table
        String tableName = "with_paimon_table";
        String hadoopConfDir = "";

        // Create a paimon table
        Schema schema =
                new Schema(
                        Lists.newArrayList(
                                new DataField(0, "col1", DataTypes.INT(), "first comment"),
                                new DataField(1, "col2", DataTypes.STRING(), "second comment"),
                                new DataField(2, "col3", DataTypes.DECIMAL(5, 3), "last comment")),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Maps.newHashMap(),
                        "");
        Identifier identifier = Identifier.create(DATABASE_TEST, tableName);
        Options options = new Options();
        options.set("warehouse", path);
        options.set("metastore", "hive");
        options.set("table.type", "external");
        options.set("hadoop-conf-dir", hadoopConfDir);
        CatalogContext context = CatalogContext.create(options);
        Catalog hiveCatalog = CatalogFactory.createCatalog(context);
        hiveCatalog.createTable(identifier, schema, false);

        // Drop hive external table
        hiveShell.execute("DROP TABLE " + tableName);

        hiveCatalog.createTable(identifier, schema, false);
    }

    @Test
    public void testCreateTableUsePartitionedBy() {
        // Use `partitioned by` to create hive partition table
        String tableName = "support_partitioned_by_table";
        hiveShell.execute("SET hive.metastore.warehouse.dir=" + path);
        String hiveSql =
                String.join(
                        "\n",
                        Arrays.asList(
                                "CREATE TABLE " + tableName + " (",
                                "user_id "
                                        + TypeInfoFactory.longTypeInfo.getTypeName()
                                        + " COMMENT 'The user_id field',",
                                "item_id "
                                        + TypeInfoFactory.longTypeInfo.getTypeName()
                                        + " COMMENT 'The item_id field',",
                                "behavior "
                                        + TypeInfoFactory.stringTypeInfo.getTypeName()
                                        + " COMMENT 'The behavior field'",
                                ")",
                                "PARTITIONED BY ( ",
                                "dt "
                                        + TypeInfoFactory.stringTypeInfo.getTypeName()
                                        + " COMMENT 'The dt field',",
                                "hh "
                                        + TypeInfoFactory.stringTypeInfo.getTypeName()
                                        + " COMMENT 'The hh field'",
                                ")",
                                "STORED BY '" + PaimonStorageHandler.class.getName() + "'",
                                "TBLPROPERTIES (",
                                "  'primary-key'='dt,hh,user_id'",
                                ")"));
        assertThatCode(() -> hiveShell.execute(hiveSql)).doesNotThrowAnyException();

        // check the paimon table schema
        Identifier identifier = Identifier.create(DATABASE_TEST, tableName);
        Path tablePath = CatalogUtils.newTableLocation(path, identifier);
        Optional<TableSchema> tableSchema =
                new SchemaManager(LocalFileIO.create(), tablePath).latest();
        assertThat(tableSchema).isPresent();
        assertThat(tableSchema.get().primaryKeys()).contains("dt", "hh", "user_id");
        assertThat(tableSchema.get().partitionKeys()).contains("dt", "hh");
        assertThat(tableSchema.get().options())
                .containsEntry(METASTORE_PARTITIONED_TABLE.key(), "true");
    }

    @Test
    public void testLowerTableName() throws Catalog.TableNotExistException {
        // Use `partitioned by` to create hive partition table
        String tableName = "UPPER_NAME";
        hiveShell.execute("SET hive.metastore.warehouse.dir=" + path);
        String hiveSql =
                String.join(
                        "\n",
                        Arrays.asList(
                                "CREATE TABLE " + tableName + " (",
                                "user_id "
                                        + TypeInfoFactory.longTypeInfo.getTypeName()
                                        + " COMMENT 'The user_id field',",
                                "item_id "
                                        + TypeInfoFactory.longTypeInfo.getTypeName()
                                        + " COMMENT 'The item_id field',",
                                "behavior "
                                        + TypeInfoFactory.stringTypeInfo.getTypeName()
                                        + " COMMENT 'The behavior field'",
                                ")",
                                "PARTITIONED BY ( ",
                                "dt "
                                        + TypeInfoFactory.stringTypeInfo.getTypeName()
                                        + " COMMENT 'The dt field',",
                                "hh "
                                        + TypeInfoFactory.stringTypeInfo.getTypeName()
                                        + " COMMENT 'The hh field'",
                                ")",
                                "STORED BY '" + PaimonStorageHandler.class.getName() + "'",
                                "TBLPROPERTIES (",
                                "  'primary-key'='dt,hh,user_id'",
                                ")"));
        assertThatCode(() -> hiveShell.execute(hiveSql)).doesNotThrowAnyException();

        // check table path from hive metastore
        List<String> queryResult =
                hiveShell.executeQuery(String.format("describe formatted %s", tableName));
        for (String result : queryResult) {
            if (result.contains("Location")) {
                String location = result.split("\t")[1];
                String tableNameFromPath = location.substring(location.lastIndexOf("/") + 1);
                assertThat(tableNameFromPath).isEqualTo(tableName.toLowerCase());
            }
        }
        // check the paimon table name and schema
        Identifier identifier = Identifier.create(DATABASE_TEST, tableName.toLowerCase());
        Path tablePath = CatalogUtils.newTableLocation(path, identifier);
        Options conf = new Options();
        conf.set(CatalogOptions.WAREHOUSE, path);
        CatalogContext catalogContext = CatalogContext.create(conf);
        Catalog catalog = CatalogFactory.createCatalog(catalogContext);
        Table table = catalog.getTable(identifier);
        assertThat(table.name()).isEqualTo(tableName.toLowerCase());
        Optional<TableSchema> tableSchema =
                new SchemaManager(LocalFileIO.create(), tablePath).latest();
        assertThat(tableSchema).isPresent();
    }

    @Test
    public void testLowerDBName() throws Catalog.TableNotExistException {
        String upperDB = "UPPER_DB";

        hiveShell.execute(String.format("create database %s", upperDB));

        String tableName = "UPPER_NAME";
        hiveShell.execute("SET hive.metastore.warehouse.dir=" + path);
        String hiveSql =
                String.join(
                        "\n",
                        Arrays.asList(
                                "CREATE TABLE " + upperDB + "." + tableName + " (",
                                "user_id "
                                        + TypeInfoFactory.longTypeInfo.getTypeName()
                                        + " COMMENT 'The user_id field',",
                                "item_id "
                                        + TypeInfoFactory.longTypeInfo.getTypeName()
                                        + " COMMENT 'The item_id field',",
                                "behavior "
                                        + TypeInfoFactory.stringTypeInfo.getTypeName()
                                        + " COMMENT 'The behavior field'",
                                ")",
                                "PARTITIONED BY ( ",
                                "dt "
                                        + TypeInfoFactory.stringTypeInfo.getTypeName()
                                        + " COMMENT 'The dt field',",
                                "hh "
                                        + TypeInfoFactory.stringTypeInfo.getTypeName()
                                        + " COMMENT 'The hh field'",
                                ")",
                                "STORED BY '" + PaimonStorageHandler.class.getName() + "'",
                                "TBLPROPERTIES (",
                                "  'primary-key'='dt,hh,user_id'",
                                ")"));
        assertThatCode(() -> hiveShell.execute(hiveSql)).doesNotThrowAnyException();

        // check table path from hive metastore
        hiveShell.execute("USE " + upperDB);
        List<String> queryResult =
                hiveShell.executeQuery(String.format("describe formatted %s", tableName));
        for (String result : queryResult) {
            if (result.contains("Location")) {
                String location = result.split("\t")[1];
                int index = location.lastIndexOf(upperDB.toLowerCase() + ".db");
                assertThat(index).isGreaterThan(0);
                String dbNameFromPath = location.substring(index, location.lastIndexOf("/"));
                assertThat(dbNameFromPath).isEqualTo(upperDB.toLowerCase() + ".db");
            }
        }

        // check the paimon db name、table name and schema
        Identifier identifier = Identifier.create(upperDB.toLowerCase(), tableName.toLowerCase());
        Path tablePath = CatalogUtils.newTableLocation(path, identifier);
        Options conf = new Options();
        conf.set(CatalogOptions.WAREHOUSE, path);
        CatalogContext catalogContext = CatalogContext.create(conf);
        Catalog catalog = CatalogFactory.createCatalog(catalogContext);
        Table table = catalog.getTable(identifier);
        assertThat(table.name()).isEqualTo(tableName.toLowerCase());
        Optional<TableSchema> tableSchema =
                new SchemaManager(LocalFileIO.create(), tablePath).latest();
        assertThat(tableSchema).isPresent();
    }

    @Test
    public void testCreateTableWithPrimaryKey() {
        String tableName = "primary_key_table";
        hiveShell.execute("SET hive.metastore.warehouse.dir=" + path);
        String hiveSql =
                String.join(
                        "\n",
                        Arrays.asList(
                                "CREATE TABLE " + tableName + " (",
                                "user_id "
                                        + TypeInfoFactory.longTypeInfo.getTypeName()
                                        + " COMMENT 'The user_id field',",
                                "item_id "
                                        + TypeInfoFactory.longTypeInfo.getTypeName()
                                        + " COMMENT 'The item_id field',",
                                "behavior "
                                        + TypeInfoFactory.stringTypeInfo.getTypeName()
                                        + " COMMENT 'The behavior field',",
                                "dt "
                                        + TypeInfoFactory.stringTypeInfo.getTypeName()
                                        + " COMMENT 'The dt field',",
                                "hh "
                                        + TypeInfoFactory.stringTypeInfo.getTypeName()
                                        + " COMMENT 'The hh field'",
                                ")",
                                "STORED BY '" + PaimonStorageHandler.class.getName() + "'",
                                "TBLPROPERTIES (",
                                "  'primary-key'='dt,hh,user_id'",
                                ")"));
        assertThatCode(() -> hiveShell.execute(hiveSql)).doesNotThrowAnyException();

        // check the paimon table schema
        Identifier identifier = Identifier.create(DATABASE_TEST, tableName);
        Path tablePath = CatalogUtils.newTableLocation(path, identifier);
        Optional<TableSchema> tableSchema =
                new SchemaManager(LocalFileIO.create(), tablePath).latest();
        assertThat(tableSchema).isPresent();
        assertThat(tableSchema.get().primaryKeys()).contains("dt", "hh", "user_id");
        assertThat(tableSchema.get().partitionKeys()).isEmpty();
    }

    @Test
    public void testCreateTableWithPartition() {
        String tableName = "partition_table";
        hiveShell.execute("SET hive.metastore.warehouse.dir=" + path);
        String hiveSql =
                String.join(
                        "\n",
                        Arrays.asList(
                                "CREATE TABLE " + tableName + " (",
                                "user_id "
                                        + TypeInfoFactory.longTypeInfo.getTypeName()
                                        + " COMMENT 'The user_id field',",
                                "item_id "
                                        + TypeInfoFactory.longTypeInfo.getTypeName()
                                        + " COMMENT 'The item_id field',",
                                "behavior "
                                        + TypeInfoFactory.stringTypeInfo.getTypeName()
                                        + " COMMENT 'The behavior field',",
                                "dt "
                                        + TypeInfoFactory.stringTypeInfo.getTypeName()
                                        + " COMMENT 'The dt field',",
                                "hh "
                                        + TypeInfoFactory.stringTypeInfo.getTypeName()
                                        + " COMMENT 'The hh field'",
                                ")",
                                "STORED BY '" + PaimonStorageHandler.class.getName() + "'",
                                "TBLPROPERTIES (",
                                "  'primary-key'='dt,hh,user_id',",
                                "  'partition'='dt,hh'",
                                ")"));
        assertThatCode(() -> hiveShell.execute(hiveSql)).doesNotThrowAnyException();

        // check the paimon table schema
        Identifier identifier = Identifier.create(DATABASE_TEST, tableName);
        Path tablePath = CatalogUtils.newTableLocation(path, identifier);
        Optional<TableSchema> tableSchema =
                new SchemaManager(LocalFileIO.create(), tablePath).latest();
        assertThat(tableSchema).isPresent();
        assertThat(tableSchema.get().primaryKeys()).contains("dt", "hh", "user_id");
        assertThat(tableSchema.get().partitionKeys()).contains("dt", "hh");
    }

    @Test
    public void testCreateTableSpecifyProperties() {
        String tableName = "specify_properties_table";
        hiveShell.execute("SET hive.metastore.warehouse.dir=" + path);
        String hiveSql =
                String.join(
                        "\n",
                        Arrays.asList(
                                "CREATE TABLE " + tableName + " (",
                                "user_id "
                                        + TypeInfoFactory.longTypeInfo.getTypeName()
                                        + " COMMENT 'The user_id field',",
                                "item_id "
                                        + TypeInfoFactory.longTypeInfo.getTypeName()
                                        + " COMMENT 'The item_id field',",
                                "behavior "
                                        + TypeInfoFactory.stringTypeInfo.getTypeName()
                                        + " COMMENT 'The behavior field',",
                                "dt "
                                        + TypeInfoFactory.stringTypeInfo.getTypeName()
                                        + " COMMENT 'The dt field',",
                                "hh "
                                        + TypeInfoFactory.stringTypeInfo.getTypeName()
                                        + " COMMENT 'The hh field'",
                                ")",
                                "STORED BY '" + PaimonStorageHandler.class.getName() + "'",
                                "TBLPROPERTIES (",
                                "  'primary-key'='dt,hh,user_id',",
                                "  'partition'='dt,hh',",
                                "  'bucket' = '2',",
                                "  'bucket-key' = 'user_id'",
                                ")"));
        assertThatCode(() -> hiveShell.execute(hiveSql)).doesNotThrowAnyException();

        // check the paimon table schema
        Identifier identifier = Identifier.create(DATABASE_TEST, tableName);
        Path tablePath = CatalogUtils.newTableLocation(path, identifier);
        Optional<TableSchema> tableSchema =
                new SchemaManager(LocalFileIO.create(), tablePath).latest();
        assertThat(tableSchema).isPresent();
        assertThat(tableSchema.get().options()).containsEntry("bucket", "2");
        assertThat(tableSchema.get().options()).containsEntry("bucket-key", "user_id");
    }

    @Test
    public void testCreateTableIsPaimonSystemTable() {
        String tableName = "test$schema";
        String hiveSql =
                String.join(
                        "\n",
                        Arrays.asList(
                                "CREATE TABLE " + tableName + " (",
                                "col1 "
                                        + TypeInfoFactory.intTypeInfo.getTypeName()
                                        + " COMMENT 'The col1 field'",
                                ")",
                                "STORED BY '" + PaimonStorageHandler.class.getName() + "'"));
        assertThatThrownBy(() -> hiveShell.execute(hiveSql))
                .hasRootCauseInstanceOf(ParseException.class)
                .hasMessageContaining(
                        "cannot recognize input near 'test' '$' 'schema' in table name");
    }

    @Test
    public void testCreateTableFailing() throws Exception {
        // Create a extern table

        {
            String tableName = "tes1";

            Schema schema =
                    new Schema(
                            Lists.newArrayList(
                                    new DataField(0, "col1", DataTypes.INT(), "first comment"),
                                    new DataField(1, "col2", DataTypes.STRING(), "second comment"),
                                    new DataField(
                                            2, "col3", DataTypes.DECIMAL(5, 3), "last comment"),
                                    new DataField(
                                            3, "col4", DataTypes.DECIMAL(5, 3), "last comment")),
                            Collections.emptyList(),
                            Collections.emptyList(),
                            Maps.newHashMap(),
                            "");
            Identifier identifier = Identifier.create(DATABASE_TEST, tableName);
            Path tablePath = CatalogUtils.newTableLocation(path, identifier);
            new SchemaManager(LocalFileIO.create(), tablePath).createTable(schema);

            String hiveSql =
                    String.join(
                            "\n",
                            Arrays.asList(
                                    "CREATE EXTERNAL TABLE " + tableName + " ",
                                    "STORED BY '" + MockPaimonStorageHandler.class.getName() + "'",
                                    "LOCATION '" + tablePath.toUri().toString() + "'"));
            try {
                hiveShell.execute(hiveSql);
            } catch (Throwable ignore) {
            } finally {
                boolean isPresent =
                        new SchemaManager(LocalFileIO.create(), tablePath).latest().isPresent();
                Assertions.assertThat(isPresent).isTrue();
            }
        }

        {
            String tableName = "tes2";

            hiveShell.execute("SET hive.metastore.warehouse.dir=" + path);
            String hiveSql =
                    String.join(
                            "\n",
                            Arrays.asList(
                                    "CREATE TABLE " + tableName + " (",
                                    "user_id "
                                            + TypeInfoFactory.longTypeInfo.getTypeName()
                                            + " COMMENT 'The user_id field',",
                                    "hh "
                                            + TypeInfoFactory.stringTypeInfo.getTypeName()
                                            + " COMMENT 'The hh field'",
                                    ")",
                                    "STORED BY '"
                                            + MockPaimonStorageHandler.class.getName()
                                            + "'"));
            try {
                hiveShell.execute(hiveSql);
            } catch (Exception ignore) {
            } finally {
                Identifier identifier = Identifier.create(DATABASE_TEST, tableName);
                Path tablePath = CatalogUtils.newTableLocation(path, identifier);
                boolean isPresent =
                        new SchemaManager(LocalFileIO.create(), tablePath).latest().isPresent();
                Assertions.assertThat(isPresent).isFalse();
            }
        }
    }

    /** Mock create table failed. */
    public static class MockPaimonMetaHook extends PaimonMetaHook {

        public MockPaimonMetaHook(Configuration conf) {
            super(conf);
        }

        @Override
        public void commitCreateTable(org.apache.hadoop.hive.metastore.api.Table table)
                throws MetaException {
            throw new RuntimeException("mock create table failed");
        }
    }

    /** StorageHanlder with MockPaimonMetaHook. */
    public static class MockPaimonStorageHandler extends PaimonStorageHandler {
        @Override
        public HiveMetaHook getMetaHook() {
            return new MockPaimonMetaHook(getConf());
        }
    }
}
