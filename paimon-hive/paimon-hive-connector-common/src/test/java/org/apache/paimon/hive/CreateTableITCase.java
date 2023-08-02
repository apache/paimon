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

import org.apache.paimon.catalog.AbstractCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;
import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

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
                                + ". Please create table by Paimon catalog first.");
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
                                new DataField(1, "col2", DataTypes.STRING(), "second comment"),
                                new DataField(2, "col3", DataTypes.DECIMAL(5, 3), "last comment")),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Maps.newHashMap(),
                        "");
        Identifier identifier = Identifier.create(DATABASE_TEST, tableName);
        Path tablePath = AbstractCatalog.dataTableLocation(path, identifier);
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
    }

    @Test
    public void testCreateTableUsePartitionedBy() {
        // Use `partitioned by` to create hive partition table
        String tableName = "not_support_partitioned_by_table";
        String hiveSql =
                String.join(
                        "\n",
                        Arrays.asList(
                                "CREATE TABLE " + tableName + " (",
                                "col1 "
                                        + TypeInfoFactory.intTypeInfo.getTypeName()
                                        + " COMMENT 'The col1 field'",
                                ")",
                                "PARTITIONED BY (dt "
                                        + TypeInfoFactory.stringTypeInfo.getTypeName()
                                        + ")",
                                "STORED BY '" + PaimonStorageHandler.class.getName() + "'"));
        assertThatThrownBy(() -> hiveShell.execute(hiveSql))
                .hasRootCauseInstanceOf(MetaException.class)
                .hasRootCauseMessage(
                        "Paimon currently does not support creating partitioned table "
                                + "with PARTITIONED BY clause. If you want to create a partitioned table, "
                                + "please set partition fields in properties.");
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
        Path tablePath = AbstractCatalog.dataTableLocation(path, identifier);
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
        Path tablePath = AbstractCatalog.dataTableLocation(path, identifier);
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
        Path tablePath = AbstractCatalog.dataTableLocation(path, identifier);
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
}
