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

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** IT cases for testing create managed table ddl. */
public class CreateTableITCase extends HiveTestBase {

    @Test
    public void testCreateTableWithEmptyDDLAndNoPaimonTable() {
        // create table with empty DDL and no paimon table
        String tableName = "empty_ddl_no_paimon_table";
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
                        "Schema file not found in location "
                                + path
                                + ". Please create table first.");
    }

    @Test
    public void testCreateTableWithEmptyDDLAndExistsPaimonTable() throws Exception {
        String tableName = "empty_ddl_exists_paimon_table";
        // create table with empty DDL and exists paimon table
        createPaimonTable();
        String hiveSql =
                String.join(
                        "\n",
                        Arrays.asList(
                                "CREATE EXTERNAL TABLE " + tableName + " ",
                                "STORED BY '" + PaimonStorageHandler.class.getName() + "'",
                                "LOCATION '" + path + "'"));
        assertThatCode(() -> hiveShell.execute(hiveSql)).doesNotThrowAnyException();
    }

    @Test
    public void testCreateTableWithExistsDDLAndNoPaimonTable() {
        // create table with exists DDL and no paimon table
        String tableName = "exists_ddl_no_paimon_table";
        assertThatCode(() -> hiveShell.execute(generateDefaultHiveSql(tableName)))
                .doesNotThrowAnyException();
    }

    @Test
    public void testCreateTableWithExistsDDLAndExistsPaimonTable() throws Exception {
        String tableName = "exists_ddl_exists_paimon_table";
        createPaimonTable();
        assertThatCode(() -> hiveShell.execute(generateDefaultHiveSql(tableName)))
                .doesNotThrowAnyException();
    }

    @Test
    public void testCreateTableWithoutColumnComment() {
        String tableName = "without_column_comment_table";
        assertThatCode(() -> hiveShell.execute(generateDefaultHiveSql(tableName)))
                .doesNotThrowAnyException();
    }

    @Test
    public void testCreateTableIsInternalTable() {
        String tableName = "internal_table";
        String hiveSql =
                String.join(
                        "\n",
                        Arrays.asList(
                                "CREATE TABLE " + tableName + " (",
                                "col1 "
                                        + TypeInfoFactory.intTypeInfo.getTypeName()
                                        + " COMMENT 'The col1 field'",
                                ")",
                                "STORED BY '" + PaimonStorageHandler.class.getName() + "'",
                                "LOCATION '" + path + "'"));
        assertThatCode(() -> hiveShell.execute(hiveSql)).doesNotThrowAnyException();
    }

    @Test
    public void testCreateTableNotSupportPartitionTable() {
        String tableName = "not_support_partition_table";
        String hiveSql =
                String.join(
                        "\n",
                        Arrays.asList(
                                "CREATE EXTERNAL TABLE " + tableName + " (",
                                "col1 "
                                        + TypeInfoFactory.intTypeInfo.getTypeName()
                                        + " COMMENT 'The col1 field'",
                                ")",
                                "PARTITIONED BY (dt "
                                        + TypeInfoFactory.stringTypeInfo.getTypeName()
                                        + ")",
                                "STORED BY '" + PaimonStorageHandler.class.getName() + "'",
                                "LOCATION '" + path + "'"));
        assertThatThrownBy(() -> hiveShell.execute(hiveSql))
                .hasRootCauseInstanceOf(MetaException.class)
                .hasRootCauseMessage(
                        "Paimon currently does not support creating partitioned table "
                                + "with PARTITIONED BY clause. If you want to query from a partitioned table, "
                                + "please add partition columns into the ordinary table columns.");
    }

    @Test
    public void testCreateTableIsPaimonSystemTable() {
        String tableName = "test$schema";
        assertThatThrownBy(() -> hiveShell.execute(generateDefaultHiveSql(tableName)))
                .hasRootCauseInstanceOf(ParseException.class)
                .hasMessageContaining(
                        "cannot recognize input near 'test' '$' 'schema' in table name");
    }
}
