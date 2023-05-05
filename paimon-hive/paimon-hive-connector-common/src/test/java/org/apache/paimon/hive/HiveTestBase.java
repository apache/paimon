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

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.hive.runner.PaimonEmbeddedHiveRunner;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;
import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

/** hive engine execute paimon table test base. */
@RunWith(PaimonEmbeddedHiveRunner.class)
public abstract class HiveTestBase {

    @Rule public TemporaryFolder folder = new TemporaryFolder();
    protected String path;

    @HiveSQL(files = {})
    protected static HiveShell hiveShell;

    @Before
    public void before() throws Exception {
        hiveShell.execute("CREATE DATABASE IF NOT EXISTS test_db");
        hiveShell.execute("USE test_db");
        path = folder.newFolder().toString();
    }

    @After
    public void after() {
        hiveShell.execute("DROP DATABASE IF EXISTS test_db CASCADE");
    }

    protected static final Schema DEFAULT_TABLE_SCHEMA =
            new Schema(
                    Lists.newArrayList(
                            new DataField(0, "col1", DataTypes.INT(), "first comment"),
                            new DataField(1, "col2", DataTypes.STRING(), "second comment"),
                            new DataField(2, "col3", DataTypes.DECIMAL(5, 3), "last comment")),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Maps.newHashMap(),
                    "");

    protected void createPaimonTable() throws Exception {
        new SchemaManager(LocalFileIO.create(), new Path(path)).createTable(DEFAULT_TABLE_SCHEMA);
    }

    protected Optional<TableSchema> paimonTableSchema() {
        return new SchemaManager(LocalFileIO.create(), new Path(path)).latest();
    }

    protected String generateDefaultHiveSql(String tableName) {
        return String.join(
                "\n",
                Arrays.asList(
                        "CREATE EXTERNAL TABLE " + tableName + " (",
                        "col1 "
                                + TypeInfoFactory.intTypeInfo.getTypeName()
                                + " COMMENT 'The col1 field',",
                        "col2 "
                                + TypeInfoFactory.stringTypeInfo.getTypeName()
                                + " COMMENT 'The col2 field',",
                        "col3 "
                                + TypeInfoFactory.getDecimalTypeInfo(5, 3).getTypeName()
                                + " COMMENT 'The col3 field'",
                        ")",
                        "STORED BY '" + PaimonStorageHandler.class.getName() + "'",
                        "LOCATION '" + path + "'"));
    }
}
