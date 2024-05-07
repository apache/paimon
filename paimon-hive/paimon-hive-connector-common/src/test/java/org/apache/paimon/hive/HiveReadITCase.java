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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.AbstractCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.hive.mapred.PaimonInputFormat;
import org.apache.paimon.hive.mapred.PaimonRecordReader;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;
import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/** IT cases for {@link PaimonRecordReader} and {@link PaimonInputFormat}. */
public class HiveReadITCase extends HiveTestBase {

    @Test
    public void testReadExternalTableWithEmptyDataAndIgnoreCase() throws Exception {
        // Create hive external table with paimon table
        String tableName = "with_ignore_case";

        // Create a paimon table
        Schema schema =
                new Schema(
                        Lists.newArrayList(
                                new DataField(0, "col1", DataTypes.INT(), "first comment"),
                                new DataField(1, "Col2", DataTypes.STRING(), "second comment")),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Maps.newHashMap(),
                        "");
        Identifier identifier = Identifier.create(DATABASE_TEST, tableName);
        Path tablePath = AbstractCatalog.newTableLocation(path, identifier);
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
                        "  `col2` string COMMENT 'second comment')",
                        "ROW FORMAT SERDE ",
                        "  'org.apache.paimon.hive.PaimonSerDe' ",
                        "STORED BY ",
                        "  'org.apache.paimon.hive.PaimonStorageHandler' ");

        hiveShell.execute("INSERT INTO " + tableName + " VALUES (1,'Hello'),(2,'Paimon')");
        result = hiveShell.executeQuery("SELECT col2, col1 FROM " + tableName);
        assertThat(result).containsExactly("Hello\t1", "Paimon\t2");
        result = hiveShell.executeQuery("SELECT col2 FROM " + tableName);
        assertThat(result).containsExactly("Hello", "Paimon");
        result = hiveShell.executeQuery("SELECT Col2 FROM " + tableName);
        assertThat(result).containsExactly("Hello", "Paimon");
        result = hiveShell.executeQuery("SELECT * FROM " + tableName + " WHERE col2 = 'Hello'");
        assertThat(result).containsExactly("1\tHello");
        result =
                hiveShell.executeQuery(
                        "SELECT * FROM " + tableName + " WHERE Col2 in ('Hello', 'Paimon')");
        assertThat(result).containsExactly("1\tHello", "2\tPaimon");
    }

    @Test
    public void testReadExternalTableWithDataAndIgnoreCase() throws Exception {
        // Create hive external table with paimon table
        String tableName = "with_data_and_ignore_case";

        // Create a paimon table
        Identifier identifier = Identifier.create(DATABASE_TEST, tableName);

        Options conf = new Options();
        conf.set(CatalogOptions.WAREHOUSE, path);
        conf.set(CoreOptions.BUCKET, 2);
        conf.set(CoreOptions.FILE_FORMAT, CoreOptions.FILE_FORMAT_AVRO);
        RowType.Builder rowType = RowType.builder();
        rowType.field("col1", DataTypes.INT());
        rowType.field("Col2", DataTypes.STRING());

        Table table =
                FileStoreTestUtils.createFileStoreTable(
                        conf,
                        rowType.build(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        identifier);

        // insert data into paimon table, make sure has some data file use older schema file.
        List<InternalRow> data =
                Arrays.asList(
                        GenericRow.of(1, BinaryString.fromString("Hello")),
                        GenericRow.of(2, BinaryString.fromString("Paimon")));

        StreamWriteBuilder streamWriteBuilder = table.newStreamWriteBuilder();
        StreamTableWrite write = streamWriteBuilder.newWrite();
        StreamTableCommit commit = streamWriteBuilder.newCommit();
        for (InternalRow rowData : data) {
            write.write(rowData);
        }
        commit.commit(0, write.prepareCommit(true, 0));
        write.close();
        commit.close();

        // add column, do some ddl which will generate a new version schema-n file.
        Path tablePath = AbstractCatalog.newTableLocation(path, identifier);
        SchemaManager schemaManager = new SchemaManager(LocalFileIO.create(), tablePath);
        schemaManager.commitChanges(SchemaChange.addColumn("N1", DataTypes.STRING()));

        // Create hive external table
        String hiveSql =
                String.join(
                        "\n",
                        Arrays.asList(
                                "CREATE EXTERNAL TABLE " + tableName + " ",
                                "STORED BY '" + PaimonStorageHandler.class.getName() + "'",
                                "LOCATION '" + tablePath.toUri().toString() + "'"));
        assertThatCode(() -> hiveShell.execute(hiveSql)).doesNotThrowAnyException();

        List<String> result =
                hiveShell.executeQuery("SELECT * FROM " + tableName + " WHERE col2 is not null");
        assertThat(result).containsExactly("1\tHello\tNULL", "2\tPaimon\tNULL");
    }
}
