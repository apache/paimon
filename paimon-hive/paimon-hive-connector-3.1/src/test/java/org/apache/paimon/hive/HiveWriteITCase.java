/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.hive;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.WriteMode;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.hive.mapred.PaimonOutputFormat;
import org.apache.paimon.hive.runner.PaimonEmbeddedHiveRunner;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.StringUtils;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.paimon.hive.FileStoreTestUtils.DATABASE_NAME;
import static org.apache.paimon.hive.FileStoreTestUtils.TABLE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

/** IT cases for {@link PaimonStorageHandler} and {@link PaimonOutputFormat}. */
@RunWith(PaimonEmbeddedHiveRunner.class)
public class HiveWriteITCase {

    @ClassRule public static TemporaryFolder folder = new TemporaryFolder();

    @HiveSQL(files = {})
    private static HiveShell hiveShell;

    private static String engine;

    private String commitUser;
    private long commitIdentifier;

    @BeforeClass
    public static void beforeClass() {
        // only run with mr
        engine = "mr";
    }

    @Before
    public void before() {
        hiveShell.execute("SET hive.execution.engine=mr");

        hiveShell.execute("CREATE DATABASE IF NOT EXISTS test_db");
        hiveShell.execute("USE test_db");

        commitUser = UUID.randomUUID().toString();
        commitIdentifier = 0;
    }

    @After
    public void after() {
        hiveShell.execute("DROP DATABASE IF EXISTS test_db CASCADE");
    }

    private String createChangelogExternalTable(
            RowType rowType,
            List<String> partitionKeys,
            List<String> primaryKeys,
            List<InternalRow> data)
            throws Exception {

        return createChangelogExternalTable(rowType, partitionKeys, primaryKeys, data, "");
    }

    private String createChangelogExternalTable(
            RowType rowType,
            List<String> partitionKeys,
            List<String> primaryKeys,
            List<InternalRow> data,
            String tableName)
            throws Exception {
        String path = folder.newFolder().toURI().toString();
        String tableNameNotNull =
                StringUtils.isNullOrWhitespaceOnly(tableName) ? TABLE_NAME : tableName;
        String tablePath = String.format("%s/default.db/%s", path, tableNameNotNull);
        Options conf = new Options();
        conf.set(CatalogOptions.WAREHOUSE, path);
        conf.set(CoreOptions.BUCKET, 1);
        conf.set(CoreOptions.FILE_FORMAT, CoreOptions.FileFormatType.AVRO);
        Identifier identifier = Identifier.create(DATABASE_NAME, tableNameNotNull);
        Table table =
                FileStoreTestUtils.createFileStoreTable(
                        conf, rowType, partitionKeys, primaryKeys, identifier);

        return writeData(table, tablePath, data);
    }

    private String createAppendOnlyExternalTable(
            RowType rowType, List<String> partitionKeys, List<InternalRow> data) throws Exception {
        return createAppendOnlyExternalTable(rowType, partitionKeys, data, "");
    }

    private String createAppendOnlyExternalTable(
            RowType rowType, List<String> partitionKeys, List<InternalRow> data, String tableName)
            throws Exception {
        String path = folder.newFolder().toURI().toString();
        String tableNameNotNull =
                StringUtils.isNullOrWhitespaceOnly(tableName) ? TABLE_NAME : tableName;
        String tablePath = String.format("%s/default.db/%s", path, tableNameNotNull);
        Options conf = new Options();
        conf.set(CatalogOptions.WAREHOUSE, path);
        conf.set(CoreOptions.BUCKET, 2);
        conf.set(CoreOptions.FILE_FORMAT, CoreOptions.FileFormatType.AVRO);
        conf.set(CoreOptions.WRITE_MODE, WriteMode.APPEND_ONLY);
        Identifier identifier = Identifier.create(DATABASE_NAME, tableNameNotNull);
        Table table =
                FileStoreTestUtils.createFileStoreTable(
                        conf, rowType, partitionKeys, Collections.emptyList(), identifier);

        return writeData(table, tablePath, data);
    }

    private String writeData(Table table, String path, List<InternalRow> data) throws Exception {
        StreamWriteBuilder streamWriteBuilder = table.newStreamWriteBuilder();
        StreamTableWrite write = streamWriteBuilder.newWrite();
        StreamTableCommit commit = streamWriteBuilder.newCommit();
        for (InternalRow rowData : data) {
            write.write(rowData);
            if (ThreadLocalRandom.current().nextInt(5) == 0) {
                commit.commit(commitIdentifier, write.prepareCommit(false, commitIdentifier));
                commitIdentifier++;
            }
        }
        commit.commit(commitIdentifier, write.prepareCommit(true, commitIdentifier));
        commitIdentifier++;
        write.close();

        String tableName = "test_table_" + (UUID.randomUUID().toString().substring(0, 4));
        hiveShell.execute(
                String.join(
                        "\n",
                        Arrays.asList(
                                "CREATE EXTERNAL TABLE " + tableName + " ",
                                "STORED BY '" + PaimonStorageHandler.class.getName() + "'",
                                "LOCATION '" + path + "'")));
        return tableName;
    }

    @Test
    public void testInsert() throws Exception {
        List<InternalRow> emptyData = Collections.emptyList();

        String outputTableName =
                createAppendOnlyExternalTable(
                        RowType.of(
                                new DataType[] {
                                    DataTypes.INT(),
                                    DataTypes.INT(),
                                    DataTypes.BIGINT(),
                                    DataTypes.STRING()
                                },
                                new String[] {"pt", "a", "b", "c"}),
                        Collections.singletonList("pt"),
                        emptyData,
                        "hive_test_table_output");

        hiveShell.execute(
                "insert into " + outputTableName + " values (1,2,3,'Hello'),(4,5,6,'Fine')");
        List<String> select = hiveShell.executeQuery("select * from " + outputTableName);
        assertThat(select).isEqualTo(Arrays.asList("1\t2\t3\tHello", "4\t5\t6\tFine"));
    }

    @Test
    public void testInsertTimestampAndDate() throws Exception {
        List<InternalRow> emptyData = Collections.emptyList();

        String outputTableName =
                createAppendOnlyExternalTable(
                        RowType.of(
                                new DataType[] {
                                    DataTypes.INT(), DataTypes.TIMESTAMP(), DataTypes.DATE()
                                },
                                new String[] {"pt", "a", "b"}),
                        Collections.singletonList("pt"),
                        emptyData,
                        "hive_test_table_output");
        hiveShell.execute(
                "insert into "
                        + outputTableName
                        + " values(1,'2023-01-13 20:00:01.123','2023-12-23')");
        List<String> select = hiveShell.executeQuery("select * from " + outputTableName);
        assertThat(select).isEqualTo(Collections.singletonList("1\t2023-01-13 20:00:01.123\t2023-12-23"));
    }
}
