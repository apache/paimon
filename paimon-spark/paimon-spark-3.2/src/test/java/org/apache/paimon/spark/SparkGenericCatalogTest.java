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

package org.apache.paimon.spark;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Base tests for spark read. */
public class SparkGenericCatalogTest {

    protected static SparkSession spark = null;

    protected static Path warehousePath = null;

    @BeforeAll
    public static void startMetastoreAndSpark(@TempDir java.nio.file.Path tempDir) {
        warehousePath = new Path("file:" + tempDir.toString());
        spark =
                SparkSession.builder()
                        .config("spark.sql.warehouse.dir", warehousePath.toString())
                        .config(
                                "spark.sql.extensions",
                                PaimonSparkSessionExtensions.class.getName())
                        .master("local[2]")
                        .getOrCreate();
        spark.conf().set("spark.sql.catalog.spark_catalog", SparkGenericCatalog.class.getName());
    }

    @AfterAll
    public static void stopMetastoreAndSpark() {
        if (spark != null) {
            spark.stop();
            spark = null;
        }
    }

    @Test
    public void testPaimonTable() throws Exception {
        spark.sql(
                "CREATE TABLE PT (a INT, b INT, c STRING) USING paimon TBLPROPERTIES"
                        + " ('file.format'='avro')");
        writeTable(
                "PT",
                GenericRow.of(1, 2, BinaryString.fromString("3")),
                GenericRow.of(4, 5, BinaryString.fromString("6")));
        assertThat(spark.sql("SELECT * FROM PT").collectAsList().stream().map(Object::toString))
                .containsExactlyInAnyOrder("[1,2,3]", "[4,5,6]");

        spark.sql("CREATE DATABASE my_db");
        spark.sql(
                "CREATE TABLE DB_PT (a INT, b INT, c STRING) USING paimon TBLPROPERTIES"
                        + " ('file.format'='avro')");
        writeTable(
                "DB_PT",
                GenericRow.of(1, 2, BinaryString.fromString("3")),
                GenericRow.of(4, 5, BinaryString.fromString("6")));
        assertThat(spark.sql("SELECT * FROM DB_PT").collectAsList().stream().map(Object::toString))
                .containsExactlyInAnyOrder("[1,2,3]", "[4,5,6]");

        assertThat(spark.sql("SHOW NAMESPACES").collectAsList().stream().map(Object::toString))
                .containsExactlyInAnyOrder("[default]", "[my_db]");
    }

    @Test
    public void testCsvTable() {
        spark.sql("CREATE TABLE CT (a INT, b INT, c STRING) USING csv");
        spark.sql("INSERT INTO CT VALUES (1, 2, '3'), (4, 5, '6')").collectAsList();
        List<Row> rows = spark.sql("SELECT * FROM CT").collectAsList();
        assertThat(rows.stream().map(Object::toString))
                .containsExactlyInAnyOrder("[1,2,3]", "[4,5,6]");
    }

    private static void writeTable(String tableName, GenericRow... rows) throws Exception {
        FileStoreTable fileStoreTable =
                FileStoreTableFactory.create(
                        LocalFileIO.create(),
                        new Path(warehousePath, String.format("default.db/%s", tableName)));
        BatchWriteBuilder writeBuilder = fileStoreTable.newBatchWriteBuilder();
        BatchTableWrite writer = writeBuilder.newWrite();
        BatchTableCommit commit = writeBuilder.newCommit();
        for (GenericRow row : rows) {
            writer.write(row);
        }
        commit.commit(writer.prepareCommit());
        writer.close();
        commit.close();
    }
}
