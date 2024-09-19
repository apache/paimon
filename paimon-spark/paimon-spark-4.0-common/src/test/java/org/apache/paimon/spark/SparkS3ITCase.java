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

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOTest;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.s3.MinioTestContainer;
import org.apache.paimon.testutils.junit.parameterized.ParameterizedTestExtension;
import org.apache.paimon.testutils.junit.parameterized.Parameters;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for using S3 in Spark. */
@ExtendWith(ParameterizedTestExtension.class)
public class SparkS3ITCase {

    @RegisterExtension
    public static final MinioTestContainer MINIO_CONTAINER = new MinioTestContainer();

    private static Path warehousePath;

    private static SparkSession spark = null;

    @BeforeAll
    public static void startMetastoreAndSpark() {
        String path = MINIO_CONTAINER.getS3UriForDefaultBucket() + "/" + UUID.randomUUID();
        warehousePath = new Path(path);
        spark = SparkSession.builder().master("local[2]").getOrCreate();
        spark.conf().set("spark.sql.catalog.paimon", SparkCatalog.class.getName());
        spark.conf().set("spark.sql.catalog.paimon.warehouse", warehousePath.toString());
        MINIO_CONTAINER
                .getS3ConfigOptions()
                .forEach((k, v) -> spark.conf().set("spark.sql.catalog.paimon." + k, v));
        spark.sql("CREATE DATABASE paimon.db");
        spark.sql("USE paimon.db");
    }

    @AfterAll
    public static void stopMetastoreAndSpark() {
        if (spark != null) {
            spark.stop();
            spark = null;
        }
    }

    @Parameters(name = "{0}")
    public static Collection<String> parameters() {
        return Arrays.asList("avro", "orc", "parquet");
    }

    private final String format;

    public SparkS3ITCase(String format) {
        this.format = format;
    }

    @AfterEach
    public void afterEach() {
        spark.sql("DROP TABLE IF EXISTS T");
    }

    @TestTemplate
    public void testWriteRead() {
        spark.sql(
                String.format(
                        "CREATE TABLE T (a INT, b INT, c STRING) TBLPROPERTIES"
                                + " ('primary-key'='a', 'bucket'='4', 'file.format'='%s')",
                        format));
        spark.sql("INSERT INTO T VALUES (1, 2, '3')").collectAsList();
        List<Row> rows = spark.sql("SELECT * FROM T").collectAsList();
        assertThat(rows.toString()).isEqualTo("[[1,2,3]]");
    }

    @TestTemplate
    public void testS3AtomicWriteMultipleThreads() throws InterruptedException, IOException {
        Path file = new Path(warehousePath, UUID.randomUUID().toString());
        Options options = new Options();
        MINIO_CONTAINER.getS3ConfigOptions().forEach(options::setString);
        FileIO fileIO = FileIO.get(file, CatalogContext.create(options));
        FileIOTest.testOverwriteFileUtf8(file, fileIO);
    }
}
