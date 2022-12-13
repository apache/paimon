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

package org.apache.flink.table.store.spark;

import org.apache.flink.core.fs.Path;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for using S3 in Spark. */
public class SparkS3ITCase {

    @ClassRule public static final MinioTestContainer MINIO_CONTAINER = new MinioTestContainer();

    private static SparkSession spark = null;

    @BeforeClass
    public static void startMetastoreAndSpark() {
        String path = MINIO_CONTAINER.getS3UriForDefaultBucket() + "/" + UUID.randomUUID();
        Path warehousePath = new Path(path);
        spark = SparkSession.builder().master("local[2]").getOrCreate();
        spark.conf().set("spark.sql.catalog.tablestore", SparkCatalog.class.getName());
        spark.conf().set("spark.sql.catalog.tablestore.warehouse", warehousePath.toString());
        MINIO_CONTAINER
                .getS3ConfigOptions()
                .toMap()
                .forEach((k, v) -> spark.conf().set("spark.sql.catalog.tablestore." + k, v));
        spark.sql("CREATE DATABASE tablestore.db");
        spark.sql("USE tablestore.db");
    }

    @AfterClass
    public static void afterEach() {
        spark.sql("DROP TABLE T");
    }

    @Test
    public void testWriteRead() {
        spark.sql(
                "CREATE TABLE T (a INT, b INT, c STRING) TBLPROPERTIES"
                        + " ('primary-key'='a', 'bucket'='4', 'file.format'='avro')");
        spark.sql("INSERT INTO T VALUES (1, 2, '3')").collectAsList();
        List<Row> rows = spark.sql("SELECT * FROM T").collectAsList();
        assertThat(rows.toString()).isEqualTo("[[1,2,3]]");
    }
}
