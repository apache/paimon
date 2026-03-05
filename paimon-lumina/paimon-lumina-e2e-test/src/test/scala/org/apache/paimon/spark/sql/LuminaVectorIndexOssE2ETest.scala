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

package org.apache.paimon.spark.sql

import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.SparkConf

import scala.collection.JavaConverters._

/**
 * End-to-end tests for Lumina vector index with table stored on Alibaba Cloud OSS.
 *
 * Data is intentionally retained on OSS after test completion for inspection. Tables and database
 * are NOT dropped during cleanup.
 *
 * This test is disabled by default and will NOT run as part of the normal test suite. To run it,
 * specify this suite explicitly and provide the required OSS configuration via environment
 * variables:
 *
 * {{{
 * export PAIMON_OSS_WAREHOUSE=oss://your-bucket/path
 * export PAIMON_OSS_ENDPOINT=oss-cn-hangzhou.aliyuncs.com
 * export PAIMON_OSS_ACCESS_KEY_ID=your-access-key-id
 * export PAIMON_OSS_ACCESS_KEY_SECRET=your-access-key-secret
 *
 * mvn -pl paimon-lumina/paimon-lumina-e2e-test \
 *   -Dtest.suites=org.apache.paimon.spark.sql.LuminaVectorIndexOssE2ETest \
 *   -Dcheckstyle.skip -Dspotless.check.skip -Denforcer.skip test
 * }}}
 */
class LuminaVectorIndexOssE2ETest extends PaimonSparkTestBase {

  private val indexType = "lumina-vector-ann"
  private val defaultOptions = "vector.dim=3,vector.index-type=DISKANN"

  private def ossWarehouse: String = sys.env.getOrElse("PAIMON_OSS_WAREHOUSE", "")
  private def ossEndpoint: String = sys.env.getOrElse("PAIMON_OSS_ENDPOINT", "")
  private def ossAccessKeyId: String = sys.env.getOrElse("PAIMON_OSS_ACCESS_KEY_ID", "")
  private def ossAccessKeySecret: String = sys.env.getOrElse("PAIMON_OSS_ACCESS_KEY_SECRET", "")

  private def ossConfigured: Boolean =
    Seq(ossWarehouse, ossEndpoint, ossAccessKeyId, ossAccessKeySecret).forall(_.nonEmpty)

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.catalog.paimon.warehouse", ossWarehouse)
      .set("spark.hadoop.fs.oss.impl", "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem")
      .set("spark.hadoop.fs.oss.endpoint", ossEndpoint)
      .set("spark.hadoop.fs.oss.accessKeyId", ossAccessKeyId)
      .set("spark.hadoop.fs.oss.accessKeySecret", ossAccessKeySecret)
  }

  override protected def beforeAll(): Unit = {
    assume(
      ossConfigured,
      "OSS E2E tests require environment variables: " +
        "PAIMON_OSS_WAREHOUSE, PAIMON_OSS_ENDPOINT, " +
        "PAIMON_OSS_ACCESS_KEY_ID, PAIMON_OSS_ACCESS_KEY_SECRET"
    )
    super.beforeAll()
  }

  // Skip table drop — retain data on OSS between tests.
  override protected def beforeEach(): Unit = {
    spark.sql(s"USE paimon")
    spark.sql(s"USE paimon.$dbName0")
  }

  // Intentionally skip super.afterAll() to avoid dropping tables and database on OSS.
  // Data is retained for inspection. The Spark session is cleaned up on JVM exit.
  override protected def afterAll(): Unit = {}

  test("OSS: end-to-end vector index write, create, and search") {
    spark.sql("""
                |CREATE TABLE IF NOT EXISTS oss_e2e (id INT, name STRING, embedding ARRAY<FLOAT>)
                |TBLPROPERTIES (
                |  'bucket' = '-1',
                |  'global-index.row-count-per-shard' = '10000',
                |  'row-tracking.enabled' = 'true',
                |  'data-evolution.enabled' = 'true')
                |""".stripMargin)

    val values = (0 until 1000)
      .map(
        i =>
          s"($i, 'item_$i', array(cast($i as float), cast(${i + 1} as float), cast(${i + 2} as float)))")
      .mkString(",")
    spark.sql(s"INSERT INTO oss_e2e VALUES $values")

    val indexResult = spark
      .sql(
        s"CALL sys.create_global_index(table => 'test.oss_e2e', index_column => 'embedding', " +
          s"index_type => '$indexType', options => '$defaultOptions')")
      .collect()
      .head
    assert(indexResult.getBoolean(0))

    val table = loadTable("oss_e2e")
    val indexEntries = table
      .store()
      .newIndexFileHandler()
      .scanEntries()
      .asScala
      .filter(_.indexFile().indexType() == indexType)
    assert(indexEntries.nonEmpty)
    assert(indexEntries.map(_.indexFile().rowCount()).sum == 1000L)

    val searchResult = spark
      .sql("""
            |SELECT id, name FROM vector_search('oss_e2e', 'embedding', array(500.0f, 501.0f, 502.0f), 10)
            |""".stripMargin)
      .collect()
    assert(searchResult.length == 10)
  }
}
