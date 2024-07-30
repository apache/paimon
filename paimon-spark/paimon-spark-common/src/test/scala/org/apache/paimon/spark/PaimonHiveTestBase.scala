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

package org.apache.paimon.spark

import org.apache.paimon.Snapshot
import org.apache.paimon.hive.TestHiveMetastore

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.paimon.Utils

import java.io.File

class PaimonHiveTestBase extends PaimonSparkTestBase {

  import PaimonHiveTestBase._

  protected lazy val tempHiveDBDir: File = Utils.createTempDir

  protected lazy val testHiveMetastore: TestHiveMetastore = new TestHiveMetastore

  protected val sparkCatalogName: String = "spark_catalog"

  protected val paimonHiveCatalogName: String = "paimon_hive"

  protected val hiveDbName: String = "test_hive"

  /**
   * Add spark_catalog ([[SparkGenericCatalog]] in hive) and paimon_hive ([[SparkCatalog]] in hive)
   * catalog
   */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.warehouse.dir", tempHiveDBDir.getCanonicalPath)
      .set("spark.sql.catalogImplementation", "hive")
      .set(s"spark.sql.catalog.$sparkCatalogName", "org.apache.paimon.spark.SparkGenericCatalog")
      .set(s"spark.sql.catalog.$paimonHiveCatalogName", classOf[SparkCatalog].getName)
      .set(s"spark.sql.catalog.$paimonHiveCatalogName.metastore", "hive")
      .set(s"spark.sql.catalog.$paimonHiveCatalogName.warehouse", tempHiveDBDir.getCanonicalPath)
      .set(s"spark.sql.catalog.$paimonHiveCatalogName.uri", hiveUri)
  }

  override protected def beforeAll(): Unit = {
    testHiveMetastore.start(hivePort)
    super.beforeAll()
    spark.sql(s"USE $sparkCatalogName")
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $hiveDbName")
  }

  override protected def afterAll(): Unit = {
    try {
      spark.sql(s"USE $sparkCatalogName")
      spark.sql("USE default")
      spark.sql(s"DROP DATABASE $hiveDbName CASCADE")
    } finally {
      super.afterAll()
      testHiveMetastore.stop()
    }
  }

  /** Default is spark_catalog */
  override protected def beforeEach(): Unit = {
    spark.sql(s"USE $sparkCatalogName")
    spark.sql(s"USE $hiveDbName")
  }
}

object PaimonHiveTestBase {

  val hiveUri: String = {
    val hadoopConf = new Configuration()
    hadoopConf.addResource(getClass.getClassLoader.getResourceAsStream("hive-site.xml"))
    hadoopConf.get("hive.metastore.uris")
  }

  val hivePort: Int = hiveUri.split(":")(2).toInt
}
