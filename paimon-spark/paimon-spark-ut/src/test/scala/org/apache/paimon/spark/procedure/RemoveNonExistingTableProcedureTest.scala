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

package org.apache.paimon.spark.procedure

import org.apache.paimon.spark.{PaimonHiveTestBase, SparkCatalog}
import org.apache.paimon.spark.PaimonHiveTestBase.hiveUri

import org.apache.spark.SparkConf

class RemoveNonExistingTableProcedureTest extends PaimonHiveTestBase {

  protected val paimonHiveNoCacheCatalogName: String = "paimon_hive_no_cache"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(s"spark.sql.catalog.$paimonHiveNoCacheCatalogName.cache-enabled", "false")
      .set(s"spark.sql.catalog.$paimonHiveNoCacheCatalogName", classOf[SparkCatalog].getName)
      .set(s"spark.sql.catalog.$paimonHiveNoCacheCatalogName.metastore", "hive")
      .set(
        s"spark.sql.catalog.$paimonHiveNoCacheCatalogName.warehouse",
        tempHiveDBDir.getCanonicalPath)
      .set(s"spark.sql.catalog.$paimonHiveNoCacheCatalogName.uri", hiveUri)
  }

  test("Paimon DDL with hive catalog: drop table which location has been deleted") {
    Seq(sparkCatalogName, paimonHiveCatalogName, paimonHiveNoCacheCatalogName).foreach {
      catalogName =>
        spark.sql(s"USE $catalogName")
        withDatabase("paimon_db") {
          spark.sql(s"CREATE DATABASE paimon_db")
          spark.sql(s"USE paimon_db")
          spark.sql("CREATE TABLE t USING paimon")
          val table = loadTable("paimon_db", "t")

          // the procedure will not drop table in metastore which exists in file system
          spark
            .sql(s"CALL sys.remove_non_existing_table(table => 'paimon_db.t')")
          assert(spark.sql("SHOW TABLES").count() == 1)

          // test drop table which location has been deleted in spark sql
          table.fileIO().delete(table.location(), true)
          spark.sql("DROP TABLE IF EXISTS t")
          if (catalogName.equals(sparkCatalogName)) {
            // tableExists in spark catalog will return true with catalog cache, then spark will drop it when drop table
            // even the table does not exist in file system, spark session catalog spark_catalog will still drop it.
            // but other catalog will not drop table, because Paimon HiveCatalog will getTable before dropTable
            assert(spark.sql("SHOW TABLES").count() == 0)
          } else {
            // tableExists in spark catalog will return false without catalog cache, then spark will not drop it when drop table
            assert(spark.sql("SHOW TABLES").count() == 1)
          }

          // test drop table in metastore but not in file system
          if (!catalogName.equals(sparkCatalogName)) {
            spark
              .sql(s"CALL sys.remove_non_existing_table(table => 'paimon_db.t')")
          }
          assert(spark.sql("SHOW TABLES").count() == 0)

          // test drop table not exists in metastore
          assertThrows[RuntimeException] {
            spark.sql(s"CALL sys.remove_non_existing_table(table => 'paimon_db.t')")
          }
        }
    }
  }
}
