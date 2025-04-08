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

package org.apache.paimon.spark.benchmark

import org.apache.paimon.spark.SparkCatalog
import org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark
import org.apache.spark.sql.paimon.Utils

import java.io.File

trait PaimonSqlBasedBenchmark extends SqlBasedBenchmark {

  protected lazy val tempDBDir: File = Utils.createTempDir

  protected lazy val sql: String => DataFrame = spark.sql

  override def getSparkSession: SparkSession = {
    SparkSession
      .builder()
      .master("local[1]")
      .appName(this.getClass.getCanonicalName)
      .config("spark.ui.enabled", value = false)
      .config("spark.sql.warehouse.dir", tempDBDir.getCanonicalPath)
      .config("spark.sql.catalog.paimon", classOf[SparkCatalog].getName)
      .config("spark.sql.catalog.paimon.warehouse", tempDBDir.getCanonicalPath)
      .config("spark.sql.defaultCatalog", "paimon")
      .config("spark.sql.extensions", classOf[PaimonSparkSessionExtensions].getName)
      .getOrCreate()
  }

  def withTempTable(tableNames: String*)(f: => Unit): Unit = {
    try f
    finally {
      tableNames.foreach(spark.catalog.dropTempView)
    }
  }

  def withTable(tableNames: String*)(f: => Unit): Unit = {
    try f
    finally {
      tableNames.foreach(name => sql(s"DROP TABLE IF EXISTS $name"))
    }
  }
}
