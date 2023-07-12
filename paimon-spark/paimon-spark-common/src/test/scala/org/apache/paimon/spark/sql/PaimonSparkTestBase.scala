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

import org.apache.paimon.spark.{PaimonSparkSessionExtension, SparkCatalog, SparkGenericCatalog}

import org.apache.spark.paimon.Utils
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.scalactic.source.Position
import org.scalatest.Tag

import java.io.File

class PaimonSparkTestBase extends QueryTest with SharedSparkSession {

  protected lazy val tempDBDir: File = Utils.createTempDir

  protected val tableName0: String = "T"

  override protected def sparkConf = {
    super.sparkConf
      .set("spark.sql.catalog.paimon", classOf[SparkCatalog].getName)
      .set("spark.sql.catalog.paimon.warehouse", tempDBDir.getCanonicalPath)
      .set("spark.sql.extensions", classOf[PaimonSparkSessionExtension].getName)
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql("CREATE DATABASE paimon.db")
    spark.sql("USE paimon.db")
  }

  override protected def afterAll(): Unit = {
    try {
      spark.sql("USE default")
      spark.sql("DROP DATABASE paimon.db CASCADE")
    } finally {
      super.afterAll()
    }
  }

  override protected def beforeEach(): Unit = {
    super.beforeAll()
    spark.sql(s"DROP TABLE IF EXISTS $tableName0")
  }

  override def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    println(testName)
    super.test(testName, testTags: _*)(testFun)(pos)
  }
}
