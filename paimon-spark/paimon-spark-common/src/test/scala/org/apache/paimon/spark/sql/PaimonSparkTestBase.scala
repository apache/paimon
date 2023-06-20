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

import org.apache.paimon.spark.SparkCatalog

import org.apache.spark.paimon.Utils
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.scalactic.source.Position
import org.scalatest.Tag

import java.io.File

class PaimonSparkTestBase extends QueryTest with SharedSparkSession {

  protected var tempDBDir: File = _

  protected val tableName0: String = "T"

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    tempDBDir = Utils.createTempDir
    spark.conf.set("spark.sql.catalog.paimon", classOf[SparkCatalog].getName)
    spark.conf.set("spark.sql.catalog.paimon.warehouse", tempDBDir.getCanonicalPath)
    spark.sql("CREATE DATABASE paimon.db")
    spark.sql("USE paimon.db")
    println(s"${tempDBDir.getCanonicalPath}")
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
