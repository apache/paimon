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

import org.apache.paimon.catalog.{Catalog, Identifier}
import org.apache.paimon.fs.FileIO
import org.apache.paimon.fs.local.LocalFileIO
import org.apache.paimon.spark.catalog.WithPaimonCatalog
import org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions
import org.apache.paimon.spark.sql.{SparkVersionSupport, WithTableOptions}
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.{Identifier => SparkIdentifier}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.paimon.Utils
import org.apache.spark.sql.test.SharedSparkSession
import org.scalactic.source.Position
import org.scalatest.Tag

import java.io.File
import java.util.TimeZone

import scala.util.Random

class PaimonSparkTestBase
  extends QueryTest
  with SharedSparkSession
  with WithTableOptions
  with SparkVersionSupport {

  protected lazy val fileIO: FileIO = LocalFileIO.create

  protected lazy val tempDBDir: File = Utils.createTempDir

  protected def paimonCatalog: Catalog = {
    spark.sessionState.catalogManager.currentCatalog.asInstanceOf[WithPaimonCatalog].paimonCatalog()
  }

  protected val dbName0: String = "test"

  protected val tableName0: String = "T"

  /** Add paimon ([[SparkCatalog]] in fileSystem) catalog */
  override protected def sparkConf: SparkConf = {
    val serializer = if (Random.nextBoolean()) {
      "org.apache.spark.serializer.KryoSerializer"
    } else {
      "org.apache.spark.serializer.JavaSerializer"
    }
    super.sparkConf
      .set("spark.sql.warehouse.dir", tempDBDir.getCanonicalPath)
      .set("spark.sql.catalog.paimon", classOf[SparkCatalog].getName)
      .set("spark.sql.catalog.paimon.warehouse", tempDBDir.getCanonicalPath)
      .set("spark.sql.extensions", classOf[PaimonSparkSessionExtensions].getName)
      .set("spark.serializer", serializer)
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql(s"USE paimon")
    spark.sql(s"CREATE DATABASE IF NOT EXISTS paimon.$dbName0")
  }

  override protected def afterAll(): Unit = {
    try {
      spark.sql(s"USE paimon")
      spark.sql(s"DROP TABLE IF EXISTS $dbName0.$tableName0")
      spark.sql("USE default")
      spark.sql(s"DROP DATABASE paimon.$dbName0 CASCADE")
    } finally {
      super.afterAll()
    }
  }

  protected def reset(): Unit = {
    afterAll()
    beforeAll()
  }

  /** Default is paimon catalog */
  override protected def beforeEach(): Unit = {
    super.beforeAll()
    spark.sql(s"USE paimon")
    spark.sql(s"USE paimon.$dbName0")
    spark.sql(s"DROP TABLE IF EXISTS $tableName0")
  }

  protected def withTempDirs(f: (File, File) => Unit): Unit = {
    withTempDir(file1 => withTempDir(file2 => f(file1, file2)))
  }

  protected def withTimeZone(timeZone: String)(f: => Unit): Unit = {
    withSparkSQLConf("spark.sql.session.timeZone" -> timeZone) {
      val originTimeZone = TimeZone.getDefault
      try {
        TimeZone.setDefault(TimeZone.getTimeZone(timeZone))
        f
      } finally {
        TimeZone.setDefault(originTimeZone)
      }
    }
  }

  // Since SPARK-46227 has changed the definition of withSQLConf that resulted in
  // incompatibility between the Spark3.x and Spark4.x, So Paimon declare a separate method
  // to provide the same function.
  protected def withSparkSQLConf(pairs: (String, String)*)(f: => Unit): Unit = {
    withSparkSQLConf0(pairs: _*)(f)
  }

  private def withSparkSQLConf0(pairs: (String, String)*)(f: => Unit): Unit = {
    val conf = SQLConf.get
    val (keys, values) = pairs.unzip
    val currentValues = keys.map {
      key =>
        if (conf.contains(key)) {
          Some(conf.getConfString(key))
        } else {
          None
        }
    }
    (keys, values).zipped.foreach {
      (k, v) =>
        if (SQLConf.isStaticConfigKey(k)) {
          throw new RuntimeException(s"Cannot modify the value of a static config: $k")
        }
        conf.setConfString(k, v)
    }
    try f
    finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => conf.setConfString(key, value)
        case (key, None) => conf.unsetConf(key)
      }
    }
  }

  override def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      println(testName)
      testFun
    }(pos)
  }

  def loadTable(tableName: String): FileStoreTable = {
    loadTable(dbName0, tableName)
  }

  def loadTable(dbName: String, tableName: String): FileStoreTable = {
    paimonCatalog.getTable(Identifier.create(dbName, tableName)).asInstanceOf[FileStoreTable]
  }

  protected def createRelationV2(tableName: String): LogicalPlan = {
    val sparkTable = SparkTable(loadTable(tableName))
    DataSourceV2Relation.create(
      sparkTable,
      Some(spark.sessionState.catalogManager.currentCatalog),
      Some(SparkIdentifier.of(Array(this.dbName0), tableName))
    )
  }

  protected def getPaimonScan(sqlText: String): PaimonScan = {
    sql(sqlText).queryExecution.optimizedPlan
      .collectFirst { case relation: DataSourceV2ScanRelation => relation }
      .get
      .scan
      .asInstanceOf[PaimonScan]
  }
}
