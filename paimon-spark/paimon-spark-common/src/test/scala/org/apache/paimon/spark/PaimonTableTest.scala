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

import org.apache.spark.sql.test.SharedSparkSession

trait PaimonTableTest extends SharedSparkSession {

  val bucket: Int

  def initProps(primaryOrBucketKeys: Seq[String], partitionKeys: Seq[String]): Map[String, String]

  /**
   * Create a table configured by the given parameters.
   *
   * @param tableName
   *   table name
   * @param columns
   *   columns string, e.g. "a INT, b INT, c STRING"
   * @param primaryOrBucketKeys
   *   for [[PaimonPrimaryKeyTable]] they are `primary-key`, if you want to specify additional
   *   `bucket-key`, you can specify that in extraProps. for [[PaimonAppendTable]] they are
   *   `bucket-key`
   * @param partitionKeys
   *   partition keys seq
   * @param extraProps
   *   extra properties map
   */
  def createTable(
      tableName: String,
      columns: String,
      primaryOrBucketKeys: Seq[String],
      partitionKeys: Seq[String] = Seq.empty,
      extraProps: Map[String, String] = Map.empty): Unit = {
    createTable0(
      tableName,
      columns,
      partitionKeys,
      initProps(primaryOrBucketKeys, partitionKeys) ++ extraProps)
  }

  private def createTable0(
      tableName: String,
      columns: String,
      partitionKeys: Seq[String],
      props: Map[String, String]): Unit = {
    val partitioned = if (partitionKeys.isEmpty) {
      ""
    } else {
      s"PARTITIONED BY (${partitionKeys.mkString(", ")})"
    }
    val tblproperties = if (props.isEmpty) {
      ""
    } else {
      val kvs = props.map(kv => s"'${kv._1}' = '${kv._2}'").mkString(", ")
      s"TBLPROPERTIES ($kvs)"
    }
    sql(s"""
           | CREATE TABLE $tableName ($columns) USING PAIMON
           | $partitioned
           | $tblproperties
           |""".stripMargin)
  }
}

trait PaimonBucketedTable {
  val bucket: Int = 3
}

trait PaimonNonBucketedTable {
  val bucket: Int = -1
}

trait PaimonPrimaryKeyTable extends PaimonTableTest {
  def initProps(
      primaryOrBucketKeys: Seq[String],
      partitionKeys: Seq[String]): Map[String, String] = {
    assert(primaryOrBucketKeys.nonEmpty)
    Map("primary-key" -> primaryOrBucketKeys.mkString(","), "bucket" -> bucket.toString)
  }
}

trait PaimonAppendTable extends PaimonTableTest {
  def initProps(
      primaryOrBucketKeys: Seq[String],
      partitionKeys: Seq[String]): Map[String, String] = {
    if (bucket == -1) {
      // Ignore bucket keys for unaware bucket table
      Map("bucket" -> bucket.toString)
    } else {
      // Filter partition keys in bucket keys for fixed bucket table
      val bucketKeys = primaryOrBucketKeys.filterNot(partitionKeys.contains(_))
      assert(bucketKeys.nonEmpty)
      Map("bucket-key" -> bucketKeys.mkString(","), "bucket" -> bucket.toString)
    }
  }
}

trait PaimonPrimaryKeyBucketedTableTest extends PaimonPrimaryKeyTable with PaimonBucketedTable

trait PaimonPrimaryKeyNonBucketTableTest extends PaimonPrimaryKeyTable with PaimonNonBucketedTable

trait PaimonAppendBucketedTableTest extends PaimonAppendTable with PaimonBucketedTable

trait PaimonAppendNonBucketTableTest extends PaimonAppendTable with PaimonNonBucketedTable
