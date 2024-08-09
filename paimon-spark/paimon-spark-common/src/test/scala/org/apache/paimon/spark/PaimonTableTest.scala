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

import scala.collection.mutable

trait PaimonTableTest extends SharedSparkSession {

  val bucket: Int

  def appendPrimaryKey(primaryKeys: Seq[String], props: mutable.Map[String, String]): Unit

  def createTable(
      tableName: String,
      columns: String,
      primaryKeys: Seq[String],
      partitionKeys: Seq[String] = Seq.empty,
      props: Map[String, String] = Map.empty): Unit = {
    val newProps: mutable.Map[String, String] =
      mutable.Map.empty[String, String] ++ Map("bucket" -> bucket.toString) ++ props
    appendPrimaryKey(primaryKeys, newProps)
    createTable0(tableName, columns, partitionKeys, newProps.toMap)
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

trait PaimonPrimaryKeyTable {
  def appendPrimaryKey(primaryKeys: Seq[String], props: mutable.Map[String, String]): Unit = {
    assert(primaryKeys.nonEmpty)
    props += ("primary-key" -> primaryKeys.mkString(","))
  }
}

trait PaimonAppendTable {
  def appendPrimaryKey(primaryKeys: Seq[String], props: mutable.Map[String, String]): Unit = {
    // nothing to do
  }
}

trait PaimonPrimaryKeyBucketedTableTest
  extends PaimonTableTest
  with PaimonPrimaryKeyTable
  with PaimonBucketedTable

trait PaimonPrimaryKeyNonBucketTableTest
  extends PaimonTableTest
  with PaimonPrimaryKeyTable
  with PaimonNonBucketedTable

trait PaimonAppendBucketedTableTest
  extends PaimonTableTest
  with PaimonAppendTable
  with PaimonBucketedTable

trait PaimonAppendNonBucketTableTest
  extends PaimonTableTest
  with PaimonAppendTable
  with PaimonNonBucketedTable
