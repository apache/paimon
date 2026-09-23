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

package org.apache.paimon.spark.table

import org.apache.paimon.spark.PaimonSparkTestWithRestCatalogBase
import org.apache.paimon.spark.SparkCatalog

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row

class FormatTableCaseInsensitiveTest extends PaimonSparkTestWithRestCatalogBase {

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
      .set("spark.sql.catalog.paimon.case-sensitive", "false")
    Seq("metastore", "uri", "token", "warehouse", "token.provider").foreach {
      key =>
        conf.getOption(s"spark.sql.catalog.paimon.$key").foreach {
          value => conf.set(s"spark.sql.catalog.paimon_cs.$key", value)
        }
    }
    conf
      .set("spark.sql.catalog.paimon_cs", classOf[SparkCatalog].getName)
      .set("spark.sql.catalog.paimon_cs.case-sensitive", "true")
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    sql("USE paimon")
    sql("CREATE DATABASE IF NOT EXISTS test_db")
    sql("USE test_db")
  }

  test("format table: case-insensitive read all and partial columns") {
    Seq("parquet", "orc").foreach {
      format =>
        val dataPath = s"${tempDBDir.getCanonicalPath}/${format}_ci"
        withTable("writer_tbl", "reader_tbl") {
          sql(s"""CREATE TABLE writer_tbl (Event_Name STRING, Campaign_ID STRING, Amount BIGINT)
                 |USING $format LOCATION '$dataPath'
                 |TBLPROPERTIES ('format-table.implementation'='paimon')
                 |""".stripMargin)
          sql("INSERT INTO writer_tbl VALUES ('install', 'c001', 100), ('purchase', 'c002', 200)")

          sql(s"""CREATE TABLE reader_tbl (event_name STRING, campaign_id STRING, amount BIGINT)
                 |USING $format LOCATION '$dataPath'
                 |TBLPROPERTIES ('format-table.implementation'='paimon')
                 |""".stripMargin)

          // case-sensitive=false: all columns
          checkAnswer(
            sql("SELECT * FROM paimon.test_db.reader_tbl ORDER BY amount"),
            Seq(Row("install", "c001", 100L), Row("purchase", "c002", 200L))
          )
          // case-sensitive=true: all columns return nulls
          checkAnswer(
            sql("SELECT * FROM paimon_cs.test_db.reader_tbl ORDER BY amount"),
            Seq(Row(null, null, null), Row(null, null, null))
          )

          // case-sensitive=false: partial columns
          checkAnswer(
            sql("SELECT event_name, amount FROM paimon.test_db.reader_tbl ORDER BY amount"),
            Seq(Row("install", 100L), Row("purchase", 200L))
          )
          // case-sensitive=true: partial columns return nulls
          checkAnswer(
            sql("SELECT event_name, amount FROM paimon_cs.test_db.reader_tbl ORDER BY amount"),
            Seq(Row(null, null), Row(null, null))
          )
        }
    }
  }

  test("parquet format table: case-insensitive read nested struct columns") {
    val dataPath = s"${tempDBDir.getCanonicalPath}/parquet_ci_nested"
    withTable("nested_writer", "nested_reader") {
      sql(s"""CREATE TABLE nested_writer (
             |  Event_Name STRING,
             |  User_Info STRUCT<User_Name: STRING, User_Age: BIGINT>
             |) USING parquet LOCATION '$dataPath'
             |TBLPROPERTIES ('format-table.implementation'='paimon')
             |""".stripMargin)
      sql(
        "INSERT INTO nested_writer VALUES ('install', named_struct('User_Name', 'alice', 'User_Age', 20L)),"
          + " ('login', named_struct('User_Name', 'bob', 'User_Age', 30L))")

      sql(s"""CREATE TABLE nested_reader (
             |  event_name STRING,
             |  user_info STRUCT<user_name: STRING, user_age: BIGINT>
             |) USING parquet LOCATION '$dataPath'
             |TBLPROPERTIES ('format-table.implementation'='paimon')
             |""".stripMargin)

      // case-sensitive=false: nested struct fields match
      checkAnswer(
        sql(
          "SELECT event_name, user_info.user_name, user_info.user_age FROM paimon.test_db.nested_reader ORDER BY event_name"),
        Seq(Row("install", "alice", 20L), Row("login", "bob", 30L))
      )
      // case-sensitive=true: returns nulls
      checkAnswer(
        sql(
          "SELECT event_name, user_info.user_name, user_info.user_age FROM paimon_cs.test_db.nested_reader ORDER BY event_name"),
        Seq(Row(null, null, null), Row(null, null, null))
      )
    }
  }
}
