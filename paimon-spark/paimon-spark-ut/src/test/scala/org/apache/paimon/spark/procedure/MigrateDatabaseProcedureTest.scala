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

import org.apache.paimon.spark.PaimonHiveTestBase

import org.apache.spark.sql.Row
class MigrateDatabaseProcedureTest extends PaimonHiveTestBase {
  Seq("parquet", "orc", "avro").foreach(
    format => {
      test(s"Paimon migrate database procedure: migrate $format non-partitioned database") {
        withTable("hive_tbl", "hive_tbl1") {
          // create hive table
          spark.sql(s"""
                       |CREATE TABLE hive_tbl (id STRING, name STRING, pt STRING)
                       |USING $format
                       |""".stripMargin)

          spark.sql(s"""
                       |CREATE TABLE hive_tbl1 (id STRING, name STRING, pt STRING)
                       |USING $format
                       |""".stripMargin)

          var rows0 = spark.sql("SHOW CREATE TABLE hive_tbl").collect()
          assert(!rows0.apply(0).toString().contains("USING paimon"))

          rows0 = spark.sql("SHOW CREATE TABLE hive_tbl1").collect()
          assert(!rows0.apply(0).toString().contains("USING paimon"))

          spark.sql(s"INSERT INTO hive_tbl VALUES ('1', 'a', 'p1'), ('2', 'b', 'p2')")

          spark.sql(
            s"CALL sys.migrate_database(source_type => 'hive', database => '$hiveDbName', options => 'file.format=$format')")

          checkAnswer(
            spark.sql(s"SELECT * FROM hive_tbl ORDER BY id"),
            Row("1", "a", "p1") :: Row("2", "b", "p2") :: Nil)

          var rows1 = spark.sql("SHOW CREATE TABLE hive_tbl").collect()
          assert(rows1.apply(0).toString().contains("USING paimon"))

          rows1 = spark.sql("SHOW CREATE TABLE hive_tbl1").collect()
          assert(rows1.apply(0).toString().contains("USING paimon"))

        }
      }
    })

  Seq("parquet", "orc", "avro").foreach(
    format => {
      test(
        s"Paimon migrate database procedure: migrate $format database with setting parallelism") {
        withTable("hive_tbl_01", "hive_tbl_02") {
          // create hive table
          spark.sql(s"""
                       |CREATE TABLE hive_tbl_01 (id STRING, name STRING, pt STRING)
                       |USING $format
                       |""".stripMargin)

          spark.sql(s"""
                       |CREATE TABLE hive_tbl_02 (id STRING, name STRING, pt STRING)
                       |USING $format
                       |""".stripMargin)

          var rows0 = spark.sql("SHOW CREATE TABLE hive_tbl_01").collect()
          assert(!rows0.apply(0).toString().contains("USING paimon"))

          rows0 = spark.sql("SHOW CREATE TABLE hive_tbl_02").collect()
          assert(!rows0.apply(0).toString().contains("USING paimon"))

          spark.sql(s"INSERT INTO hive_tbl_01 VALUES ('1', 'a', 'p1'), ('2', 'b', 'p2')")

          spark.sql(
            s"CALL sys.migrate_database(source_type => 'hive', database => '$hiveDbName', options => 'file.format=$format', parallelism => 6)")

          checkAnswer(
            spark.sql(s"SELECT * FROM hive_tbl_01 ORDER BY id"),
            Row("1", "a", "p1") :: Row("2", "b", "p2") :: Nil)

          var rows1 = spark.sql("SHOW CREATE TABLE hive_tbl_01").collect()
          assert(rows1.apply(0).toString().contains("USING paimon"))

          rows1 = spark.sql("SHOW CREATE TABLE hive_tbl_02").collect()
          assert(rows1.apply(0).toString().contains("USING paimon"))
        }
      }
    })
}
