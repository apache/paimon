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

class MigrateFileProcedureTest extends PaimonHiveTestBase {
  Seq("parquet", "orc", "avro").foreach(
    format => {
      test(s"Paimon migrate file procedure: migrate $format non-partitioned table") {
        withTable("hive_tbl", "paimon_tbl") {
          // create hive table
          spark.sql(s"""
                       |CREATE TABLE hive_tbl (id STRING, name STRING, pt STRING)
                       |USING $format
                       |""".stripMargin)

          spark.sql(s"INSERT INTO hive_tbl VALUES ('1', 'a', 'p1'), ('2', 'b', 'p2')")

          // create paimon table
          spark.sql(s"""
                       |CREATE TABLE paimon_tbl (id STRING, name STRING, pt STRING)
                       |USING PAIMON
                       |TBLPROPERTIES ('file.format'='$format', 'bucket'='-1')
                       |""".stripMargin)

          spark.sql(s"INSERT INTO paimon_tbl VALUES ('3', 'c', 'p1'), ('4', 'd', 'p2')")

          spark.sql(
            s"CALL sys.migrate_file(source_type => 'hive', source_table => '$hiveDbName.hive_tbl', target_table => '$hiveDbName.paimon_tbl')")

          checkAnswer(
            spark.sql("SELECT * FROM paimon_tbl ORDER BY id"),
            Row("1", "a", "p1") :: Row("2", "b", "p2") :: Row("3", "c", "p1") :: Row(
              "4",
              "d",
              "p2") :: Nil)
        }
      }
    })

  Seq("parquet", "orc", "avro").foreach(
    format => {
      test(
        s"Paimon migrate file procedure: migrate $format non-partitioned table with delete source table") {
        withTable("hive_tbl", "paimon_tbl") {
          // create hive table
          spark.sql(s"""
                       |CREATE TABLE hive_tbl (id STRING, name STRING, pt STRING)
                       |USING $format
                       |""".stripMargin)

          spark.sql(s"INSERT INTO hive_tbl VALUES ('1', 'a', 'p1'), ('2', 'b', 'p2')")

          // create paimon table
          spark.sql(s"""
                       |CREATE TABLE paimon_tbl (id STRING, name STRING, pt STRING)
                       |USING PAIMON
                       |TBLPROPERTIES ('file.format'='$format', 'bucket'='-1')
                       |""".stripMargin)

          spark.sql(s"INSERT INTO paimon_tbl VALUES ('3', 'c', 'p1'), ('4', 'd', 'p2')")

          spark.sql(
            s"CALL sys.migrate_file(source_type => 'hive', source_table => '$hiveDbName.hive_tbl', target_table => '$hiveDbName.paimon_tbl', delete_origin => false)")

          checkAnswer(spark.sql("SELECT * FROM hive_tbl ORDER BY id"), Nil)

          checkAnswer(
            spark.sql("SELECT * FROM paimon_tbl ORDER BY id"),
            Row("1", "a", "p1") :: Row("2", "b", "p2") :: Row("3", "c", "p1") :: Row(
              "4",
              "d",
              "p2") :: Nil)
        }
      }
    })

  Seq("parquet", "orc", "avro").foreach(
    format => {
      test(s"Paimon migrate file procedure: migrate $format partitioned table") {
        withTable("hive_tbl", "paimon_tbl") {
          // create hive table
          spark.sql(s"""
                       |CREATE TABLE hive_tbl (id STRING, name STRING, pt STRING)
                       |USING $format
                       |PARTITIONED BY (pt)
                       |""".stripMargin)

          spark.sql(s"INSERT INTO hive_tbl VALUES ('1', 'a', 'p1'), ('2', 'b', 'p2')")

          // create paimon table
          spark.sql(s"""
                       |CREATE TABLE paimon_tbl (id STRING, name STRING, pt STRING)
                       |USING PAIMON
                       |TBLPROPERTIES ('file.format'='$format', 'bucket'='-1')
                       |PARTITIONED BY (pt)
                       |""".stripMargin)

          spark.sql(s"INSERT INTO paimon_tbl VALUES ('3', 'c', 'p1'), ('4', 'd', 'p2')")

          spark.sql(
            s"CALL sys.migrate_file(source_type => 'hive', source_table => '$hiveDbName.hive_tbl', target_table => '$hiveDbName.paimon_tbl')")

          checkAnswer(
            spark.sql("SELECT * FROM paimon_tbl ORDER BY id"),
            Row("1", "a", "p1") :: Row("2", "b", "p2") :: Row("3", "c", "p1") :: Row(
              "4",
              "d",
              "p2") :: Nil)
        }
      }
    })

  Seq("parquet", "orc", "avro").foreach(
    format => {
      test(
        s"Paimon migrate file procedure: migrate $format partitioned table with delete source table") {
        withTable("hive_tbl", "paimon_tbl") {
          // create hive table
          spark.sql(s"""
                       |CREATE TABLE hive_tbl (id STRING, name STRING, pt STRING)
                       |USING $format
                       |PARTITIONED BY (pt)
                       |""".stripMargin)

          spark.sql(s"INSERT INTO hive_tbl VALUES ('1', 'a', 'p1'), ('2', 'b', 'p2')")

          // create paimon table
          spark.sql(s"""
                       |CREATE TABLE paimon_tbl (id STRING, name STRING, pt STRING)
                       |USING PAIMON
                       |TBLPROPERTIES ('file.format'='$format', 'bucket'='-1')
                       |PARTITIONED BY (pt)
                       |""".stripMargin)

          spark.sql(s"INSERT INTO paimon_tbl VALUES ('3', 'c', 'p1'), ('4', 'd', 'p2')")

          spark.sql(
            s"CALL sys.migrate_file(source_type => 'hive', source_table => '$hiveDbName.hive_tbl', target_table => '$hiveDbName.paimon_tbl', delete_origin => false)")

          checkAnswer(
            spark.sql("SELECT * FROM paimon_tbl ORDER BY id"),
            Row("1", "a", "p1") :: Row("2", "b", "p2") :: Row("3", "c", "p1") :: Row(
              "4",
              "d",
              "p2") :: Nil)

          checkAnswer(spark.sql("SELECT * FROM hive_tbl ORDER BY id"), Nil)
        }
      }
    })
}
