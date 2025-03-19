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

import java.util.concurrent.ThreadLocalRandom

class MigrateFileProcedureTest extends PaimonHiveTestBase {

  private val random = ThreadLocalRandom.current().nextInt(10000)

  Seq("parquet", "orc", "avro").foreach(
    format => {
      test(s"Paimon migrate file procedure: migrate $format non-partitioned table") {
        withTable(s"hive_tbl1$random$format", s"paimon_tbl1$random$format") {
          // create hive table
          spark.sql(s"""
                       |CREATE TABLE hive_tbl1$random$format (id STRING, name STRING, pt STRING)
                       |USING $format
                       |""".stripMargin)

          spark.sql(
            s"INSERT INTO hive_tbl1$random$format VALUES ('1', 'a', 'p1'), ('2', 'b', 'p2')")

          // create paimon table
          spark.sql(s"""
                       |CREATE TABLE paimon_tbl1$random$format (id STRING, name STRING, pt STRING)
                       |USING PAIMON
                       |TBLPROPERTIES ('file.format'='$format', 'bucket'='-1')
                       |""".stripMargin)

          spark.sql(
            s"INSERT INTO paimon_tbl1$random$format VALUES ('3', 'c', 'p1'), ('4', 'd', 'p2')")

          spark.sql(
            s"CALL sys.migrate_table(source_type => 'hive', table => '$hiveDbName.hive_tbl1$random$format', target_table => '$hiveDbName.paimon_tbl1$random$format')")

          checkAnswer(
            spark.sql(s"SELECT * FROM paimon_tbl1$random$format ORDER BY id"),
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
        s"Paimon migrate file procedure: migrate $format non-partitioned table with parallelism") {
        withTable(s"hive_tbl_02$random$format", s"paimon_tbl_02$random$format") {
          // create hive table
          spark.sql(s"""
                       |CREATE TABLE hive_tbl_02$random$format (id STRING, name STRING, pt STRING)
                       |USING $format
                       |""".stripMargin)

          spark.sql(
            s"INSERT INTO hive_tbl_02$random$format VALUES ('1', 'a', 'p1'), ('2', 'b', 'p2')")

          // create paimon table
          spark.sql(s"""
                       |CREATE TABLE paimon_tbl_02$random$format (id STRING, name STRING, pt STRING)
                       |USING PAIMON
                       |TBLPROPERTIES ('file.format'='$format', 'bucket'='-1')
                       |""".stripMargin)

          spark.sql(
            s"INSERT INTO paimon_tbl_02$random$format VALUES ('3', 'c', 'p1'), ('4', 'd', 'p2')")

          spark.sql(
            s"CALL sys.migrate_table(source_type => 'hive', table => '$hiveDbName.hive_tbl_02$random$format', target_table => '$hiveDbName.paimon_tbl_02$random$format', parallelism => 6)")

          checkAnswer(
            spark.sql(s"SELECT * FROM paimon_tbl_02$random$format ORDER BY id"),
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
        withTable(s"hive_tbl3$random$format", s"paimon_tbl3$random$format") {
          // create hive table
          spark.sql(s"""
                       |CREATE TABLE hive_tbl3$random$format (id STRING, name STRING, pt STRING)
                       |USING $format
                       |""".stripMargin)

          spark.sql(
            s"INSERT INTO hive_tbl3$random$format VALUES ('1', 'a', 'p1'), ('2', 'b', 'p2')")

          // create paimon table
          spark.sql(s"""
                       |CREATE TABLE paimon_tbl3$random$format (id STRING, name STRING, pt STRING)
                       |USING PAIMON
                       |TBLPROPERTIES ('file.format'='$format', 'bucket'='-1')
                       |""".stripMargin)

          spark.sql(
            s"INSERT INTO paimon_tbl3$random$format VALUES ('3', 'c', 'p1'), ('4', 'd', 'p2')")

          spark.sql(
            s"CALL sys.migrate_table(source_type => 'hive', table => '$hiveDbName.hive_tbl3$random$format', target_table => '$hiveDbName.paimon_tbl3$random$format', delete_origin => false)")

          checkAnswer(spark.sql(s"SELECT * FROM hive_tbl3$random$format ORDER BY id"), Nil)

          checkAnswer(
            spark.sql(s"SELECT * FROM paimon_tbl3$random$format ORDER BY id"),
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
        withTable(s"hive_tbl4$random$format", s"paimon_tbl4$random$format") {
          // create hive table
          spark.sql(s"""
                       |CREATE TABLE hive_tbl4$random$format (id STRING, name STRING, pt STRING)
                       |USING $format
                       |PARTITIONED BY (pt)
                       |""".stripMargin)

          spark.sql(
            s"INSERT INTO hive_tbl4$random$format VALUES ('1', 'a', 'p1'), ('2', 'b', 'p2')")

          // create paimon table
          spark.sql(s"""
                       |CREATE TABLE paimon_tbl4$random$format (id STRING, name STRING, pt STRING)
                       |USING PAIMON
                       |TBLPROPERTIES ('file.format'='$format', 'bucket'='-1')
                       |PARTITIONED BY (pt)
                       |""".stripMargin)

          spark.sql(
            s"INSERT INTO paimon_tbl4$random$format VALUES ('3', 'c', 'p1'), ('4', 'd', 'p2')")

          spark.sql(
            s"CALL sys.migrate_table(source_type => 'hive', table => '$hiveDbName.hive_tbl4$random$format', target_table => '$hiveDbName.paimon_tbl4$random$format')")

          checkAnswer(
            spark.sql(s"SELECT * FROM paimon_tbl4$random$format ORDER BY id"),
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
        withTable(s"hive_tbl5$random$format", s"paimon_tbl5$random$format") {
          // create hive table
          spark.sql(s"""
                       |CREATE TABLE hive_tbl5$random$format (id STRING, name STRING, pt STRING)
                       |USING $format
                       |PARTITIONED BY (pt)
                       |""".stripMargin)

          spark.sql(
            s"INSERT INTO hive_tbl5$random$format VALUES ('1', 'a', 'p1'), ('2', 'b', 'p2')")

          // create paimon table
          spark.sql(s"""
                       |CREATE TABLE paimon_tbl5$random$format (id STRING, name STRING, pt STRING)
                       |USING PAIMON
                       |TBLPROPERTIES ('file.format'='$format', 'bucket'='-1')
                       |PARTITIONED BY (pt)
                       |""".stripMargin)

          spark.sql(
            s"INSERT INTO paimon_tbl5$random$format VALUES ('3', 'c', 'p1'), ('4', 'd', 'p2')")

          spark.sql(
            s"CALL sys.migrate_table(source_type => 'hive', table => '$hiveDbName.hive_tbl5$random$format', target_table => '$hiveDbName.paimon_tbl5$random$format', delete_origin => false)")

          checkAnswer(
            spark.sql(s"SELECT * FROM paimon_tbl5$random$format ORDER BY id"),
            Row("1", "a", "p1") :: Row("2", "b", "p2") :: Row("3", "c", "p1") :: Row(
              "4",
              "d",
              "p2") :: Nil)

          checkAnswer(spark.sql(s"SELECT * FROM hive_tbl5$random$format ORDER BY id"), Nil)
        }
      }
    })
}
