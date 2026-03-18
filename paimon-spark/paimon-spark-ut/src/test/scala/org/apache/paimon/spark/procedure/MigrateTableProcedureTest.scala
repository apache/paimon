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
import org.assertj.core.api.Assertions.assertThatThrownBy

import java.util.concurrent.ThreadLocalRandom

class MigrateTableProcedureTest extends PaimonHiveTestBase {

  private val random = ThreadLocalRandom.current().nextInt(10000)

  Seq("parquet", "orc", "avro").foreach(
    format => {
      test(s"Paimon migrate table procedure: migrate $format non-partitioned table") {
        withTable(s"hive_tbl$random") {
          // create hive table
          spark.sql(s"""
                       |CREATE TABLE hive_tbl$random (id STRING, name STRING, pt STRING)
                       |USING $format
                       |""".stripMargin)

          spark.sql(s"INSERT INTO hive_tbl$random VALUES ('1', 'a', 'p1'), ('2', 'b', 'p2')")

          spark.sql(
            s"CALL sys.migrate_table(source_type => 'hive', table => '$hiveDbName.hive_tbl$random', options => 'file.format=$format')")

          checkAnswer(
            spark.sql(s"SELECT * FROM hive_tbl$random ORDER BY id"),
            Row("1", "a", "p1") :: Row("2", "b", "p2") :: Nil)
        }
      }
    })

  Seq("parquet", "orc", "avro").foreach(
    format => {
      test(
        s"Paimon migrate table procedure: migrate $format non-partitioned table with setting parallelism") {
        withTable(s"hive_tbl_01$random") {
          // create hive table
          spark.sql(s"""
                       |CREATE TABLE hive_tbl_01$random (id STRING, name STRING, pt STRING)
                       |USING $format
                       |""".stripMargin)

          spark.sql(s"INSERT INTO hive_tbl_01$random VALUES ('1', 'a', 'p1'), ('2', 'b', 'p2')")

          spark.sql(
            s"CALL sys.migrate_table(source_type => 'hive', table => '$hiveDbName.hive_tbl_01$random', options => 'file.format=$format', parallelism => 6)")

          checkAnswer(
            spark.sql(s"SELECT * FROM hive_tbl_01$random ORDER BY id"),
            Row("1", "a", "p1") :: Row("2", "b", "p2") :: Nil)
        }
      }
    })

  Seq("parquet", "orc", "avro").foreach(
    format => {
      test(s"Paimon migrate table procedure: migrate $format table with options_map") {
        withTable(s"hive_tbl$random") {
          // create hive table
          spark.sql(s"""
                       |CREATE TABLE hive_tbl$random (id STRING, name STRING, pt STRING)
                       |USING $format
                       |""".stripMargin)

          spark.sql(s"INSERT INTO hive_tbl$random VALUES ('1', 'a', 'p1'), ('2', 'b', 'p2')")

          spark.sql(
            s"CALL sys.migrate_table(source_type => 'hive', table => '$hiveDbName.hive_tbl$random', options => 'file.format=$format', options_map => map('orc.encrypt', 'pii:id,name'))")

          checkAnswer(
            spark.sql(s"SELECT * FROM hive_tbl$random ORDER BY id"),
            Row("1", "a", "p1") :: Row("2", "b", "p2") :: Nil)

          assert(
            spark
              .sql(s"SHOW CREATE TABLE hive_tbl$random")
              .collect()
              .apply(0)
              .toString()
              .contains("'orc.encrypt' = 'pii:id,name',"))
        }
      }
    })

  Seq("parquet", "orc", "avro").foreach(
    format => {
      test(
        s"Paimon migrate table procedure: migrate $format non-partitioned table with set target table") {
        withTable(s"hive_tbl_$random", s"target_$random") {
          // create hive table
          spark.sql(s"""
                       |CREATE TABLE hive_tbl_$random (id STRING, name STRING, pt STRING)
                       |USING $format
                       |""".stripMargin)

          spark.sql(s"INSERT INTO hive_tbl_$random VALUES ('1', 'a', 'p1'), ('2', 'b', 'p2')")

          spark.sql(
            s"CALL sys.migrate_table(source_type => 'hive', table => '$hiveDbName.hive_tbl_$random', options => 'file.format=$format', target_table => '$hiveDbName.target_$random', delete_origin => false)")

          checkAnswer(
            spark.sql(s"SELECT * FROM target_$random ORDER BY id"),
            Row("1", "a", "p1") :: Row("2", "b", "p2") :: Nil)

          checkAnswer(spark.sql(s"SELECT * FROM hive_tbl_$random ORDER BY id"), Nil)

        }
      }
    })

  Seq("parquet", "orc", "avro").foreach(
    format => {
      test(s"Paimon migrate table procedure: migrate $format partitioned table") {
        withTable(s"hive_tbl$random") {
          // create hive table
          spark.sql(s"""
                       |CREATE TABLE hive_tbl$random (id STRING, name STRING, pt STRING)
                       |USING $format
                       |PARTITIONED BY (pt)
                       |""".stripMargin)

          spark.sql(s"INSERT INTO hive_tbl$random VALUES ('1', 'a', 'p1'), ('2', 'b', 'p2')")

          spark.sql(
            s"CALL sys.migrate_table(source_type => 'hive', table => '$hiveDbName.hive_tbl$random', options => 'file.format=$format')")

          checkAnswer(
            spark.sql(s"SELECT * FROM hive_tbl$random ORDER BY id"),
            Row("1", "a", "p1") :: Row("2", "b", "p2") :: Nil)
        }
      }
    })

  test(s"Paimon migrate table procedure: migrate partitioned table with null partition") {
    withTable(s"hive_tbl$random") {
      // create hive table
      spark.sql(s"""
                   |CREATE TABLE hive_tbl$random (id STRING, name STRING, pt INT)
                   |USING parquet
                   |PARTITIONED BY (pt)
                   |""".stripMargin)

      spark.sql(s"INSERT INTO hive_tbl$random VALUES ('1', 'a', 1), ('2', 'b', null)")

      checkAnswer(
        spark.sql(s"SELECT * FROM hive_tbl$random ORDER BY id"),
        Row("1", "a", 1) :: Row("2", "b", null) :: Nil)

      spark.sql(
        s"""CALL sys.migrate_table(source_type => 'hive', table => '$hiveDbName.hive_tbl$random',
           |options => 'file.format=orc')
           |""".stripMargin)

      checkAnswer(
        spark.sql(s"SELECT * FROM hive_tbl$random ORDER BY id"),
        Row("1", "a", 1) :: Row("2", "b", null) :: Nil)
    }
  }

  test(s"Paimon migrate table procedure: migrate table with wrong options") {
    withTable(s"hive_tbl$random") {
      // create hive table
      spark.sql(s"""
                   |CREATE TABLE hive_tbl$random (id STRING, name STRING, pt INT)
                   |USING parquet
                   |PARTITIONED BY (pt)
                   |""".stripMargin)

      spark.sql(s"INSERT INTO hive_tbl$random VALUES ('1', 'a', 1)")

      assertThatThrownBy(
        () =>
          spark.sql(
            s"""CALL sys.migrate_table(source_type => 'hive', table => '$hiveDbName.hive_tbl$random',
               |options => 'file.format=parquet,bucket=1')
               |""".stripMargin))
        .hasMessageContaining("Hive migrator only support unaware-bucket target table")
    }
  }

  test(s"Paimon migrate table procedure: select migrate table with partition filter") {
    Seq("parquet", "orc", "avro").foreach(
      format => {
        withTable(s"migrate_tbl$random") {
          spark.sql(s"""
                       |CREATE TABLE migrate_tbl$random (id STRING, name STRING, pt INT)
                       |USING $format
                       |PARTITIONED BY (pt)
                       |""".stripMargin)

          spark.sql(s"INSERT INTO migrate_tbl$random VALUES ('1', 'a', 1), ('2', 'b', 2)")

          spark.sql(
            s"""CALL sys.migrate_table(source_type => 'hive', table => '$hiveDbName.migrate_tbl$random',
               |options => 'file.format=$format')
               |""".stripMargin)

          checkAnswer(
            spark.sql(s"SELECT * FROM migrate_tbl$random WHERE pt = 1 ORDER BY id"),
            Row("1", "a", 1) :: Nil)
          checkAnswer(
            spark.sql(s"SELECT * FROM migrate_tbl$random WHERE pt IS NOT null ORDER BY id"),
            Row("1", "a", 1) :: Row("2", "b", 2) :: Nil)
          checkAnswer(
            spark.sql(
              s"SELECT * FROM migrate_tbl$random WHERE name LIKE 'a%' OR pt IS null ORDER BY id"),
            Row("1", "a", 1) :: Nil)
        }
      })
  }
}
