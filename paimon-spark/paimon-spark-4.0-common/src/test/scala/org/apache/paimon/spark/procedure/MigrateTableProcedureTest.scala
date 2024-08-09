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

class MigrateTableProcedureTest extends PaimonHiveTestBase {
  Seq("parquet", "orc", "avro").foreach(
    format => {
      test(s"Paimon migrate table procedure: migrate $format non-partitioned table") {
        withTable("hive_tbl") {
          // create hive table
          spark.sql(s"""
                       |CREATE TABLE hive_tbl (id STRING, name STRING, pt STRING)
                       |USING $format
                       |""".stripMargin)

          spark.sql(s"INSERT INTO hive_tbl VALUES ('1', 'a', 'p1'), ('2', 'b', 'p2')")

          spark.sql(
            s"CALL sys.migrate_table(source_type => 'hive', table => '$hiveDbName.hive_tbl', options => 'file.format=$format')")

          checkAnswer(
            spark.sql(s"SELECT * FROM hive_tbl ORDER BY id"),
            Row("1", "a", "p1") :: Row("2", "b", "p2") :: Nil)
        }
      }
    })

  Seq("parquet", "orc", "avro").foreach(
    format => {
      test(
        s"Paimon migrate table procedure: migrate $format non-partitioned table with set target table") {
        withTable("hive_tbl_rn") {
          // create hive table
          spark.sql(s"""
                       |CREATE TABLE hive_tbl_$format (id STRING, name STRING, pt STRING)
                       |USING $format
                       |""".stripMargin)

          spark.sql(s"INSERT INTO hive_tbl_$format VALUES ('1', 'a', 'p1'), ('2', 'b', 'p2')")

          spark.sql(
            s"CALL sys.migrate_table(source_type => 'hive', table => '$hiveDbName.hive_tbl_$format', options => 'file.format=$format', target_table => '$hiveDbName.target_$format', delete_origin => false)")

          checkAnswer(
            spark.sql(s"SELECT * FROM target_$format ORDER BY id"),
            Row("1", "a", "p1") :: Row("2", "b", "p2") :: Nil)

          checkAnswer(spark.sql(s"SELECT * FROM hive_tbl_$format ORDER BY id"), Nil)

        }
      }
    })

  Seq("parquet", "orc", "avro").foreach(
    format => {
      test(s"Paimon migrate table procedure: migrate $format partitioned table") {
        withTable("hive_tbl") {
          // create hive table
          spark.sql(s"""
                       |CREATE TABLE hive_tbl (id STRING, name STRING, pt STRING)
                       |USING $format
                       |PARTITIONED BY (pt)
                       |""".stripMargin)

          spark.sql(s"INSERT INTO hive_tbl VALUES ('1', 'a', 'p1'), ('2', 'b', 'p2')")

          spark.sql(
            s"CALL sys.migrate_table(source_type => 'hive', table => '$hiveDbName.hive_tbl', options => 'file.format=$format')")

          checkAnswer(
            spark.sql(s"SELECT * FROM hive_tbl ORDER BY id"),
            Row("1", "a", "p1") :: Row("2", "b", "p2") :: Nil)
        }
      }
    })

  test(s"Paimon migrate table procedure: migrate partitioned table with null partition") {
    withTable("hive_tbl") {
      // create hive table
      spark.sql(s"""
                   |CREATE TABLE hive_tbl (id STRING, name STRING, pt INT)
                   |USING parquet
                   |PARTITIONED BY (pt)
                   |""".stripMargin)

      spark.sql(s"INSERT INTO hive_tbl VALUES ('1', 'a', 1), ('2', 'b', null)")

      checkAnswer(
        spark.sql(s"SELECT * FROM hive_tbl ORDER BY id"),
        Row("1", "a", 1) :: Row("2", "b", null) :: Nil)

      spark.sql(
        s"""CALL sys.migrate_table(source_type => 'hive', table => '$hiveDbName.hive_tbl',
           |options => 'file.format=parquet,partition.default-name=__HIVE_DEFAULT_PARTITION__')
           |""".stripMargin)

      checkAnswer(
        spark.sql(s"SELECT * FROM hive_tbl ORDER BY id"),
        Row("1", "a", 1) :: Row("2", "b", null) :: Nil)
    }
  }

  test(s"Paimon migrate table procedure: migrate table with wrong options") {
    withTable("hive_tbl") {
      // create hive table
      spark.sql(s"""
                   |CREATE TABLE hive_tbl (id STRING, name STRING, pt INT)
                   |USING parquet
                   |PARTITIONED BY (pt)
                   |""".stripMargin)

      spark.sql(s"INSERT INTO hive_tbl VALUES ('1', 'a', 1)")

      assertThatThrownBy(
        () =>
          spark.sql(
            s"""CALL sys.migrate_table(source_type => 'hive', table => '$hiveDbName.hive_tbl',
               |options => 'file.format=parquet,bucket=1')
               |""".stripMargin))
        .hasMessageContaining("Hive migrator only support unaware-bucket target table")
    }
  }

  test(s"Paimon migrate table procedure: select migrate table with partition filter") {
    Seq("parquet", "orc", "avro").foreach(
      format => {
        withTable("migrate_tbl") {
          spark.sql(s"""
                       |CREATE TABLE migrate_tbl (id STRING, name STRING, pt INT)
                       |USING $format
                       |PARTITIONED BY (pt)
                       |""".stripMargin)

          spark.sql(s"INSERT INTO migrate_tbl VALUES ('1', 'a', 1), ('2', 'b', 2)")

          spark.sql(
            s"""CALL sys.migrate_table(source_type => 'hive', table => '$hiveDbName.migrate_tbl',
               |options => 'file.format=$format')
               |""".stripMargin)

          checkAnswer(
            spark.sql(s"SELECT * FROM migrate_tbl WHERE pt = 1 ORDER BY id"),
            Row("1", "a", 1) :: Nil)
          checkAnswer(
            spark.sql(s"SELECT * FROM migrate_tbl WHERE pt IS NOT null ORDER BY id"),
            Row("1", "a", 1) :: Row("2", "b", 2) :: Nil)
          checkAnswer(
            spark.sql(s"SELECT * FROM migrate_tbl WHERE name LIKE 'a%' OR pt IS null ORDER BY id"),
            Row("1", "a", 1) :: Nil)
        }
      })
  }
}
