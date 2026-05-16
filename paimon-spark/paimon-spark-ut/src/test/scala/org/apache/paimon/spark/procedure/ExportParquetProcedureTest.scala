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

import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.sql.Row
import org.assertj.core.api.Assertions.assertThat

import java.io.File
import java.util.concurrent.ThreadLocalRandom

class ExportParquetProcedureTest extends PaimonSparkTestBase {

  test("Paimon export parquet procedure: project columns and filter rows") {
    val random = ThreadLocalRandom.current().nextInt(100000)
    withTable(s"tbl$random") {
      sql(s"""
             |CREATE TABLE tbl$random (
             |  id INT,
             |  name STRING,
             |  score INT,
             |  dt STRING
             |)
             |PARTITIONED BY (dt)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO tbl$random VALUES
             |  (1, 'a', 10, '2026-05-13'),
             |  (2, 'b', 20, '2026-05-14'),
             |  (3, 'c', 30, '2026-05-14')
             |""".stripMargin)

      withTempDir {
        dir =>
          val output = new File(dir, "export").getAbsolutePath

          checkAnswer(
            spark.sql(s"""
                         |CALL sys.export_parquet(
                         |  table => 'tbl$random',
                         |  columns => 'id,name',
                         |  output_path => '$output',
                         |  where => "dt = '2026-05-14' and score >= 20",
                         |  parallelism => 2)
                         |""".stripMargin),
            Row(true, 2L) :: Nil
          )

          assertThat(new File(output, "_SUCCESS")).exists()
          assertThat(new File(output).listFiles().filter(_.getName.endsWith(".parquet")).length)
            .isGreaterThan(0)

          val exported = spark.read.parquet(output)
          assertThat(exported.schema.fieldNames).containsExactly("id", "name")
          checkAnswer(exported.orderBy("id"), Row(2, "b") :: Row(3, "c") :: Nil)
      }
    }
  }

  test("Paimon export parquet procedure: export all columns without filter") {
    val random = ThreadLocalRandom.current().nextInt(100000)
    withTable(s"tbl_all$random") {
      sql(s"""
             |CREATE TABLE tbl_all$random (
             |  id INT,
             |  name STRING,
             |  score INT
             |)
             |""".stripMargin)

      sql(s"""
             |INSERT INTO tbl_all$random VALUES
             |  (1, 'a', 10),
             |  (2, 'b', 20)
             |""".stripMargin)

      withTempDir {
        dir =>
          val output = new File(dir, "export-all").getAbsolutePath

          checkAnswer(
            spark.sql(s"""
                         |CALL sys.export_parquet(
                         |  table => 'tbl_all$random',
                         |  columns => '*',
                         |  output_path => '$output')
                         |""".stripMargin),
            Row(true, 2L) :: Nil
          )

          assertThat(new File(output, "_SUCCESS")).exists()
          val exported = spark.read.parquet(output)
          assertThat(exported.schema.fieldNames).containsExactly("id", "name", "score")
          checkAnswer(exported.orderBy("id"), Row(1, "a", 10) :: Row(2, "b", 20) :: Nil)
      }
    }
  }
}
