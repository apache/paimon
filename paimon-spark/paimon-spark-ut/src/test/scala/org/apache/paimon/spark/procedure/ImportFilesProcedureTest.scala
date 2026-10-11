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
import org.apache.paimon.table.source.DataSplit

import org.apache.spark.sql.Row

import java.io.File

import scala.collection.JavaConverters._

class ImportFilesProcedureTest extends PaimonSparkTestBase {

  Seq("parquet", "orc", "avro").foreach {
    format =>
      test(s"import $format files with real counts into a partition") {
        withTempDir {
          dir =>
            val location = new File(dir, "external").getCanonicalPath
            spark
              .sql("SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3")
              .repartition(2)
              .write
              .format(format)
              .save(location)
            val sourceFiles = new File(location).listFiles.filter(_.getName.endsWith(s".$format"))

            spark.sql(s"""
                         |CREATE TABLE T (id INT, dt STRING, hour INT)
                         |PARTITIONED BY (dt, hour)
                         |TBLPROPERTIES ('bucket'='-1', 'file.format'='$format')
                         |""".stripMargin)
            spark.sql("INSERT INTO T VALUES (4, 'p1', 12), (5, 'p2', 13)")
            spark.sql("CACHE TABLE T")
            try {
              checkAnswer(
                spark.sql(s"""
                             |CALL sys.import_files(
                             |  table => 'test.T',
                             |  location => '$location',
                             |  partition => 'hour=12,dt=p1')
                             |""".stripMargin),
                Row(sourceFiles.length.toLong)
              )

              assert(sourceFiles.forall(_.exists()))
              val table =
                paimonCatalog.getTable(org.apache.paimon.catalog.Identifier.create("test", "T"))
              val externalFiles = table.newReadBuilder.newScan.plan.splits.asScala
                .flatMap(_.asInstanceOf[DataSplit].dataFiles.asScala)
                .filter(_.externalPath.isPresent)
              assert(externalFiles.size == sourceFiles.length)
              assert(externalFiles.forall(_.rowCount >= 0L))
              assert(externalFiles.map(_.rowCount).sum == 3L)

              checkAnswer(
                spark.sql("SELECT * FROM T WHERE dt = 'p1' ORDER BY id"),
                Seq(Row(1, "p1", 12), Row(2, "p1", 12), Row(3, "p1", 12), Row(4, "p1", 12)))
              checkAnswer(spark.sql("SELECT COUNT(*) FROM T"), Row(5L))
              checkAnswer(
                spark.sql("SELECT dt, COUNT(*) FROM T GROUP BY dt"),
                Seq(Row("p1", 4L), Row("p2", 1L)))
            } finally {
              spark.sql("UNCACHE TABLE T")
            }

            checkAnswer(spark.sql("SELECT COUNT(*) FROM T"), Row(5L))
            checkAnswer(
              spark.sql("SELECT dt, COUNT(*) FROM T GROUP BY dt"),
              Seq(Row("p1", 4L), Row("p2", 1L)))
            spark.sql("ALTER TABLE T ADD COLUMN added INT")
            checkAnswer(spark.sql("SELECT COUNT(*) FROM T WHERE added IS NULL"), Row(5L))
            checkAnswer(
              spark.sql("SELECT id FROM T WHERE id > 2 ORDER BY id LIMIT 2"),
              Seq(Row(3), Row(4)))
            assert(spark.sql("SELECT * FROM T LIMIT 3").collect().length == 3)
            spark.sql("ANALYZE TABLE T COMPUTE STATISTICS FOR COLUMNS id")
            checkAnswer(spark.sql("SELECT COUNT(*) FROM T"), Row(5L))
        }
      }
  }

  test("import into an unpartitioned table without optional arguments") {
    withTempDir {
      dir =>
        val location = new File(dir, "external").getCanonicalPath
        spark.sql("SELECT 1 AS id UNION ALL SELECT 2").coalesce(1).write.parquet(location)
        spark.sql("CREATE TABLE T (id INT) TBLPROPERTIES ('bucket'='-1', 'file.format'='parquet')")
        checkAnswer(spark.sql(s"CALL sys.import_files('test.T', '$location')"), Row(1L))
        checkAnswer(spark.sql("SELECT COUNT(*) FROM T"), Row(2L))
    }
  }
}
