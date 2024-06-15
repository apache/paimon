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

package org.apache.paimon.spark.sql

import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.table.source.DataSplit

import org.apache.spark.sql.{Row, SparkSession}
import org.junit.jupiter.api.Assertions

import java.util

import scala.collection.JavaConverters._

class PaimonQueryTest extends PaimonSparkTestBase {

  fileFormats.foreach {
    fileFormat =>
      bucketModes.foreach {
        bucketMode =>
          test(s"Query metadata columns: file.format=$fileFormat, bucket=$bucketMode") {
            withTable("T") {

              spark.sql(
                s"""
                   |CREATE TABLE T (id INT, name STRING)
                   |TBLPROPERTIES ('primary-key' = 'id', 'file.format'='$fileFormat', 'bucket'='$bucketMode')
                   |""".stripMargin)

              spark.sql("""
                          |INSERT INTO T
                          |VALUES (1, 'x1'), (2, 'x3'), (3, 'x3'), (4, 'x4'), (5, 'x5')
                          |""".stripMargin)

              val location = loadTable("T").location().toUri.toString
              val res = spark.sql(
                s"""
                   |SELECT SUM(cnt)
                   |FROM (
                   |  SELECT __paimon_file_path AS path, count(1) AS cnt, count(distinct __paimon_row_index) AS dc
                   |  FROM T
                   |  GROUP BY __paimon_file_path
                   |)
                   |WHERE startswith(path, '$location') and endswith(path, '.$fileFormat') and cnt == dc
                   |""".stripMargin)
              checkAnswer(res, Row(5))
            }
          }
      }

  }

  fileFormats.foreach {
    fileFormat =>
      bucketModes.foreach {
        bucketMode =>
          test(s"Query input_file_name(): file.format=$fileFormat, bucket=$bucketMode") {
            val _spark: SparkSession = spark
            import _spark.implicits._

            withTable("T") {
              val bucketProp = if (bucketMode != -1) {
                s", 'bucket-key'='id', 'bucket' = '$bucketMode' "
              } else {
                ""
              }

              spark.sql(s"""
                           |CREATE TABLE T (id INT, name STRING)
                           |TBLPROPERTIES ('file.format'='$fileFormat' $bucketProp)
                           |""".stripMargin)

              val location = loadTable("T").location().toUri.toString

              spark.sql("INSERT INTO T VALUES (1, 'x1'), (3, 'x3')")

              val res1 = spark.sql(s"""
                                      |SELECT *,
                                      |startswith(input_file_name(), '$location') AS start,
                                      |endswith(input_file_name(), '.$fileFormat') AS end
                                      |FROM T
                                      |ORdER BY id
                                      |""".stripMargin)
              checkAnswer(res1, Row(1, "x1", true, true) :: Row(3, "x3", true, true) :: Nil)

              spark.sql("INSERT INTO T VALUES (2, 'x2'), (4, 'x4'), (6, 'x6')")

              val res2 =
                spark.sql("SELECT input_file_name() FROM T").distinct().as[String].collect().sorted
              val allDataFiles = getAllFiles("T", Seq.empty, null)
              Assertions.assertTrue(res2.sameElements(allDataFiles))
            }
          }

      }
  }

  fileFormats.foreach {
    fileFormat =>
      bucketModes.foreach {
        bucketMode =>
          test(
            s"Query input_file_name() for partitioned table: file.format=$fileFormat, bucket=$bucketMode") {
            val _spark: SparkSession = spark
            import _spark.implicits._

            val bucketProp = if (bucketMode != -1) {
              s", 'bucket-key'='id', 'bucket' = '$bucketMode' "
            } else {
              ""
            }

            withTable("T") {
              spark.sql(s"""
                           |CREATE TABLE T (id INT, name STRING, pt STRING)
                           |PARTITIONED BY (pt)
                           |TBLPROPERTIES ('file.format'='$fileFormat' $bucketProp)
                           |""".stripMargin)

              val location = loadTable("T").location().toUri.toString

              spark.sql("INSERT INTO T VALUES (1, 'x1', '2024'), (3, 'x3', '2024')")

              val res1 = spark.sql(s"""
                                      |SELECT id, name, pt,
                                      |startswith(input_file_name(), '$location') AS start,
                                      |endswith(input_file_name(), '.$fileFormat') AS end
                                      |FROM T
                                      |ORdER BY id
                                      |""".stripMargin)
              checkAnswer(
                res1,
                Row(1, "x1", "2024", true, true) :: Row(3, "x3", "2024", true, true) :: Nil)

              spark.sql("""
                          |INSERT INTO T
                          |VALUES (2, 'x2', '2025'), (4, 'x4', '2025'), (6, 'x6', '2026')
                          |""".stripMargin)

              val res2 =
                spark
                  .sql("SELECT input_file_name() FROM T WHERE pt='2026'")
                  .distinct()
                  .as[String]
                  .collect()
                  .sorted
              val partitionFilter = new util.HashMap[String, String]()
              partitionFilter.put("pt", "2026")
              val partialDataFiles = getAllFiles("T", Seq("pt"), partitionFilter)
              Assertions.assertTrue(res2.sameElements(partialDataFiles))

              val res3 =
                spark.sql("SELECT input_file_name() FROM T").distinct().as[String].collect().sorted
              val allDataFiles = getAllFiles("T", Seq("pt"), null)
              Assertions.assertTrue(res3.sameElements(allDataFiles))
            }
          }

      }
  }

  private def getAllFiles(
      tableName: String,
      partitions: Seq[String],
      partitionFilter: java.util.Map[String, String]): Array[String] = {
    val paimonTable = loadTable(tableName)
    val location = paimonTable.location()

    val files = paimonTable
      .newSnapshotReader()
      .withPartitionFilter(partitionFilter)
      .read()
      .splits()
      .asScala
      .collect { case ds: DataSplit => ds }
      .flatMap {
        ds =>
          val prefix = if (partitions.isEmpty) {
            s"$location/bucket-${ds.bucket}"
          } else {
            val partitionPath = partitions.zipWithIndex
              .map {
                case (pt, index) =>
                  s"$pt=" + ds.partition().getString(index)
              }
              .mkString("/")
            s"$location/$partitionPath/bucket-${ds.bucket}"
          }
          ds.dataFiles().asScala.map(f => prefix + "/" + f.fileName)
      }
    files.sorted.toArray
  }
}
