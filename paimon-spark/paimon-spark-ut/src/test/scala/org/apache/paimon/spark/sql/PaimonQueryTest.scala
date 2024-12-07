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
  import testImplicits._

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

  test("Query metadata columns for bucket") {
    withTable("T") {
      spark.sql(
        """
          |CREATE TABLE T (c1 INT, c2 STRING) TBLPROPERTIES ('bucket-key'='c1', 'bucket'='3')
          |""".stripMargin)

      spark.sql("""
                  |INSERT INTO T
                  |VALUES (1, 'x1'), (2, 'x3'), (3, 'x3'), (4, 'x4'), (5, 'x5'), (6, 'x6')
                  |""".stripMargin)

      val res = spark.sql("""
                            |SELECT __paimon_partition, __paimon_bucket FROM T
                            |GROUP BY __paimon_partition, __paimon_bucket
                            |ORDER BY __paimon_partition, __paimon_bucket
                            |""".stripMargin)
      checkAnswer(res, Row(Row(), 0) :: Row(Row(), 1) :: Row(Row(), 2) :: Nil)
    }
  }

  test("Query metadata columns for partition") {
    withTable("T") {
      spark.sql("""
                  |CREATE TABLE T (c1 INT) PARTITIONED BY(p1 INT, p2 String)
                  |""".stripMargin)

      spark.sql("""
                  |INSERT INTO T
                  |VALUES (1, 1, 'x1'), (2, 1, 'x2'), (3, 2, 'x1'), (4, 2, 'x2'), (5, 3, 'x3')
                  |""".stripMargin)

      val res = spark.sql("""
                            |SELECT __paimon_partition, __paimon_bucket FROM T
                            |GROUP BY __paimon_partition, __paimon_bucket
                            |ORDER BY __paimon_partition, __paimon_bucket
                            |""".stripMargin)
      checkAnswer(
        res,
        Row(Row(1, "x1"), 0) :: Row(Row(1, "x2"), 0) :: Row(Row(2, "x1"), 0) ::
          Row(Row(2, "x2"), 0) :: Row(Row(3, "x3"), 0) :: Nil)
    }
  }

  fileFormats.foreach {
    fileFormat =>
      bucketModes.foreach {
        bucketMode =>
          test(s"Query input_file_name(): file.format=$fileFormat, bucket=$bucketMode") {

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

  test("Paimon Query: query nested cols") {
    withPk.foreach {
      hasPk =>
        fileFormats.foreach {
          fileFormat =>
            bucketModes.foreach {
              bucketMode =>
                val key = if (hasPk) "primary-key" else "bucket-key"
                val props = if (bucketMode != -1) {
                  s", '$key'='name', 'bucket' = '$bucketMode' "
                } else {
                  ""
                }
                withTable("students") {
                  sql(s"""
                         |CREATE TABLE students (
                         |  name STRING,
                         |  course STRUCT<course_name: STRING, grade: DOUBLE>,
                         |  teacher STRUCT<name: STRING, address: STRUCT<street: STRING, city: STRING>>,
                         |  m MAP<STRING, STRUCT<s:STRING, i INT, d: DOUBLE>>,
                         |  l ARRAY<STRUCT<s:STRING, i INT, d: DOUBLE>>,
                         |  s STRUCT<s1: STRING, s2: MAP<STRING, STRUCT<s:STRING, i INT, a: ARRAY<STRUCT<s:STRING, i INT, d: DOUBLE>>>>>,
                         |  m2 MAP<STRUCT<s:STRING, i INT, d: DOUBLE>, STRUCT<s:STRING, i INT, d: DOUBLE>>
                         |) USING paimon
                         |TBLPROPERTIES ('file.format'='$fileFormat' $props)
                         |""".stripMargin)

                  sql(s"""
                         |INSERT INTO students VALUES (
                         |  'Alice',
                         |  STRUCT('Math', 85.0),
                         |  STRUCT('John', STRUCT('Street 1', 'City 1')),
                         |  MAP('k1', STRUCT('s1', 1, 1.0), 'k2', STRUCT('s11', 11, 11.0)),
                         |  ARRAY(STRUCT('s1', 1, 1.0), STRUCT('s11', 11, 11.0)),
                         |  STRUCT('a', MAP('k1', STRUCT('s1', 1, ARRAY(STRUCT('s1', 1, 1.0))), 'k3', STRUCT('s11', 11, ARRAY(STRUCT('s11', 11, 11.0))))),
                         |  MAP(STRUCT('k1', 1, 1.0), STRUCT('s1', 1, 1.0), STRUCT('k2', 1, 1.0), STRUCT('s11', 11, 11.0)))
                         |""".stripMargin)

                  sql(
                    s"""
                       |INSERT INTO students VALUES (
                       |  'Bob',
                       |  STRUCT('Biology', 92.0),
                       |  STRUCT('Jane', STRUCT('Street 2', 'City 2')),
                       |  MAP('k2', STRUCT('s2', 2, 2.0)),
                       |  ARRAY(STRUCT('s2', 2, 2.0), STRUCT('s22', 22, 22.0)),
                       |  STRUCT('b', MAP('k2', STRUCT('s22', 22, ARRAY(STRUCT('s22', 22, 22.0))))),
                       |  MAP(STRUCT('k2', 2, 2.0), STRUCT('s22', 22, 22.0)))
                       |""".stripMargin)

                  sql(s"""
                         |INSERT INTO students VALUES (
                         |  'Cathy',
                         |  STRUCT('History', 95.0),
                         |  STRUCT('Jane', STRUCT('Street 3', 'City 3')),
                         |  MAP('k1', STRUCT('s3', 3, 3.0), 'k2', STRUCT('s33', 33, 33.0)),
                         |  ARRAY(STRUCT('s3', 3, 3.0)),
                         |  STRUCT('c', MAP('k1', STRUCT('s3', 3, ARRAY(STRUCT('s3', 3, 3.0))), 'k2', STRUCT('s33', 33, ARRAY(STRUCT('s33', 33, 33.0))))),
                         |  MAP(STRUCT('k1', 3, 3.0), STRUCT('s3', 3, 3.0), STRUCT('k2', 3, 3.0), STRUCT('s33', 33, 33.0)))
                         |""".stripMargin)

                  // Since Spark 4.0, when `spark.sql.ansi.enabled` is `true` and `array[i]` does not exist, an exception
                  // will be thrown instead of returning null. Here, just disabled it and return null for test.
                  withSparkSQLConf("spark.sql.ansi.enabled" -> "false") {
                    checkAnswer(
                      sql(s"""
                             |SELECT
                             |  course.grade, name, teacher.address, course.course_name,
                             |  m['k1'].d, m['k1'].s,
                             |  l[1].d, l[1].s,
                             |  s.s2['k2'].a[0].i,
                             |  map_keys(m2).i
                             |FROM students ORDER BY name
                             |""".stripMargin),
                      Seq(
                        Row(
                          85.0,
                          "Alice",
                          Row("Street 1", "City 1"),
                          "Math",
                          1.0,
                          "s1",
                          11.0,
                          "s11",
                          null,
                          Seq(1, 1)),
                        Row(
                          92.0,
                          "Bob",
                          Row("Street 2", "City 2"),
                          "Biology",
                          null,
                          null,
                          22.0,
                          "s22",
                          22,
                          Seq(2)),
                        Row(
                          95.0,
                          "Cathy",
                          Row("Street 3", "City 3"),
                          "History",
                          3.0,
                          "s3",
                          null,
                          null,
                          33,
                          Seq(3, 3))
                      )
                    )
                  }
                }
            }
        }
    }
  }

  test("Paimon Query: query nested array cols") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (
            | id INT,
            | array_array ARRAY<ARRAY<INT>>,
            | array_map ARRAY<MAP<STRING, STRING>>,
            | array_struct ARRAY<STRUCT<s1: INT, s2: STRING>>
            |)
            |""".stripMargin)

      sql("""
            |INSERT INTO t VALUES (
            | 1,
            | array(array(1, 3)),
            | array(map('k1', 'v1'), map('k2', 'v2')),
            | array(struct(1, 's1'), struct(2, 's2'))
            |)
            |""".stripMargin)

      checkAnswer(
        sql(s"""
               |SELECT
               |  array_array[0][1],
               |  array_map[0]['k1'],
               |  array_struct[1].s2
               |FROM t
               |""".stripMargin),
        Row(3, "v1", "s2")
      )
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
