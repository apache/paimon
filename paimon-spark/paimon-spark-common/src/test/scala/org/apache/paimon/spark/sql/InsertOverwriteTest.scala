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
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import java.sql.Date

class InsertOverwriteTest extends PaimonSparkTestBase {

  withPk.foreach {
    hasPk =>
      bucketModes.foreach {
        bucket =>
          test(s"insert overwrite non-partitioned table: hasPk: $hasPk, bucket: $bucket") {
            val primaryKeysProp = if (hasPk) {
              "'primary-key'='a,b',"
            } else {
              ""
            }

            spark.sql(s"""
                         |CREATE TABLE T (a INT, b INT, c STRING)
                         |TBLPROPERTIES ($primaryKeysProp 'bucket'='$bucket')
                         |""".stripMargin)

            spark.sql("INSERT INTO T values (1, 1, '1'), (2, 2, '2')")
            checkAnswer(
              spark.sql("SELECT * FROM T ORDER BY a, b"),
              Row(1, 1, "1") :: Row(2, 2, "2") :: Nil)

            spark.sql("INSERT OVERWRITE T VALUES (1, 3, '3'), (2, 4, '4')");
            checkAnswer(
              spark.sql("SELECT * FROM T ORDER BY a, b"),
              Row(1, 3, "3") :: Row(2, 4, "4") :: Nil)
          }
      }
  }

  withPk.foreach {
    hasPk =>
      bucketModes.foreach {
        bucket =>
          test(s"insert overwrite single-partitioned table: hasPk: $hasPk, bucket: $bucket") {
            val primaryKeysProp = if (hasPk) {
              "'primary-key'='a,b',"
            } else {
              ""
            }

            spark.sql(s"""
                         |CREATE TABLE T (a INT, b INT, c STRING)
                         |TBLPROPERTIES ($primaryKeysProp 'bucket'='$bucket')
                         |PARTITIONED BY (a)
                         |""".stripMargin)

            spark.sql("INSERT INTO T values (1, 1, '1'), (2, 2, '2')")
            checkAnswer(
              spark.sql("SELECT * FROM T ORDER BY a, b"),
              Row(1, 1, "1") :: Row(2, 2, "2") :: Nil)

            // overwrite the whole table
            spark.sql("INSERT OVERWRITE T VALUES (1, 3, '3'), (2, 4, '4')")
            checkAnswer(
              spark.sql("SELECT * FROM T ORDER BY a, b"),
              Row(1, 3, "3") :: Row(2, 4, "4") :: Nil)

            // overwrite the a=1 partition
            spark.sql("INSERT OVERWRITE T PARTITION (a = 1) VALUES (5, '5'), (7, '7')")
            checkAnswer(
              spark.sql("SELECT * FROM T ORDER BY a, b"),
              Row(1, 5, "5") :: Row(1, 7, "7") :: Row(2, 4, "4") :: Nil)

          }
      }
  }

  withPk.foreach {
    hasPk =>
      bucketModes.foreach {
        bucket =>
          test(s"insert overwrite mutil-partitioned table: hasPk: $hasPk, bucket: $bucket") {
            val primaryKeysProp = if (hasPk) {
              "'primary-key'='a,pt1,pt2',"
            } else {
              ""
            }

            spark.sql(s"""
                         |CREATE TABLE T (a INT, b STRING, pt1 STRING, pt2 INT)
                         |TBLPROPERTIES ($primaryKeysProp 'bucket'='$bucket')
                         |PARTITIONED BY (pt1, pt2)
                         |""".stripMargin)

            spark.sql(
              "INSERT INTO T values (1, 'a', 'ptv1', 11), (2, 'b', 'ptv1', 11), (3, 'c', 'ptv1', 22), (4, 'd', 'ptv2', 22)")
            checkAnswer(
              spark.sql("SELECT * FROM T ORDER BY a"),
              Row(1, "a", "ptv1", 11) :: Row(2, "b", "ptv1", 11) :: Row(3, "c", "ptv1", 22) :: Row(
                4,
                "d",
                "ptv2",
                22) :: Nil)

            // overwrite the pt2=22 partition
            spark.sql(
              "INSERT OVERWRITE T PARTITION (pt2 = 22) VALUES (3, 'c2', 'ptv1'), (4, 'd2', 'ptv3')")
            checkAnswer(
              spark.sql("SELECT * FROM T ORDER BY a"),
              Row(1, "a", "ptv1", 11) :: Row(2, "b", "ptv1", 11) :: Row(3, "c2", "ptv1", 22) :: Row(
                4,
                "d2",
                "ptv3",
                22) :: Nil)

            // overwrite the pt1=ptv3 partition
            spark.sql("INSERT OVERWRITE T PARTITION (pt1 = 'ptv3') VALUES (4, 'd3', 22)")
            checkAnswer(
              spark.sql("SELECT * FROM T ORDER BY a"),
              Row(1, "a", "ptv1", 11) :: Row(2, "b", "ptv1", 11) :: Row(3, "c2", "ptv1", 22) :: Row(
                4,
                "d3",
                "ptv3",
                22) :: Nil)

            // overwrite the pt1=ptv1, pt2=11 partition
            spark.sql("INSERT OVERWRITE T PARTITION (pt1 = 'ptv1', pt2=11) VALUES (5, 'e')")
            checkAnswer(
              spark.sql("SELECT * FROM T ORDER BY a"),
              Row(3, "c2", "ptv1", 22) :: Row(4, "d3", "ptv3", 22) :: Row(
                5,
                "e",
                "ptv1",
                11) :: Nil)

            // overwrite the whole table
            spark.sql(
              "INSERT OVERWRITE T VALUES (1, 'a5', 'ptv1', 11), (3, 'c5', 'ptv1', 22), (4, 'd5', 'ptv3', 22)")
            checkAnswer(
              spark.sql("SELECT * FROM T ORDER BY a"),
              Row(1, "a5", "ptv1", 11) :: Row(3, "c5", "ptv1", 22) :: Row(
                4,
                "d5",
                "ptv3",
                22) :: Nil)
          }
      }
  }

  // These cases that date/timestamp/bool is used as the partition field type are to be supported.
  Seq(IntegerType, LongType, FloatType, DoubleType, DecimalType).foreach {
    dataType =>
      test(s"insert overwrite table using $dataType as the partition field type") {
        case class PartitionSQLAndValue(sql: Any, value: Any)

        val (ptField, sv1, sv2) = dataType match {
          case IntegerType =>
            ("INT", PartitionSQLAndValue(1, 1), PartitionSQLAndValue(2, 2))
          case LongType =>
            ("LONG", PartitionSQLAndValue(1L, 1L), PartitionSQLAndValue(2L, 2L))
          case FloatType =>
            ("FLOAT", PartitionSQLAndValue(12.3f, 12.3f), PartitionSQLAndValue(45.6f, 45.6f))
          case DoubleType =>
            ("DOUBLE", PartitionSQLAndValue(12.3d, 12.3), PartitionSQLAndValue(45.6d, 45.6))
          case DecimalType =>
            (
              "DECIMAL(5, 2)",
              PartitionSQLAndValue(11.222, 11.22),
              PartitionSQLAndValue(66.777, 66.78))
        }

        spark.sql(s"""
                     |CREATE TABLE T (a INT, b STRING, pt $ptField)
                     |PARTITIONED BY (pt)
                     |""".stripMargin)

        spark.sql(s"INSERT INTO T SELECT 1, 'a', ${sv1.sql} UNION ALL SELECT 2, 'b', ${sv2.sql}")
        checkAnswer(
          spark.sql("SELECT * FROM T ORDER BY a"),
          Row(1, "a", sv1.value) :: Row(2, "b", sv2.value) :: Nil)

        // overwrite the whole table
        spark.sql(
          s"INSERT OVERWRITE T SELECT 3, 'c', ${sv1.sql} UNION ALL SELECT 4, 'd', ${sv2.sql}")
        checkAnswer(
          spark.sql("SELECT * FROM T ORDER BY a"),
          Row(3, "c", sv1.value) :: Row(4, "d", sv2.value) :: Nil)

        // overwrite the a=1 partition
        spark.sql(s"INSERT OVERWRITE T PARTITION (pt = ${sv1.value}) VALUES (5, 'e'), (7, 'g')")
        checkAnswer(
          spark.sql("SELECT * FROM T ORDER BY a"),
          Row(4, "d", sv2.value) :: Row(5, "e", sv1.value) :: Row(7, "g", sv1.value) :: Nil)
      }
  }

  withPk.foreach {
    hasPk =>
      bucketModes.foreach {
        bucket =>
          test(
            s"dynamic insert overwrite single-partitioned table: hasPk: $hasPk, bucket: $bucket") {
            val primaryKeysProp = if (hasPk) {
              "'primary-key'='a,b',"
            } else {
              ""
            }

            spark.sql(s"""
                         |CREATE TABLE T (a INT, b INT, c STRING)
                         |TBLPROPERTIES ($primaryKeysProp 'bucket'='$bucket')
                         |PARTITIONED BY (a)
                         |""".stripMargin)

            spark.sql("INSERT INTO T values (1, 1, '1'), (2, 2, '2')")
            checkAnswer(
              spark.sql("SELECT * FROM T ORDER BY a, b"),
              Row(1, 1, "1") :: Row(2, 2, "2") :: Nil)

            // overwrite the whole table
            spark.sql("INSERT OVERWRITE T VALUES (1, 3, '3'), (2, 4, '4')")
            checkAnswer(
              spark.sql("SELECT * FROM T ORDER BY a, b"),
              Row(1, 3, "3") :: Row(2, 4, "4") :: Nil)

            withSQLConf("spark.sql.sources.partitionOverwriteMode" -> "dynamic") {
              // dynamic overwrite the a=1 partition
              spark.sql("INSERT OVERWRITE T VALUES (1, 5, '5'), (1, 7, '7')")
              checkAnswer(
                spark.sql("SELECT * FROM T ORDER BY a, b"),
                Row(1, 5, "5") :: Row(1, 7, "7") :: Row(2, 4, "4") :: Nil)
            }
          }
      }
  }

  withPk.foreach {
    hasPk =>
      bucketModes.foreach {
        bucket =>
          test(
            s"dynamic insert overwrite mutil-partitioned table: hasPk: $hasPk, bucket: $bucket") {
            val primaryKeysProp = if (hasPk) {
              "'primary-key'='a,pt1,pt2',"
            } else {
              ""
            }

            spark.sql(s"""
                         |CREATE TABLE T (a INT, b STRING, pt1 STRING, pt2 INT)
                         |TBLPROPERTIES ($primaryKeysProp 'bucket'='$bucket')
                         |PARTITIONED BY (pt1, pt2)
                         |""".stripMargin)

            spark.sql(
              "INSERT INTO T values (1, 'a', 'ptv1', 11), (2, 'b', 'ptv1', 11), (3, 'c', 'ptv1', 22), (4, 'd', 'ptv2', 22)")
            checkAnswer(
              spark.sql("SELECT * FROM T ORDER BY a, b"),
              Row(1, "a", "ptv1", 11) :: Row(2, "b", "ptv1", 11) :: Row(3, "c", "ptv1", 22) :: Row(
                4,
                "d",
                "ptv2",
                22) :: Nil)

            withSQLConf("spark.sql.sources.partitionOverwriteMode" -> "dynamic") {
              // dynamic overwrite the pt2=22 partition
              spark.sql(
                "INSERT OVERWRITE T PARTITION (pt2 = 22) VALUES (3, 'c2', 'ptv1'), (4, 'd2', 'ptv3')")
              checkAnswer(
                spark.sql("SELECT * FROM T ORDER BY a, b"),
                Row(1, "a", "ptv1", 11) :: Row(2, "b", "ptv1", 11) :: Row(
                  3,
                  "c2",
                  "ptv1",
                  22) :: Row(4, "d", "ptv2", 22) :: Row(4, "d2", "ptv3", 22) :: Nil
              )

              // dynamic overwrite the pt1=ptv3 partition
              spark.sql("INSERT OVERWRITE T PARTITION (pt1 = 'ptv3') VALUES (4, 'd3', 22)")
              checkAnswer(
                spark.sql("SELECT * FROM T ORDER BY a, b"),
                Row(1, "a", "ptv1", 11) :: Row(2, "b", "ptv1", 11) :: Row(
                  3,
                  "c2",
                  "ptv1",
                  22) :: Row(4, "d", "ptv2", 22) :: Row(4, "d3", "ptv3", 22) :: Nil
              )

              // dynamic overwrite the pt1=ptv1, pt2=11 partition
              spark.sql("INSERT OVERWRITE T PARTITION (pt1 = 'ptv1', pt2=11) VALUES (5, 'e')")
              checkAnswer(
                spark.sql("SELECT * FROM T ORDER BY a, b"),
                Row(3, "c2", "ptv1", 22) :: Row(4, "d", "ptv2", 22) :: Row(
                  4,
                  "d3",
                  "ptv3",
                  22) :: Row(5, "e", "ptv1", 11) :: Nil)

              // dynamic overwrite the whole table
              spark.sql(
                "INSERT OVERWRITE T VALUES (1, 'a5', 'ptv1', 11), (3, 'c5', 'ptv1', 22), (4, 'd5', 'ptv3', 22)")
              checkAnswer(
                spark.sql("SELECT * FROM T ORDER BY a, b"),
                Row(1, "a5", "ptv1", 11) :: Row(3, "c5", "ptv1", 22) :: Row(
                  4,
                  "d",
                  "ptv2",
                  22) :: Row(4, "d5", "ptv3", 22) :: Nil)
            }
          }
      }
  }

  test(s"insert overwrite date type partition table") {
    spark.sql(s"""
                 |CREATE TABLE T (
                 |  id STRING,
                 |  dt date)
                 |PARTITIONED BY (dt)
                 |TBLPROPERTIES (
                 |  'primary-key' = 'id,dt',
                 |  'bucket' = '3'
                 |);
                 |""".stripMargin)

    spark.sql("INSERT OVERWRITE T partition (dt='2024-04-18') values(1)")
    checkAnswer(spark.sql("SELECT * FROM T"), Row("1", Date.valueOf("2024-04-18")))
  }
}
