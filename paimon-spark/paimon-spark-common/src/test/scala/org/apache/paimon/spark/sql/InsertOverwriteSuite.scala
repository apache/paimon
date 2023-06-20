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

import org.apache.paimon.WriteMode.{APPEND_ONLY, CHANGE_LOG}

import org.apache.spark.sql.Row
import org.scalactic.source.Position

class InsertOverwriteSuite extends PaimonSparkTestBase {

  // 3: fixed bucket, -1: dynamic bucket
  private val bucketModes = Seq(3, -1)

  Seq(APPEND_ONLY, CHANGE_LOG).foreach {
    writeMode =>
      bucketModes.foreach {
        bucket =>
          test(s"insert overwrite non-partitioned table: write-mode: $writeMode, bucket: $bucket") {
            val primaryKeysProp = if (writeMode == CHANGE_LOG) {
              "'primary-key'='a,b',"
            } else {
              ""
            }

            spark.sql(
              s"""
                 |CREATE TABLE T (a INT, b INT, c STRING)
                 |TBLPROPERTIES ($primaryKeysProp 'write-mode'='${writeMode.toString}', 'bucket'='$bucket')
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

  Seq(APPEND_ONLY, CHANGE_LOG).foreach {
    writeMode =>
      bucketModes.foreach {
        bucket =>
          test(
            s"insert overwrite single-partitioned table: write-mode: $writeMode, bucket: $bucket") {
            val primaryKeysProp = if (writeMode == CHANGE_LOG) {
              "'primary-key'='a,b',"
            } else {
              ""
            }

            spark.sql(
              s"""
                 |CREATE TABLE T (a INT, b INT, c STRING)
                 |TBLPROPERTIES ($primaryKeysProp 'write-mode'='${writeMode.toString}', 'bucket'='$bucket')
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

  Seq(APPEND_ONLY, CHANGE_LOG).foreach {
    writeMode =>
      bucketModes.foreach {
        bucket =>
          test(
            s"insert overwrite mutil-partitioned table: write-mode: $writeMode, bucket: $bucket") {
            val primaryKeysProp = if (writeMode == CHANGE_LOG) {
              "'primary-key'='a,pt1,pt2',"
            } else {
              ""
            }

            spark.sql(
              s"""
                 |CREATE TABLE T (a INT, b STRING, pt1 STRING, pt2 INT)
                 |TBLPROPERTIES ($primaryKeysProp 'write-mode'='${writeMode.toString}', 'bucket'='$bucket')
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
}
