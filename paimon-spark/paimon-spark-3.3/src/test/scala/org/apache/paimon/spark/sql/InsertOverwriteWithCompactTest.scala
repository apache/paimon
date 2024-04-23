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

class InsertOverwriteWithCompactTest extends PaimonSparkTestBase {

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
                         |TBLPROPERTIES ($primaryKeysProp 'bucket'='$bucket',
                         |'num-sorted-run.compaction-trigger'='1', 'target-file-size'='1b')
                         |""".stripMargin)

            spark.sql("INSERT INTO T values (1, 1, '1'), (2, 2, '2')")
            checkAnswer(
              spark.sql("SELECT * FROM T ORDER BY a, b"),
              Row(1, 1, "1") :: Row(2, 2, "2") :: Nil)

            spark.sql("INSERT INTO T values (3, 3, '1'), (4, 4, '2')")
            checkAnswer(
              spark.sql("SELECT * FROM T ORDER BY a, b"),
              Row(1, 1, "1") :: Row(2, 2, "2") :: Row(3, 3, "1") :: Row(4, 4, "2") :: Nil)

            // make sure at least two files in the bucket after insert overwrite, so that compaction will be invoked
            spark.sql(generateInsertStatement)
            checkAnswer(spark.sql("SELECT * FROM T ORDER BY a, b"), generateInsertValues)
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
                         |TBLPROPERTIES ($primaryKeysProp 'bucket'='$bucket',
                         |'num-sorted-run.compaction-trigger'='1', 'target-file-size'='1b')
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

            // make sure at least two files in the bucket after insert overwrite, so that compaction will be invoked
            spark.sql(generateInsertStatement)
            checkAnswer(spark.sql("SELECT * FROM T ORDER BY a, b"), generateInsertValues)

          }
      }
  }

  def generateInsertStatement(): String = {
    val records = (1 to 3100).map(i => s"(1, $i, '1')")
    s"INSERT OVERWRITE T VALUES ${records.mkString(",")}"
  }

  def generateInsertValues(): Seq[Row] = {
    (1 to 3100).map(i => Row(1, i, "1"))
  }
}
