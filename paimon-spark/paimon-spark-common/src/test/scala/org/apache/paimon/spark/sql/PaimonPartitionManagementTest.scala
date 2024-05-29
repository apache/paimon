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

import org.apache.spark.sql.{AnalysisException, Row}

class PaimonPartitionManagementTest extends PaimonSparkTestBase {

  withPk.foreach {
    hasPk =>
      bucketModes.foreach {
        bucket =>
          test(s"Partition for non-partitioned table: hasPk: $hasPk, bucket: $bucket") {
            val prop = if (hasPk) {
              s"'primary-key'='a,b', 'bucket' = '$bucket' "
            } else if (bucket != -1) {
              s"'bucket-key'='a,b', 'bucket' = '$bucket' "
            } else {
              "'write-only'='true'"
            }

            spark.sql(
              s"""
                 |CREATE TABLE T (a VARCHAR(10), b CHAR(10),c BIGINT,dt VARCHAR(8),hh VARCHAR(4))
                 |TBLPROPERTIES ($prop)
                 |""".stripMargin)
            spark.sql("INSERT INTO T VALUES('a','b',1,'20230816','1132')")
            spark.sql("INSERT INTO T VALUES('a','b',1,'20230816','1133')")
            spark.sql("INSERT INTO T VALUES('a','b',1,'20230816','1134')")
            spark.sql("INSERT INTO T VALUES('a','b',2,'20230817','1132')")
            spark.sql("INSERT INTO T VALUES('a','b',2,'20230817','1133')")
            spark.sql("INSERT INTO T VALUES('a','b',2,'20230817','1134')")

            assertThrows[AnalysisException] {
              spark.sql("alter table T drop partition (dt='20230816', hh='1134')")
            }

            assertThrows[AnalysisException] {
              spark.sql("alter table T drop partition (dt='20230816')")
            }

            assertThrows[AnalysisException] {
              spark.sql("alter table T drop partition (hh='1134')")
            }

            assertThrows[AnalysisException] {
              spark.sql("show partitions T partition (dt='20230816', hh='1134')")
            }

            assertThrows[AnalysisException] {
              spark.sql("show partitions T partition (dt='20230816')")
            }

            assertThrows[AnalysisException] {
              spark.sql("show partitions T partition (hh='1134')")
            }
          }
      }
  }

  withPk.foreach {
    hasPk =>
      bucketModes.foreach {
        bucket =>
          test(s"Partition for partitioned table: hasPk: $hasPk, bucket: $bucket") {
            val prop = if (hasPk) {
              s"'primary-key'='a,b,dt,hh', 'bucket' = '$bucket' "
            } else if (bucket != -1) {
              s"'bucket-key'='a,b', 'bucket' = '$bucket' "
            } else {
              "'write-only'='true'"
            }

            spark.sql(s"""
                         |CREATE TABLE T (a VARCHAR(10), b CHAR(10),c BIGINT,dt LONG,hh VARCHAR(4))
                         |PARTITIONED BY (dt, hh)
                         |TBLPROPERTIES ($prop)
                         |""".stripMargin)

            spark.sql("INSERT INTO T VALUES('a','b',1,20230816,'1132')")
            spark.sql("INSERT INTO T VALUES('a','b',1,20230816,'1133')")
            spark.sql("INSERT INTO T VALUES('a','b',1,20230816,'1134')")
            spark.sql("INSERT INTO T VALUES('a','b',2,20230817,'1132')")
            spark.sql("INSERT INTO T VALUES('a','b',2,20230817,'1133')")
            spark.sql("INSERT INTO T VALUES('a','b',2,20230817,'1134')")

            checkAnswer(
              spark.sql("show partitions T "),
              Row("dt=20230816/hh=1132") :: Row("dt=20230816/hh=1133")
                :: Row("dt=20230816/hh=1134") :: Row("dt=20230817/hh=1132") :: Row(
                  "dt=20230817/hh=1133") :: Row("dt=20230817/hh=1134") :: Nil
            )

            checkAnswer(
              spark.sql("show partitions T PARTITION (dt=20230817, hh='1132')"),
              Row("dt=20230817/hh=1132") :: Nil)

            checkAnswer(
              spark.sql("show partitions T PARTITION (hh='1132')"),
              Row("dt=20230816/hh=1132") :: Row("dt=20230817/hh=1132") :: Nil)

            checkAnswer(
              spark.sql("show partitions T PARTITION (dt=20230816)"),
              Row("dt=20230816/hh=1132") :: Row("dt=20230816/hh=1133") :: Row(
                "dt=20230816/hh=1134") :: Nil)

            checkAnswer(spark.sql("show partitions T PARTITION (hh='1135')"), Nil)

            checkAnswer(spark.sql("show partitions T PARTITION (dt=20230818)"), Nil)

            spark.sql("alter table T drop partition (dt=20230816, hh='1134')")

            spark.sql("alter table T drop partition (dt=20230817, hh='1133')")

            assertThrows[AnalysisException] {
              spark.sql("alter table T drop partition (dt=20230816)")
            }

            assertThrows[AnalysisException] {
              spark.sql("alter table T drop partition (hh='1134')")
            }

            checkAnswer(
              spark.sql("show partitions T "),
              Row("dt=20230816/hh=1132") :: Row("dt=20230816/hh=1133")
                :: Row("dt=20230817/hh=1132") :: Row("dt=20230817/hh=1134") :: Nil)

            checkAnswer(
              spark.sql("show partitions T PARTITION (dt=20230817, hh='1132')"),
              Row("dt=20230817/hh=1132") :: Nil)

            checkAnswer(
              spark.sql("show partitions T PARTITION (hh='1132')"),
              Row("dt=20230817/hh=1132") :: Row("dt=20230816/hh=1132") :: Nil)

            checkAnswer(
              spark.sql("show partitions T PARTITION (hh='1134')"),
              Row("dt=20230817/hh=1134") :: Nil)

            checkAnswer(
              spark.sql("show partitions T PARTITION (dt=20230817)"),
              Row("dt=20230817/hh=1132") :: Row("dt=20230817/hh=1134") :: Nil)

            checkAnswer(
              spark.sql("select * from T"),
              Row("a", "b         ", 1L, 20230816L, "1132") :: Row(
                "a",
                "b         ",
                1L,
                20230816L,
                "1133") :: Row("a", "b         ", 2L, 20230817L, "1132") :: Row(
                "a",
                "b         ",
                2L,
                20230817L,
                "1134") :: Nil
            )
          }
      }
  }
}
