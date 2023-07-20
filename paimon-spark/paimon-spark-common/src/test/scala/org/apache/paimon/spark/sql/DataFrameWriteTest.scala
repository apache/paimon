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

import org.apache.paimon.WriteMode._

import org.apache.spark.sql.Row
import org.scalactic.source.Position

class DataFrameWriteTest extends PaimonSparkTestBase {

  writeModes.foreach {
    writeMode =>
      bucketModes.foreach {
        bucket =>
          test(s"Write data into Paimon directly: write-mode: $writeMode, bucket: $bucket") {

            val _spark = spark
            import _spark.implicits._

            val primaryKeysProp = if (writeMode == CHANGE_LOG) {
              "'primary-key'='a',"
            } else {
              ""
            }

            spark.sql(
              s"""
                 |CREATE TABLE T (a INT, b STRING)
                 |TBLPROPERTIES ($primaryKeysProp 'write-mode'='${writeMode.toString}', 'bucket'='$bucket')
                 |""".stripMargin)

            val paimonTable = loadTable("T")
            val location = paimonTable.location().getPath

            val df1 = Seq((1, "a"), (2, "b")).toDF("a", "b")
            df1.write.format("paimon").mode("append").save(location)
            checkAnswer(
              spark.sql("SELECT * FROM T ORDER BY a, b"),
              Row(1, "a") :: Row(2, "b") :: Nil)

            val df2 = Seq((1, "a2"), (3, "c")).toDF("a", "b")
            df2.write.format("paimon").mode("append").save(location)
            val expected = if (writeMode == CHANGE_LOG) {
              Row(1, "a2") :: Row(2, "b") :: Row(3, "c") :: Nil
            } else {
              Row(1, "a") :: Row(1, "a2") :: Row(2, "b") :: Row(3, "c") :: Nil
            }
            checkAnswer(spark.sql("SELECT * FROM T ORDER BY a, b"), expected)

            val df3 = Seq((4, "d"), (5, "e")).toDF("a", "b")
            df3.write.format("paimon").mode("overwrite").save(location)
            checkAnswer(
              spark.sql("SELECT * FROM T ORDER BY a, b"),
              Row(4, "d") :: Row(5, "e") :: Nil)
          }
      }
  }
}
