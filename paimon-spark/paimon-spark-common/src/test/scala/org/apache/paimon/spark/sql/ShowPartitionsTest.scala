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

class ShowPartitionsTest extends PaimonSparkTestBase {

  import testImplicits._

  test(s"Paimon show partitions: non-partitioned") {
    withTable("source", "target") {

      Seq((1, 100, "c11"), (3, 300, "c33")).toDF("a", "b", "c").createOrReplaceTempView("source")

      spark.sql(s"""
                   |CREATE TABLE target (a INT, b INT, c LONG, d STRING)
                   |TBLPROPERTIES ('primary-key'='a,c,d')
                   |""".stripMargin)
      spark.sql(
        "INSERT INTO target values (1, 11, 111, 'a'), (2, 22, 222, 'b'), (3, 33, 333, 'b'), (4, 44, 444, 'a')")

      checkAnswer(spark.sql("SHOW PARTITIONS target"), Row("") :: Nil)
    }
  }

  test(s"Paimon show partitions: partitioned") {
    withTable("source", "target") {

      Seq((1, 100, "c11"), (3, 300, "c33")).toDF("a", "b", "c").createOrReplaceTempView("source")

      spark.sql(s"""
                   |CREATE TABLE target (a INT, b INT, c LONG, d STRING)
                   |TBLPROPERTIES ('primary-key'='a,c,d')
                   |PARTITIONED BY (c,d)
                   |""".stripMargin)
      spark.sql(
        "INSERT INTO target values (1, 11, 111, 'a'), (2, 22, 222, 'b'), (3, 33, 333, 'b'), (4, 44, 444, 'a')")

      checkAnswer(
        spark.sql("SHOW PARTITIONS target"),
        Row("c=111/d=a") :: Row("c=222/d=b") :: Row("c=333/d=b") :: Row("c=444/d=a") :: Nil)

      checkAnswer(
        spark.sql("SHOW PARTITIONS target partition(d = 'a')"),
        Row("c=111/d=a") :: Row("c=444/d=a") :: Nil)

      checkAnswer(
        spark.sql("SHOW PARTITIONS target partition(c= '111', d = 'a')"),
        Row("c=111/d=a") :: Nil)
    }
  }
}
