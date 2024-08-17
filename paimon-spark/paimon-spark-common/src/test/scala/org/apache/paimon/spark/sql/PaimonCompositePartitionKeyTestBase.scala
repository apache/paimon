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
import org.apache.spark.sql.catalyst.trees.TreePattern.DYNAMIC_PRUNING_SUBQUERY
import org.junit.jupiter.api.Assertions

abstract class PaimonCompositePartitionKeyTestBase extends PaimonSparkTestBase {

  import testImplicits._

  test("PaimonCompositePartitionKeyTest") {
    withTable("source", "t") {
      Seq((1L, "x1", "2023"), (2L, "x2", "2023"), (5L, "x5", "2025"), (6L, "x6", "2026"))
        .toDF("a", "b", "pt t")
        .createOrReplaceTempView("source")

      spark.sql("""
                  |CREATE TABLE t (id INT, name STRING, `pt t` STRING) PARTITIONED BY (`pt t`)
                  |""".stripMargin)

      spark.sql(
        """
          |INSERT INTO t(`id`, `name`, `pt t`) VALUES (1, "a", "2023"), (3, "c", "2023"), (5, "e", "2025"), (7, "g", "2027")
          |""".stripMargin)

      val df1 = spark.sql("""
                            |SELECT t.id, t.name, source.b FROM source join t
                            |ON source.`pt t` = t.`pt t` AND source.`pt t` = '2023'
                            |ORDER BY t.id, source.b
                            |""".stripMargin)
      val qe1 = df1.queryExecution
      Assertions.assertFalse(qe1.analyzed.containsPattern(DYNAMIC_PRUNING_SUBQUERY))
      checkAnswer(
        df1,
        Row(1, "a", "x1") :: Row(1, "a", "x2") :: Row(3, "c", "x1") :: Row(3, "c", "x2") :: Nil)

      val df2 = spark.sql("""
                            |SELECT t.*, source.b FROM source join t
                            |ON source.a = t.id AND source.`pt t` = t.`pt t` AND source.a > 3
                            |""".stripMargin)
      val qe2 = df1.queryExecution
      Assertions.assertFalse(qe2.analyzed.containsPattern(DYNAMIC_PRUNING_SUBQUERY))
      checkAnswer(df2, Row(5, "e", "2025", "x5") :: Nil)
    }
  }
}
