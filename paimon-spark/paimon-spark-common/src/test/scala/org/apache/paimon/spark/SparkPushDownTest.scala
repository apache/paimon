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
package org.apache.paimon.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.junit.jupiter.api.Assertions

class SparkPushDownTest extends PaimonSparkTestBase {

  test(s"Paimon push down: apply partition filter push down with non-partitioned table") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING, pt STRING)
                 |TBLPROPERTIES ('primary-key'='id, pt', 'bucket'='2')
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, 'a', 'p1'), (2, 'b', 'p1'), (3, 'c', 'p2')")

    val q = "SELECT * FROM T WHERE pt = 'p1'"
    Assertions.assertTrue(checkEqualToFilterExists(q, "pt", Literal("p1")))
    checkAnswer(spark.sql(q), Row(1, "a", "p1") :: Row(2, "b", "p1") :: Nil)
  }

  test(s"Paimon push down: apply partition filter push down with partitioned table") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING, pt STRING)
                 |TBLPROPERTIES ('primary-key'='id, pt', 'bucket'='2')
                 |PARTITIONED BY (pt)
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, 'a', 'p1'), (2, 'b', 'p1'), (3, 'c', 'p2'), (4, 'd', 'p3')")

    // partition filter push down did not hit cases:
    // case 1
    var q = "SELECT * FROM T WHERE id = '1'"
    Assertions.assertTrue(checkFilterExists(q))
    checkAnswer(spark.sql(q), Row(1, "a", "p1") :: Nil)

    // case 2
    // filter "id = '1' or pt = 'p1'" can't push down completely, it still need to be evaluated after scanning
    q = "SELECT * FROM T WHERE id = '1' or pt = 'p1'"
    Assertions.assertTrue(checkEqualToFilterExists(q, "pt", Literal("p1")))
    checkAnswer(spark.sql(q), Row(1, "a", "p1") :: Row(2, "b", "p1") :: Nil)

    // partition filter push down hit cases:
    // case 1
    q = "SELECT * FROM T WHERE pt = 'p1'"
    Assertions.assertFalse(checkFilterExists(q))
    checkAnswer(spark.sql(q), Row(1, "a", "p1") :: Row(2, "b", "p1") :: Nil)

    // case 2
    q = "SELECT * FROM T WHERE id = '1' and pt = 'p1'"
    Assertions.assertFalse(checkEqualToFilterExists(q, "pt", Literal("p1")))
    checkAnswer(spark.sql(q), Row(1, "a", "p1") :: Nil)

    // case 3
    q = "SELECT * FROM T WHERE pt < 'p3'"
    Assertions.assertFalse(checkFilterExists(q))
    checkAnswer(spark.sql(q), Row(1, "a", "p1") :: Row(2, "b", "p1") :: Row(3, "c", "p2") :: Nil)
  }

  private def checkFilterExists(sql: String): Boolean = {
    spark.sql(sql).queryExecution.optimizedPlan.exists {
      case Filter(_: Expression, _) => true
      case _ => false
    }
  }

  private def checkEqualToFilterExists(sql: String, name: String, value: Literal): Boolean = {
    spark.sql(sql).queryExecution.optimizedPlan.exists {
      case Filter(c: Expression, _) =>
        c.exists {
          case EqualTo(a: AttributeReference, r: Literal) =>
            a.name.equals(name) && r.equals(value)
          case _ => false
        }
      case _ => false
    }
  }

}
