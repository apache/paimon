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

import org.apache.paimon.spark.{PaimonSparkTestBase, PaimonTableTest}

import org.apache.spark.sql.Row

trait MergeIntoNotMatchedBySourceTest extends PaimonSparkTestBase with PaimonTableTest {

  import testImplicits._

  test(s"Paimon MergeInto: only not matched by source") {
    withTable("source", "target") {

      Seq((1, 100, "c11"), (3, 300, "c33")).toDF("a", "b", "c").createOrReplaceTempView("source")

      createTable("target", "a INT, b INT, c STRING", Seq("a"))
      spark.sql("INSERT INTO target values (1, 10, 'c1'), (2, 20, 'c2'), (5, 50, 'c5')")

      spark.sql(s"""
                   |MERGE INTO target
                   |USING source
                   |ON target.a = source.a
                   |WHEN NOT MATCHED BY SOURCE AND a % 2 = 0 THEN
                   |UPDATE SET b = b * 10
                   |WHEN NOT MATCHED BY SOURCE THEN
                   |DELETE
                   |""".stripMargin)

      checkAnswer(
        spark.sql("SELECT * FROM target ORDER BY a, b"),
        Row(1, 10, "c1") :: Row(2, 200, "c2") :: Nil)
    }
  }

  test(s"Paimon MergeInto: star with not matched by source") {
    withTable("source", "target") {

      Seq((1, 100, "c11"), (3, 300, "c33")).toDF("a", "b", "c").createOrReplaceTempView("source")

      createTable("target", "a INT, b INT, c STRING", Seq("a"))
      spark.sql("INSERT INTO target values (1, 10, 'c1'), (2, 20, 'c2')")

      spark.sql(s"""
                   |MERGE INTO target
                   |USING source
                   |ON target.a = source.a
                   |WHEN MATCHED THEN
                   |UPDATE SET *
                   |WHEN NOT MATCHED THEN
                   |INSERT *
                   |WHEN NOT MATCHED BY SOURCE THEN
                   |DELETE
                   |""".stripMargin)

      checkAnswer(
        spark.sql("SELECT * FROM target ORDER BY a, b"),
        Row(1, 100, "c11") :: Row(3, 300, "c33") :: Nil)
    }
  }

  test("Paimon MergeInto: update nested column with not matched by source") {
    withTable("source", "target") {

      Seq((1, 100, "x1", "y1"), (3, 300, "x3", "y3"))
        .toDF("a", "b", "c1", "c2")
        .createOrReplaceTempView("source")

      createTable("target", "a INT, b INT, c STRUCT<c1:STRING, c2:STRING>", Seq("a"))
      spark.sql("INSERT INTO target values (1, 10, struct('x', 'y')), (2, 20, struct('x', 'y'))")

      spark.sql(s"""
                   |MERGE INTO target
                   |USING source
                   |ON target.a = source.a
                   |WHEN MATCHED THEN
                   |UPDATE SET c.c1 = source.c1
                   |WHEN NOT MATCHED BY SOURCE THEN
                   |UPDATE set c.c2 = "y2"
                   |""".stripMargin)

      checkAnswer(
        spark.sql("SELECT * FROM target ORDER BY a"),
        Row(1, 10, Row("x1", "y")) :: Row(2, 20, Row("x", "y2")) :: Nil)
    }
  }

  test(s"Paimon MergeInto: multiple clauses with not matched by source") {
    withTable("source", "target") {

      Seq((1, 100, "c11"), (3, 300, "c33"), (5, 500, "c55"), (7, 700, "c77"), (9, 900, "c99"))
        .toDF("a", "b", "c")
        .createOrReplaceTempView("source")

      createTable("target", "a INT, b INT, c STRING", Seq("a"))
      spark.sql(
        "INSERT INTO target values (1, 10, 'c1'), (2, 20, 'c2'), (3, 30, 'c3'), (4, 40, 'c4'), (5, 50, 'c5')")

      spark.sql(s"""
                   |MERGE INTO target
                   |USING source
                   |ON target.a = source.a
                   |WHEN MATCHED AND target.a = 5 THEN
                   |UPDATE SET b = source.b + target.b
                   |WHEN MATCHED AND source.c > 'c2' THEN
                   |UPDATE SET *
                   |WHEN MATCHED THEN
                   |DELETE
                   |WHEN NOT MATCHED AND c > 'c9' THEN
                   |INSERT (a, b, c) VALUES (a, b * 1.1, c)
                   |WHEN NOT MATCHED THEN
                   |INSERT *
                   |WHEN NOT MATCHED BY SOURCE AND a = 2 THEN
                   |UPDATE SET b = b * 10
                   |WHEN NOT MATCHED BY SOURCE THEN
                   |DELETE
                   |""".stripMargin)

      checkAnswer(
        spark.sql("SELECT * FROM target ORDER BY a, b"),
        Row(2, 200, "c2") :: Row(3, 300, "c33") :: Row(5, 550, "c5") :: Row(7, 700, "c77") :: Row(
          9,
          990,
          "c99") :: Nil
      )
    }
  }

  test(s"Paimon MergeInto: multiple clauses with not matched by source with alias") {
    withTable("source", "target") {

      Seq((1, 100, "c11"), (3, 300, "c33"), (5, 500, "c55"), (7, 700, "c77"), (9, 900, "c99"))
        .toDF("a", "b", "c")
        .createOrReplaceTempView("source")

      createTable("target", "a INT, b INT, c STRING", Seq("a"))
      spark.sql(
        "INSERT INTO target values (1, 10, 'c1'), (2, 20, 'c2'), (3, 30, 'c3'), (4, 40, 'c4'), (5, 50, 'c5')")

      spark.sql(s"""
                   |MERGE INTO target t
                   |USING source s
                   |ON t.a = s.a
                   |WHEN MATCHED AND t.a = 5 THEN
                   |UPDATE SET t.b = s.b + t.b
                   |WHEN MATCHED AND s.c > 'c2' THEN
                   |UPDATE SET *
                   |WHEN MATCHED THEN
                   |DELETE
                   |WHEN NOT MATCHED AND s.c > 'c9' THEN
                   |INSERT (t.a, t.b, t.c) VALUES (s.a, s.b * 1.1, s.c)
                   |WHEN NOT MATCHED THEN
                   |INSERT *
                   |WHEN NOT MATCHED BY SOURCE AND t.a = 2 THEN
                   |UPDATE SET t.b = t.b * 10
                   |WHEN NOT MATCHED BY SOURCE THEN
                   |DELETE
                   |""".stripMargin)

      checkAnswer(
        spark.sql("SELECT * FROM target ORDER BY a, b"),
        Row(2, 200, "c2") :: Row(3, 300, "c33") :: Row(5, 550, "c5") :: Row(7, 700, "c77") :: Row(
          9,
          990,
          "c99") :: Nil
      )
    }
  }
}
