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

import org.apache.paimon.Snapshot
import org.apache.paimon.spark.{PaimonAppendTable, PaimonPrimaryKeyTable, PaimonSparkTestBase, PaimonTableTest}

import org.apache.spark.sql.Row

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt

abstract class MergeIntoTableTestBase extends PaimonSparkTestBase with PaimonTableTest {

  import testImplicits._

  test(s"Paimon MergeInto: only update") {
    withTable("source", "target") {

      Seq((1, 100, "c11"), (3, 300, "c33")).toDF("a", "b", "c").createOrReplaceTempView("source")

      createTable("target", "a INT, b INT, c STRING", Seq("a"))
      spark.sql("INSERT INTO target values (1, 10, 'c1'), (2, 20, 'c2')")

      spark.sql(s"""
                   |MERGE INTO target
                   |USING source
                   |ON target.a = source.a
                   |WHEN MATCHED THEN
                   |UPDATE SET a = source.a, b = source.b, c = source.c
                   |""".stripMargin)

      checkAnswer(
        spark.sql("SELECT * FROM target ORDER BY a, b"),
        Row(1, 100, "c11") :: Row(2, 20, "c2") :: Nil)
    }
  }

  test(s"Paimon MergeInto: two paimon table") {
    withTable("source", "target") {
      createTable("target", "a INT, b INT, c STRING", Seq("a"))
      createTable("source", "a INT, b INT, c STRING", Seq("a"))

      spark.sql("INSERT INTO source values (1, 100, 'c11'), (3, 300, 'c33')")
      spark.sql("INSERT INTO target values (1, 10, 'c1'), (2, 20, 'c2')")

      spark.sql(s"""
                   |MERGE INTO target
                   |USING source
                   |ON target.a = source.a
                   |WHEN MATCHED THEN
                   |UPDATE SET a = source.a, b = source.b, c = source.c
                   |""".stripMargin)

      checkAnswer(
        spark.sql("SELECT * FROM target ORDER BY a, b"),
        Row(1, 100, "c11") :: Row(2, 20, "c2") :: Nil)
    }
  }

  test(s"Paimon MergeInto: only delete") {
    withTable("source", "target") {

      Seq((1, 100, "c11"), (3, 300, "c33")).toDF("a", "b", "c").createOrReplaceTempView("source")

      createTable("target", "a INT, b INT, c STRING", Seq("a"))
      spark.sql("INSERT INTO target values (1, 10, 'c1'), (2, 20, 'c2')")

      spark.sql(s"""
                   |MERGE INTO target
                   |USING source
                   |ON target.a = source.a
                   |WHEN MATCHED THEN DELETE
                   |""".stripMargin)

      checkAnswer(spark.sql("SELECT * FROM target ORDER BY a, b"), Row(2, 20, "c2") :: Nil)
    }
  }

  test(s"Paimon MergeInto: only insert") {
    withTable("source", "target") {

      Seq((1, 100, "c11"), (3, 300, "c33")).toDF("a", "b", "c").createOrReplaceTempView("source")

      createTable("target", "a INT, b INT, c STRING", Seq("a"))
      spark.sql("INSERT INTO target values (1, 10, 'c1'), (2, 20, 'c2')")

      spark.sql(s"""
                   |MERGE INTO target
                   |USING source
                   |ON target.a = source.a
                   |WHEN NOT MATCHED
                   |THEN INSERT (a, b, c) values (a, b, c)
                   |""".stripMargin)

      checkAnswer(
        spark.sql("SELECT * FROM target ORDER BY a, b"),
        Row(1, 10, "c1") :: Row(2, 20, "c2") :: Row(3, 300, "c33") :: Nil)
    }
  }

  test(s"Paimon MergeInto: update + insert") {
    withTable("source", "target") {

      Seq((1, 100, "c11"), (3, 300, "c33")).toDF("a", "b", "c").createOrReplaceTempView("source")

      createTable("target", "a INT, b INT, c STRING", Seq("a"))
      spark.sql("INSERT INTO target values (1, 10, 'c1'), (2, 20, 'c2')")

      spark.sql(s"""
                   |MERGE INTO target
                   |USING source
                   |ON target.a = source.a
                   |WHEN MATCHED THEN
                   |UPDATE SET a = source.a, b = source.b, c = source.c
                   |WHEN NOT MATCHED
                   |THEN INSERT (a, b, c) values (a, b, c)
                   |""".stripMargin)

      checkAnswer(
        spark.sql("SELECT * FROM target ORDER BY a, b"),
        Row(1, 100, "c11") :: Row(2, 20, "c2") :: Row(3, 300, "c33") :: Nil)
    }
  }

  test(s"Paimon MergeInto: partial insert with null") {
    withTable("source", "target") {

      Seq((1, 100, "c11"), (3, 300, "c33")).toDF("a", "b", "c").createOrReplaceTempView("source")

      createTable("target", "a INT, b INT, c STRING", Seq("a"))
      spark.sql("INSERT INTO target values (1, 10, 'c1'), (2, 20, 'c2')")

      spark.sql(s"""
                   |MERGE INTO target
                   |USING source
                   |ON target.a = source.a
                   |WHEN NOT MATCHED
                   |THEN INSERT (a) values (a)
                   |""".stripMargin)

      checkAnswer(
        spark.sql("SELECT * FROM target ORDER BY a, b"),
        Row(1, 10, "c1") :: Row(2, 20, "c2") :: Row(3, null, null) :: Nil)
    }
  }

  test(s"Paimon MergeInto: partial insert with default value") {
    // The syntax for specifying column default values when creating Paimon tables is only supported in Spark 3.4 and above
    if (gteqSpark3_4) {
      withTable("source", "target") {

        Seq((1, 100, "c11"), (3, 300, "c33")).toDF("a", "b", "c").createOrReplaceTempView("source")

        createTable("target", "a INT, b INT DEFAULT 6, c STRING DEFAULT 'test'", Seq("a"))
        spark.sql("INSERT INTO target values (1, 10, 'c1'), (2, 20, 'c2')")

        spark.sql(s"""
                     |MERGE INTO target
                     |USING source
                     |ON target.a = source.a
                     |WHEN NOT MATCHED
                     |THEN INSERT (a) values (a)
                     |""".stripMargin)

        checkAnswer(
          spark.sql("SELECT * FROM target ORDER BY a, b"),
          Row(1, 10, "c1") :: Row(2, 20, "c2") :: Row(3, 6, "test") :: Nil)
      }
    }
  }

  test(s"Paimon MergeInto: update + insert with data file path") {
    withTable("source", "target") {

      Seq((1, 100, "c11"), (3, 300, "c33")).toDF("a", "b", "c").createOrReplaceTempView("source")

      createTable(
        "target",
        "a INT, b INT, c STRING",
        Seq("a"),
        Seq(),
        Map("data-file.path-directory" -> "data"))
      spark.sql("INSERT INTO target values (1, 10, 'c1'), (2, 20, 'c2')")

      spark.sql(s"""
                   |MERGE INTO target
                   |USING source
                   |ON target.a = source.a
                   |WHEN MATCHED THEN
                   |UPDATE SET a = source.a, b = source.b, c = source.c
                   |WHEN NOT MATCHED
                   |THEN INSERT (a, b, c) values (a, b, c)
                   |""".stripMargin)

      checkAnswer(
        spark.sql("SELECT * FROM target ORDER BY a, b"),
        Seq(Row(1, 100, "c11"), Row(2, 20, "c2"), Row(3, 300, "c33")))
    }
  }

  test(s"Paimon MergeInto: delete + insert") {
    withTable("source", "target") {

      Seq((1, 100, "c11"), (3, 300, "c33")).toDF("a", "b", "c").createOrReplaceTempView("source")

      createTable("target", "a INT, b INT, c STRING", Seq("a"))
      spark.sql("INSERT INTO target values (1, 10, 'c1'), (2, 20, 'c2')")

      spark.sql(s"""
                   |MERGE INTO target
                   |USING source
                   |ON target.a = source.a
                   |WHEN MATCHED THEN
                   |DELETE
                   |WHEN NOT MATCHED
                   |THEN INSERT (a, b, c) values (a, b, c)
                   |""".stripMargin)

      checkAnswer(
        spark.sql("SELECT * FROM target ORDER BY a, b"),
        Row(2, 20, "c2") :: Row(3, 300, "c33") :: Nil)
    }
  }

  test(s"Paimon MergeInto: conditional update") {
    withTable("source", "target") {

      Seq((1, 100, "c11"), (3, 300, "c33")).toDF("a", "b", "c").createOrReplaceTempView("source")

      createTable("target", "a INT, b INT, c STRING", Seq("a"))
      spark.sql("INSERT INTO target values (1, 10, 'c1'), (2, 20, 'c2'), (3, 30, 'c3')")

      spark.sql(s"""
                   |MERGE INTO target
                   |USING source
                   |ON target.a = source.a
                   |WHEN MATCHED AND source.b > 200 THEN
                   |UPDATE SET b = source.b, c = source.c
                   |WHEN MATCHED THEN
                   |DELETE
                   |WHEN NOT MATCHED
                   |THEN INSERT (a, b, c) values (a, b, c)
                   |""".stripMargin)

      checkAnswer(
        spark.sql("SELECT * FROM target ORDER BY a, b"),
        Row(2, 20, "c2") :: Row(3, 300, "c33") :: Nil)
    }
  }

  test(s"Paimon MergeInto: conditional insert") {
    withTable("source", "target") {

      Seq((1, 100, "c11"), (3, 300, "c33")).toDF("a", "b", "c").createOrReplaceTempView("source")

      createTable("target", "a INT, b INT, c STRING", Seq("a"))
      spark.sql("INSERT INTO target values (1, 10, 'c1'), (2, 20, 'c2')")

      spark.sql(s"""
                   |MERGE INTO target
                   |USING source
                   |ON target.a = source.a
                   |WHEN MATCHED THEN
                   |UPDATE SET b = source.b, c = source.c
                   |WHEN NOT MATCHED AND b < 300 THEN
                   |INSERT (a, b, c) values (a, b, c)
                   |""".stripMargin)

      checkAnswer(
        spark.sql("SELECT * FROM target ORDER BY a, b"),
        Row(1, 100, "c11") :: Row(2, 20, "c2") :: Nil)
    }
  }

  test(s"Paimon MergeInto: conditional delete") {
    withTable("source", "target") {

      Seq((1, 100, "c11"), (3, 300, "c33")).toDF("a", "b", "c").createOrReplaceTempView("source")

      createTable("target", "a INT, b INT, c STRING", Seq("a"))
      spark.sql("INSERT INTO target values (1, 10, 'c1'), (2, 20, 'c2')")

      spark.sql(s"""
                   |MERGE INTO target
                   |USING source
                   |ON target.a = source.a
                   |WHEN MATCHED AND target.c < 'c1' THEN
                   |DELETE
                   |WHEN NOT MATCHED THEN
                   |INSERT (a, b, c) values (a, b, c)
                   |""".stripMargin)

      checkAnswer(
        spark.sql("SELECT * FROM target ORDER BY a, b"),
        Row(1, 10, "c1") :: Row(2, 20, "c2") :: Row(3, 300, "c33") :: Nil)
    }
  }

  test(s"Paimon MergeInto: star") {
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
                   |""".stripMargin)

      checkAnswer(
        spark.sql("SELECT * FROM target ORDER BY a, b"),
        Row(1, 100, "c11") :: Row(2, 20, "c2") :: Row(3, 300, "c33") :: Nil)
    }
  }

  test(s"Paimon MergeInto: multiple clauses") {
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
                   |""".stripMargin)

      checkAnswer(
        spark.sql("SELECT * FROM target ORDER BY a, b"),
        Row(2, 20, "c2") :: Row(3, 300, "c33") :: Row(4, 40, "c4") :: Row(5, 550, "c5") :: Row(
          7,
          700,
          "c77") :: Row(9, 990, "c99") :: Nil
      )
    }
  }

  test(s"Paimon MergeInto: source and target are empty") {
    withTable("source", "target") {

      Seq.empty[(Int, Int, String)].toDF("a", "b", "c").createOrReplaceTempView("source")

      createTable("target", "a INT, b INT, c STRING", Seq("a"))
      spark.sql(s"""
                   |MERGE INTO target
                   |USING source
                   |ON target.a = source.a
                   |WHEN MATCHED THEN
                   |UPDATE SET a = source.a, b = source.b, c = source.c
                   |WHEN NOT MATCHED
                   |THEN INSERT *
                   |""".stripMargin)

      checkAnswer(spark.sql("SELECT * FROM target ORDER BY a, b"), Nil)
    }
  }

  test(s"Paimon MergeInto: update value from both source and target table") {
    withTable("source", "target") {

      Seq((1, 100, "c11"), (3, 300, "c33")).toDF("a", "b", "c").createOrReplaceTempView("source")

      createTable("target", "a INT, b INT, c STRING", Seq("a"))
      spark.sql("INSERT INTO target values (1, 10, 'c1'), (2, 20, 'c2')")

      spark.sql(s"""
                   |MERGE INTO target
                   |USING source
                   |ON target.a = source.a
                   |WHEN MATCHED THEN
                   |UPDATE SET b = target.b * 11, c = source.c
                   |WHEN NOT MATCHED
                   |THEN INSERT (a, b, c) values (source.a, source.b * 2, source.c)
                   |""".stripMargin)

      checkAnswer(
        spark.sql("SELECT * FROM target ORDER BY a, b"),
        Row(1, 110, "c11") :: Row(2, 20, "c2") :: Row(3, 600, "c33") :: Nil)
    }
  }

  test(s"Paimon MergeInto: insert/update columns in wrong order") {
    withTable("source", "target") {

      Seq((1, 100, "c11"), (3, 300, "c33")).toDF("a", "b", "c").createOrReplaceTempView("source")

      createTable("target", "a INT, b INT, c STRING", Seq("a"))
      spark.sql("INSERT INTO target values (1, 10, 'c1'), (2, 20, 'c2')")

      spark.sql(s"""
                   |MERGE INTO target
                   |USING source
                   |ON target.a = source.a
                   |WHEN MATCHED THEN
                   |UPDATE SET c = source.c, b = source.b
                   |WHEN NOT MATCHED
                   |THEN INSERT (b, c, a) values (b, c, a)
                   |""".stripMargin)

      checkAnswer(
        spark.sql("SELECT * FROM target ORDER BY a, b"),
        Row(1, 100, "c11") :: Row(2, 20, "c2") :: Row(3, 300, "c33") :: Nil)
    }
  }

  test(s"Paimon MergeInto: miss some columns in update") {
    withTable("source", "target") {

      Seq((1, 100, "c11"), (3, 300, "c33")).toDF("a", "b", "c").createOrReplaceTempView("source")

      createTable("target", "a INT, b INT, c STRING", Seq("a"))
      spark.sql("INSERT INTO target values (1, 10, 'c1'), (2, 20, 'c2')")

      spark.sql(s"""
                   |MERGE INTO target
                   |USING source
                   |ON target.a = source.a
                   |WHEN MATCHED THEN
                   |UPDATE SET c = source.c
                   |WHEN NOT MATCHED
                   |THEN INSERT *
                   |""".stripMargin)

      checkAnswer(
        spark.sql("SELECT * FROM target ORDER BY a, b"),
        Row(1, 10, "c11") :: Row(2, 20, "c2") :: Row(3, 300, "c33") :: Nil)
    }
  }

  test("Paimon MergeInto: fail in case that miss some columns in insert") {
    withTable("source", "target") {

      Seq((1, "c11"), (3, "c33")).toDF("a", "c").createOrReplaceTempView("source")

      createTable("target", "a INT, b INT, c STRING", Seq("a"))
      spark.sql("INSERT INTO target values (1, 10, 'c1'), (2, 20, 'c2')")

      val error = intercept[RuntimeException] {
        spark.sql(s"""
                     |MERGE INTO target
                     |USING source
                     |ON target.a = source.a
                     |WHEN MATCHED THEN
                     |UPDATE SET *
                     |WHEN NOT MATCHED
                     |THEN INSERT *
                     |""".stripMargin)
      }.getMessage
      assert(error.contains("cannot resolve b from Project"))
    }
  }

  test("Paimon MergeInto: source is a query") {
    withTable("source", "target") {

      Seq((1, 100, "c11"), (3, 300, "c33"), (4, 400, "c44"))
        .toDF("a", "b", "c")
        .createOrReplaceTempView("source")

      createTable("target", "a INT, b INT, c STRING", Seq("a"))
      spark.sql("INSERT INTO target values (1, 10, 'c1'), (2, 20, 'c2')")

      spark.sql(s"""
                   |MERGE INTO target
                   |USING (SELECT a, b, c FROM source WHERE a % 2 = 1) AS src
                   |ON target.a = src.a
                   |WHEN MATCHED THEN
                   |UPDATE SET b = src.b, c = src.c
                   |WHEN NOT MATCHED
                   |THEN INSERT *
                   |""".stripMargin)

      checkAnswer(
        spark.sql("SELECT * FROM target ORDER BY a, b"),
        Row(1, 100, "c11") :: Row(2, 20, "c2") :: Row(3, 300, "c33") :: Nil)
    }
  }

  test("Paimon MergeInto: fail in case that more than one source rows match the same target row") {
    withTable("source", "target") {

      Seq((1, 100, "c11"), (1, 1000, "c111"), (3, 300, "c33"))
        .toDF("a", "b", "c")
        .createOrReplaceTempView("source")

      createTable("target", "a INT, b INT, c STRING", Seq("a"))
      spark.sql("INSERT INTO target values (1, 10, 'c1'), (2, 20, 'c2')")

      val error = intercept[RuntimeException] {
        spark.sql(s"""
                     |MERGE INTO target
                     |USING source
                     |ON target.a = source.a
                     |WHEN MATCHED THEN
                     |UPDATE SET b = source.b, c = source.c
                     |WHEN NOT MATCHED
                     |THEN INSERT (a, b, c) values (a, b, c)
                     |""".stripMargin)
      }.getMessage
      assert(error.contains("match more than one source rows"))
    }
  }

  test("Paimon MergeInto: fail in case that update/insert same column multiple times") {
    withTable("source", "target") {

      Seq((1, 100, "c11"), (3, 300, "c33")).toDF("a", "b", "c").createOrReplaceTempView("source")

      createTable("target", "a INT, b INT, c STRING", Seq("a"))
      spark.sql("INSERT INTO target values (1, 10, 'c1'), (2, 20, 'c2')")

      val error1 = intercept[RuntimeException] {
        spark.sql(s"""
                     |MERGE INTO target
                     |USING source
                     |ON target.a = source.a
                     |WHEN MATCHED THEN
                     |UPDATE SET a = source.a, b = source.b, b = source.c
                     |WHEN NOT MATCHED
                     |THEN INSERT *
                     |""".stripMargin)
      }.getMessage
      assert(error1.contains("Conflicting update/insert on attrs: b"))

      val error2 = intercept[RuntimeException] {
        spark.sql(s"""
                     |MERGE INTO target
                     |USING source
                     |ON target.a = source.a
                     |WHEN MATCHED THEN
                     |UPDATE SET *
                     |WHEN NOT MATCHED
                     |THEN INSERT (a, a, c) VALUES (a, b, c)
                     |""".stripMargin)
      }.getMessage
      assert(error2.contains("Conflicting update/insert on attrs: a"))
    }
  }

  test("Paimon MergeInto: update nested column") {
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
                   |""".stripMargin)

      checkAnswer(
        spark.sql("SELECT * FROM target ORDER BY a"),
        Row(1, 10, Row("x1", "y")) :: Row(2, 20, Row("x", "y")) :: Nil)
    }
  }

  test(s"Paimon MergeInto: update on source eq target condition") {
    withTable("source", "target") {
      Seq((1, 100, "c11"), (3, 300, "c33")).toDF("a", "b", "c").createOrReplaceTempView("source")

      createTable("target", "a INT, b INT, c STRING", Seq("a"))
      sql("INSERT INTO target values (1, 10, 'c1'), (2, 20, 'c2')")

      sql(s"""
             |MERGE INTO target
             |USING source
             |ON source.a = target.a
             |WHEN MATCHED THEN
             |UPDATE SET a = source.a, b = source.b, c = source.c
             |""".stripMargin)

      checkAnswer(
        sql("SELECT * FROM target ORDER BY a, b"),
        Row(1, 100, "c11") :: Row(2, 20, "c2") :: Nil)
    }
  }

  test(s"Paimon MergeInto: merge into with alias") {
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
                   |""".stripMargin)

      checkAnswer(
        spark.sql("SELECT * FROM target ORDER BY a, b"),
        Row(2, 20, "c2") :: Row(3, 300, "c33") :: Row(4, 40, "c4") :: Row(5, 550, "c5") :: Row(
          7,
          700,
          "c77") :: Row(9, 990, "c99") :: Nil
      )
    }
  }

  test(s"Paimon MergeInto: merge into with varchar") {
    withTable("source", "target") {
      createTable("source", "a INT, b VARCHAR(32)", Seq("a"))
      createTable("target", "a INT, b VARCHAR(32)", Seq("a"))
      sql("INSERT INTO target values (1, 'Alice'), (2, 'Bob')")
      sql("INSERT INTO source values (1, 'Eve'), (3, 'Cat')")

      sql(s"""
             |MERGE INTO target
             |USING source
             |ON target.a = source.a
             |WHEN MATCHED THEN
             |UPDATE SET a = source.a, b = source.b
             |""".stripMargin)
      checkAnswer(sql("SELECT * FROM target ORDER BY a, b"), Seq(Row(1, "Eve"), Row(2, "Bob")))
    }
  }
}

trait MergeIntoPrimaryKeyTableTest extends PaimonSparkTestBase with PaimonPrimaryKeyTable {
  import testImplicits._

  test("Paimon MergeInto: fail in case that maybe update primary key column") {
    withTable("source", "target") {

      Seq((101, 10, "c111"), (103, 30, "c333"))
        .toDF("a", "b", "c")
        .createOrReplaceTempView("source")

      createTable("target", "a INT, b INT, c STRING", Seq("a"))
      spark.sql("INSERT INTO target values (1, 10, 'c1'), (2, 20, 'c2')")

      val error = intercept[RuntimeException] {
        spark.sql(s"""
                     |MERGE INTO target
                     |USING source
                     |ON target.b = source.b
                     |WHEN MATCHED THEN
                     |UPDATE SET a = source.a, b = source.b, c = source.c
                     |WHEN NOT MATCHED
                     |THEN INSERT *
                     |""".stripMargin)
      }.getMessage
      assert(error.contains("Can't update the primary key column"))

      spark.sql(s"""
                   |MERGE INTO target
                   |USING source
                   |ON target.b = source.b
                   |WHEN MATCHED THEN
                   |UPDATE SET c = source.c
                   |WHEN NOT MATCHED
                   |THEN INSERT *
                   |""".stripMargin)
      checkAnswer(
        spark.sql("SELECT * FROM target ORDER BY a, b"),
        Row(1, 10, "c111") :: Row(2, 20, "c2") :: Row(103, 30, "c333") :: Nil)
    }
  }
}

trait MergeIntoAppendTableTest extends PaimonSparkTestBase with PaimonAppendTable {

  test("Paimon MergeInto: non pk table commit kind") {
    withTable("s", "t") {
      createTable("s", "id INT, b INT, c INT", Seq("id"))
      sql("INSERT INTO s VALUES (1, 1, 1)")

      createTable("t", "id INT, b INT, c INT", Seq("id"))
      sql("INSERT INTO t VALUES (2, 2, 2)")

      sql("""
            |MERGE INTO t
            |USING s
            |ON t.id = s.id
            |WHEN NOT MATCHED THEN
            |INSERT (id, b, c) VALUES (s.id, s.b, s.c);
            |""".stripMargin)

      val table = loadTable("t")
      var latestSnapshot = table.latestSnapshot().get()
      assert(latestSnapshot.id == 2)
      // no old data is deleted, so the commit kind is APPEND
      assert(latestSnapshot.commitKind.equals(Snapshot.CommitKind.APPEND))

      sql("INSERT INTO s VALUES (2, 22, 22)")
      sql("""
            |MERGE INTO t
            |USING s
            |ON t.id = s.id
            |WHEN MATCHED THEN
            |UPDATE SET id = s.id, b = s.b, c = s.c
            |""".stripMargin)
      latestSnapshot = table.latestSnapshot().get()
      assert(latestSnapshot.id == 3)
      // new data is updated, so the commit kind is OVERWRITE
      assert(latestSnapshot.commitKind.equals(Snapshot.CommitKind.OVERWRITE))
    }
  }

  test("Paimon MergeInto: concurrent merge and compact") {
    for (dvEnabled <- Seq("true", "false")) {
      withTable("s", "t") {
        sql("CREATE TABLE s (id INT, b INT, c INT)")
        sql("INSERT INTO s VALUES (1, 1, 1)")

        sql(
          s"CREATE TABLE t (id INT, b INT, c INT) TBLPROPERTIES ('deletion-vectors.enabled' = '$dvEnabled')")
        sql("INSERT INTO t VALUES (1, 1, 1)")

        val mergeInto = Future {
          for (_ <- 1 to 10) {
            try {
              sql("""
                    |MERGE INTO t
                    |USING s
                    |ON t.id = s.id
                    |WHEN MATCHED THEN
                    |UPDATE SET t.id = s.id, t.b = s.b + t.b, t.c = s.c + t.c
                    |""".stripMargin)
            } catch {
              case a: Throwable =>
                assert(
                  a.getMessage.contains("Conflicts during commits") || a.getMessage.contains(
                    "Missing file"))
            }
            checkAnswer(sql("SELECT count(*) FROM t"), Seq(Row(1)))
          }
        }

        val compact = Future {
          for (_ <- 1 to 10) {
            try {
              sql("CALL sys.compact(table => 't', order_strategy => 'order', order_by => 'id')")
            } catch {
              case a: Throwable => assert(a.getMessage.contains("Conflicts during commits"))
            }
            checkAnswer(sql("SELECT count(*) FROM t"), Seq(Row(1)))
          }
        }

        Await.result(mergeInto, 60.seconds)
        Await.result(compact, 60.seconds)
      }
    }
  }
}
