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

import org.apache.paimon.spark.{PaimonPrimaryKeyTable, PaimonSparkTestBase, PaimonTableTest}

import org.apache.spark.sql.Row

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

      Seq((1, 100, "c11"), (3, 300, "c33")).toDF("a", "b", "c").createOrReplaceTempView("source")

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
                     |THEN INSERT (a, b) VALUES (a, b)
                     |""".stripMargin)
      }.getMessage
      assert(error.contains("Can't align the table's columns in insert clause."))
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
      assert(error.contains("match more then one source rows"))
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
