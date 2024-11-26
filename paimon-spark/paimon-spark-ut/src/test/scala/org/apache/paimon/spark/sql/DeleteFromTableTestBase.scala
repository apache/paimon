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

import org.apache.paimon.CoreOptions
import org.apache.paimon.CoreOptions.MergeEngine
import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.sql.Row
import org.assertj.core.api.Assertions.{assertThat, assertThatThrownBy}

abstract class DeleteFromTableTestBase extends PaimonSparkTestBase {

  import testImplicits._

  test(s"Paimon Delete: append-only table") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING, dt STRING)
                 |""".stripMargin)

    spark.sql("""
                |INSERT INTO T
                |VALUES (1, 'a', '2024'), (2, 'b', '2024'), (3, 'c', '2025'), (4, 'd', '2025')
                |""".stripMargin)

    spark.sql("DELETE FROM T WHERE name = 'a'")
    checkAnswer(
      spark.sql("SELECT * FROM T ORDER BY id"),
      Seq((2, "b", "2024"), (3, "c", "2025"), (4, "d", "2025")).toDF()
    )

    spark.sql("DELETE FROM T WHERE dt = '2025'")
    checkAnswer(
      spark.sql("SELECT * FROM T ORDER BY id"),
      Seq((2, "b", "2024")).toDF()
    )
  }

  test(s"Paimon Delete: append-only table with partition") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING, dt STRING) PARTITIONED BY (dt)
                 |""".stripMargin)

    spark.sql("""
                |INSERT INTO T
                |VALUES (1, 'a', '2024'), (2, 'b', '2024'), (3, 'c', '2025'), (4, 'd', '2025'),
                |(5, 'a', '2026'), (6, 'b', '2026'), (7, 'c', '2027'), (8, 'd', '2027')
                |""".stripMargin)

    spark.sql("DELETE FROM T WHERE name = 'a'")
    checkAnswer(
      spark.sql("SELECT * FROM T ORDER BY id"),
      Seq(
        (2, "b", "2024"),
        (3, "c", "2025"),
        (4, "d", "2025"),
        (6, "b", "2026"),
        (7, "c", "2027"),
        (8, "d", "2027")).toDF()
    )

    spark.sql("DELETE FROM T WHERE dt = '2025'")
    checkAnswer(
      spark.sql("SELECT * FROM T ORDER BY id"),
      Seq((2, "b", "2024"), (6, "b", "2026"), (7, "c", "2027"), (8, "d", "2027")).toDF()
    )

    spark.sql("DELETE FROM T WHERE dt IN ('2026', '2027')")
    checkAnswer(
      spark.sql("SELECT * FROM T ORDER BY id"),
      Seq((2, "b", "2024")).toDF()
    )

    spark.sql("DELETE FROM T WHERE dt < '2023' OR  dt > '2025'")
    checkAnswer(
      spark.sql("SELECT * FROM T ORDER BY id"),
      Seq((2, "b", "2024")).toDF()
    )
  }

  test("Paimon Delete: append-only table, condition contains IN/NOT IN subquery") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING, dt STRING) PARTITIONED BY (dt)
                 |""".stripMargin)

    spark.sql("""
                |INSERT INTO T
                |VALUES (1, 'a', '2024'), (2, 'b', '2024'),
                | (3, 'c', '2025'), (4, 'd', '2025'),
                | (5, 'e', '2026'), (6, 'f', '2026')
                |""".stripMargin)

    Seq(2, 4, 6).toDF("key").createOrReplaceTempView("source")

    spark.sql("""
                |DELETE FROM T
                |WHERE id >= (SELECT MAX(key) FROM source)""".stripMargin)
    checkAnswer(
      spark.sql("SELECT * FROM T ORDER BY id"),
      Seq((1, "a", "2024"), (2, "b", "2024"), (3, "c", "2025"), (4, "d", "2025"), (5, "e", "2026"))
        .toDF()
    )

    // IN
    spark.sql("""
                |DELETE FROM T
                |WHERE id IN (SELECT key FROM source)""".stripMargin)
    checkAnswer(
      spark.sql("SELECT * FROM T ORDER BY id"),
      Seq((1, "a", "2024"), (3, "c", "2025"), (5, "e", "2026")).toDF()
    )

    // NOT IN: (4, 5, 6)
    spark.sql("""
                |DELETE FROM T
                |WHERE id NOT IN (SELECT key + key % 3 FROM source)""".stripMargin)
    checkAnswer(
      spark.sql("SELECT * FROM T ORDER BY id"),
      Seq((5, "e", "2026")).toDF()
    )
  }

  test("Paimon Delete: append-only table, condition contains EXISTS/NOT EXISTS subquery") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING, dt STRING) PARTITIONED BY (dt)
                 |""".stripMargin)

    spark.sql("""
                |INSERT INTO T
                |VALUES (1, 'a', '2024'), (2, 'b', '2024'), (3, 'c', '2025'), (4, 'd', '2025')
                |""".stripMargin)

    Seq(2, 4, 6).toDF("key").createOrReplaceTempView("source")

    // EXISTS
    spark.sql("""
                |DELETE FROM T
                |WHERE EXiSTS (SELECT * FROM source WHERE key > 7)""".stripMargin)
    checkAnswer(
      spark.sql("SELECT * FROM T ORDER BY id"),
      Seq((1, "a", "2024"), (2, "b", "2024"), (3, "c", "2025"), (4, "d", "2025")).toDF())

    // NOT EXISTS
    spark.sql("""
                |DELETE FROM T
                |WHERE NOT EXiSTS (SELECT * FROM source WHERE key > 5)""".stripMargin)
    checkAnswer(
      spark.sql("SELECT * FROM T ORDER BY id"),
      Seq((1, "a", "2024"), (2, "b", "2024"), (3, "c", "2025"), (4, "d", "2025")).toDF()
    )
    spark.sql("""
                |DELETE FROM T
                |WHERE NOT EXiSTS (SELECT * FROM source WHERE key > 7)""".stripMargin)
    checkAnswer(
      spark.sql("SELECT * FROM T ORDER BY id"),
      spark.emptyDataFrame
    )
  }

  CoreOptions.MergeEngine.values().foreach {
    mergeEngine =>
      {
        test(s"test delete with merge engine $mergeEngine") {
          val otherOptions =
            if ("first-row".equals(mergeEngine.toString)) "'changelog-producer' = 'lookup'," else ""
          spark.sql(s"""
                       |CREATE TABLE T (id INT, name STRING, age INT)
                       |TBLPROPERTIES (
                       |  $otherOptions
                       |  'primary-key' = 'id',
                       |  'merge-engine' = '$mergeEngine',
                       |  'write-only' = 'true')
                       |""".stripMargin)

          spark.sql("INSERT INTO T VALUES (1, 'a', NULL)")
          spark.sql("INSERT INTO T VALUES (2, 'b', NULL)")
          spark.sql("INSERT INTO T VALUES (1, NULL, 16)")

          if (mergeEngine != MergeEngine.DEDUPLICATE) {
            assertThatThrownBy(() => spark.sql("DELETE FROM T WHERE id = 1"))
              .hasMessageContaining("please use 'COMPACT' procedure first")
            spark.sql("CALL sys.compact(table => 'T')")
          }

          spark.sql("DELETE FROM T WHERE id = 1")
          assertThat(spark.sql("SELECT * FROM T").collectAsList().toString)
            .isEqualTo("[[2,b,null]]")
        }
      }
  }

  test(s"test delete with primary key") {
    spark.sql(
      s"""
         |CREATE TABLE T (id INT, name STRING, dt STRING)
         |TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1', 'merge-engine' = 'deduplicate')
         |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, 'a', '11'), (2, 'b', '22'), (3, 'c', '33')")

    spark.sql("DELETE FROM T WHERE id = 1")

    val rows1 = spark.sql("SELECT * FROM T").collectAsList()
    assertThat(rows1.toString).isEqualTo("[[2,b,22], [3,c,33]]")

    spark.sql("DELETE FROM T WHERE id < 3")

    val rows2 = spark.sql("SELECT * FROM T").collectAsList()
    assertThat(rows2.toString).isEqualTo("[[3,c,33]]")
  }

  test(s"test delete with non-primary key") {
    spark.sql(
      s"""
         |CREATE TABLE T (id INT, name STRING, dt STRING)
         |TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1', 'merge-engine' = 'deduplicate')
         |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, 'a', '11'), (2, 'b', '22'), (3, 'c', '33'), (4, 'a', '44')")

    spark.sql("DELETE FROM T WHERE name = 'a'")

    val rows1 = spark.sql("SELECT * FROM T").collectAsList()
    assertThat(rows1.toString).isEqualTo("[[2,b,22], [3,c,33]]")

    spark.sql("DELETE FROM T WHERE name < 'c'")

    val rows2 = spark.sql("SELECT * FROM T").collectAsList()
    assertThat(rows2.toString).isEqualTo("[[3,c,33]]")
  }

  test(s"test delete with no where") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING, dt STRING)
                 |TBLPROPERTIES ('primary-key' = 'id', 'merge-engine' = 'deduplicate')
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, 'a', '11'), (2, 'b', '22'), (3, 'c', '33')")

    spark.sql("DELETE FROM T")

    val rows = spark.sql("SELECT * FROM T").collectAsList()
    assertThat(rows.toString).isEqualTo("[]")
  }

  test(s"test delete with in condition") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING, dt STRING)
                 |TBLPROPERTIES ('primary-key' = 'id', 'merge-engine' = 'deduplicate')
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, 'a', '11'), (2, 'b', '22'), (3, 'c', '33')")

    spark.sql("DELETE FROM T WHERE id IN (1, 2)")

    val rows = spark.sql("SELECT * FROM T").collectAsList()
    assertThat(rows.toString).isEqualTo("[[3,c,33]]")
  }

  test(s"test delete with in subquery") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING, dt STRING)
                 |TBLPROPERTIES ('primary-key' = 'id', 'merge-engine' = 'deduplicate')
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, 'a', '11'), (2, 'b', '22'), (3, 'c', '33')")

    import testImplicits._
    val df = Seq(1, 2).toDF("id")
    df.createOrReplaceTempView("deleted_ids")
    spark.sql("DELETE FROM T WHERE id IN (SELECT * FROM deleted_ids)")

    val rows = spark.sql("SELECT * FROM T").collectAsList()
    assertThat(rows.toString).isEqualTo("[[3,c,33]]")
  }

  test(s"test delete is drop partition") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING, dt STRING, hh STRING)
                 |TBLPROPERTIES ('primary-key' = 'id, dt, hh', 'merge-engine' = 'deduplicate')
                 |PARTITIONED BY (dt, hh)
                 |""".stripMargin)

    spark.sql(
      "INSERT INTO T VALUES " +
        "(1, 'a', '2023-10-01', '12')," +
        "(2, 'b', '2023-10-01', '12')," +
        "(3, 'c', '2023-10-02', '12')," +
        "(4, 'd', '2023-10-02', '13')," +
        "(5, 'e', '2023-10-02', '14')," +
        "(6, 'f', '2023-10-02', '15')")

    // delete isn't drop partition
    spark.sql("DELETE FROM T WHERE name = 'a' and hh = '12'")
    val rows1 = spark.sql("SELECT * FROM T ORDER BY id").collectAsList()
    assertThat(rows1.toString).isEqualTo(
      "[[2,b,2023-10-01,12], [3,c,2023-10-02,12], [4,d,2023-10-02,13], [5,e,2023-10-02,14], [6,f,2023-10-02,15]]")

    // delete is drop partition
    spark.sql("DELETE FROM T WHERE hh = '12'")
    val rows2 = spark.sql("SELECT * FROM T ORDER BY id").collectAsList()
    assertThat(rows2.toString).isEqualTo(
      "[[4,d,2023-10-02,13], [5,e,2023-10-02,14], [6,f,2023-10-02,15]]")

    spark.sql("DELETE FROM T WHERE dt = '2023-10-02' and hh = '13'")
    val rows3 = spark.sql("SELECT * FROM T ORDER BY id").collectAsList()
    assertThat(rows3.toString).isEqualTo("[[5,e,2023-10-02,14], [6,f,2023-10-02,15]]")

    spark.sql("DELETE FROM T WHERE dt = '2023-10-02'")
    val rows4 = spark.sql("SELECT * FROM T ORDER BY id").collectAsList()
    assertThat(rows4.toString).isEqualTo("[]")
  }

  test(s"test delete producer changelog") {
    spark.sql(
      s"""
         |CREATE TABLE T (id INT, name STRING, dt STRING, hh STRING)
         |TBLPROPERTIES ('primary-key' = 'id, dt, hh', 'merge-engine' = 'deduplicate', 'changelog-producer'='input', 'delete.force-produce-changelog'='true')
         |PARTITIONED BY (dt, hh)
         |""".stripMargin)

    spark.sql(
      "INSERT INTO T VALUES " +
        "(1, 'a', '2023-10-01', '12')," +
        "(2, 'b', '2023-10-01', '12')," +
        "(3, 'c', '2023-10-02', '12')," +
        "(4, 'd', '2023-10-02', '13')," +
        "(5, 'e', '2023-10-02', '14')," +
        "(6, 'f', '2023-10-02', '15')")

    // delete isn't drop partition
    spark.sql("DELETE FROM T WHERE name = 'a' and hh = '12'")
    assertThat(spark.sql("SELECT * FROM `T$audit_log` WHERE rowkind='-D'").collectAsList().size())
      .isEqualTo(1)

    // delete is drop partition
    spark.sql("DELETE FROM T WHERE hh = '12'")
    assertThat(spark.sql("SELECT * FROM `T$audit_log` WHERE rowkind='-D'").collectAsList().size())
      .isEqualTo(3)
  }

  test("Paimon Delete: delete null partition with specified default partition name") {
    spark.sql(s"""
                 |CREATE TABLE T (a INT, dt STRING)
                 |PARTITIONED BY (dt)
                 |TBLPROPERTIES ('partition.default-name'='__TEST_DEFAULT_PARTITION__')
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, '20240601'), (2, null)")
    checkAnswer(
      spark.sql("SELECT * FROM T ORDER BY a"),
      Row(1, "20240601") :: Row(2, null) :: Nil
    )

    spark.sql("DELETE FROM T WHERE dt IS null")
    checkAnswer(
      spark.sql("SELECT * FROM T ORDER BY a"),
      Row(1, "20240601") :: Nil
    )
  }
}
