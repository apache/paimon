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

import org.apache.paimon.spark.{PaimonInputPartition, PaimonScan, PaimonSparkTestBase, SparkTable}
import org.apache.paimon.table.source.{DataSplit, Split}

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.trees.TreePattern.{DYNAMIC_PRUNING_SUBQUERY, LIMIT, SORT}
import org.apache.spark.sql.connector.read.{InputPartition, ScanBuilder, SupportsPushDownLimit, SupportsPushDownTopN}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.assertj.core.api.{Assertions => assertj}
import org.junit.jupiter.api.Assertions

abstract class PaimonPushDownTestBase extends PaimonSparkTestBase with AdaptiveSparkPlanHelper {

  import testImplicits._

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
    // filter "id = '1' or pt = 'p1'" can't push down completely, it still needs to be evaluated after scanning
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

  test(s"Paimon push down: apply CONCAT") {
    // Spark support push down CONCAT since Spark 3.4.
    if (gteqSpark3_4) {
      withTable("t") {
        sql(
          """
            |CREATE TABLE t (id int, value int, year STRING, month STRING, day STRING, hour STRING)
            |using paimon
            |PARTITIONED BY (year, month, day, hour)
            |""".stripMargin)

        sql("""
              |INSERT INTO t values
              |(1, 100, '2024', '07', '15', '21'),
              |(2, 200, '2025', '07', '15', '21'),
              |(3, 300, '2025', '07', '16', '22'),
              |(4, 400, '2025', '07', '16', '23'),
              |(5, 440, '2025', '07', '16', '23'),
              |(6, 500, '2025', '07', '17', '00'),
              |(7, 600, '2025', '07', '17', '02')
              |""".stripMargin)

        val q =
          """
            |SELECT * FROM t
            |WHERE CONCAT(year,'-',month,'-',day,'-',hour) BETWEEN '2025-07-16-21' AND '2025-07-17-01'
            |ORDER BY id
            |""".stripMargin
        assert(!checkFilterExists(q))

        checkAnswer(
          spark.sql(q),
          Seq(
            Row(3, 300, "2025", "07", "16", "22"),
            Row(4, 400, "2025", "07", "16", "23"),
            Row(5, 440, "2025", "07", "16", "23"),
            Row(6, 500, "2025", "07", "17", "00"))
        )
      }
    }
  }

  test(s"Paimon push down: apply TRIM/LTRM/RTRIM") {
    // Spark support push down TRIM/LTRM/RTRIM since Spark 3.4.
    if (gteqSpark3_4) {
      withTable("t") {
        sql("""
              |CREATE TABLE t (id int, value int, dt STRING)
              |using paimon
              |PARTITIONED BY (dt)
              |""".stripMargin)

        sql("""
              |INSERT INTO t values
              |(1, 100, 'chelloc'), (1, 100, 'caa'), (1, 100, 'bbc')
              |""".stripMargin)

        val q =
          """
            |SELECT * FROM t
            |WHERE TRIM('c', dt) = 'hello'
            |""".stripMargin
        assert(!checkFilterExists(q))

        checkAnswer(
          spark.sql(q),
          Seq(Row(1, 100, "chelloc"))
        )

        val q1 =
          """
            |SELECT * FROM t
            |WHERE LTRIM('c', dt) = 'aa'
            |""".stripMargin
        assert(!checkFilterExists(q1))

        checkAnswer(
          spark.sql(q1),
          Seq(Row(1, 100, "caa"))
        )

        val q2 =
          """
            |SELECT * FROM t
            |WHERE RTRIM('c', dt) = 'bb'
            |""".stripMargin
        assert(!checkFilterExists(q2))

        checkAnswer(
          spark.sql(q2),
          Seq(Row(1, 100, "bbc"))
        )
      }
    }
  }

  test(s"Paimon push down: apply UPPER") {
    // Spark support push down UPPER since Spark 3.4.
    if (gteqSpark3_4) {
      withTable("t") {
        sql("""
              |CREATE TABLE t (id int, value int, dt STRING)
              |using paimon
              |PARTITIONED BY (dt)
              |""".stripMargin)

        sql("""
              |INSERT INTO t values
              |(1, 100, 'hello')
              |""".stripMargin)

        val q =
          """
            |SELECT * FROM t
            |WHERE UPPER(dt) = 'HELLO'
            |""".stripMargin
        assert(!checkFilterExists(q))

        checkAnswer(
          spark.sql(q),
          Seq(Row(1, 100, "hello"))
        )
      }
    }
  }

  test(s"Paimon push down: apply LOWER") {
    // Spark support push down LOWER since Spark 3.4.
    if (gteqSpark3_4) {
      withTable("t") {
        sql("""
              |CREATE TABLE t (id int, value int, dt STRING)
              |using paimon
              |PARTITIONED BY (dt)
              |""".stripMargin)

        sql("""
              |INSERT INTO t values
              |(1, 100, 'hello')
              |""".stripMargin)

        val q =
          """
            |SELECT * FROM t
            |WHERE LOWER(dt) = 'hello'
            |""".stripMargin
        assert(!checkFilterExists(q))

        checkAnswer(
          spark.sql(q),
          Seq(Row(1, 100, "hello"))
        )
      }
    }
  }

  test(s"Paimon push down: apply SUBSTRING") {
    // Spark support push down LOWER since Spark 3.4.
    if (gteqSpark3_4) {
      withTable("t") {
        sql("""
              |CREATE TABLE t (id int, value int, dt STRING)
              |using paimon
              |PARTITIONED BY (dt)
              |""".stripMargin)

        sql("""
              |INSERT INTO t values
              |(1, 100, '_hello')
              |""".stripMargin)

        val q =
          """
            |SELECT * FROM t
            |WHERE SUBSTRING(dt, 2) = 'hello'
            |""".stripMargin
        assert(!checkFilterExists(q))

        checkAnswer(
          spark.sql(q),
          Seq(Row(1, 100, "_hello"))
        )

        val q1 =
          """
            |SELECT * FROM t
            |WHERE SUBSTRING(dt, 2, 2) = 'he'
            |""".stripMargin
        assert(!checkFilterExists(q1))

        checkAnswer(
          spark.sql(q1),
          Seq(Row(1, 100, "_hello"))
        )
      }
    }
  }

  test(s"Paimon push down: apply CAST") {
    if (gteqSpark3_4) {
      withSparkSQLConf("spark.sql.ansi.enabled" -> "true") {
        withTable("t") {
          sql("""
                |CREATE TABLE t (id int, value int, dt STRING)
                |using paimon
                |PARTITIONED BY (dt)
                |""".stripMargin)

          sql("""
                |INSERT INTO t values
                |(1, 100, '1')
                |""".stripMargin)

          val q = "SELECT * FROM t WHERE dt = 1"
          assert(!checkFilterExists(q))
          checkAnswer(sql(q), Seq(Row(1, 100, "1")))
        }
      }
    }
  }

  test("Paimon pushDown: limit for append-only tables with deletion vector") {
    withTable("dv_test") {
      spark.sql(
        """
          |CREATE TABLE dv_test (c1 INT, c2 STRING)
          |TBLPROPERTIES ('deletion-vectors.enabled' = 'true', 'source.split.target-size' = '1')
          |""".stripMargin)

      spark.sql("insert into table dv_test values(1, 'a'),(2, 'b'),(3, 'c')")
      assert(spark.sql("select * from dv_test limit 2").count() == 2)

      spark.sql("delete from dv_test where c1 = 1")
      assert(spark.sql("select * from dv_test limit 2").count() == 2)
    }
  }

  test("Paimon pushDown: limit for append-only tables") {
    assume(gteqSpark3_3)
    spark.sql(s"""
                 |CREATE TABLE T (a INT, b STRING, c STRING)
                 |PARTITIONED BY (c)
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, 'a', '11'), (2, 'b', '11')")
    spark.sql("INSERT INTO T VALUES (3, 'c', '22'), (4, 'd', '22')")

    checkAnswer(
      spark.sql("SELECT * FROM T ORDER BY a"),
      Row(1, "a", "11") :: Row(2, "b", "11") :: Row(3, "c", "22") :: Row(4, "d", "22") :: Nil)

    val scanBuilder = getScanBuilder()
    Assertions.assertTrue(scanBuilder.isInstanceOf[SupportsPushDownLimit])

    val dataSplitsWithoutLimit = scanBuilder.build().asInstanceOf[PaimonScan].inputSplits
    Assertions.assertTrue(dataSplitsWithoutLimit.length >= 2)

    // It still returns false even it can push down limit.
    Assertions.assertFalse(scanBuilder.asInstanceOf[SupportsPushDownLimit].pushLimit(1))
    val dataSplitsWithLimit = scanBuilder.build().asInstanceOf[PaimonScan].inputSplits
    Assertions.assertEquals(1, dataSplitsWithLimit.length)

    Assertions.assertEquals(1, spark.sql("SELECT * FROM T LIMIT 1").count())
  }

  test("Paimon pushDown: limit for primary key table") {
    assume(gteqSpark3_3)
    spark.sql(s"""
                 |CREATE TABLE T (a INT, b STRING, c STRING)
                 |TBLPROPERTIES ('primary-key'='a')
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, 'a', '11'), (2, 'b', '22')")
    spark.sql("INSERT INTO T VALUES (3, 'c', '11'), (4, 'd', '22')")

    val scanBuilder = getScanBuilder()
    Assertions.assertTrue(scanBuilder.isInstanceOf[SupportsPushDownLimit])

    // Case 1: All dataSplits is rawConvertible.
    val dataSplitsWithoutLimit = scanBuilder.build().asInstanceOf[PaimonScan].inputSplits
    Assertions.assertEquals(4, dataSplitsWithoutLimit.length)
    // All dataSplits is rawConvertible.
    dataSplitsWithoutLimit.foreach(
      splits => {
        Assertions.assertTrue(splits.asInstanceOf[DataSplit].rawConvertible())
      })

    // It still returns false even it can push down limit.
    Assertions.assertFalse(scanBuilder.asInstanceOf[SupportsPushDownLimit].pushLimit(1))
    val dataSplitsWithLimit = scanBuilder.build().asInstanceOf[PaimonScan].inputSplits
    Assertions.assertEquals(1, dataSplitsWithLimit.length)
    Assertions.assertEquals(1, spark.sql("SELECT * FROM T LIMIT 1").count())

    Assertions.assertFalse(scanBuilder.asInstanceOf[SupportsPushDownLimit].pushLimit(2))
    val dataSplitsWithLimit1 = scanBuilder.build().asInstanceOf[PaimonScan].inputSplits
    Assertions.assertEquals(2, dataSplitsWithLimit1.length)
    Assertions.assertEquals(2, spark.sql("SELECT * FROM T LIMIT 2").count())

    // Case 2: Update 2 rawConvertible dataSplits to convert to nonRawConvertible.
    spark.sql("INSERT INTO T VALUES (1, 'a2', '11'), (2, 'b2', '22')")
    val scanBuilder2 = getScanBuilder()
    val dataSplitsWithoutLimit2 = scanBuilder2.build().asInstanceOf[PaimonScan].inputSplits
    Assertions.assertEquals(4, dataSplitsWithoutLimit2.length)
    // Now, we have 4 dataSplits, and 2 dataSplit is nonRawConvertible, 2 dataSplit is rawConvertible.
    Assertions.assertEquals(
      2,
      dataSplitsWithoutLimit2.count(split => { split.asInstanceOf[DataSplit].rawConvertible() }))

    // Return 2 dataSplits.
    Assertions.assertFalse(scanBuilder2.asInstanceOf[SupportsPushDownLimit].pushLimit(2))
    val dataSplitsWithLimit2 = scanBuilder2.build().asInstanceOf[PaimonScan].inputSplits
    Assertions.assertEquals(2, dataSplitsWithLimit2.length)
    Assertions.assertEquals(2, spark.sql("SELECT * FROM T LIMIT 2").count())

    // 2 dataSplits cannot meet the limit requirement, so need to scan all dataSplits.
    Assertions.assertFalse(scanBuilder2.asInstanceOf[SupportsPushDownLimit].pushLimit(3))
    val dataSplitsWithLimit22 = scanBuilder2.build().asInstanceOf[PaimonScan].inputSplits
    // Need to scan all dataSplits.
    Assertions.assertEquals(4, dataSplitsWithLimit22.length)
    Assertions.assertEquals(3, spark.sql("SELECT * FROM T LIMIT 3").count())

    // Case 3: Update the remaining 2 rawConvertible dataSplits to make all dataSplits is nonRawConvertible.
    spark.sql("INSERT INTO T VALUES (3, 'c', '11'), (4, 'd', '22')")
    val scanBuilder3 = getScanBuilder()
    val dataSplitsWithoutLimit3 = scanBuilder3.build().asInstanceOf[PaimonScan].inputSplits
    Assertions.assertEquals(4, dataSplitsWithoutLimit3.length)

    // All dataSplits is nonRawConvertible.
    dataSplitsWithoutLimit3.foreach(
      splits => {
        Assertions.assertFalse(splits.asInstanceOf[DataSplit].rawConvertible())
      })

    Assertions.assertFalse(scanBuilder3.asInstanceOf[SupportsPushDownLimit].pushLimit(1))
    val dataSplitsWithLimit3 = scanBuilder3.build().asInstanceOf[PaimonScan].inputSplits
    // Need to scan all dataSplits.
    Assertions.assertEquals(4, dataSplitsWithLimit3.length)
    Assertions.assertEquals(1, spark.sql("SELECT * FROM T LIMIT 1").count())
  }

  test("Paimon pushDown: limit for table with deletion vector") {
    assume(gteqSpark3_3)
    Seq(true, false).foreach(
      deletionVectorsEnabled => {
        Seq(true, false).foreach(
          primaryKeyTable => {
            withTable("T") {
              sql(s"""
                     |CREATE TABLE T (id INT)
                     |TBLPROPERTIES (
                     | 'deletion-vectors.enabled' = $deletionVectorsEnabled,
                     | '${if (primaryKeyTable) "primary-key" else "bucket-key"}' = 'id',
                     | 'bucket' = '10'
                     |)
                     |""".stripMargin)

              sql("INSERT INTO T SELECT id FROM range (1, 50000)")
              sql("DELETE FROM T WHERE id % 13 = 0")
              Assertions.assertEquals(100, spark.sql("SELECT * FROM T LIMIT 100").count())

              val withoutLimit = getScanBuilder().build().asInstanceOf[PaimonScan].inputSplits
              assert(withoutLimit.length == 10)

              val scanBuilder = getScanBuilder().asInstanceOf[SupportsPushDownLimit]
              scanBuilder.pushLimit(1)
              val withLimit = scanBuilder.build().asInstanceOf[PaimonScan].inputSplits
              if (deletionVectorsEnabled || !primaryKeyTable) {
                assert(withLimit.length == 1)
              } else {
                assert(withLimit.length == 10)
              }
            }
          })
      })
  }

  test("Paimon pushDown: runtime filter") {
    withTable("source", "t") {
      Seq((1L, "x1", "2023"), (2L, "x2", "2023"), (5L, "x5", "2025"), (6L, "x6", "2026"))
        .toDF("a", "b", "pt")
        .createOrReplaceTempView("source")

      spark.sql("""
                  |CREATE TABLE t (id INT, name STRING, pt STRING) PARTITIONED BY (pt)
                  |""".stripMargin)

      spark.sql(
        """
          |INSERT INTO t VALUES (1, "a", "2023"), (3, "c", "2023"), (5, "e", "2025"), (7, "g", "2027")
          |""".stripMargin)

      val df1 = spark.sql("""
                            |SELECT t.id, t.name, source.b FROM source join t
                            |ON source.pt = t.pt AND source.pt = '2023'
                            |ORDER BY t.id, source.b
                            |""".stripMargin)
      val qe1 = df1.queryExecution
      Assertions.assertFalse(qe1.analyzed.containsPattern(DYNAMIC_PRUNING_SUBQUERY))
      Assertions.assertTrue(qe1.optimizedPlan.containsPattern(DYNAMIC_PRUNING_SUBQUERY))
      Assertions.assertTrue(qe1.sparkPlan.containsPattern(DYNAMIC_PRUNING_SUBQUERY))
      checkAnswer(
        df1,
        Row(1, "a", "x1") :: Row(1, "a", "x2") :: Row(3, "c", "x1") :: Row(3, "c", "x2") :: Nil)

      val df2 = spark.sql("""
                            |SELECT t.*, source.b FROM source join t
                            |ON source.a = t.id AND source.pt = t.pt AND source.a > 3
                            |""".stripMargin)
      val qe2 = df1.queryExecution
      Assertions.assertFalse(qe2.analyzed.containsPattern(DYNAMIC_PRUNING_SUBQUERY))
      Assertions.assertTrue(qe2.optimizedPlan.containsPattern(DYNAMIC_PRUNING_SUBQUERY))
      Assertions.assertTrue(qe2.sparkPlan.containsPattern(DYNAMIC_PRUNING_SUBQUERY))
      checkAnswer(df2, Row(5, "e", "2025", "x5") :: Nil)
    }
  }

  test("Paimon pushDown: combine runtime partition filter and partition filter") {
    withTable("fact_table", "dim_table", "filter_table") {
      sql("""
            |CREATE TABLE fact_table (
            |  id INT,
            |  amount DOUBLE,
            |  category STRING,
            |  date_pt STRING
            |)
            |PARTITIONED BY (date_pt)
            |""".stripMargin)

      sql("""
            |CREATE TABLE dim_table (
            |  category STRING,
            |  category_name STRING,
            |  region STRING,
            |  pt STRING
            |)
            |PARTITIONED BY (pt)
            |""".stripMargin)

      sql("""
            |CREATE TABLE filter_table (
            |  region STRING,
            |  date_pt STRING
            |)
            |""".stripMargin)

      sql("""
            |INSERT INTO fact_table VALUES
            |(1, 100.0, 'A', '2023-01-01'),
            |(2, 200.0, 'B', '2023-01-02'),
            |(3, 150.0, 'A', '2023-01-15'),
            |(4, 250.0, 'C', '2023-02-01'),
            |(5, 300.0, 'A', '2023-02-15'),
            |(6, 180.0, 'B', '2024-01-01'),
            |(7, 220.0, 'A', '2024-01-15'),
            |(8, 400.0, 'C', '2024-02-01'),
            |(9, 350.0, 'B', '2024-02-15'),
            |(10, 500.0, 'A', '2025-03-01'),
            |(11, 450.0, 'C', '2025-03-15'),
            |(12, 600.0, 'B', '2025-04-01')
            |""".stripMargin)

      sql("""
            |INSERT INTO dim_table VALUES
            |('A', 'Category A', 'East', '2023-01'),
            |('B', 'Category B', 'West', '2023-01'),
            |('C', 'Category C', 'North', '2023-02'),
            |('A', 'Category A', 'East', '2024-01'),
            |('B', 'Category B', 'West', '2024-02'),
            |('C', 'Category C', 'North', '2024-02')
            |""".stripMargin)

      sql("""
            |INSERT INTO filter_table VALUES
            |('East', '2023-01-01'),
            |('East', '2023-01-15'),
            |('East', '2024-01-15')
            |""".stripMargin)

      val df = sql("""
                     |SELECT
                     |  f.id,
                     |  f.amount,
                     |  f.category,
                     |  d.category_name,
                     |  d.region,
                     |  f.date_pt
                     |FROM fact_table f
                     |JOIN dim_table d
                     |  ON f.category = d.category
                     |  AND SUBSTRING(f.date_pt, 1, 7) = d.pt
                     |JOIN filter_table ft
                     |  ON d.region = ft.region
                     |  AND f.date_pt = ft.date_pt
                     |WHERE d.region = 'East' AND f.date_pt < '2024-01-15'
                     |ORDER BY f.id
                     |""".stripMargin)

      checkAnswer(
        df,
        Seq(
          Row(1, 100.0, "A", "Category A", "East", "2023-01-01"),
          Row(3, 150.0, "A", "Category A", "East", "2023-01-15")
        )
      )

      val filteredSplits = collectFilteredInputSplits(df.queryExecution.executedPlan, "fact_table")
      assert(filteredSplits.size == 2)
    }
  }

  test("Paimon pushDown: TopN for append-only tables") {
    assume(gteqSpark3_3)
    spark.sql("""
                |CREATE TABLE T (pt INT, id INT, price BIGINT) PARTITIONED BY (pt)
                |TBLPROPERTIES ('file-index.range-bitmap.columns'='id')
                |""".stripMargin)
    Assertions.assertTrue(getScanBuilder().isInstanceOf[SupportsPushDownTopN])

    spark.sql("""
                |INSERT INTO T VALUES
                |(1, 10, 100L),
                |(2, 20, 200L),
                |(3, 30, 300L)
                |""".stripMargin)
    spark.sql("""
                |INSERT INTO T VALUES
                |(4, 40, 400L),
                |(5, 50, 500L)
                |""".stripMargin)
    spark.sql("""
                |INSERT INTO T VALUES
                |(6, NULL, 600L),
                |(6, NULL, 600L),
                |(6, 60, 600L),
                |(7, NULL, 700L),
                |(7, NULL, 700L),
                |(7, 70, 700L)
                |""".stripMargin)

    // disable stats
    spark.sql("""
                |ALTER TABLE T SET TBLPROPERTIES ('metadata.stats-mode' = 'none')
                |""".stripMargin)
    spark.sql("""
                |INSERT INTO T VALUES
                |(8, 80, 800L),
                |(9, 90, 900L)
                |""".stripMargin)
    spark.sql("""
                |ALTER TABLE T UNSET TBLPROPERTIES ('metadata.stats-mode')
                |""".stripMargin)

    // test ASC
    checkAnswer(
      spark.sql("SELECT id FROM T ORDER BY id ASC NULLS LAST LIMIT 5"),
      Row(10) :: Row(20) :: Row(30) :: Row(40) :: Row(50) :: Nil)
    checkAnswer(
      spark.sql("SELECT id FROM T ORDER BY id ASC NULLS FIRST LIMIT 5"),
      Row(null) :: Row(null) :: Row(null) :: Row(null) :: Row(10) :: Nil)

    // test DESC
    checkAnswer(
      spark.sql("SELECT id FROM T ORDER BY id DESC NULLS LAST LIMIT 5"),
      Row(90) :: Row(80) :: Row(70) :: Row(60) :: Row(50) :: Nil)
    checkAnswer(
      spark.sql("SELECT id FROM T ORDER BY id DESC NULLS FIRST LIMIT 5"),
      Row(null) :: Row(null) :: Row(null) :: Row(null) :: Row(90) :: Nil)

    // test with partition
    checkAnswer(
      spark.sql("SELECT id FROM T WHERE pt=6 ORDER BY id DESC LIMIT 5"),
      Row(60) :: Row(null) :: Row(null) :: Nil)
    checkAnswer(
      spark.sql("SELECT id FROM T WHERE pt=6 ORDER BY id ASC LIMIT 3"),
      Row(null) :: Row(null) :: Row(60) :: Nil)

    // test plan
    val df1 = spark.sql("SELECT * FROM T ORDER BY id DESC LIMIT 1")
    val qe1 = df1.queryExecution
    Assertions.assertTrue(qe1.optimizedPlan.containsPattern(SORT))
    Assertions.assertTrue(qe1.optimizedPlan.containsPattern(LIMIT))
  }

  test("Paimon pushDown: multi TopN for append-only tables") {
    assume(gteqSpark3_3)
    spark.sql("""
                |CREATE TABLE T (pt INT, id INT, price BIGINT) PARTITIONED BY (pt)
                |TBLPROPERTIES ('file-index.range-bitmap.columns'='id')
                |""".stripMargin)
    Assertions.assertTrue(getScanBuilder().isInstanceOf[SupportsPushDownTopN])

    spark.sql("""
                |INSERT INTO T VALUES
                |(1, 10, 100L),
                |(1, 20, 100L),
                |(1, 20, 200L),
                |(1, 20, 200L),
                |(1, 20, 200L),
                |(1, 20, 200L),
                |(1, 20, 300L),
                |(1, 30, 100L)
                |""".stripMargin)

    // test ASC
    checkAnswer(
      spark.sql("SELECT id, price FROM T ORDER BY id ASC, price ASC LIMIT 2"),
      Row(10, 100L) :: Row(20, 100L) :: Nil)
    checkAnswer(
      spark.sql("SELECT id, price FROM T ORDER BY id ASC, price DESC LIMIT 2"),
      Row(10, 100L) :: Row(20, 300L) :: Nil)

    // test DESC
    checkAnswer(
      spark.sql("SELECT id, price FROM T ORDER BY id DESC, price ASC LIMIT 2"),
      Row(30, 100L) :: Row(20, 100L) :: Nil)
    checkAnswer(
      spark.sql("SELECT id, price FROM T ORDER BY id DESC, price DESC LIMIT 2"),
      Row(30, 100L) :: Row(20, 300L) :: Nil)
  }

  test("Paimon pushDown: TopN for primary-key tables with deletion vector") {
    assume(gteqSpark3_3)
    withTable("dv_test") {
      spark.sql("""
                  |CREATE TABLE dv_test (id INT, c1 INT, c2 STRING) TBLPROPERTIES (
                  |'primary-key'='id',
                  |'deletion-vectors.enabled' = 'true',
                  |'file-index.range-bitmap.columns'='c1'
                  |)
                  |""".stripMargin)

      spark.sql(
        "insert into table dv_test values(1, 1, 'a'),(2, 2,'b'),(3, 3, 'c'),(4, 4, 'd'),(5, 5, 'e'),(6, NULL, 'f')")
      checkAnswer(
        spark.sql("SELECT * FROM dv_test ORDER BY c1 ASC NULLS FIRST LIMIT 3"),
        Row(6, null, "f") :: Row(1, 1, "a") :: Row(2, 2, "b") :: Nil)
      checkAnswer(
        spark.sql("SELECT * FROM dv_test ORDER BY c1 ASC NULLS LAST LIMIT 3"),
        Row(1, 1, "a") :: Row(2, 2, "b") :: Row(3, 3, "c") :: Nil)
      checkAnswer(
        spark.sql("SELECT * FROM dv_test ORDER BY c1 DESC NULLS FIRST LIMIT 3"),
        Row(6, null, "f") :: Row(5, 5, "e") :: Row(4, 4, "d") :: Nil)
      checkAnswer(
        spark.sql("SELECT * FROM dv_test ORDER BY c1 DESC NULLS LAST LIMIT 3"),
        Row(5, 5, "e") :: Row(4, 4, "d") :: Row(3, 3, "c") :: Nil)

      spark.sql("delete from dv_test where id IN (1, 5)")

      checkAnswer(
        spark.sql("SELECT * FROM dv_test ORDER BY c1 ASC NULLS FIRST LIMIT 3"),
        Row(6, null, "f") :: Row(2, 2, "b") :: Row(3, 3, "c") :: Nil)
      checkAnswer(
        spark.sql("SELECT * FROM dv_test ORDER BY c1 ASC NULLS LAST LIMIT 3"),
        Row(2, 2, "b") :: Row(3, 3, "c") :: Row(4, 4, "d") :: Nil)
      checkAnswer(
        spark.sql("SELECT * FROM dv_test ORDER BY c1 DESC NULLS FIRST LIMIT 3"),
        Row(6, null, "f") :: Row(4, 4, "d") :: Row(3, 3, "c") :: Nil)
      checkAnswer(
        spark.sql("SELECT * FROM dv_test ORDER BY c1 DESC NULLS LAST LIMIT 3"),
        Row(4, 4, "d") :: Row(3, 3, "c") :: Row(2, 2, "b") :: Nil)
    }
  }

  test("Paimon pushDown: TopN for append-only tables with deletion vector") {
    assume(gteqSpark3_3)
    withTable("dv_test") {
      spark.sql("""
                  |CREATE TABLE dv_test (c1 INT, c2 STRING) TBLPROPERTIES (
                  |'deletion-vectors.enabled' = 'true',
                  |'file-index.range-bitmap.columns'='c1'
                  |)
                  |""".stripMargin)

      spark.sql(
        "insert into table dv_test values(1, 'a'),(2, 'b'),(3, 'c'),(4, 'd'),(5, 'e'),(NULL, 'f')")
      checkAnswer(
        spark.sql("SELECT * FROM dv_test ORDER BY c1 ASC NULLS FIRST LIMIT 3"),
        Row(null, "f") :: Row(1, "a") :: Row(2, "b") :: Nil)
      checkAnswer(
        spark.sql("SELECT * FROM dv_test ORDER BY c1 ASC NULLS LAST LIMIT 3"),
        Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Nil)
      checkAnswer(
        spark.sql("SELECT * FROM dv_test ORDER BY c1 DESC NULLS FIRST LIMIT 3"),
        Row(null, "f") :: Row(5, "e") :: Row(4, "d") :: Nil)
      checkAnswer(
        spark.sql("SELECT * FROM dv_test ORDER BY c1 DESC NULLS LAST LIMIT 3"),
        Row(5, "e") :: Row(4, "d") :: Row(3, "c") :: Nil)

      spark.sql("delete from dv_test where c1 IN (1, 5)")

      checkAnswer(
        spark.sql("SELECT * FROM dv_test ORDER BY c1 ASC NULLS FIRST LIMIT 3"),
        Row(null, "f") :: Row(2, "b") :: Row(3, "c") :: Nil)
      checkAnswer(
        spark.sql("SELECT * FROM dv_test ORDER BY c1 ASC NULLS LAST LIMIT 3"),
        Row(2, "b") :: Row(3, "c") :: Row(4, "d") :: Nil)
      checkAnswer(
        spark.sql("SELECT * FROM dv_test ORDER BY c1 DESC NULLS FIRST LIMIT 3"),
        Row(null, "f") :: Row(4, "d") :: Row(3, "c") :: Nil)
      checkAnswer(
        spark.sql("SELECT * FROM dv_test ORDER BY c1 DESC NULLS LAST LIMIT 3"),
        Row(4, "d") :: Row(3, "c") :: Row(2, "b") :: Nil)
    }
  }

  test(s"Paimon pushdown: parquet in-filter") {
    withTable("T") {
      spark.sql(s"""
                   |CREATE TABLE T (a INT, b STRING) using paimon TBLPROPERTIES
                   |(
                   |'file.format' = 'parquet',
                   |'parquet.block.size' = '100',
                   |'target-file-size' = '10g',
                   |'parquet.filter.stats.enabled' = 'false' -- disable stats filter
                   |)
                   |""".stripMargin)

      val data = (0 to 1000).flatMap {
        i =>
          val uuid = java.util.UUID.randomUUID().toString
          List.fill(10)((i, uuid))
      }
      data.toDF("a", "b").createOrReplaceTempView("source")
      spark.sql("insert into T select * from source cluster by a")
      val rows = spark.sql(s"select * from T where a in (${(100 to 150).mkString(",")})").collect()
      val expected =
        spark.sql(s"select * from source where a in (${(100 to 150).mkString(",")})").collect()

      assertj.assertThat(rows).containsExactlyInAnyOrder(expected: _*)
    }
  }

  test(s"Paimon pushdown: parquet decimal filter") {
    withTable("T") {
      spark.sql(s"""
                   |CREATE TABLE T (a DECIMAL(18,4), b STRING) using paimon TBLPROPERTIES
                   |(
                   |'file.format' = 'parquet',
                   |'parquet.block.size' = '100',
                   |'target-file-size' = '10g'
                   |)
                   |""".stripMargin)

      spark.sql("""INSERT INTO T VALUES
                  |(CAST(100.1234 AS DECIMAL(18,4)), 'a'),
                  |(CAST(200.5678 AS DECIMAL(18,4)), 'b'),
                  |(CAST(300.9999 AS DECIMAL(18,4)), 'c'),
                  |(CAST(150.0000 AS DECIMAL(18,4)), 'd')
                  |""".stripMargin)

      // Test equals filter
      checkAnswer(
        spark.sql("SELECT * FROM T WHERE a = CAST(100.1234 AS DECIMAL(18,4))"),
        Row(new java.math.BigDecimal("100.1234"), "a") :: Nil
      )

      // Test comparison filter
      checkAnswer(
        spark.sql("SELECT * FROM T WHERE a < CAST(200.0000 AS DECIMAL(18,4)) ORDER BY a"),
        Row(new java.math.BigDecimal("100.1234"), "a") ::
          Row(new java.math.BigDecimal("150.0000"), "d") :: Nil
      )

      // Test in filter
      checkAnswer(
        spark.sql(
          "SELECT * FROM T WHERE a IN (CAST(100.1234 AS DECIMAL(18,4)), CAST(300.9999 AS DECIMAL(18,4))) ORDER BY a"),
        Row(new java.math.BigDecimal("100.1234"), "a") ::
          Row(new java.math.BigDecimal("300.9999"), "c") :: Nil
      )
    }
  }

  private def getScanBuilder(tableName: String = "T"): ScanBuilder = {
    SparkTable(loadTable(tableName)).newScanBuilder(CaseInsensitiveStringMap.empty())
  }

  def checkFilterExists(sql: String): Boolean = {
    spark.sql(sql).queryExecution.optimizedPlan.exists {
      case Filter(_: Expression, _) => true
      case _ => false
    }
  }

  def checkEqualToFilterExists(sql: String, name: String, value: Literal): Boolean = {
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

  def collectFilteredInputSplits(plan: SparkPlan, tableName: String): Seq[Split] = {
    flatMap(plan) {
      case s: BatchScanExec =>
        s.scan match {
          case p: PaimonScan if p.table.name() == tableName =>
            val filteredPartitionsField = s.getClass.getDeclaredField("filteredPartitions")
            filteredPartitionsField.setAccessible(true)
            val filteredPartitions = if (gteqSpark3_3) {
              filteredPartitionsField.get(s).asInstanceOf[Seq[Seq[InputPartition]]].flatten
            } else {
              filteredPartitionsField.get(s).asInstanceOf[Seq[InputPartition]]
            }
            filteredPartitions.flatMap { case p: PaimonInputPartition => p.splits }
          case _ => Nil
        }
      case _ => Nil
    }
  }
}
