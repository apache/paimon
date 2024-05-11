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
import org.junit.jupiter.api.Assertions

import java.sql.Date

class DataFrameWriteTest extends PaimonSparkTestBase {

  test("Paimon: DataFrameWrite.saveAsTable") {

    import testImplicits._

    Seq((1L, "x1"), (2L, "x2"))
      .toDF("a", "b")
      .write
      .format("paimon")
      .mode("append")
      .option("primary-key", "a")
      .option("bucket", "-1")
      .option("target-file-size", "256MB")
      .option("write.merge-schema", "true")
      .option("write.merge-schema.explicit-cast", "true")
      .saveAsTable("test_ctas")

    val paimonTable = loadTable("test_ctas")
    Assertions.assertEquals(1, paimonTable.primaryKeys().size())
    Assertions.assertEquals("a", paimonTable.primaryKeys().get(0))

    // check all the core options
    Assertions.assertEquals("-1", paimonTable.options().get("bucket"))
    Assertions.assertEquals("256MB", paimonTable.options().get("target-file-size"))

    // non-core options should not be here.
    Assertions.assertFalse(paimonTable.options().containsKey("write.merge-schema"))
    Assertions.assertFalse(paimonTable.options().containsKey("write.merge-schema.explicit-cast"))
  }

  withPk.foreach {
    hasPk =>
      bucketModes.foreach {
        bucket =>
          test(s"Write data into Paimon directly: has-pk: $hasPk, bucket: $bucket") {

            val _spark = spark
            import _spark.implicits._

            val prop = if (hasPk) {
              s"'primary-key'='a', 'bucket' = '$bucket' "
            } else if (bucket != -1) {
              s"'bucket-key'='a', 'bucket' = '$bucket' "
            } else {
              ""
            }

            spark.sql(s"""
                         |CREATE TABLE T (a INT, b STRING)
                         |TBLPROPERTIES ($prop)
                         |""".stripMargin)

            val paimonTable = loadTable("T")
            val location = paimonTable.location().toString

            val df1 = Seq((1, "a"), (2, "b")).toDF("a", "b")
            df1.write.format("paimon").mode("append").save(location)
            checkAnswer(
              spark.sql("SELECT * FROM T ORDER BY a, b"),
              Row(1, "a") :: Row(2, "b") :: Nil)

            val df2 = Seq((1, "a2"), (3, "c")).toDF("a", "b")
            df2.write.format("paimon").mode("append").save(location)
            val expected = if (hasPk) {
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

  withPk.foreach {
    hasPk =>
      bucketModes.foreach {
        bucket =>
          test(s"Schema evolution: write data into Paimon: $hasPk, bucket: $bucket") {
            val _spark = spark
            import _spark.implicits._

            val prop = if (hasPk) {
              s"'primary-key'='a', 'bucket' = '$bucket' "
            } else if (bucket != -1) {
              s"'bucket-key'='a', 'bucket' = '$bucket' "
            } else {
              ""
            }

            spark.sql(s"""
                         |CREATE TABLE T (a INT, b STRING)
                         |TBLPROPERTIES ($prop)
                         |""".stripMargin)

            val paimonTable = loadTable("T")
            val location = paimonTable.location().toString

            val df1 = Seq((1, "a"), (2, "b")).toDF("a", "b")
            df1.write.format("paimon").mode("append").save(location)
            checkAnswer(
              spark.sql("SELECT * FROM T ORDER BY a, b"),
              Row(1, "a") :: Row(2, "b") :: Nil)

            // Case 1: two additional fields
            val df2 = Seq((1, "a2", 123L, Map("k" -> 11.1)), (3, "c", 345L, Map("k" -> 33.3)))
              .toDF("a", "b", "c", "d")
            df2.write
              .format("paimon")
              .mode("append")
              .option("write.merge-schema", "true")
              .save(location)
            val expected2 = if (hasPk) {
              Row(1, "a2", 123L, Map("k" -> 11.1)) ::
                Row(2, "b", null, null) :: Row(3, "c", 345L, Map("k" -> 33.3)) :: Nil
            } else {
              Row(1, "a", null, null) :: Row(1, "a2", 123L, Map("k" -> 11.1)) :: Row(
                2,
                "b",
                null,
                null) :: Row(3, "c", 345L, Map("k" -> 33.3)) :: Nil
            }
            checkAnswer(spark.sql("SELECT * FROM T ORDER BY a, b"), expected2)

            // Case 2: two fields with the evolved types: Int -> Long, Long -> Decimal
            val df3 = Seq(
              (2L, "b2", BigDecimal.decimal(234), Map("k" -> 22.2)),
              (4L, "d", BigDecimal.decimal(456), Map("k" -> 44.4))).toDF("a", "b", "c", "d")
            df3.write
              .format("paimon")
              .mode("append")
              .option("write.merge-schema", "true")
              .save(location)
            val expected3 = if (hasPk) {
              Row(1L, "a2", BigDecimal.decimal(123), Map("k" -> 11.1)) :: Row(
                2L,
                "b2",
                BigDecimal.decimal(234),
                Map("k" -> 22.2)) :: Row(3L, "c", BigDecimal.decimal(345), Map("k" -> 33.3)) :: Row(
                4L,
                "d",
                BigDecimal.decimal(456),
                Map("k" -> 44.4)) :: Nil
            } else {
              Row(1L, "a", null, null) :: Row(
                1L,
                "a2",
                BigDecimal.decimal(123),
                Map("k" -> 11.1)) :: Row(2L, "b", null, null) :: Row(
                2L,
                "b2",
                BigDecimal.decimal(234),
                Map("k" -> 22.2)) :: Row(3L, "c", BigDecimal.decimal(345), Map("k" -> 33.3)) :: Row(
                4L,
                "d",
                BigDecimal.decimal(456),
                Map("k" -> 44.4)) :: Nil
            }
            checkAnswer(spark.sql("SELECT * FROM T ORDER BY a, b"), expected3)

          }
      }
  }

  withPk.foreach {
    hasPk =>
      bucketModes.foreach {
        bucket =>
          test(
            s"Schema evolution: write data into Paimon with allowExplicitCast = true: $hasPk, bucket: $bucket") {
            val _spark = spark
            import _spark.implicits._

            val prop = if (hasPk) {
              s"'primary-key'='a', 'bucket' = '$bucket' "
            } else if (bucket != -1) {
              s"'bucket-key'='a', 'bucket' = '$bucket' "
            } else {
              ""
            }

            spark.sql(s"""
                         |CREATE TABLE T (a INT, b STRING)
                         |TBLPROPERTIES ($prop)
                         |""".stripMargin)

            val paimonTable = loadTable("T")
            val location = paimonTable.location().toString

            val df1 = Seq((1, "2023-08-01"), (2, "2023-08-02")).toDF("a", "b")
            df1.write.format("paimon").mode("append").save(location)
            checkAnswer(
              spark.sql("SELECT * FROM T ORDER BY a, b"),
              Row(1, "2023-08-01") :: Row(2, "2023-08-02") :: Nil)

            // Case 1: two additional fields: DoubleType and TimestampType
            val ts = java.sql.Timestamp.valueOf("2023-08-01 10:00:00.0")
            val df2 = Seq((1, "2023-08-01", 12.3d, ts), (3, "2023-08-03", 34.5d, ts))
              .toDF("a", "b", "c", "d")
            df2.write
              .format("paimon")
              .mode("append")
              .option("write.merge-schema", "true")
              .save(location)
            val expected2 = if (hasPk) {
              Row(1, "2023-08-01", 12.3d, ts) ::
                Row(2, "2023-08-02", null, null) :: Row(3, "2023-08-03", 34.5d, ts) :: Nil
            } else {
              Row(1, "2023-08-01", null, null) :: Row(1, "2023-08-01", 12.3d, ts) :: Row(
                2,
                "2023-08-02",
                null,
                null) :: Row(3, "2023-08-03", 34.5d, ts) :: Nil
            }
            checkAnswer(spark.sql("SELECT * FROM T ORDER BY a, b"), expected2)

            // Case 2: a: Int -> Long, b: String -> Date, c: Long -> Int, d: Map -> String
            val date = java.sql.Date.valueOf("2023-07-31")
            val df3 = Seq((2L, date, 234, null), (4L, date, 456, "2023-08-01 11:00:00.0")).toDF(
              "a",
              "b",
              "c",
              "d")

            // throw UnsupportedOperationException if write.merge-schema.explicit-cast = false
            assertThrows[UnsupportedOperationException] {
              df3.write
                .format("paimon")
                .mode("append")
                .option("write.merge-schema", "true")
                .save(location)
            }
            // merge schema and write data when write.merge-schema.explicit-cast = true
            df3.write
              .format("paimon")
              .mode("append")
              .option("write.merge-schema", "true")
              .option("write.merge-schema.explicit-cast", "true")
              .save(location)
            val expected3 = if (hasPk) {
              Row(1L, Date.valueOf("2023-08-01"), 12, ts.toString) :: Row(
                2L,
                date,
                234,
                null) :: Row(3L, Date.valueOf("2023-08-03"), 34, ts.toString) :: Row(
                4L,
                date,
                456,
                "2023-08-01 11:00:00.0") :: Nil
            } else {
              Row(1L, Date.valueOf("2023-08-01"), null, null) :: Row(
                1L,
                Date.valueOf("2023-08-01"),
                12,
                ts.toString) :: Row(2L, date, 234, null) :: Row(
                2L,
                Date.valueOf("2023-08-02"),
                null,
                null) :: Row(3L, Date.valueOf("2023-08-03"), 34, ts.toString) :: Row(
                4L,
                date,
                456,
                "2023-08-01 11:00:00.0") :: Nil
            }
            checkAnswer(
              spark.sql("SELECT a, b, c, substring(d, 0, 21) FROM T ORDER BY a, b"),
              expected3)

          }
      }
  }

}
