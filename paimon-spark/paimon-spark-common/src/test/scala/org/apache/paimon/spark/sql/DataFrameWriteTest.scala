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
import org.apache.spark.sql.types.DecimalType
import org.junit.jupiter.api.Assertions

import java.sql.{Date, Timestamp}

class DataFrameWriteTest extends PaimonSparkTestBase {
  import testImplicits._

  test("Paimon: DataFrameWrite.saveAsTable") {
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

  fileFormats.foreach {
    fileFormat =>
      test(s"Paimon: DataFrameWrite.saveAsTable in ByName mode, file.format: $fileFormat") {
        withTable("t1", "t2") {
          spark.sql(s"""
                       |CREATE TABLE t1 (col1 STRING, col2 INT, col3 DOUBLE)
                       |TBLPROPERTIES ('file.format' = '$fileFormat')
                       |""".stripMargin)

          spark.sql(s"""
                       |CREATE TABLE t2 (col2 INT, col3 DOUBLE, col1 STRING)
                       |TBLPROPERTIES ('file.format' = '$fileFormat')
                       |""".stripMargin)

          sql(s"""
                 |INSERT INTO TABLE t1 VALUES
                 |("Hello", 1, 1.1),
                 |("World", 2, 2.2),
                 |("Paimon", 3, 3.3);
                 |""".stripMargin)

          spark.table("t1").write.format("paimon").mode("append").saveAsTable("t2")
          checkAnswer(
            sql("SELECT * FROM t2 ORDER BY col2"),
            Row(1, 1.1d, "Hello") :: Row(2, 2.2d, "World") :: Row(3, 3.3d, "Paimon") :: Nil)
        }
      }
  }

  fileFormats.foreach {
    fileFormat =>
      test(
        s"Paimon: DataFrameWrite.saveAsTable with complex data type in ByName mode, file.format: $fileFormat") {
        withTable("t1", "t2") {
          spark.sql(
            s"""
               |CREATE TABLE t1 (a STRING, b INT, c STRUCT<c1:DOUBLE, c2:LONG>, d ARRAY<STRUCT<d1 TIMESTAMP, d2 MAP<STRING, STRING>>>, e ARRAY<INT>)
               |TBLPROPERTIES ('file.format' = '$fileFormat')
               |""".stripMargin)

          spark.sql(
            s"""
               |CREATE TABLE t2 (b INT, c STRUCT<c2:LONG, c1:DOUBLE>, d ARRAY<STRUCT<d2 MAP<STRING, STRING>, d1 TIMESTAMP>>, e ARRAY<INT>, a STRING)
               |TBLPROPERTIES ('file.format' = '$fileFormat')
               |""".stripMargin)

          sql(s"""
                 |INSERT INTO TABLE t1 VALUES
                 |("Hello", 1, struct(1.1, 1000), array(struct(timestamp'2024-01-01 00:00:00', map("k1", "v1")), struct(timestamp'2024-08-01 00:00:00', map("k1", "v11"))), array(123, 345)),
                 |("World", 2, struct(2.2, 2000), array(struct(timestamp'2024-02-01 00:00:00', map("k2", "v2"))), array(234, 456)),
                 |("Paimon", 3, struct(3.3, 3000), null, array(345, 567));
                 |""".stripMargin)

          spark.table("t1").write.format("paimon").mode("append").saveAsTable("t2")
          checkAnswer(
            sql("SELECT * FROM t2 ORDER BY b"),
            Row(
              1,
              Row(1000L, 1.1d),
              Array(
                Row(Map("k1" -> "v1"), Timestamp.valueOf("2024-01-01 00:00:00")),
                Row(Map("k1" -> "v11"), Timestamp.valueOf("2024-08-01 00:00:00"))),
              Array(123, 345),
              "Hello"
            )
              :: Row(
                2,
                Row(2000L, 2.2d),
                Array(Row(Map("k2" -> "v2"), Timestamp.valueOf("2024-02-01 00:00:00"))),
                Array(234, 456),
                "World")
              :: Row(3, Row(3000L, 3.3d), null, Array(345, 567), "Paimon") :: Nil
          )
        }
      }
  }

  withPk.foreach {
    hasPk =>
      bucketModes.foreach {
        bucket =>
          test(s"Write data into Paimon directly: has-pk: $hasPk, bucket: $bucket") {

            val prop = if (hasPk) {
              s"'primary-key'='a', 'bucket' = '$bucket' "
            } else if (bucket != -1) {
              s"'bucket-key'='a', 'bucket' = '$bucket' "
            } else {
              "'write-only'='true'"
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

  fileFormats.foreach {
    format =>
      withPk.foreach {
        hasPk =>
          bucketModes.foreach {
            bucket =>
              test(
                s"Schema evolution: write data into Paimon: $hasPk, bucket: $bucket, format: $format") {
                val _spark = spark
                import _spark.implicits._

                val prop = if (hasPk) {
                  s"'primary-key'='a', 'bucket' = '$bucket', 'file.format' = '$format'"
                } else if (bucket != -1) {
                  s"'bucket-key'='a', 'bucket' = '$bucket', 'file.format' = '$format'"
                } else {
                  s"'write-only'='true', 'file.format' = '$format'"
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
                    Map("k" -> 22.2)) :: Row(
                    3L,
                    "c",
                    BigDecimal.decimal(345),
                    Map("k" -> 33.3)) :: Row(
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
                    Map("k" -> 22.2)) :: Row(
                    3L,
                    "c",
                    BigDecimal.decimal(345),
                    Map("k" -> 33.3)) :: Row(
                    4L,
                    "d",
                    BigDecimal.decimal(456),
                    Map("k" -> 44.4)) :: Nil
                }
                checkAnswer(spark.sql("SELECT * FROM T ORDER BY a, b"), expected3)

                // Case 3: insert Decimal(20,18) to Decimal(38,18)
                val df4 = Seq((99L, "df4", BigDecimal.decimal(4.0), Map("4" -> 4.1)))
                  .toDF("a", "b", "c", "d")
                  .selectExpr("a", "b", "cast(c as decimal(20,18)) as c", "d")
                df4.write
                  .format("paimon")
                  .mode("append")
                  .option("write.merge-schema", "true")
                  .save(location)
                val expected4 =
                  expected3 ++ Seq(Row(99L, "df4", BigDecimal.decimal(4.0), Map("4" -> 4.1)))
                checkAnswer(spark.sql("SELECT * FROM T ORDER BY a, b"), expected4)
                val decimalType =
                  spark.table("T").schema.apply(2).dataType.asInstanceOf[DecimalType]
                assert(decimalType.precision == 38)
                assert(decimalType.scale == 18)
              }
          }
      }
  }

  withPk.foreach {
    hasPk =>
      bucketModes.foreach {
        bucket =>
          test(
            s"Schema evolution: write data into Paimon with allowExplicitCast = true: $hasPk, bucket: $bucket") {

            val prop = if (hasPk) {
              s"'primary-key'='a', 'bucket' = '$bucket' "
            } else if (bucket != -1) {
              s"'bucket-key'='a', 'bucket' = '$bucket' "
            } else {
              "'write-only'='true'"
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

  withPk.foreach {
    hasPk =>
      test(s"Support v2 write with overwrite, hasPk: $hasPk") {
        withTable("t") {
          val prop = if (hasPk) {
            "'primary-key'='c1'"
          } else {
            "'write-only'='true'"
          }
          spark.sql(s"""
                       |CREATE TABLE t (c1 INT, c2 STRING) PARTITIONED BY(p1 String, p2 string)
                       |TBLPROPERTIES ($prop)
                       |""".stripMargin)

          spark
            .range(3)
            .selectExpr("id as c1", "id as c2", "'a' as p1", "id as p2")
            .writeTo("t")
            .overwrite($"p1" === "a")
          checkAnswer(
            spark.sql("SELECT * FROM t ORDER BY c1"),
            Row(0, "0", "a", "0") :: Row(1, "1", "a", "1") :: Row(2, "2", "a", "2") :: Nil
          )

          spark
            .range(7, 10)
            .selectExpr("id as c1", "id as c2", "'a' as p1", "id as p2")
            .writeTo("t")
            .overwrite($"p1" === "a")
          checkAnswer(
            spark.sql("SELECT * FROM t ORDER BY c1"),
            Row(7, "7", "a", "7") :: Row(8, "8", "a", "8") :: Row(9, "9", "a", "9") :: Nil
          )

          spark
            .range(2)
            .selectExpr("id as c1", "id as c2", "'a' as p1", "9 as p2")
            .writeTo("t")
            .overwrite(($"p1" <=> "a").and($"p2" === "9"))
          checkAnswer(
            spark.sql("SELECT * FROM t ORDER BY c1"),
            Row(0, "0", "a", "9") :: Row(1, "1", "a", "9") :: Row(7, "7", "a", "7") ::
              Row(8, "8", "a", "8") :: Nil
          )

          // bad case
          val msg1 = intercept[Exception] {
            spark
              .range(2)
              .selectExpr("id as c1", "id as c2", "'a' as p1", "id as p2")
              .writeTo("t")
              .overwrite($"p1" =!= "a")
          }.getMessage
          assert(msg1.contains("Only support Overwrite filters with Equal and EqualNullSafe"))

          val msg2 = intercept[Exception] {
            spark
              .range(2)
              .selectExpr("id as c1", "id as c2", "'a' as p1", "id as p2")
              .writeTo("t")
              .overwrite($"p1" === $"c2")
          }.getMessage
          assert(msg2.contains("Table does not support overwrite by expression"))

          val msg3 = intercept[Exception] {
            spark
              .range(2)
              .selectExpr("id as c1", "id as c2", "'a' as p1", "id as p2")
              .writeTo("t")
              .overwrite($"c1" === ($"c2" + 1))
          }.getMessage
          assert(msg3.contains("cannot translate expression to source filter"))

          val msg4 = intercept[Exception] {
            spark
              .range(2)
              .selectExpr("id as c1", "id as c2", "'a' as p1", "id as p2")
              .writeTo("t")
              .overwrite(($"p1" === "a").and($"p1" === "b"))
          }.getMessage
          assert(msg4.contains("Only support Overwrite with one filter for each partition column"))

          // Overwrite a partition which is not the specified
          val msg5 = intercept[Exception] {
            spark
              .range(2)
              .selectExpr("id as c1", "id as c2", "'a' as p1", "id as p2")
              .writeTo("t")
              .overwrite($"p1" === "b")
          }.getMessage
          assert(msg5.contains("does not belong to this partition"))
        }
      }
  }
}
