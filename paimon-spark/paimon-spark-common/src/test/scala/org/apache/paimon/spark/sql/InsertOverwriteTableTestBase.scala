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
import org.apache.spark.sql.types._

import java.sql.{Date, Timestamp}

abstract class InsertOverwriteTableTestBase extends PaimonSparkTestBase {

  fileFormats.foreach {
    fileFormat =>
      Seq(true, false).foreach {
        isPartitioned =>
          test(
            s"Paimon: insert into/overwrite in ByName mode, file.format: $fileFormat, isPartitioned: $isPartitioned") {
            withTable("t1", "t2") {
              val partitionedSQL = if (isPartitioned) {
                "PARTITIONED BY (col4)"
              } else {
                ""
              }
              spark.sql(s"""
                           |CREATE TABLE t1 (col1 STRING, col2 INT, col3 DOUBLE, col4 STRING)
                           |$partitionedSQL
                           |TBLPROPERTIES ('file.format' = '$fileFormat')
                           |""".stripMargin)

              spark.sql(
                s"""
                   |CREATE TABLE t2 (col2 INT, col3 DOUBLE, col1 STRING NOT NULL, col4 STRING)
                   |$partitionedSQL
                   |TBLPROPERTIES ('file.format' = '$fileFormat')
                   |""".stripMargin)

              sql(s"""
                     |INSERT INTO TABLE t1 VALUES
                     |("Hello", 1, 1.1, "pt1"),
                     |("Paimon", 3, 3.3, "pt2");
                     |""".stripMargin)

              sql("INSERT INTO t2 (col1, col2, col3, col4) SELECT * FROM t1")
              checkAnswer(
                sql("SELECT * FROM t2 ORDER BY col2"),
                Row(1, 1.1d, "Hello", "pt1") :: Row(3, 3.3d, "Paimon", "pt2") :: Nil)

              sql("INSERT INTO TABLE t1 BY NAME SELECT col3, col2, col4, col1 FROM t1")
              sql("INSERT OVERWRITE t2 (col1, col2, col3, col4) SELECT * FROM t1")
              checkAnswer(
                sql("SELECT * FROM t2 ORDER BY col2"),
                Row(1, 1.1d, "Hello", "pt1") :: Row(1, 1.1d, "Hello", "pt1") ::
                  Row(3, 3.3d, "Paimon", "pt2") :: Row(3, 3.3d, "Paimon", "pt2") :: Nil
              )

              // null for non-specified column
              sql("INSERT OVERWRITE TABLE t2 BY NAME SELECT col1, col2 FROM t2 ")
              checkAnswer(
                sql("SELECT * FROM t2 ORDER BY col2"),
                Row(1, null, "Hello", null) :: Row(1, null, "Hello", null) ::
                  Row(3, null, "Paimon", null) :: Row(3, null, "Paimon", null) :: Nil
              )

              // by name bad case
              // names conflict
              val msg1 = intercept[Exception] {
                sql("INSERT INTO TABLE t1 BY NAME SELECT col1, col2 as col1 FROM t1")
              }
              assert(msg1.getMessage.contains("due to column name conflicts"))
              // name does not match
              val msg2 = intercept[Exception] {
                sql("INSERT INTO TABLE t1 BY NAME SELECT col1, col2 as colx FROM t1")
              }
              assert(msg2.getMessage.contains("due to unknown column names"))
              // query column size bigger than table's
              val msg3 = intercept[Exception] {
                sql(
                  "INSERT INTO TABLE t1 BY NAME SELECT col1, col2, col3, col4, col4 as col5 FROM t1")
              }
              assert(
                msg3.getMessage.contains(
                  "the number of data columns don't match with the table schema"))
              // non-nullable column has no specified value
              val msg4 = intercept[Exception] {
                sql("INSERT INTO TABLE t2 BY NAME SELECT col2 FROM t2")
              }
              assert(msg4.getMessage.contains("non-nullable column `col1` has no specified value"))

              // by position
              // column size does not match
              val msg5 = intercept[Exception] {
                sql("INSERT INTO TABLE t1 VALUES(1)")
              }
              assert(
                msg5.getMessage.contains(
                  "the number of data columns don't match with the table schema"))
            }
          }
      }
  }

  withPk.foreach {
    hasPk =>
      bucketModes.foreach {
        bucket =>
          test(s"insert overwrite non-partitioned table: hasPk: $hasPk, bucket: $bucket") {
            val prop = if (hasPk) {
              s"'primary-key'='a,b', 'bucket' = '$bucket' "
            } else if (bucket != -1) {
              s"'bucket-key'='a,b', 'bucket' = '$bucket' "
            } else {
              "'write-only'='true'"
            }

            spark.sql(s"""
                         |CREATE TABLE T (a INT, b INT, c STRING)
                         |TBLPROPERTIES ($prop)
                         |""".stripMargin)

            spark.sql("INSERT INTO T values (1, 1, '1'), (2, 2, '2')")
            checkAnswer(
              spark.sql("SELECT * FROM T ORDER BY a, b"),
              Row(1, 1, "1") :: Row(2, 2, "2") :: Nil)

            spark.sql("INSERT OVERWRITE T VALUES (1, 3, '3'), (2, 4, '4')");
            checkAnswer(
              spark.sql("SELECT * FROM T ORDER BY a, b"),
              Row(1, 3, "3") :: Row(2, 4, "4") :: Nil)
          }
      }
  }

  withPk.foreach {
    hasPk =>
      bucketModes.foreach {
        bucket =>
          test(s"insert overwrite single-partitioned table: hasPk: $hasPk, bucket: $bucket") {
            val prop = if (hasPk) {
              s"'primary-key'='a,b', 'bucket' = '$bucket' "
            } else if (bucket != -1) {
              s"'bucket-key'='b', 'bucket' = '$bucket' "
            } else {
              "'write-only'='true'"
            }

            spark.sql(s"""
                         |CREATE TABLE T (a INT, b INT, c STRING)
                         |TBLPROPERTIES ($prop)
                         |PARTITIONED BY (a)
                         |""".stripMargin)

            spark.sql("INSERT INTO T values (1, 1, '1'), (2, 2, '2')")
            checkAnswer(
              spark.sql("SELECT * FROM T ORDER BY a, b"),
              Row(1, 1, "1") :: Row(2, 2, "2") :: Nil)

            // overwrite the whole table
            spark.sql("INSERT OVERWRITE T VALUES (1, 3, '3'), (2, 4, '4')")
            checkAnswer(
              spark.sql("SELECT * FROM T ORDER BY a, b"),
              Row(1, 3, "3") :: Row(2, 4, "4") :: Nil)

            // overwrite the a=1 partition
            spark.sql("INSERT OVERWRITE T PARTITION (a = 1) VALUES (5, '5'), (7, '7')")
            checkAnswer(
              spark.sql("SELECT * FROM T ORDER BY a, b"),
              Row(1, 5, "5") :: Row(1, 7, "7") :: Row(2, 4, "4") :: Nil)

          }
      }
  }

  withPk.foreach {
    hasPk =>
      bucketModes.foreach {
        bucket =>
          test(s"insert overwrite mutil-partitioned table: hasPk: $hasPk, bucket: $bucket") {
            val prop = if (hasPk) {
              s"'primary-key'='a,pt1,pt2', 'bucket' = '$bucket' "
            } else if (bucket != -1) {
              s"'bucket-key'='a', 'bucket' = '$bucket' "
            } else {
              "'write-only'='true'"
            }

            spark.sql(s"""
                         |CREATE TABLE T (a INT, b STRING, pt1 STRING, pt2 INT)
                         |TBLPROPERTIES ($prop)
                         |PARTITIONED BY (pt1, pt2)
                         |""".stripMargin)

            spark.sql(
              "INSERT INTO T values (1, 'a', 'ptv1', 11), (2, 'b', 'ptv1', 11), (3, 'c', 'ptv1', 22), (4, 'd', 'ptv2', 22)")
            checkAnswer(
              spark.sql("SELECT * FROM T ORDER BY a"),
              Row(1, "a", "ptv1", 11) :: Row(2, "b", "ptv1", 11) :: Row(3, "c", "ptv1", 22) :: Row(
                4,
                "d",
                "ptv2",
                22) :: Nil)

            // overwrite the pt2=22 partition
            spark.sql(
              "INSERT OVERWRITE T PARTITION (pt2 = 22) VALUES (3, 'c2', 'ptv1'), (4, 'd2', 'ptv3')")
            checkAnswer(
              spark.sql("SELECT * FROM T ORDER BY a"),
              Row(1, "a", "ptv1", 11) :: Row(2, "b", "ptv1", 11) :: Row(3, "c2", "ptv1", 22) :: Row(
                4,
                "d2",
                "ptv3",
                22) :: Nil)

            // overwrite the pt1=ptv3 partition
            spark.sql("INSERT OVERWRITE T PARTITION (pt1 = 'ptv3') VALUES (4, 'd3', 22)")
            checkAnswer(
              spark.sql("SELECT * FROM T ORDER BY a"),
              Row(1, "a", "ptv1", 11) :: Row(2, "b", "ptv1", 11) :: Row(3, "c2", "ptv1", 22) :: Row(
                4,
                "d3",
                "ptv3",
                22) :: Nil)

            // overwrite the pt1=ptv1, pt2=11 partition
            spark.sql("INSERT OVERWRITE T PARTITION (pt1 = 'ptv1', pt2=11) VALUES (5, 'e')")
            checkAnswer(
              spark.sql("SELECT * FROM T ORDER BY a"),
              Row(3, "c2", "ptv1", 22) :: Row(4, "d3", "ptv3", 22) :: Row(
                5,
                "e",
                "ptv1",
                11) :: Nil)

            // overwrite the whole table
            spark.sql(
              "INSERT OVERWRITE T VALUES (1, 'a5', 'ptv1', 11), (3, 'c5', 'ptv1', 22), (4, 'd5', 'ptv3', 22)")
            checkAnswer(
              spark.sql("SELECT * FROM T ORDER BY a"),
              Row(1, "a5", "ptv1", 11) :: Row(3, "c5", "ptv1", 22) :: Row(
                4,
                "d5",
                "ptv3",
                22) :: Nil)
          }
      }
  }

  // These cases that date/timestamp/bool is used as the partition field type are to be supported.
  Seq(IntegerType, LongType, FloatType, DoubleType, DecimalType).foreach {
    dataType =>
      test(s"insert overwrite table using $dataType as the partition field type") {
        case class PartitionSQLAndValue(sql: Any, value: Any)

        val (ptField, sv1, sv2) = dataType match {
          case IntegerType =>
            ("INT", PartitionSQLAndValue(1, 1), PartitionSQLAndValue(2, 2))
          case LongType =>
            ("LONG", PartitionSQLAndValue(1L, 1L), PartitionSQLAndValue(2L, 2L))
          case FloatType =>
            ("FLOAT", PartitionSQLAndValue(12.3f, 12.3f), PartitionSQLAndValue(45.6f, 45.6f))
          case DoubleType =>
            ("DOUBLE", PartitionSQLAndValue(12.3d, 12.3), PartitionSQLAndValue(45.6d, 45.6))
          case DecimalType =>
            (
              "DECIMAL(5, 2)",
              PartitionSQLAndValue(11.222, 11.22),
              PartitionSQLAndValue(66.777, 66.78))
        }

        spark.sql(s"""
                     |CREATE TABLE T (a INT, b STRING, pt $ptField)
                     |PARTITIONED BY (pt)
                     |""".stripMargin)

        spark.sql(s"INSERT INTO T SELECT 1, 'a', ${sv1.sql} UNION ALL SELECT 2, 'b', ${sv2.sql}")
        checkAnswer(
          spark.sql("SELECT * FROM T ORDER BY a"),
          Row(1, "a", sv1.value) :: Row(2, "b", sv2.value) :: Nil)

        // overwrite the whole table
        spark.sql(
          s"INSERT OVERWRITE T SELECT 3, 'c', ${sv1.sql} UNION ALL SELECT 4, 'd', ${sv2.sql}")
        checkAnswer(
          spark.sql("SELECT * FROM T ORDER BY a"),
          Row(3, "c", sv1.value) :: Row(4, "d", sv2.value) :: Nil)

        // overwrite the a=1 partition
        spark.sql(s"INSERT OVERWRITE T PARTITION (pt = ${sv1.value}) VALUES (5, 'e'), (7, 'g')")
        checkAnswer(
          spark.sql("SELECT * FROM T ORDER BY a"),
          Row(4, "d", sv2.value) :: Row(5, "e", sv1.value) :: Row(7, "g", sv1.value) :: Nil)
      }
  }

  withPk.foreach {
    hasPk =>
      bucketModes.foreach {
        bucket =>
          test(
            s"dynamic insert overwrite single-partitioned table: hasPk: $hasPk, bucket: $bucket") {
            val prop = if (hasPk) {
              s"'primary-key'='a,b', 'bucket' = '$bucket' "
            } else if (bucket != -1) {
              s"'bucket-key'='b', 'bucket' = '$bucket' "
            } else {
              "'write-only'='true'"
            }

            spark.sql(s"""
                         |CREATE TABLE T (a INT, b INT, c STRING)
                         |TBLPROPERTIES ($prop)
                         |PARTITIONED BY (a)
                         |""".stripMargin)

            spark.sql("INSERT INTO T values (1, 1, '1'), (2, 2, '2')")
            checkAnswer(
              spark.sql("SELECT * FROM T ORDER BY a, b"),
              Row(1, 1, "1") :: Row(2, 2, "2") :: Nil)

            // overwrite the whole table
            spark.sql("INSERT OVERWRITE T VALUES (1, 3, '3'), (2, 4, '4')")
            checkAnswer(
              spark.sql("SELECT * FROM T ORDER BY a, b"),
              Row(1, 3, "3") :: Row(2, 4, "4") :: Nil)

            withSQLConf("spark.sql.sources.partitionOverwriteMode" -> "dynamic") {
              // dynamic overwrite the a=1 partition
              spark.sql("INSERT OVERWRITE T VALUES (1, 5, '5'), (1, 7, '7')")
              checkAnswer(
                spark.sql("SELECT * FROM T ORDER BY a, b"),
                Row(1, 5, "5") :: Row(1, 7, "7") :: Row(2, 4, "4") :: Nil)
            }
          }
      }
  }

  withPk.foreach {
    hasPk =>
      bucketModes.foreach {
        bucket =>
          test(
            s"dynamic insert overwrite mutil-partitioned table: hasPk: $hasPk, bucket: $bucket") {
            val prop = if (hasPk) {
              s"'primary-key'='a,pt1,pt2', 'bucket' = '$bucket' "
            } else if (bucket != -1) {
              s"'bucket-key'='a', 'bucket' = '$bucket' "
            } else {
              "'write-only'='true'"
            }

            spark.sql(s"""
                         |CREATE TABLE T (a INT, b STRING, pt1 STRING, pt2 INT)
                         |TBLPROPERTIES ($prop)
                         |PARTITIONED BY (pt1, pt2)
                         |""".stripMargin)

            spark.sql(
              "INSERT INTO T values (1, 'a', 'ptv1', 11), (2, 'b', 'ptv1', 11), (3, 'c', 'ptv1', 22), (4, 'd', 'ptv2', 22)")
            checkAnswer(
              spark.sql("SELECT * FROM T ORDER BY a, b"),
              Row(1, "a", "ptv1", 11) :: Row(2, "b", "ptv1", 11) :: Row(3, "c", "ptv1", 22) :: Row(
                4,
                "d",
                "ptv2",
                22) :: Nil)

            withSQLConf("spark.sql.sources.partitionOverwriteMode" -> "dynamic") {
              // dynamic overwrite the pt2=22 partition
              spark.sql(
                "INSERT OVERWRITE T PARTITION (pt2 = 22) VALUES (3, 'c2', 'ptv1'), (4, 'd2', 'ptv3')")
              checkAnswer(
                spark.sql("SELECT * FROM T ORDER BY a, b"),
                Row(1, "a", "ptv1", 11) :: Row(2, "b", "ptv1", 11) :: Row(
                  3,
                  "c2",
                  "ptv1",
                  22) :: Row(4, "d", "ptv2", 22) :: Row(4, "d2", "ptv3", 22) :: Nil
              )

              // dynamic overwrite the pt1=ptv3 partition
              spark.sql("INSERT OVERWRITE T PARTITION (pt1 = 'ptv3') VALUES (4, 'd3', 22)")
              checkAnswer(
                spark.sql("SELECT * FROM T ORDER BY a, b"),
                Row(1, "a", "ptv1", 11) :: Row(2, "b", "ptv1", 11) :: Row(
                  3,
                  "c2",
                  "ptv1",
                  22) :: Row(4, "d", "ptv2", 22) :: Row(4, "d3", "ptv3", 22) :: Nil
              )

              // dynamic overwrite the pt1=ptv1, pt2=11 partition
              spark.sql("INSERT OVERWRITE T PARTITION (pt1 = 'ptv1', pt2=11) VALUES (5, 'e')")
              checkAnswer(
                spark.sql("SELECT * FROM T ORDER BY a, b"),
                Row(3, "c2", "ptv1", 22) :: Row(4, "d", "ptv2", 22) :: Row(
                  4,
                  "d3",
                  "ptv3",
                  22) :: Row(5, "e", "ptv1", 11) :: Nil)

              // dynamic overwrite the whole table
              spark.sql(
                "INSERT OVERWRITE T VALUES (1, 'a5', 'ptv1', 11), (3, 'c5', 'ptv1', 22), (4, 'd5', 'ptv3', 22)")
              checkAnswer(
                spark.sql("SELECT * FROM T ORDER BY a, b"),
                Row(1, "a5", "ptv1", 11) :: Row(3, "c5", "ptv1", 22) :: Row(
                  4,
                  "d",
                  "ptv2",
                  22) :: Row(4, "d5", "ptv3", 22) :: Nil)
            }
          }
      }
  }

  test(s"insert overwrite date type partition table") {
    spark.sql(s"""
                 |CREATE TABLE T (
                 |  id STRING,
                 |  dt date)
                 |PARTITIONED BY (dt)
                 |TBLPROPERTIES (
                 |  'primary-key' = 'id,dt',
                 |  'bucket' = '3'
                 |);
                 |""".stripMargin)

    spark.sql("INSERT OVERWRITE T partition (dt='2024-04-18') values(1)")
    checkAnswer(spark.sql("SELECT * FROM T"), Row("1", Date.valueOf("2024-04-18")))
  }

  test("Paimon Insert: all data types") {
    spark.sql("""
                |CREATE TABLE T (
                |id bigint, name string, birth date, age int, marital boolean,
                |height float, weight double,
                |interests array<string>, scores map<string, double>, avg_score decimal(5, 2),
                |address struct<province:string, city:string, district:string>,
                |create_time timestamp
                |)
                |""".stripMargin)

    spark.sql(
      """
        |INSERT INTO T
        |SELECT 1L, "yann", TO_DATE('1990-01-01', 'yyyy-MM-dd'), 32, true, 123.4F, 56.7D,
        |array("abc", "def"), map("math", 90D, "history", 60D), 75.000, struct("Zhejiang", "Hangzhou", "Xihu"),
        |TO_TIMESTAMP('2024-07-01 16:00:00', 'yyyy-MM-dd kk:mm:ss')
        |UNION ALL
        |SELECT 2L, "mai", TO_DATE('2021-06-01', 'yyyy-MM-dd'), 3, false, 98.7F, 12.3D,
        |array("def", "xyz"), null, null, struct("Zhejiang", "Hangzhou", "Xihu"),
        |TO_TIMESTAMP('2024-07-01 16:00:00', 'yyyy-MM-dd kk:mm:ss')
        |;
        |""".stripMargin)

    checkAnswer(
      spark.sql("SELECT * FROM T ORDER BY id"),
      Row(
        1L,
        "yann",
        Date.valueOf("1990-01-01"),
        32,
        true,
        123.4f,
        56.7d,
        Array("abc", "def"),
        Map("math" -> 90d, "history" -> 60d),
        BigDecimal.apply(75.00),
        Row("Zhejiang", "Hangzhou", "Xihu"),
        Timestamp.valueOf("2024-07-01 16:00:00")
      ) ::
        Row(
          2L,
          "mai",
          Date.valueOf("2021-06-01"),
          3,
          false,
          98.7f,
          12.3d,
          Array("def", "xyz"),
          null,
          null,
          Row("Zhejiang", "Hangzhou", "Xihu"),
          Timestamp.valueOf("2024-07-01 16:00:00")
        ) :: Nil
    )
  }
}
