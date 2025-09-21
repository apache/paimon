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

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row

class V2WriteMergeSchemaTest extends PaimonSparkTestBase {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.paimon.write.use-v2-write", "true")
      .set("spark.paimon.write.merge-schema", "true")
      .set("spark.paimon.write.merge-schema.explicit-cast", "true")
  }

  import testImplicits._

  test("Write merge schema: dataframe write") {
    withTable("t") {
      sql("CREATE TABLE t (a INT, b STRING)")
      Seq((1, "1"), (2, "2"))
        .toDF("a", "b")
        .writeTo("t")
        .option("write.merge-schema", "true")
        .append()

      // new columns
      Seq((3, "3", 3))
        .toDF("a", "b", "c")
        .writeTo("t")
        .option("write.merge-schema", "true")
        .append()
      checkAnswer(
        sql("SELECT * FROM t ORDER BY a"),
        Seq(Row(1, "1", null), Row(2, "2", null), Row(3, "3", 3))
      )

      // missing columns and new columns
      Seq(("4", "4", 4))
        .toDF("d", "b", "c")
        .writeTo("t")
        .option("write.merge-schema", "true")
        .append()
      checkAnswer(
        sql("SELECT * FROM t ORDER BY a"),
        Seq(
          Row(null, "4", 4, "4"),
          Row(1, "1", null, null),
          Row(2, "2", null, null),
          Row(3, "3", 3, null))
      )
    }
  }

  test("Write merge schema: sql write") {
    withTable("t") {
      sql("CREATE TABLE t (a INT, b STRING)")
      sql("INSERT INTO t VALUES (1, '1'), (2, '2')")

      // new columns
      sql("INSERT INTO t BY NAME SELECT 3 AS a, '3' AS b, 3 AS c")
      checkAnswer(
        sql("SELECT * FROM t ORDER BY a"),
        Seq(Row(1, "1", null), Row(2, "2", null), Row(3, "3", 3))
      )

      // missing columns and new columns
      sql("INSERT INTO t BY NAME SELECT '4' AS d, '4' AS b, 4 AS c")
      checkAnswer(
        sql("SELECT * FROM t ORDER BY a"),
        Seq(
          Row(null, "4", 4, "4"),
          Row(1, "1", null, null),
          Row(2, "2", null, null),
          Row(3, "3", 3, null))
      )
    }
  }

  test("Write merge schema: fail when merge schema is disabled but new columns are provided") {
    withTable("t") {
      withSparkSQLConf("spark.paimon.write.merge-schema" -> "false") {
        sql("CREATE TABLE t (a INT, b STRING)")
        sql("INSERT INTO t VALUES (1, '1'), (2, '2')")

        val error = intercept[RuntimeException] {
          spark.sql("INSERT INTO t BY NAME SELECT 3 AS a, '3' AS b, 3 AS c")
        }.getMessage
        assert(error.contains("the number of data columns don't match with the table schema's"))
      }
    }
  }

  test("Write merge schema: numeric types") {
    withTable("t") {
      sql("CREATE TABLE t (a INT, b STRING)")
      sql("INSERT INTO t VALUES (1, '1'), (2, '2')")

      // new columns with numeric types
      sql(
        "INSERT INTO t BY NAME SELECT 3 AS a, '3' AS b, " +
          "cast(10 as byte) AS byte_col, " +
          "cast(1000 as short) AS short_col, " +
          "100000 AS int_col, " +
          "10000000000L AS long_col, " +
          "cast(1.23 as float) AS float_col, " +
          "4.56 AS double_col, " +
          "cast(7.89 as decimal(10,2)) AS decimal_col")
      checkAnswer(
        sql("SELECT * FROM t ORDER BY a"),
        Seq(
          Row(1, "1", null, null, null, null, null, null, null),
          Row(2, "2", null, null, null, null, null, null, null),
          Row(
            3,
            "3",
            10.toByte,
            1000.toShort,
            100000,
            10000000000L,
            1.23f,
            4.56d,
            java.math.BigDecimal.valueOf(7.89))
        )
      )

      // missing columns and new columns with numeric types
      sql(
        "INSERT INTO t BY NAME SELECT '4' AS d, '4' AS b, " +
          "cast(20 as byte) AS byte_col, " +
          "cast(2000 as short) AS short_col, " +
          "200000 AS int_col, " +
          "20000000000L AS long_col, " +
          "cast(2.34 as float) AS float_col, " +
          "5.67 AS double_col, " +
          "cast(8.96 as decimal(10,2)) AS decimal_col")
      checkAnswer(
        sql("SELECT * FROM t ORDER BY a"),
        Seq(
          Row(
            null,
            "4",
            20.toByte,
            2000.toShort,
            200000,
            20000000000L,
            2.34f,
            5.67d,
            java.math.BigDecimal.valueOf(8.96),
            "4"),
          Row(1, "1", null, null, null, null, null, null, null, null),
          Row(2, "2", null, null, null, null, null, null, null, null),
          Row(
            3,
            "3",
            10.toByte,
            1000.toShort,
            100000,
            10000000000L,
            1.23f,
            4.56d,
            java.math.BigDecimal.valueOf(7.89),
            null)
        )
      )
    }
  }

  test("Write merge schema: date and time types") {
    withTable("t") {
      sql("CREATE TABLE t (a INT, b STRING)")
      sql("INSERT INTO t VALUES (1, '1'), (2, '2')")

      // new columns with date and time types
      sql(
        "INSERT INTO t BY NAME SELECT 3 AS a, '3' AS b, " +
          "cast('2023-01-01' as date) AS date_col, " +
          "cast('2023-01-01 12:00:00' as timestamp) AS timestamp_col")
      checkAnswer(
        sql("SELECT * FROM t ORDER BY a"),
        Seq(
          Row(1, "1", null, null),
          Row(2, "2", null, null),
          Row(
            3,
            "3",
            java.sql.Date.valueOf("2023-01-01"),
            java.sql.Timestamp.valueOf("2023-01-01 12:00:00"))
        )
      )

      // missing columns and new columns with date and time types
      sql(
        "INSERT INTO t BY NAME SELECT '4' AS d, '4' AS b, " +
          "cast('2023-12-31' as date) AS date_col, " +
          "cast('2023-12-31 23:59:59' as timestamp) AS timestamp_col")
      checkAnswer(
        sql("SELECT * FROM t ORDER BY a"),
        Seq(
          Row(
            null,
            "4",
            java.sql.Date.valueOf("2023-12-31"),
            java.sql.Timestamp.valueOf("2023-12-31 23:59:59"),
            "4"),
          Row(1, "1", null, null, null),
          Row(2, "2", null, null, null),
          Row(
            3,
            "3",
            java.sql.Date.valueOf("2023-01-01"),
            java.sql.Timestamp.valueOf("2023-01-01 12:00:00"),
            null)
        )
      )
    }
  }

  test("Write merge schema: complex types") {
    withTable("t") {
      sql("CREATE TABLE t (a INT, b STRING)")
      sql("INSERT INTO t VALUES (1, '1'), (2, '2')")

      // new columns with complex types
      sql(
        "INSERT INTO t BY NAME SELECT 3 AS a, '3' AS b, " +
          "array(1, 2, 3) AS array_col, " +
          "map('key1', 'value1', 'key2', 'value2') AS map_col, " +
          "struct('x', 1) AS struct_col")
      checkAnswer(
        sql("SELECT * FROM t ORDER BY a"),
        Seq(
          Row(1, "1", null, null, null),
          Row(2, "2", null, null, null),
          Row(3, "3", Array(1, 2, 3), Map("key1" -> "value1", "key2" -> "value2"), Row("x", 1))
        )
      )

      // missing columns and new columns with complex types
      sql(
        "INSERT INTO t BY NAME SELECT '4' AS d, '4' AS b, " +
          "array(4, 5, 6) AS array_col, " +
          "map('key3', 'value3') AS map_col, " +
          "struct('y', 2) AS struct_col")
      checkAnswer(
        sql("SELECT * FROM t ORDER BY a"),
        Seq(
          Row(null, "4", Array(4, 5, 6), Map("key3" -> "value3"), Row("y", 2), "4"),
          Row(1, "1", null, null, null, null),
          Row(2, "2", null, null, null, null),
          Row(
            3,
            "3",
            Array(1, 2, 3),
            Map("key1" -> "value1", "key2" -> "value2"),
            Row("x", 1),
            null)
        )
      )
    }
  }

  test("Write merge schema: binary and boolean types") {
    withTable("t") {
      sql("CREATE TABLE t (a INT, b STRING)")
      sql("INSERT INTO t VALUES (1, '1'), (2, '2')")

      // new columns with binary and boolean types
      sql(
        "INSERT INTO t BY NAME SELECT 3 AS a, '3' AS b, " +
          "cast('binary_data' as binary) AS binary_col, " +
          "true AS boolean_col")
      checkAnswer(
        sql("SELECT * FROM t ORDER BY a"),
        Seq(
          Row(1, "1", null, null),
          Row(2, "2", null, null),
          Row(3, "3", "binary_data".getBytes("UTF-8"), true)
        )
      )

      // missing columns and new columns with binary and boolean types
      sql(
        "INSERT INTO t BY NAME SELECT '4' AS d, '4' AS b, " +
          "cast('more_data' as binary) AS binary_col, " +
          "false AS boolean_col")
      checkAnswer(
        sql("SELECT * FROM t ORDER BY a"),
        Seq(
          Row(null, "4", "more_data".getBytes("UTF-8"), false, "4"),
          Row(1, "1", null, null, null),
          Row(2, "2", null, null, null),
          Row(3, "3", "binary_data".getBytes("UTF-8"), true, null)
        )
      )
    }
  }

}
