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

class WriteMergeSchemaTest extends PaimonSparkTestBase {

  // todo: fix this
  override protected def sparkConf: SparkConf = {
    super.sparkConf.set("spark.sql.catalog.paimon.cache-enabled", "false")
  }

  import testImplicits._

  test("Write merge schema: dataframe write") {
    withTable("t") {
      sql("CREATE TABLE t (a INT, b STRING)")
      Seq((1, "1"), (2, "2"))
        .toDF("a", "b")
        .write
        .format("paimon")
        .mode("append")
        .saveAsTable("t")

      // new columns
      Seq((3, "3", 3))
        .toDF("a", "b", "c")
        .write
        .format("paimon")
        .mode("append")
        .option("write.merge-schema", "true")
        .saveAsTable("t")
      checkAnswer(
        sql("SELECT * FROM t ORDER BY a"),
        Seq(Row(1, "1", null), Row(2, "2", null), Row(3, "3", 3))
      )

      // missing columns and new columns
      Seq(("4", "4", 4))
        .toDF("d", "b", "c")
        .write
        .format("paimon")
        .mode("append")
        .option("write.merge-schema", "true")
        .saveAsTable("t")
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
      withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
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
      withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
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
  }

  test("Write merge schema: date and time types") {
    withTable("t") {
      withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
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
  }

  test("Write merge schema: complex types") {
    withTable("t") {
      withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
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
  }

  test("Write merge schema: nested struct with new fields by sql") {
    withTable("t") {
      withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
        sql("CREATE TABLE t (id INT, info STRUCT<key1 STRUCT<key2 STRING, key3 STRING>>)")
        sql("INSERT INTO t VALUES (1, struct(struct('v2a', 'v3a')))")
        sql("INSERT INTO t VALUES (2, struct(struct('v2b', 'v3b')))")
        checkAnswer(
          sql("SELECT * FROM t ORDER BY id"),
          Seq(Row(1, Row(Row("v2a", "v3a"))), Row(2, Row(Row("v2b", "v3b"))))
        )

        // insert data with new nested field key4 added BEFORE key3 to verify byName semantics
        // inside struct (not positional). The named_struct ensures field names are explicit.
        sql("INSERT INTO t BY NAME " +
          "SELECT 3 AS id, " +
          "named_struct('key1', named_struct('key2', 'v2c', 'key4', 'v4c', 'key3', 'v3c')) AS info")
        checkAnswer(
          sql("SELECT * FROM t ORDER BY id"),
          Seq(
            Row(1, Row(Row("v2a", "v3a", null))),
            Row(2, Row(Row("v2b", "v3b", null))),
            Row(3, Row(Row("v2c", "v3c", "v4c"))))
        )
      }
    }
  }

  test("Write merge schema: binary and boolean types") {
    withTable("t") {
      withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
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

  test("Write merge schema: nested struct with new fields by dataframe") {
    withTable("t") {
      sql("CREATE TABLE t (id INT, info STRUCT<key1 STRUCT<key2 STRING, key3 STRING>>)")
      sql("INSERT INTO t VALUES (1, struct(struct('v2a', 'v3a')))")

      // Construct a DataFrame with new nested field key4 in different order using explicit schema
      import org.apache.spark.sql.types._
      val dataSchema = StructType(
        Seq(
          StructField("id", IntegerType),
          StructField(
            "info",
            StructType(
              Seq(
                StructField(
                  "key1",
                  StructType(
                    Seq(
                      StructField("key2", StringType),
                      StructField("key4", StringType),
                      StructField("key3", StringType)
                    )))
              ))
          )
        ))
      val data = java.util.Arrays.asList(
        Row(2, Row(Row("v2b", "v4b", "v3b")))
      )
      spark
        .createDataFrame(data, dataSchema)
        .write
        .format("paimon")
        .mode("append")
        .option("write.merge-schema", "true")
        .saveAsTable("t")

      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, Row(Row("v2a", "v3a", null))), Row(2, Row(Row("v2b", "v3b", "v4b"))))
      )
    }
  }

  test("Write merge schema: deeply nested struct with new fields") {
    withTable("t") {
      withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
        sql("CREATE TABLE t (id INT, data STRUCT<a STRUCT<b STRUCT<c1 STRING, c2 STRING>>>)")
        sql("INSERT INTO t VALUES (1, struct(struct(struct('c1v', 'c2v'))))")

        // Add new field c3 at the deepest level
        sql(
          "INSERT INTO t BY NAME " +
            "SELECT 2 AS id, " +
            "named_struct('a', named_struct('b', named_struct('c1', 'c1v2', 'c3', 'c3v2', 'c2', 'c2v2'))) AS data")
        checkAnswer(
          sql("SELECT * FROM t ORDER BY id"),
          Seq(
            Row(1, Row(Row(Row("c1v", "c2v", null)))),
            Row(2, Row(Row(Row("c1v2", "c2v2", "c3v2")))))
        )
      }
    }
  }

  test("Write merge schema: nested struct new fields and top-level new column together") {
    withTable("t") {
      withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
        sql("CREATE TABLE t (id INT, info STRUCT<f1 STRING, f2 STRING>)")
        sql("INSERT INTO t VALUES (1, struct('a', 'b'))")

        // Add both a new nested field f3 and a new top-level column extra
        sql(
          "INSERT INTO t BY NAME " +
            "SELECT 2 AS id, " +
            "named_struct('f1', 'c', 'f3', 'd', 'f2', 'e') AS info, " +
            "'top' AS extra")
        checkAnswer(
          sql("SELECT * FROM t ORDER BY id"),
          Seq(Row(1, Row("a", "b", null), null), Row(2, Row("c", "e", "d"), "top"))
        )
      }
    }
  }

  test("Write merge schema: nested struct with missing fields") {
    withTable("t") {
      withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
        sql("CREATE TABLE t (id INT, info STRUCT<f1 STRING, f2 STRING, f3 STRING>)")
        sql("INSERT INTO t VALUES (1, struct('a', 'b', 'c'))")

        // Data missing f1, no new fields — pure nested field missing
        sql(
          "INSERT INTO t BY NAME " +
            "SELECT 2 AS id, " +
            "named_struct('f2', 'y', 'f3', 'z') AS info")
        checkAnswer(
          sql("SELECT * FROM t ORDER BY id"),
          Seq(Row(1, Row("a", "b", "c")), Row(2, Row(null, "y", "z")))
        )

        // Data missing f2, and adding new field f4 in the middle
        sql(
          "INSERT INTO t BY NAME " +
            "SELECT 3 AS id, " +
            "named_struct('f1', 'x', 'f4', 'w', 'f3', 'z') AS info")

        // Data missing f1, f4 at the end (no new fields, just missing + reorder)
        sql(
          "INSERT INTO t BY NAME " +
            "SELECT 4 AS id, " +
            "named_struct('f2', 'p', 'f3', 'q', 'f4', 'r') AS info")
        checkAnswer(
          sql("SELECT * FROM t ORDER BY id"),
          Seq(
            Row(1, Row("a", "b", "c", null)),
            Row(2, Row(null, "y", "z", null)),
            Row(3, Row("x", null, "z", "w")),
            Row(4, Row(null, "p", "q", "r")))
        )
      }
    }
  }

  test("Write merge schema: nested struct with type evolution") {
    withTable("t") {
      withSparkSQLConf(
        "spark.paimon.write.merge-schema" -> "true",
        "spark.paimon.write.merge-schema.explicit-cast" -> "true") {
        sql("CREATE TABLE t (id INT, info STRUCT<f1 INT, f2 STRING>)")
        sql("INSERT INTO t VALUES (1, struct(10, 'a'))")

        // f1 evolves from INT to BIGINT, and add new field f3
        sql(
          "INSERT INTO t BY NAME " +
            "SELECT 2 AS id, " +
            "named_struct('f1', cast(20 as bigint), 'f3', 'new', 'f2', 'b') AS info")
        checkAnswer(
          sql("SELECT * FROM t ORDER BY id"),
          Seq(Row(1, Row(10L, "a", null)), Row(2, Row(20L, "b", "new")))
        )
      }
    }
  }
}
