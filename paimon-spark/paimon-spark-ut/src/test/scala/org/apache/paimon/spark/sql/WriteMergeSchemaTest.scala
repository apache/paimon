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
        assert(error.contains("extra columns"))
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
        "spark.paimon.write.merge-schema.type-widening" -> "true") {
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

  test("Write merge schema: case-insensitive column matching with new column") {
    withTable("t") {
      sql("CREATE TABLE t (id INT, name STRING)")
      sql("INSERT INTO t VALUES (1, 'a'), (2, 'b')")

      withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
        sql("INSERT INTO t BY NAME SELECT 3 AS ID, 'c' AS NAME, 100 AS extra")
      }

      val columnNames = spark.table("t").schema.fieldNames
      assert(
        columnNames.length == 3,
        s"Expected 3 columns (id, name, extra) but got ${columnNames.length}: ${columnNames.mkString(", ")}")

      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, "a", null), Row(2, "b", null), Row(3, "c", 100)))
    }
  }

  test("Write merge schema: case-insensitive dataframe write") {
    withTable("t") {
      sql("CREATE TABLE t (id INT, name STRING)")
      Seq((1, "a"), (2, "b"))
        .toDF("id", "name")
        .write
        .format("paimon")
        .mode("append")
        .saveAsTable("t")

      Seq((3, "c", 100))
        .toDF("ID", "NAME", "extra")
        .write
        .format("paimon")
        .mode("append")
        .option("write.merge-schema", "true")
        .saveAsTable("t")

      val columnNames = spark.table("t").schema.fieldNames
      assert(
        columnNames.length == 3,
        s"Expected 3 columns but got ${columnNames.length}: ${columnNames.mkString(", ")}")

      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, "a", null), Row(2, "b", null), Row(3, "c", 100)))
    }
  }

  test("Write merge schema: only case differs, no schema change") {
    withTable("t") {
      sql("CREATE TABLE t (id INT, name STRING)")
      sql("INSERT INTO t VALUES (1, 'a')")

      withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
        sql("INSERT INTO t BY NAME SELECT 2 AS ID, 'b' AS NAME")
      }

      val columnNames = spark.table("t").schema.fieldNames
      assert(
        columnNames.toSeq == Seq("id", "name"),
        s"Schema changed unexpectedly: ${columnNames.mkString(", ")}")

      checkAnswer(sql("SELECT * FROM t ORDER BY id"), Seq(Row(1, "a"), Row(2, "b")))
    }
  }

  test("Write merge schema: repeated writes with alternating case") {
    withTable("t") {
      sql("CREATE TABLE t (id INT, name STRING)")
      sql("INSERT INTO t VALUES (1, 'a')")

      withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
        sql("INSERT INTO t BY NAME SELECT 2 AS ID, 'b' AS NAME")
        sql("INSERT INTO t BY NAME SELECT 3 AS Id, 'c' AS NaMe")
        sql("INSERT INTO t BY NAME SELECT 4 AS iD, 'd' AS nAmE")
      }

      val columnNames = spark.table("t").schema.fieldNames
      assert(columnNames.length == 2, s"Expected 2 columns but got: ${columnNames.mkString(", ")}")

      checkAnswer(
        sql("SELECT * FROM t ORDER BY id"),
        Seq(Row(1, "a"), Row(2, "b"), Row(3, "c"), Row(4, "d")))
    }
  }

  test("Write merge schema: nested struct case mismatch with new sub-field") {
    withTable("t") {
      withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
        sql("CREATE TABLE t (id INT, info STRUCT<key1: STRING, key2: STRING>)")
        sql("INSERT INTO t VALUES (1, named_struct('key1', 'a', 'key2', 'b'))")

        sql(
          "INSERT INTO t BY NAME SELECT 2 AS id, " +
            "named_struct('KEY1', 'A', 'KEY2', 'B', 'key3', 'C') AS info")

        val infoFields = spark
          .table("t")
          .schema("info")
          .dataType
          .asInstanceOf[org.apache.spark.sql.types.StructType]
          .fieldNames
        assert(
          infoFields.length == 3,
          s"Expected 3 sub-fields but got ${infoFields.length}: ${infoFields.mkString(", ")}")

        checkAnswer(
          sql("SELECT id, info.key1, info.key2, info.key3 FROM t ORDER BY id"),
          Seq(Row(1, "a", "b", null), Row(2, "A", "B", "C")))
      }
    }
  }

  test("Merge into with merge-schema: source uppercase should not create duplicate columns") {
    withTable("t") {
      sql("""CREATE TABLE t (id INT, name STRING, value INT)
            | USING paimon
            | TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)
      sql("INSERT INTO t VALUES (1, 'a', 10), (2, 'b', 20)")

      spark
        .sql("SELECT 1 AS ID, 'A' AS NAME, 100 AS VALUE UNION ALL SELECT 3 AS ID, 'c' AS NAME, 30 AS VALUE")
        .createOrReplaceTempView("s")

      withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
        sql("""MERGE INTO t USING s ON t.id = s.ID
              | WHEN MATCHED THEN UPDATE SET *
              | WHEN NOT MATCHED THEN INSERT *""".stripMargin)
      }

      val columnNames = spark.table("t").schema.fieldNames.toSeq
      assert(
        columnNames.size == 3,
        s"Expected 3 columns but got ${columnNames.size}: ${columnNames.mkString(", ")}")

      checkAnswer(
        sql("SELECT id, name, value FROM t ORDER BY id"),
        Seq(Row(1, "A", 100), Row(2, "b", 20), Row(3, "c", 30)))
    }
  }

  test("Merge into with merge-schema: append-only target evolves schema") {
    withTable("t") {
      sql("CREATE TABLE t (id INT, name STRING) USING paimon")
      sql("INSERT INTO t VALUES (1, 'a'), (2, 'b')")

      spark
        .sql("SELECT 1 AS id, 'a2' AS name, 100 AS value UNION ALL SELECT 3 AS id, 'c' AS name, 30 AS value")
        .createOrReplaceTempView("s")

      withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
        sql("""MERGE INTO t USING s ON t.id = s.id
              | WHEN MATCHED THEN UPDATE SET *
              | WHEN NOT MATCHED THEN INSERT *""".stripMargin)
      }

      assert(spark.table("t").schema.fieldNames.toSeq == Seq("id", "name", "value"))
      checkAnswer(
        sql("SELECT id, name, value FROM t ORDER BY id"),
        Seq(Row(1, "a2", 100), Row(2, "b", null), Row(3, "c", 30)))
    }
  }

  test("Merge into with merge-schema: extra column with case-mismatched existing columns") {
    withTable("t") {
      sql("""CREATE TABLE t (id INT, name STRING)
            | USING paimon
            | TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)
      sql("INSERT INTO t VALUES (1, 'a'), (2, 'b')")

      spark
        .sql("SELECT 1 AS ID, 'A' AS NAME, 100 AS extra_col UNION ALL SELECT 3 AS ID, 'c' AS NAME, 30 AS extra_col")
        .createOrReplaceTempView("s")

      withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
        sql("""MERGE INTO t USING s ON t.id = s.ID
              | WHEN MATCHED THEN UPDATE SET *
              | WHEN NOT MATCHED THEN INSERT *""".stripMargin)
      }

      val columnNames = spark.table("t").schema.fieldNames.toSeq
      assert(
        columnNames.size == 3,
        s"Expected 3 columns (id, name, extra_col) but got ${columnNames.size}: ${columnNames.mkString(", ")}")

      checkAnswer(
        sql("SELECT id, name, extra_col FROM t ORDER BY id"),
        Seq(Row(1, "A", 100), Row(2, "b", null), Row(3, "c", 30)))
    }
  }

  test("Merge into with merge-schema: only case differs, schema should not change") {
    withTable("t") {
      sql("""CREATE TABLE t (id INT, name STRING)
            | USING paimon
            | TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)
      sql("INSERT INTO t VALUES (1, 'a')")

      spark.sql("SELECT 1 AS ID, 'A' AS NAME").createOrReplaceTempView("s")

      withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
        sql("""MERGE INTO t USING s ON t.id = s.ID
              | WHEN MATCHED THEN UPDATE SET *""".stripMargin)
      }

      val columnNames = spark.table("t").schema.fieldNames
      assert(
        columnNames.toSeq == Seq("id", "name"),
        s"Schema changed unexpectedly: ${columnNames.mkString(", ")}")

      checkAnswer(sql("SELECT * FROM t"), Seq(Row(1, "A")))
    }
  }

  test("Merge into with merge-schema: nested struct fields case mismatch") {
    withTable("t") {
      sql("""CREATE TABLE t (id INT, info STRUCT<key1: STRING, key2: STRING>)
            | USING paimon
            | TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)
      sql("INSERT INTO t VALUES (1, named_struct('key1', 'a', 'key2', 'b'))")

      spark
        .sql("SELECT 1 AS id, named_struct('KEY1', 'A', 'KEY2', 'B', 'key3', 'C') AS info")
        .createOrReplaceTempView("s")

      withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
        sql("""MERGE INTO t USING s ON t.id = s.id
              | WHEN MATCHED THEN UPDATE SET *
              | WHEN NOT MATCHED THEN INSERT *""".stripMargin)
      }

      val infoFields = spark
        .table("t")
        .schema("info")
        .dataType
        .asInstanceOf[org.apache.spark.sql.types.StructType]
        .fieldNames
      assert(
        infoFields.length == 3,
        s"Expected 3 sub-fields but got ${infoFields.length}: ${infoFields.mkString(", ")}")

      checkAnswer(
        sql("SELECT id, info.key1, info.key2, info.key3 FROM t"),
        Seq(Row(1, "A", "B", "C")))
    }
  }

  test("Merge into with merge-schema: repeated writes with alternating case") {
    withTable("t") {
      sql("""CREATE TABLE t (id INT, name STRING)
            | USING paimon
            | TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)
      sql("INSERT INTO t VALUES (1, 'a')")

      withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
        spark.sql("SELECT 2 AS ID, 'b' AS NAME").createOrReplaceTempView("s1")
        sql("""MERGE INTO t USING s1 ON t.id = s1.ID
              | WHEN NOT MATCHED THEN INSERT *""".stripMargin)

        spark.sql("SELECT 3 AS Id, 'c' AS NaMe").createOrReplaceTempView("s2")
        sql("""MERGE INTO t USING s2 ON t.id = s2.Id
              | WHEN NOT MATCHED THEN INSERT *""".stripMargin)
      }

      val columnNames = spark.table("t").schema.fieldNames
      assert(columnNames.length == 2, s"Expected 2 columns but got: ${columnNames.mkString(", ")}")

      checkAnswer(sql("SELECT * FROM t ORDER BY id"), Seq(Row(1, "a"), Row(2, "b"), Row(3, "c")))
    }
  }

  test("Merge into without merge-schema: case-insensitive matching works normally") {
    withTable("t") {
      sql("""CREATE TABLE t (id INT, name STRING)
            | USING paimon
            | TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)
      sql("INSERT INTO t VALUES (1, 'a'), (2, 'b')")

      spark
        .sql("SELECT 1 AS ID, 'A' AS NAME UNION ALL SELECT 3 AS ID, 'c' AS NAME")
        .createOrReplaceTempView("s")

      sql("""MERGE INTO t USING s ON t.id = s.ID
            | WHEN MATCHED THEN UPDATE SET *
            | WHEN NOT MATCHED THEN INSERT *""".stripMargin)

      val schema = spark.table("t").schema
      assert(
        schema.fieldNames.length == 2,
        s"Expected 2 columns but got: ${schema.fieldNames.mkString(", ")}")

      checkAnswer(
        sql("SELECT id, name FROM t ORDER BY id"),
        Seq(Row(1, "A"), Row(2, "b"), Row(3, "c")))
    }
  }

  test("Merge into without merge-schema: ARRAY<INT> target with ARRAY<BIGINT> source") {
    withTable("t") {
      sql("""CREATE TABLE t (id INT, ports ARRAY<INT>)
            | USING paimon
            | TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)
      sql("INSERT INTO t VALUES (1, array(80, 443))")

      spark
        .sql("SELECT 1 AS id, array(cast(8080 as bigint), cast(9090 as bigint)) AS ports")
        .createOrReplaceTempView("s")

      sql("""MERGE INTO t USING s ON t.id = s.id
            | WHEN MATCHED THEN UPDATE SET *
            | WHEN NOT MATCHED THEN INSERT *""".stripMargin)

      val portsType = spark.table("t").schema("ports").dataType
      assert(
        portsType.simpleString == "array<int>",
        s"Expected array<int> but got ${portsType.simpleString}")

      checkAnswer(sql("SELECT * FROM t"), Seq(Row(1, Seq(8080, 9090))))
    }
  }

  test("Write merge schema: INT to BIGINT keeps target type") {
    withTable("t") {
      sql("CREATE TABLE t (id INT, value INT)")
      sql("INSERT INTO t VALUES (1, 100)")

      withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
        sql("INSERT INTO t BY NAME SELECT 2 AS id, cast(200 as bigint) AS value")
      }

      val valueType = spark.table("t").schema("value").dataType
      assert(valueType.simpleString == "int", s"Expected int but got ${valueType.simpleString}")

      checkAnswer(sql("SELECT * FROM t ORDER BY id"), Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("Merge into with merge-schema: case-sensitive mode treats different case as new columns") {
    withTable("t") {
      sql("""CREATE TABLE t (id INT, name STRING)
            | USING paimon
            | TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)
      sql("INSERT INTO t VALUES (1, 'a')")

      spark
        .sql("SELECT 1 AS ID, 'A' AS NAME, 100 AS extra")
        .createOrReplaceTempView("s")

      withSparkSQLConf(
        "spark.paimon.write.merge-schema" -> "true",
        "spark.sql.caseSensitive" -> "true") {
        sql("""MERGE INTO t USING s ON t.id = s.ID
              | WHEN MATCHED THEN UPDATE SET *
              | WHEN NOT MATCHED THEN INSERT *""".stripMargin)
      }

      val columnNames = spark.table("t").schema.fieldNames
      assert(
        columnNames.length == 5,
        s"Expected 5 columns (id, name, ID, NAME, extra) but got ${columnNames.length}: ${columnNames.mkString(", ")}")
    }
  }

  test("Write merge schema: array of struct missing nested field by dataframe") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (
            |  ids ARRAY<BIGINT>,
            |  a BIGINT,
            |  b BIGINT,
            |  items ARRAY<STRUCT<
            |    f1: INT,
            |    f2: STRING,
            |    f3: STRING,
            |    f4: STRING,
            |    f5: STRING,
            |    f6: INT>>)
            |""".stripMargin)

      val sourceDataFrame = sql("""
                                  |SELECT
                                  |  array(1, 2) AS ids,
                                  |  1 AS a,
                                  |  2 AS b,
                                  |  array(named_struct(
                                  |    'f1', 10,
                                  |    'f2', 'v2',
                                  |    'f3', 'v3',
                                  |    'f4', 'v4',
                                  |    'f5', 'v5')) AS items
                                  |""".stripMargin)

      val alignedDataFrame = sourceDataFrame.select(
        sourceDataFrame("ids").cast("ARRAY<BIGINT>").as("ids"),
        sourceDataFrame("a").cast("BIGINT").as("a"),
        sourceDataFrame("b").cast("BIGINT").as("b"),
        sourceDataFrame("items")
      )

      alignedDataFrame.write
        .format("paimon")
        .mode("append")
        .option("write.merge-schema", "true")
        .saveAsTable("t")

      checkAnswer(
        sql("SELECT * FROM t"),
        Seq(Row(Seq(1L, 2L), 1L, 2L, Seq(Row(10, "v2", "v3", "v4", "v5", null)))))
    }
  }

  test("Merge into with merge-schema: ARRAY<INT> to ARRAY<BIGINT> keeps target type") {
    withTable("t") {
      sql("""CREATE TABLE t (id INT, ports ARRAY<INT>)
            | USING paimon
            | TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)
      sql("INSERT INTO t VALUES (1, array(80, 443))")

      spark
        .sql("SELECT 1 AS id, array(cast(8080 as bigint), cast(9090 as bigint)) AS ports")
        .createOrReplaceTempView("s")

      withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
        sql("""MERGE INTO t USING s ON t.id = s.id
              | WHEN MATCHED THEN UPDATE SET *
              | WHEN NOT MATCHED THEN INSERT *""".stripMargin)
      }

      val portsType = spark.table("t").schema("ports").dataType
      assert(
        portsType.simpleString == "array<int>",
        s"Expected array<int> but got ${portsType.simpleString}")

      checkAnswer(sql("SELECT * FROM t"), Seq(Row(1, Seq(8080, 9090))))
    }
  }

  test("Write merge schema: case-sensitive mode treats different case as new columns") {
    withTable("t") {
      sql("CREATE TABLE t (id INT, name STRING)")
      sql("INSERT INTO t VALUES (1, 'a')")

      withSparkSQLConf(
        "spark.paimon.write.merge-schema" -> "true",
        "spark.sql.caseSensitive" -> "true") {
        sql("INSERT INTO t BY NAME SELECT 2 AS ID, 'b' AS NAME, 100 AS extra")
      }

      val columnNames = spark.table("t").schema.fieldNames
      assert(
        columnNames.length == 5,
        s"Expected 5 columns (id, name, ID, NAME, extra) but got ${columnNames.length}: ${columnNames.mkString(", ")}")
    }
  }

  test("Merge into with type-widening=true: INT to BIGINT widens existing column") {
    withTable("t") {
      sql("""CREATE TABLE t (id INT, v INT)
            | USING paimon
            | TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)
      sql("INSERT INTO t VALUES (1, 10)")

      spark.sql("SELECT 1 AS id, cast(200 AS bigint) AS v").createOrReplaceTempView("s")

      withSparkSQLConf(
        "spark.paimon.write.merge-schema" -> "true",
        "spark.paimon.write.merge-schema.type-widening" -> "true") {
        sql("""MERGE INTO t USING s ON t.id = s.id
              | WHEN MATCHED THEN UPDATE SET *""".stripMargin)
      }

      assert(spark.table("t").schema("v").dataType.simpleString == "bigint")
      checkAnswer(sql("SELECT * FROM t"), Seq(Row(1, 200L)))
    }
  }

  test("Merge into with type-widening=true: ARRAY element widening throws (known limitation)") {
    withTable("t") {
      sql("""CREATE TABLE t (id INT, ports ARRAY<INT>)
            | USING paimon
            | TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')""".stripMargin)
      sql("INSERT INTO t VALUES (1, array(80))")

      spark
        .sql("SELECT 2 AS id, array(cast(9090 AS bigint)) AS ports")
        .createOrReplaceTempView("s")

      // KNOWN LIMITATION: widening the element type of ARRAY/MAP is not yet supported by
      // SchemaManager.generateTableSchema (CastExecutors has no ARRAY<INT> -> ARRAY<BIGINT>
      // rule). Tracked as a follow-up; until then type-widening on complex element types throws.
      withSparkSQLConf(
        "spark.paimon.write.merge-schema" -> "true",
        "spark.paimon.write.merge-schema.type-widening" -> "true") {
        intercept[Exception] {
          sql("""MERGE INTO t USING s ON t.id = s.id
                | WHEN MATCHED THEN UPDATE SET *
                | WHEN NOT MATCHED THEN INSERT *""".stripMargin)
        }
      }
    }
  }
}
