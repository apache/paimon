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

import org.apache.paimon.CoreOptions.BucketFunctionType
import org.apache.paimon.catalog.Identifier
import org.apache.paimon.schema.Schema
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.types.DataTypes

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions

import java.sql.Timestamp
import java.time.LocalDateTime

class SparkWriteWithNoExtensionITCase extends SparkWriteITCase {

  /** Disable the spark extension. */
  override protected def sparkConf: SparkConf = {
    super.sparkConf.remove("spark.sql.extensions")
    super.sparkConf.set("spark.paimon.requiredSparkConfsCheck.enabled", "false")
  }
}

class SparkV2WriteITCase extends SparkWriteITCase {
  override protected def sparkConf: SparkConf = {
    super.sparkConf.set("spark.paimon.write.use-v2-write", "true")
  }
}

class SparkWriteITCase extends PaimonSparkTestBase {

  test("Paimon Write : Postpone Bucket") {
    withTable("PostponeTable") {
      spark.sql("""
                  |CREATE TABLE PostponeTable (
                  |  id INT,
                  |  v1 INT,
                  |  v2 INT
                  |) TBLPROPERTIES (
                  | 'bucket' = '-2',
                  | 'primary-key' = 'id',
                  | 'file.format' = 'parquet',
                  | 'postpone.batch-write-fixed-bucket' = 'false'
                  |)
                  |""".stripMargin)

      spark.sql("INSERT INTO PostponeTable VALUES (1, 1, 1)")

      val table = loadTable("PostponeTable")
      val snapshot = table.latestSnapshot.get
      val manifestEntry = table.manifestFileReader
        .read(table.manifestListReader.read(snapshot.deltaManifestList).get(0).fileName)
        .get(0)
      val file = manifestEntry.file
      assertThat(manifestEntry.bucket()).isEqualTo(-2)
      // default format for postpone bucket is avro
      assertThat(file.fileName).endsWith(".avro")
    }
  }

  test("Paimon Write: AllTypes") {
    withTable("AllTypesTable") {
      val createTableSQL =
        """
          |CREATE TABLE AllTypesTable (
          |  byte_col BYTE NOT NULL,
          |  short_col SHORT,
          |  int_col INT NOT NULL,
          |  long_col LONG,
          |  float_col FLOAT,
          |  double_col DOUBLE NOT NULL,
          |  decimal_col DECIMAL(10,2),
          |  string_col STRING,
          |  binary_col BINARY,
          |  boolean_col BOOLEAN NOT NULL,
          |  date_col DATE,
          |  timestamp_col TIMESTAMP,
          |  timestamp_ntz_col TIMESTAMP_NTZ,
          |  array_col ARRAY<INT>,
          |  map_col MAP<STRING,INT>,
          |  struct_col STRUCT<f1:INT, f2:STRING>
          |) TBLPROPERTIES (
          | 'bucket' = '2',
          | 'bucket-key' = 'int_col'
          |)
          |""".stripMargin
      sql(createTableSQL)

      sql("""
            |INSERT INTO AllTypesTable VALUES (
            |  1Y, -- byte_col (NOT NULL)
            |  100S, -- short_col
            |  42, -- int_col (NOT NULL)
            |  9999999999L, -- long_col
            |  3.14F, -- float_col
            |  CAST(2.71828 as double), -- double_col (NOT NULL)
            |  CAST('123.45' AS DECIMAL(10,2)), -- decimal_col
            |  'test_string', -- string_col
            |  unhex('0001'), -- binary_col
            |  true, -- boolean_col (NOT NULL)
            |  DATE '2023-10-01', -- date_col
            |  TIMESTAMP '2023-10-01 12:34:56', -- timestamp_col
            |  TIMESTAMP_NTZ '2023-10-01 12:34:56', -- timestamp_ntz_col
            |  ARRAY(1, 2, 3), -- array_col
            |  MAP('key1', 1, 'key2', 2), -- map_col
            |  NAMED_STRUCT('f1', 10, 'f2', 'struct_field') -- struct_col
            |)
            |""".stripMargin)

      checkAnswer(
        sql("SELECT * FROM AllTypesTable"),
        Row(
          1.toByte, // byte_col
          100.toShort, // short_col
          42, // int_col
          9999999999L, // long_col
          3.14f, // float_col
          2.71828, // double_col
          new java.math.BigDecimal("123.45"), // decimal_col
          "test_string", // string_col
          Array(0x00, 0x01), // binary_col
          true, // boolean_col
          java.sql.Date.valueOf("2023-10-01"), // date_col
          java.sql.Timestamp.valueOf("2023-10-01 12:34:56"), // timestamp_col
          LocalDateTime.parse("2023-10-01T12:34:56"), // timestamp_ntz_col
          Array(1, 2, 3), // array_col
          Map("key1" -> 1, "key2" -> 2), // map_col
          Row(10, "struct_field") // struct_col
        ) :: Nil
      )
    }
  }

  test("Paimon Write : Nested type") {
    withTable("NestedTypesTable") {
      val createTableSQL =
        """
          |CREATE TABLE NestedTypesTable (
          |  id INT NOT NULL,
          |  map_col MAP<STRING, ARRAY<INT>>,
          |  struct_col STRUCT<
          |    name: STRING,
          |    details: MAP<STRING, INT>,
          |    scores: ARRAY<DOUBLE>
          |  >,
          |  nested_array_col ARRAY<STRUCT<
          |    map_field: MAP<STRING, INT>,
          |    sub_array: ARRAY<INT>
          |  >> NOT NULL
          |)
          |""".stripMargin
      spark.sql(createTableSQL)

      spark.sql("""
                  |INSERT INTO NestedTypesTable VALUES
                  |(
                  |  1,
                  |  MAP('key1', ARRAY(1, 2, 3), 'key2', ARRAY(4, 5)),  -- map_col
                  |  STRUCT(                                            -- struct_col
                  |    'user1',
                  |    MAP('age', 25, 'score', 99),
                  |    ARRAY(CAST(90.5 as double), CAST(88.0 as double))
                  |  ),
                  |  ARRAY(                                             -- nested_array_col
                  |    STRUCT(MAP('a', 1), ARRAY(10, 20)),
                  |    STRUCT(MAP('b', 2, 'c', 3), ARRAY(30))
                  |  )
                  |)
                  |""".stripMargin)

      checkAnswer(
        spark.sql("SELECT * FROM NestedTypesTable WHERE id = 1"),
        Row(
          1, // id
          Map( // map_col
            "key1" -> Seq(1, 2, 3),
            "key2" -> Seq(4, 5)),
          Row( // struct_col
            "user1",
            Map("age" -> 25, "score" -> 99),
            Seq(90.5, 88.0)),
          Seq( // nested_array_col
            Row(Map("a" -> 1), Seq(10, 20)),
            Row(Map("b" -> 2, "c" -> 3), Seq(30)))
        ) :: Nil
      )
    }
  }

  test("Paimon write: nested type with timestamp/timestamp_ntz") {
    withTable("NestedTimestampTable") {
      val createTableSQL =
        """
          |CREATE TABLE NestedTimestampTable (
          |  id INT NOT NULL,
          |  struct_col STRUCT<
          |    ts_ltz: TIMESTAMP,
          |    ts_ntz: TIMESTAMP_NTZ,
          |    map_field: MAP<STRING, TIMESTAMP_NTZ>
          |  >,
          |  array_col ARRAY<STRUCT<
          |    ts_ltz: TIMESTAMP,
          |    ts_ntz: TIMESTAMP_NTZ
          |  >> NOT NULL
          |)
          |""".stripMargin
      spark.sql(createTableSQL)

      spark.sql("""
                  |INSERT INTO NestedTimestampTable VALUES (
                  |  1,
                  |  STRUCT(
                  |    TIMESTAMP '2023-10-01 12:00:00',
                  |    TIMESTAMP_NTZ '2023-10-01 12:00:00',
                  |    MAP('ntz1', TIMESTAMP_NTZ '2023-10-01 08:00:00')
                  |  ),
                  |  ARRAY(
                  |    STRUCT(
                  |      TIMESTAMP '2023-10-01 13:00:00',
                  |      TIMESTAMP_NTZ '2023-10-01 13:00:00'
                  |    )
                  |  )
                  |)
                  |""".stripMargin)

      val expectedTsLtz = Timestamp.valueOf("2023-10-01 12:00:00")
      val expectedTsNtz = LocalDateTime.parse("2023-10-01T12:00:00")
      checkAnswer(
        spark.sql("SELECT struct_col.ts_ltz, struct_col.ts_ntz FROM NestedTimestampTable"),
        Row(expectedTsLtz, expectedTsNtz) :: Nil
      )

      val mapValue = spark
        .sql("SELECT struct_col.map_field['ntz1'] FROM NestedTimestampTable")
        .collect()(0)
        .getAs[LocalDateTime](0)
      Assertions.assertEquals(
        LocalDateTime.parse("2023-10-01T08:00:00"),
        mapValue
      )

      // timestamp in array
      checkAnswer(
        spark.sql("SELECT array_col[0].ts_ltz, array_col[0].ts_ntz FROM NestedTimestampTable"),
        Row(
          Timestamp.valueOf("2023-10-01 13:00:00"),
          LocalDateTime.parse("2023-10-01T13:00:00")
        ) :: Nil
      )

    }
  }

  test("Paimon write: write table with timestamp3 bucket key") {
    withTable("t") {
      // create timestamp3 table using table api
      val schema = Schema.newBuilder
        .column("id", DataTypes.INT)
        .column("ts3", DataTypes.TIMESTAMP(3))
        .option("bucket-key", "ts3")
        .option("bucket", "1024")
        .option("file.format", "avro")
        .build
      paimonCatalog.createTable(Identifier.create(dbName0, "t"), schema, false)

      // insert using table api
      val table = loadTable("t")
      val writeBuilder = table.newBatchWriteBuilder
      val write = writeBuilder.newWrite
      write.write(
        GenericRow.of(
          1,
          org.apache.paimon.data.Timestamp
            .fromSQLTimestamp(java.sql.Timestamp.valueOf("2024-01-01 00:00:00"))))
      val commit = writeBuilder.newCommit
      commit.commit(write.prepareCommit())
      commit.close()
      write.close()

      // write using spark sql
      sql("INSERT INTO t VALUES (2, TIMESTAMP '2024-01-01 00:00:00')")

      // check bucket id
      checkAnswer(
        sql("SELECT ts3, __paimon_bucket FROM t WHERE id = 1"),
        sql("SELECT ts3, __paimon_bucket FROM t WHERE id = 2")
      )
    }
  }

  BucketFunctionType
    .values()
    .foreach(
      funcType => {
        test(s"Paimon: Bucket table using $funcType bucket function") {
          withTable("T") {
            spark.sql(s"""
                         |CREATE TABLE T (a INT, b STRING, c INT) TBLPROPERTIES
                         |('bucket-function.type' = '$funcType',
                         |'bucket-key' = 'a',
                         |'bucket' = '4',
                         |'metadata.stats-mode' = 'none'
                         |)
                         |""".stripMargin)

            // disable filter by manifest and let bucket filter work
            for (i <- 0 until 10) {
              spark.sql(s"INSERT INTO T (a, b, c) VALUES ($i, '$i', $i)")
            }

            for (i <- 0 until 10) {
              val rows = spark.sql(s"SELECT * FROM T where a = $i").collect()
              Assertions.assertEquals(1, rows.length)
              Assertions.assertEquals(Row.fromSeq(Seq(i, String.valueOf(i), i)), rows(0))
            }
          }
        }
      })
}
