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

import org.apache.paimon.data.Decimal
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.types.{DataField, DataFieldStats}

import org.apache.spark.sql.Row
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Assertions

import java.util
import java.util.Objects

class AnalyzeTableTest extends PaimonSparkTestBase {
  test(s"test update with primary key") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING, dt STRING)
                 |TBLPROPERTIES ('primary-key' = 'id', 'merge-engine' = 'deduplicate')
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, 'a', '11'), (2, 'b', '22'), (3, 'c', '33')")

    spark.sql(s"ANALYZE TABLE T COMPUTE STATISTICS FOR COLUMNS id")

//    spark.sql(s"ANALYZE TABLE T COMPUTE STATISTICS")

    print()

  }

  test("Paimon analyze: analyze all supported cols") {
    spark.sql(
      s"""
         |CREATE TABLE T (id STRING, name STRING, byte_col BYTE, short_col SHORT, int_col INT, long_col LONG,
         |float_col FLOAT, double_col DOUBLE, decimal_col DECIMAL(10, 5), boolean_col BOOLEAN, date_col DATE,
         |timestamp_col TIMESTAMP, binary_col BINARY)
         |USING PAIMON
         |TBLPROPERTIES ('primary-key'='id')
         |""".stripMargin)

    spark.sql(
      s"INSERT INTO T VALUES ('1', 'a', 1, 1, 1, 1, 1.0, 1.0, 12.12345, true, to_date('2020-01-01'), to_timestamp('2020-01-01 00:00:00'), binary('example binary1'))")
    spark.sql(
      s"INSERT INTO T VALUES ('2', 'aaa', 1, null, 1, 1, 1.0, 1.0, 12.12345, true, to_date('2020-01-02'), to_timestamp('2020-01-02 00:00:00'), binary('example binary1'))")
    spark.sql(
      s"INSERT INTO T VALUES ('3', 'bbbb', 2, 1, 1, 1, 1.0, 1.0, 22.12345, true, to_date('2020-01-02'), to_timestamp('2020-01-02 00:00:00'), null)")
    spark.sql(
      s"INSERT INTO T VALUES ('4', 'bbbbbbbb', 2, 2, 2, 2, 2.0, 2.0, 22.12345, false, to_date('2020-01-01'), to_timestamp('2020-01-01 00:00:00'), binary('example binary2'))")

    spark.sql(s"ANALYZE TABLE T COMPUTE STATISTICS FOR ALL COLUMNS")

    val options = loadTable("T").options()
    Assertions.assertEquals(options.get("rowCount"), "4")

    var fields = loadTable("T").schema().fields()
    assertStatsEqual(fields, "id", new DataFieldStats(4, null, null, 0, 1, 1))
    assertStatsEqual(fields, "name", new DataFieldStats(4, null, null, 0, 4, 8))
    assertStatsEqual(fields, "byte_col", new DataFieldStats(2, null, null, 0, 1, 1))
    assertStatsEqual(fields, "short_col", new DataFieldStats(2, null, null, 1, 2, 2))
    assertStatsEqual(fields, "int_col", new DataFieldStats(2, 1, 2, 0, 4, 4))
    assertStatsEqual(fields, "long_col", new DataFieldStats(2, 1L, 2L, 0, 8, 8))
    assertStatsEqual(fields, "float_col", new DataFieldStats(2, null, null, 0, 4, 4))
    assertStatsEqual(fields, "double_col", new DataFieldStats(2, null, null, 0, 8, 8))
    assertStatsEqual(
      fields,
      "decimal_col",
      new DataFieldStats(
        2,
        Decimal.fromBigDecimal(new java.math.BigDecimal("12.12345"), 10, 5),
        Decimal.fromBigDecimal(new java.math.BigDecimal("22.12345"), 10, 5),
        0,
        8,
        8)
    )
    assertStatsEqual(fields, "boolean_col", new DataFieldStats(2, null, null, 0, 1, 1))
    assertStatsEqual(fields, "date_col", new DataFieldStats(2, 18262, 18263, 0, 4, 4))
    assertStatsEqual(fields, "timestamp_col", new DataFieldStats(2, null, null, 0, 8, 8))
    assertStatsEqual(fields, "binary_col", new DataFieldStats(2, null, null, 1, 15, 15))

    spark.sql(
      s"INSERT INTO T VALUES ('5', 'bbbbbbbbbbbbbbbb', 3, 3, 3, 3, 3.0, 3.0, 32.12345, false, to_date('2020-01-01'), to_timestamp('2020-01-01 00:00:00'), binary('binary3'))")

    spark.sql(s"ANALYZE TABLE T COMPUTE STATISTICS FOR ALL COLUMNS")
    fields = loadTable("T").schema().fields()
    assertStatsEqual(fields, "id", new DataFieldStats(5, null, null, 0, 1, 1))
    assertStatsEqual(fields, "name", new DataFieldStats(5, null, null, 0, 7, 16))
    assertStatsEqual(fields, "byte_col", new DataFieldStats(3, null, null, 0, 1, 1))
    assertStatsEqual(fields, "short_col", new DataFieldStats(3, null, null, 1, 2, 2))
    assertStatsEqual(fields, "int_col", new DataFieldStats(3, 1, 3, 0, 4, 4))
    assertStatsEqual(fields, "long_col", new DataFieldStats(3, 1L, 3L, 0, 8, 8))
    assertStatsEqual(fields, "float_col", new DataFieldStats(3, null, null, 0, 4, 4))
    assertStatsEqual(fields, "double_col", new DataFieldStats(3, null, null, 0, 8, 8))
    assertStatsEqual(
      fields,
      "decimal_col",
      new DataFieldStats(
        3,
        Decimal.fromBigDecimal(new java.math.BigDecimal("12.12345"), 10, 5),
        Decimal.fromBigDecimal(new java.math.BigDecimal("32.12345"), 10, 5),
        0,
        8,
        8)
    )
    assertStatsEqual(fields, "boolean_col", new DataFieldStats(2, null, null, 0, 1, 1))
    assertStatsEqual(fields, "date_col", new DataFieldStats(2, 18262, 18263, 0, 4, 4))
    assertStatsEqual(fields, "timestamp_col", new DataFieldStats(2, null, null, 0, 8, 8))
    assertStatsEqual(fields, "binary_col", new DataFieldStats(3, null, null, 1, 13, 15))

    spark.sql(s"select * from T")
  }

  test("Paimon analyze: analyze specialized cols") {
    spark.sql(s"""
                 |CREATE TABLE T (id STRING, name STRING, i INT, l LONG)
                 |USING PAIMON
                 |TBLPROPERTIES ('primary-key'='id')
                 |""".stripMargin)

    assertThatThrownBy(() => spark.sql(s"ANALYZE TABLE T COMPUTE STATISTICS FOR COLUMNS fake_col"))
      .hasMessageContaining("not found")
  }

  test("Paimon analyze: analyze unsupported cols") {
    spark.sql(
      s"""
         |CREATE TABLE T (id STRING, m MAP<INT, STRING>, l ARRAY<INT>, s STRUCT<i:INT, s:STRING>)
         |USING PAIMON
         |TBLPROPERTIES ('primary-key'='id')
         |""".stripMargin)

    spark.sql(s"INSERT INTO T VALUES ('1', map(1, 'a'), array(1), struct(1, 'a'))")

    assertThatThrownBy(() => spark.sql(s"ANALYZE TABLE T COMPUTE STATISTICS FOR COLUMNS m"))
      .hasMessageContaining("not supported")

    assertThatThrownBy(() => spark.sql(s"ANALYZE TABLE T COMPUTE STATISTICS FOR COLUMNS l"))
      .hasMessageContaining("not supported")

    assertThatThrownBy(() => spark.sql(s"ANALYZE TABLE T COMPUTE STATISTICS FOR COLUMNS s"))
      .hasMessageContaining("not supported")
  }

  test("Paimon analyze: analyze non-exist cols") {
    spark.sql(s"""
                 |CREATE TABLE T (id STRING, name STRING, i INT, l LONG)
                 |USING PAIMON
                 |TBLPROPERTIES ('primary-key'='id')
                 |""".stripMargin)

    spark.sql(s"INSERT INTO T VALUES ('1', 'a', 1, 1)")
    spark.sql(s"INSERT INTO T VALUES ('2', 'aaa', 1, 2)")

    spark.sql(s"ANALYZE TABLE T COMPUTE STATISTICS FOR COLUMNS name, i")
    val fields = loadTable("T").schema().fields()
    assertStatsEqual(fields, "id", null)
    assertStatsEqual(fields, "name", new DataFieldStats(2, null, null, 0, 2, 3))
    assertStatsEqual(fields, "i", new DataFieldStats(1, 1, 1, 0, 4, 4))
    assertStatsEqual(fields, "l", null)

    checkAnswer(
      spark.sql(s"SELECT * from T ORDER BY id"),
      Row("1", "a", 1, 1) :: Row("2", "aaa", 1, 2) :: Nil)
  }

  def assertStatsEqual(lst: util.List[DataField], name: String, stats: DataFieldStats): Unit = {
    Assertions.assertTrue(
      Objects
        .equals(lst.stream().filter(f => f.name().equals(name)).findFirst().get.stats(), stats))
  }
}
