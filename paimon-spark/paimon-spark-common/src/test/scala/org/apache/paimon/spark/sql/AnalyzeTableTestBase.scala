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
import org.apache.paimon.fs.{FileIO, Path}
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.stats.ColStats
import org.apache.paimon.utils.DateTimeUtils

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Assertions

abstract class AnalyzeTableTestBase extends PaimonSparkTestBase {

  test("Paimon analyze: analyze table only") {
    spark.sql(s"""
                 |CREATE TABLE T (id STRING, name STRING, i INT, l LONG)
                 |USING PAIMON
                 |TBLPROPERTIES ('primary-key'='id')
                 |""".stripMargin)

    spark.sql(s"INSERT INTO T VALUES ('1', 'a', 1, 1)")
    spark.sql(s"INSERT INTO T VALUES ('2', 'aaa', 1, 2)")

    spark.sql(s"ANALYZE TABLE T COMPUTE STATISTICS")

    val stats = loadTable("T").statistics().get()
    Assertions.assertEquals(2L, stats.mergedRecordCount().getAsLong)
    Assertions.assertTrue(stats.mergedRecordSize().isPresent)
    Assertions.assertTrue(stats.colStats().isEmpty)
  }

  test("Paimon analyze: test statistic system table") {
    spark.sql(s"""
                 |CREATE TABLE T (id STRING, name STRING, i INT, l LONG)
                 |USING PAIMON
                 |TBLPROPERTIES ('primary-key'='id')
                 |""".stripMargin)

    spark.sql(s"INSERT INTO T VALUES ('1', 'a', 1, 1)")
    spark.sql(s"INSERT INTO T VALUES ('2', 'aaa', 1, 2)")
    Assertions.assertEquals(0, spark.sql("select * from `T$statistics`").count())

    spark.sql(s"ANALYZE TABLE T COMPUTE STATISTICS")

    val df =
      spark.sql("select snapshot_id, schema_id, mergedRecordCount, colstat from `T$statistics`")
    Assertions.assertEquals(df.collect().size, 1)
    checkAnswer(
      spark.sql("SELECT snapshot_id, schema_id, mergedRecordCount, colstat from `T$statistics`"),
      Row(2, 0, 2, "{ }"))
  }

  test("Paimon analyze: analyze table without snapshot") {
    spark.sql(s"CREATE TABLE T (id STRING, name STRING)")
    spark.sql(s"ANALYZE TABLE T COMPUTE STATISTICS")
    spark.sql(s"ANALYZE TABLE T COMPUTE STATISTICS FOR ALL COLUMNS")
    Assertions.assertEquals(0, spark.sql("select * from `T$statistics`").count())
  }

  test("Paimon analyze: analyze no scan") {
    spark.sql(s"CREATE TABLE T (id STRING, name STRING)")
    assertThatThrownBy(() => spark.sql(s"ANALYZE TABLE T COMPUTE STATISTICS NOSCAN"))
      .hasMessageContaining("NOSCAN is ineffective with paimon")
  }

  test("Paimon analyze: analyze table partition") {
    spark.sql(s"CREATE TABLE T (id STRING, name STRING, pt STRING) PARTITIONED BY (id, pt) ")
    assertThatThrownBy(() => spark.sql(s"ANALYZE TABLE T PARTITION (pt = '1') COMPUTE STATISTICS"))
      .hasMessageContaining("Analyze table partition is not supported")
  }

  test("Paimon analyze: analyze all supported cols") {
    spark.sql(
      s"""
         |CREATE TABLE T (id STRING, name STRING, byte_col BYTE, short_col SHORT, int_col INT, long_col LONG,
         |float_col FLOAT, double_col DOUBLE, decimal_col DECIMAL(10, 5), boolean_col BOOLEAN, date_col DATE,
         |timestamp_col TIMESTAMP, binary BINARY)
         |USING PAIMON
         |TBLPROPERTIES ('primary-key'='id')
         |""".stripMargin)

    spark.sql(
      s"INSERT INTO T VALUES ('1', 'a', 1, 1, 1, 1, 1.0, 1.0, 12.12345, true, cast('2020-01-01' as date), cast('2020-01-01 00:00:00' as timestamp), binary('example binary1'))")
    spark.sql(
      s"INSERT INTO T VALUES ('2', 'aaa', 1, null, 1, 1, 1.0, 1.0, 12.12345, true, cast('2020-01-02' as date), cast('2020-01-02 00:00:00' as timestamp), binary('example binary1'))")
    spark.sql(
      s"INSERT INTO T VALUES ('3', 'bbbb', 2, 1, 1, 1, 1.0, 1.0, 22.12345, true, cast('2020-01-02' as date), cast('2020-01-02 00:00:00' as timestamp), null)")
    spark.sql(
      s"INSERT INTO T VALUES ('4', 'bbbbbbbb', 2, 2, 2, 2, 2.0, 2.0, 22.12345, false, cast('2020-01-01' as date), cast('2020-01-01 00:00:00' as timestamp), binary('example binary2'))")

    spark.sql(s"ANALYZE TABLE T COMPUTE STATISTICS FOR ALL COLUMNS")

    var stats = loadTable("T").statistics().get()
    Assertions.assertEquals(4L, stats.mergedRecordCount().getAsLong)
    Assertions.assertTrue(stats.mergedRecordSize().isPresent)

    var colStats = stats.colStats()
    Assertions.assertEquals(ColStats.newColStats(0, 4, null, null, 0, 1, 1), colStats.get("id"))
    Assertions.assertEquals(ColStats.newColStats(1, 4, null, null, 0, 4, 8), colStats.get("name"))
    Assertions.assertEquals(
      ColStats.newColStats(2, 2, 1.toByte, 2.toByte, 0, 1, 1),
      colStats.get("byte_col"))
    Assertions.assertEquals(
      ColStats.newColStats(3, 2, 1.toShort, 2.toShort, 1, 2, 2),
      colStats.get("short_col"))
    Assertions.assertEquals(ColStats.newColStats(4, 2, 1, 2, 0, 4, 4), colStats.get("int_col"))
    Assertions.assertEquals(ColStats.newColStats(5, 2, 1L, 2L, 0, 8, 8), colStats.get("long_col"))
    Assertions.assertEquals(
      ColStats.newColStats(6, 2, 1.0f, 2.0f, 0, 4, 4),
      colStats.get("float_col"))
    Assertions.assertEquals(
      ColStats.newColStats(7, 2, 1.0d, 2.0d, 0, 8, 8),
      colStats.get("double_col"))
    Assertions.assertEquals(
      ColStats.newColStats(
        8,
        2,
        Decimal.fromBigDecimal(new java.math.BigDecimal("12.12345"), 10, 5),
        Decimal.fromBigDecimal(new java.math.BigDecimal("22.12345"), 10, 5),
        0,
        8,
        8),
      colStats.get("decimal_col")
    )
    Assertions.assertEquals(
      ColStats.newColStats(9, 2, false, true, 0, 1, 1),
      colStats.get("boolean_col"))
    Assertions.assertEquals(
      ColStats.newColStats(10, 2, 18262, 18263, 0, 4, 4),
      colStats.get("date_col"))
    Assertions.assertEquals(
      ColStats.newColStats(
        11,
        2,
        DateTimeUtils.parseTimestampData("2020-01-01 00:00:00", 0),
        DateTimeUtils.parseTimestampData("2020-01-02 00:00:00", 0),
        0,
        8,
        8),
      colStats.get("timestamp_col")
    )
    Assertions.assertEquals(
      ColStats.newColStats(12, 2, null, null, 1, 15, 15),
      colStats.get("binary"))

    spark.sql(
      s"INSERT INTO T VALUES ('5', 'bbbbbbbbbbbbbbbb', 3, 3, 3, 3, 3.0, 3.0, 32.12345, false, cast('2020-01-03' as date), cast('2020-01-03 00:00:00' as timestamp), binary('binary3'))")

    spark.sql(s"ANALYZE TABLE T COMPUTE STATISTICS FOR ALL COLUMNS")

    stats = loadTable("T").statistics().get()
    Assertions.assertEquals(5L, stats.mergedRecordCount().getAsLong)
    Assertions.assertTrue(stats.mergedRecordSize().isPresent)

    colStats = stats.colStats()
    Assertions.assertEquals(ColStats.newColStats(0, 5, null, null, 0, 1, 1), colStats.get("id"))
    Assertions.assertEquals(ColStats.newColStats(1, 5, null, null, 0, 7, 16), colStats.get("name"))
    Assertions.assertEquals(
      ColStats.newColStats(2, 3, 1.toByte, 3.toByte, 0, 1, 1),
      colStats.get("byte_col"))
    Assertions.assertEquals(
      ColStats.newColStats(3, 3, 1.toShort, 3.toShort, 1, 2, 2),
      colStats.get("short_col"))
    Assertions.assertEquals(ColStats.newColStats(4, 3, 1, 3, 0, 4, 4), colStats.get("int_col"))
    Assertions.assertEquals(ColStats.newColStats(5, 3, 1L, 3L, 0, 8, 8), colStats.get("long_col"))
    Assertions.assertEquals(
      ColStats.newColStats(6, 3, 1.0f, 3.0f, 0, 4, 4),
      colStats.get("float_col"))
    Assertions.assertEquals(
      ColStats.newColStats(7, 3, 1.0d, 3.0d, 0, 8, 8),
      colStats.get("double_col"))
    Assertions.assertEquals(
      ColStats.newColStats(
        8,
        3,
        Decimal.fromBigDecimal(new java.math.BigDecimal("12.12345"), 10, 5),
        Decimal.fromBigDecimal(new java.math.BigDecimal("32.12345"), 10, 5),
        0,
        8,
        8),
      colStats.get("decimal_col")
    )
    Assertions.assertEquals(
      ColStats.newColStats(9, 2, false, true, 0, 1, 1),
      colStats.get("boolean_col"))
    Assertions.assertEquals(
      ColStats.newColStats(10, 3, 18262, 18264, 0, 4, 4),
      colStats.get("date_col"))
    Assertions.assertEquals(
      ColStats.newColStats(
        11,
        3,
        DateTimeUtils.parseTimestampData("2020-01-01 00:00:00", 0),
        DateTimeUtils.parseTimestampData("2020-01-03 00:00:00", 0),
        0,
        8,
        8),
      colStats.get("timestamp_col")
    )
    Assertions.assertEquals(
      ColStats.newColStats(12, 3, null, null, 1, 13, 15),
      colStats.get("binary"))
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

    assertThatThrownBy(() => spark.sql(s"ANALYZE TABLE T COMPUTE STATISTICS FOR COLUMNS fake_col"))
      .hasMessageContaining("not found")
  }

  test("Paimon analyze: analyze specialized cols") {
    spark.sql(s"""
                 |CREATE TABLE T (id STRING, name STRING, i INT, l LONG)
                 |USING PAIMON
                 |TBLPROPERTIES ('primary-key'='id')
                 |""".stripMargin)

    spark.sql(s"INSERT INTO T VALUES ('1', 'a', 1, 1)")
    spark.sql(s"INSERT INTO T VALUES ('2', 'aaa', 1, 2)")

    spark.sql(s"ANALYZE TABLE T COMPUTE STATISTICS FOR COLUMNS name, i")

    val colStats = loadTable("T").statistics().get().colStats()
    Assertions.assertEquals(null, colStats.get("id"))
    Assertions.assertEquals(ColStats.newColStats(1, 2, null, null, 0, 2, 3), colStats.get("name"))
    Assertions.assertEquals(ColStats.newColStats(2, 1, 1, 1, 0, 4, 4), colStats.get("i"))
    Assertions.assertEquals(null, colStats.get("l"))

    checkAnswer(
      spark.sql(s"SELECT * from T ORDER BY id"),
      Row("1", "a", 1, 1) :: Row("2", "aaa", 1, 2) :: Nil)
  }

  test("Paimon analyze: statistics expire and clean") {
    spark.sql(s"""
                 |CREATE TABLE T (id STRING, name STRING)
                 |USING PAIMON
                 |TBLPROPERTIES ('primary-key'='id')
                 |""".stripMargin)

    val table = loadTable("T")
    val tableLocation = table.location()
    val fileIO = table.fileIO()

    spark.sql(s"INSERT INTO T VALUES ('1', 'a')")
    spark.sql(s"ANALYZE TABLE T COMPUTE STATISTICS")

    spark.sql(s"INSERT INTO T VALUES ('2', 'b')")
    spark.sql(s"ANALYZE TABLE T COMPUTE STATISTICS")
    Assertions.assertEquals(2, statsFileCount(tableLocation, fileIO))

    // test expire statistic
    spark.sql("CALL sys.expire_snapshots(table => 'test.T', retain_max => 1)")
    Assertions.assertEquals(1, statsFileCount(tableLocation, fileIO))

    val orphanStats = new Path(tableLocation, "statistics/stats-orphan-0")
    fileIO.writeFileUtf8(orphanStats, "x")
    Assertions.assertEquals(2, statsFileCount(tableLocation, fileIO))

    // test clean orhan statistic
    Thread.sleep(1001)
    val older_than = DateTimeUtils.formatLocalDateTime(
      DateTimeUtils.toLocalDateTime(System.currentTimeMillis()),
      3)
    spark.sql(s"CALL sys.remove_orphan_files(table => 'T', older_than => '$older_than')")
    Assertions.assertEquals(1, statsFileCount(tableLocation, fileIO))
  }

  test("Paimon analyze: spark use table stats") {
    spark.sql(s"""
                 |CREATE TABLE T (id STRING, name STRING, i INT, l LONG)
                 |USING PAIMON
                 |TBLPROPERTIES ('primary-key'='id')
                 |""".stripMargin)

    spark.sql(s"INSERT INTO T VALUES ('1', 'a', 1, 1)")
    spark.sql(s"INSERT INTO T VALUES ('2', 'aaa', 1, 2)")
    spark.sql(s"ANALYZE TABLE T COMPUTE STATISTICS")

    val stats = getScanStatistic("SELECT * FROM T")
    Assertions.assertEquals(2L, stats.rowCount.get.longValue())
  }

  test("Paimon analyze: spark use col stats") {
    spark.sql(s"""
                 |CREATE TABLE T (id STRING, name STRING, i INT, l LONG)
                 |USING PAIMON
                 |TBLPROPERTIES ('primary-key'='id')
                 |""".stripMargin)

    spark.sql(s"INSERT INTO T VALUES ('1', 'a', 1, 1)")
    spark.sql(s"INSERT INTO T VALUES ('2', 'aaa', 1, 2)")
    spark.sql(s"ANALYZE TABLE T COMPUTE STATISTICS FOR ALL COLUMNS")

    val stats = getScanStatistic("SELECT * FROM T")
    Assertions.assertEquals(2L, stats.rowCount.get.longValue())
    Assertions.assertEquals(if (gteqSpark3_4) 4 else 0, stats.attributeStats.size)
  }

  test("Paimon analyze: partition filter push down hit") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING, pt INT)
                 |TBLPROPERTIES ('primary-key'='id, pt', 'bucket'='2')
                 |PARTITIONED BY (pt)
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, 'a', 1), (2, 'b', 1), (3, 'c', 2), (4, 'd', 3)")
    spark.sql(s"ANALYZE TABLE T COMPUTE STATISTICS FOR ALL COLUMNS")

    // paimon will reserve partition filter and not return it to spark, we need to ensure stats are filtered correctly.
    // partition push down hit
    var sql = "SELECT * FROM T WHERE pt < 1"
    Assertions.assertEquals(
      if (gteqSpark3_4) 0L else 4L,
      getScanStatistic(sql).rowCount.get.longValue())
    checkAnswer(spark.sql(sql), Nil)

    // partition push down hit and select without it
    sql = "SELECT id FROM T WHERE pt < 1"
    Assertions.assertEquals(
      if (gteqSpark3_4) 0L else 4L,
      getScanStatistic(sql).rowCount.get.longValue())
    checkAnswer(spark.sql(sql), Nil)

    // partition push down not hit
    sql = "SELECT * FROM T WHERE id < 1"
    Assertions.assertEquals(4L, getScanStatistic(sql).rowCount.get.longValue())
    checkAnswer(spark.sql(sql), Nil)
  }

  protected def statsFileCount(tableLocation: Path, fileIO: FileIO): Int = {
    fileIO.listStatus(new Path(tableLocation, "statistics")).length
  }

  protected def getScanStatistic(sql: String): Statistics = {
    val relation = spark
      .sql(sql)
      .queryExecution
      .optimizedPlan
      .collectFirst { case relation: DataSourceV2ScanRelation => relation }
      .get
    relation.computeStats()
  }

}
