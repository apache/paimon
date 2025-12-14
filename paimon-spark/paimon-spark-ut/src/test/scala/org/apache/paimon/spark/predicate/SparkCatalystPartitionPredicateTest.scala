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

package org.apache.paimon.spark.predicate

import org.apache.paimon.data.DataFormatTestUtil.internalRowToString
import org.apache.paimon.partition.PartitionPredicate
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.table.DataTable
import org.apache.paimon.types.RowType

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.filter.SparkCatalystPartitionPredicate
import org.apache.spark.sql.catalyst.filter.SparkCatalystPartitionPredicate.extractSupportedPartitionFilters
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.assertj.core.api.Assertions.assertThat

import java.util.{List => JList}

import scala.collection.JavaConverters._

class SparkCatalystPartitionPredicateTest extends PaimonSparkTestBase {

  test("SparkCatalystPartitionPredicate: basic test") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (id INT, value INT, year STRING, month STRING, day STRING, hour STRING)
            |PARTITIONED BY (year, month, day, hour)
            |""".stripMargin)

      sql("""
            |INSERT INTO t values
            |(1, 100, '2024', '07', '15', '21'),
            |(3, 300, '2025', '07', '16', '22'),
            |(4, 400, '2025', '07', '16', '23'),
            |(5, 440, '2025', '07', '16', '25'),
            |(6, 500, '2025', '07', '17', '00'),
            |(7, 600, '2025', '07', '17', '02')
            |""".stripMargin)

      val q =
        """
          |SELECT * FROM t
          |WHERE CONCAT_WS('-', year, month, day, hour)
          |BETWEEN '2025-07-16-21' AND '2025-07-16-24'
          |AND value > 400
          |ORDER BY id
          |""".stripMargin

      val table = loadTable("t")
      val partitionRowType = table.rowType().project(table.partitionKeys())

      val filters = extractCatalystFilters(q)
      assert(filters.size == 4)
      val partitionFilters = extractSupportedPartitionFilters(filters, partitionRowType)
      assert(partitionFilters.size == 2)

      val partitionPredicate = SparkCatalystPartitionPredicate(partitionFilters, partitionRowType)
      assertThat[String](getFilteredPartitions(table, partitionRowType, partitionPredicate))
        .containsExactlyInAnyOrder("+I[2025, 07, 16, 22]", "+I[2025, 07, 16, 23]")
    }
  }

  test("SparkCatalystPartitionPredicate: varchar partition column") {
    withTable("t_varchar") {
      sql("""
            |CREATE TABLE t_varchar (id INT, value INT, region VARCHAR(20), city VARCHAR(20))
            |PARTITIONED BY (region, city)
            |""".stripMargin)

      sql("""
            |INSERT INTO t_varchar values
            |(1, 100, 'north', 'beijing'),
            |(2, 200, 'south', 'shanghai'),
            |(3, 300, 'east', 'hangzhou'),
            |(4, 400, 'west', 'chengdu'),
            |(5, 500, 'north', 'tianjin')
            |""".stripMargin)

      val q =
        """
          |SELECT * FROM t_varchar
          |WHERE CONCAT_WS('-', region, city)
          |BETWEEN 'north-beijing' AND 'south-shanghai'
          |ORDER BY id
          |""".stripMargin

      val table = loadTable("t_varchar")
      val partitionRowType = table.rowType().project(table.partitionKeys())
      val partitionFilters =
        extractSupportedPartitionFilters(extractCatalystFilters(q), partitionRowType)

      val partitionPredicate = SparkCatalystPartitionPredicate(partitionFilters, partitionRowType)
      assertThat[String](getFilteredPartitions(table, partitionRowType, partitionPredicate))
        .containsExactlyInAnyOrder(
          "+I[north, beijing]",
          "+I[south, shanghai]",
          "+I[north, tianjin]")

      // swap cols
      val q2 =
        """
          |SELECT * FROM t_varchar
          |WHERE CONCAT_WS('-', city, region)
          |BETWEEN 'beijing-north' AND 'chengdu-west'
          |ORDER BY id
          |""".stripMargin

      val partitionFilters2 =
        extractSupportedPartitionFilters(extractCatalystFilters(q2), partitionRowType)
      val partitionPredicate2 = SparkCatalystPartitionPredicate(partitionFilters2, partitionRowType)
      assertThat[String](getFilteredPartitions(table, partitionRowType, partitionPredicate2))
        .containsExactlyInAnyOrder("+I[north, beijing]", "+I[west, chengdu]")
    }
  }

  test("SparkCatalystPartitionPredicate: cast") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (id int, value int, dt STRING)
            |using paimon
            |PARTITIONED BY (dt)
            |""".stripMargin)

      sql("""
            |INSERT INTO t values
            |(1, 100, '1'), (2, 111, '2')
            |""".stripMargin)

      val q = "SELECT * FROM t WHERE dt = 1"
      val table = loadTable("t")
      val partitionRowType = table.rowType().project(table.partitionKeys())
      val partitionFilters =
        extractSupportedPartitionFilters(extractCatalystFilters(q), partitionRowType)

      val partitionPredicate = SparkCatalystPartitionPredicate(partitionFilters, partitionRowType)
      assertThat[String](getFilteredPartitions(table, partitionRowType, partitionPredicate))
        .containsExactlyInAnyOrder("+I[1]")
    }
  }

  test("SparkCatalystPartitionPredicate: null partition") {
    withTable("t") {
      sql("""
            |CREATE TABLE t (id INT, value INT, region STRING, city INT)
            |PARTITIONED BY (region, city)
            |""".stripMargin)

      sql("INSERT INTO t values (1, 100, 'north', null)")

      val table = loadTable("t")
      val partitionRowType = table.rowType().project(table.partitionKeys())

      val q =
        """
          |SELECT * FROM t
          |WHERE CONCAT_WS('-', region, city) = 'north'
          |""".stripMargin
      checkAnswer(sql(q), Seq(Row(1, 100, "north", null)))

      val partitionFilters =
        extractSupportedPartitionFilters(extractCatalystFilters(q), partitionRowType)
      val partitionPredicate = SparkCatalystPartitionPredicate(partitionFilters, partitionRowType)
      assertThat[String](getFilteredPartitions(table, partitionRowType, partitionPredicate))
        .containsExactlyInAnyOrder("+I[north, NULL]")

      val q2 =
        """
          |SELECT * FROM t
          |WHERE CONCAT_WS('-', region, city) != 'north'
          |""".stripMargin
      checkAnswer(sql(q2), Seq())

      val partitionFilters2 =
        extractSupportedPartitionFilters(extractCatalystFilters(q2), partitionRowType)
      val partitionPredicate2 = SparkCatalystPartitionPredicate(partitionFilters2, partitionRowType)
      assert(getFilteredPartitions(table, partitionRowType, partitionPredicate2).isEmpty)
    }
  }

  def extractCatalystFilters(sqlStr: String): Seq[Expression] = {
    var filters: Seq[Expression] = Seq.empty
    // Set ansi false to make sure some filters like `Cast` not push down
    withSparkSQLConf("spark.sql.ansi.enabled" -> "false") {
      filters = sql(sqlStr).queryExecution.optimizedPlan
        .collect { case Filter(condition, _) => condition }
        .flatMap(splitConjunctivePredicates)
    }
    filters
  }

  def getFilteredPartitions(
      table: DataTable,
      partitionRowType: RowType,
      partitionPredicate: PartitionPredicate): JList[String] = {
    table
      .newScan()
      .withPartitionFilter(partitionPredicate)
      .listPartitions()
      .asScala
      .map(r => internalRowToString(r, partitionRowType))
      .asJava
  }
}
