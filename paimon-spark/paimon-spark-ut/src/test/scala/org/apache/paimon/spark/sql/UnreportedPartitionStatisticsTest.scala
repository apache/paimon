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

import org.apache.paimon.partition.PartitionStatistics
import org.apache.paimon.spark.{PaimonSparkTestBase, SparkCatalog, SparkTable}
import org.apache.paimon.table.{Table => PaimonTable}

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{Identifier => SparkIdentifier, Table => SparkConnectorTable}

import java.util.{Map => JMap}

import scala.collection.JavaConverters._

/** Verifies that Spark display commands do not show negative or missing statistics as measurements. */
class UnreportedPartitionStatisticsTest extends PaimonSparkTestBase {

  private val partition = "20250915"

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set("spark.sql.catalog.paimon", classOf[UnreportedStatisticsSparkCatalog].getName)

  override protected def beforeEach(): Unit = {
    UnreportedStatistics.reset()
    super.beforeEach()
  }

  override protected def afterEach(): Unit = {
    try {
      super.afterEach()
    } finally {
      UnreportedStatistics.reset()
    }
  }

  test("DESCRIBE PARTITION omits unreported statistics") {
    val tableName = "describe_unreported_statistics"
    withTable(tableName) {
      createPartitionedTable(tableName)
      insertTwoRows(tableName)

      val details = UnreportedStatistics.withAllUnreportedMetadata {
        partitionBlockOf(tableName)
      }
      val rendered = details.mkString("\n")

      // Unknown statistics must not suppress the remaining partition details.
      assert(details.exists(_._1 == "Partition Values"), rendered)
      assert(!details.exists(_._1 == "Partition Parameters"), rendered)
      assert(!details.exists(_._1 == "Partition Statistics"), rendered)
      // An unreported creation time would otherwise read as a 1969 date.
      assert(details.toMap.apply("Created Time") == "UNKNOWN", rendered)
      assert(!details.exists { case (_, value) => value.contains("-1") }, rendered)
    }
  }

  test("DESCRIBE PARTITION preserves reported statistics") {
    val tableName = "describe_reported_statistics"
    withTable(tableName) {
      createPartitionedTable(tableName)
      insertTwoRows(tableName)

      val details = partitionBlockOf(tableName).toMap
      val rendered = details.mkString("\n")

      assert(
        details("Partition Parameters")
          .contains(s"${PartitionStatistics.FIELD_RECORD_COUNT}=2"),
        rendered)
      assert(details("Partition Statistics").matches("""\d+ bytes, 2 rows"""), rendered)
      assert(details("Created Time") != "UNKNOWN", rendered)
    }
  }

  test("SHOW TABLE EXTENDED PARTITION renders unreported statistics as UNKNOWN") {
    val tableName = "show_unreported_statistics"
    withTable(tableName) {
      createPartitionedTable(tableName)
      insertTwoRows(tableName)

      val information = UnreportedStatistics.withAllUnreportedMetadata {
        partitionInformationOf(tableName)
      }

      assert(information.contains("Partition Statistics: UNKNOWN rows, UNKNOWN bytes"), information)
      assert(!information.contains("Partition Parameters"), information)
      assert(!information.contains("-1"), information)
    }
  }

  test("SHOW TABLE EXTENDED PARTITION renders missing statistics as UNKNOWN") {
    val tableName = "show_missing_statistics"
    withTable(tableName) {
      createPartitionedTable(tableName)
      insertTwoRows(tableName)

      val information = UnreportedStatistics.withMissingMetadata(
        PartitionStatistics.FIELD_RECORD_COUNT,
        PartitionStatistics.FIELD_FILE_SIZE_IN_BYTES) {
        partitionInformationOf(tableName)
      }

      assert(information.contains("Partition Statistics: UNKNOWN rows, UNKNOWN bytes"), information)
      assert(information.contains(s"${PartitionStatistics.FIELD_FILE_COUNT}="), information)
      assert(!information.contains(s"${PartitionStatistics.FIELD_RECORD_COUNT}="), information)
      assert(
        !information.contains(s"${PartitionStatistics.FIELD_FILE_SIZE_IN_BYTES}="),
        information)
    }
  }

  test("SHOW TABLE EXTENDED PARTITION preserves reported statistics") {
    val tableName = "show_reported_statistics"
    withTable(tableName) {
      createPartitionedTable(tableName)
      insertTwoRows(tableName)

      val information = partitionInformationOf(tableName)

      assert(information.contains(s"${PartitionStatistics.FIELD_RECORD_COUNT}=2"), information)
      val statistics = """Partition Statistics: 2 rows, (\d+) bytes""".r
      assert(statistics.findFirstMatchIn(information).exists(_.group(1).toLong > 0L), information)
    }
  }

  test("DESCRIBE PARTITION handles row count and size independently") {
    val tableName = "describe_mixed_statistics"
    withTable(tableName) {
      createPartitionedTable(tableName)
      insertTwoRows(tableName)

      val rowCountUnknown =
        UnreportedStatistics.withUnreportedMetadata(PartitionStatistics.FIELD_RECORD_COUNT) {
          partitionBlockOf(tableName)
        }
      val rowCountUnknownMap = rowCountUnknown.toMap
      val rowCountUnknownRendered = rowCountUnknown.mkString("\n")
      val rowCountUnknownParameters = rowCountUnknownMap("Partition Parameters")
      assert(
        rowCountUnknownParameters.contains(s"${PartitionStatistics.FIELD_FILE_SIZE_IN_BYTES}="),
        rowCountUnknownRendered)
      assert(
        !rowCountUnknownParameters.contains(s"${PartitionStatistics.FIELD_RECORD_COUNT}="),
        rowCountUnknownRendered)
      assert(
        rowCountUnknownMap("Partition Statistics").matches("""\d+ bytes"""),
        rowCountUnknownRendered)

      val sizeUnknown =
        UnreportedStatistics.withUnreportedMetadata(PartitionStatistics.FIELD_FILE_SIZE_IN_BYTES) {
          partitionBlockOf(tableName)
        }
      val sizeUnknownMap = sizeUnknown.toMap
      val sizeUnknownRendered = sizeUnknown.mkString("\n")
      val sizeUnknownParameters = sizeUnknownMap("Partition Parameters")
      assert(
        sizeUnknownParameters.contains(s"${PartitionStatistics.FIELD_RECORD_COUNT}=2"),
        sizeUnknownRendered)
      assert(
        !sizeUnknownParameters.contains(s"${PartitionStatistics.FIELD_FILE_SIZE_IN_BYTES}="),
        sizeUnknownRendered)
      assert(!sizeUnknownMap.contains("Partition Statistics"), sizeUnknownRendered)
    }
  }

  private def createPartitionedTable(tableName: String): Unit =
    sql(s"CREATE TABLE $tableName (id INT, dt STRING) PARTITIONED BY (dt)")

  private def insertTwoRows(tableName: String): Unit =
    sql(s"INSERT INTO $tableName VALUES (1, '$partition'), (2, '$partition')")

  /** The "# Detailed Partition Information" rows of DESCRIBE, as (col_name, data_type) pairs. */
  private def partitionBlockOf(tableName: String): Seq[(String, String)] = {
    val rows = sql(s"DESCRIBE FORMATTED $tableName PARTITION (dt = '$partition')")
      .collect()
      .map(row => (row.getString(0), row.getString(1)))
      .toSeq
    val header = rows.indexWhere(_._1 == "# Detailed Partition Information")
    assert(header >= 0, rows.mkString("\n"))
    // The block runs from the header to the blank row that closes it.
    rows.drop(header + 1).takeWhile(_._1.nonEmpty)
  }

  private def partitionInformationOf(tableName: String): String =
    sql(s"SHOW TABLE EXTENDED IN $dbName0 LIKE '$tableName' PARTITION (dt = '$partition')")
      .select("information")
      .collect()
      .head
      .getString(0)
}

/** Which statistics the display fixtures currently leave unreported. */
private[sql] object UnreportedStatistics {

  sealed trait MetadataMode
  case class Unreported(fields: Set[String]) extends MetadataMode
  case class Missing(fields: Set[String]) extends MetadataMode

  /** The partition metadata override applied by [[UnreportedStatisticsSparkCatalog]]. */
  @volatile var metadataMode: Option[MetadataMode] = None

  def reset(): Unit = {
    metadataMode = None
  }

  def withAllUnreportedMetadata[T](body: => T): T =
    withUnreportedMetadata(
      PartitionStatistics.FIELD_RECORD_COUNT,
      PartitionStatistics.FIELD_FILE_SIZE_IN_BYTES,
      PartitionStatistics.FIELD_FILE_COUNT,
      PartitionStatistics.FIELD_LAST_FILE_CREATION_TIME
    )(body)

  def withUnreportedMetadata[T](fields: String*)(body: => T): T =
    withMetadataMode(Unreported(fields.toSet))(body)

  def withMissingMetadata[T](fields: String*)(body: => T): T =
    withMetadataMode(Missing(fields.toSet))(body)

  private def withMetadataMode[T](mode: MetadataMode)(body: => T): T = {
    metadataMode = Some(mode)
    try body
    finally metadataMode = None
  }
}

/** Catalog fixture that masks selected partition statistics. */
private[sql] class UnreportedStatisticsSparkCatalog extends SparkCatalog {

  override def loadTable(ident: SparkIdentifier): SparkConnectorTable = {
    super.loadTable(ident) match {
      case table: SparkTable =>
        UnreportedStatistics.metadataMode match {
          case Some(mode) => new OverriddenMetadataTable(table.table, mode)
          case None => table
        }
      case table => table
    }
  }
}

/** Table wrapper that replaces or removes partition statistics for display tests. */
private[sql] class OverriddenMetadataTable(
    paimonTable: PaimonTable,
    mode: UnreportedStatistics.MetadataMode)
  extends SparkTable(paimonTable) {

  override def loadPartitionMetadata(ident: InternalRow): JMap[String, String] = {
    val reported = super.loadPartitionMetadata(ident).asScala
    mode match {
      case UnreportedStatistics.Unreported(fields) =>
        reported.map {
          case (field, value) =>
            field -> (if (fields.contains(field)) PartitionStatistics.UNKNOWN.toString else value)
        }.asJava
      case UnreportedStatistics.Missing(fields) =>
        reported.filterNot { case (field, _) => fields.contains(field) }.asJava
    }
  }
}
