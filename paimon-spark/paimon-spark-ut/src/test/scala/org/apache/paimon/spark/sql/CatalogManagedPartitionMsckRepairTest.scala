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

import org.apache.paimon.catalog.Identifier
import org.apache.paimon.fs.Path
import org.apache.paimon.partition.Partition
import org.apache.paimon.spark.{PaimonSparkTestWithRestCatalogBase, SparkCatalog}
import org.apache.paimon.spark.execution.PaimonRepairFormatTablePartitionsExec
import org.apache.paimon.spark.format.PaimonFormatTable
import org.apache.paimon.table.FormatTable
import org.apache.paimon.table.format.FormatTablePartitionManager

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.catalog.{Identifier => SparkIdentifier, Table => SparkTable}
import org.apache.spark.sql.execution.CommandResultExec

import java.lang.reflect.InvocationTargetException
import java.util.{List => JList, Map => JMap}

import scala.collection.JavaConverters._
import scala.collection.mutable

class CatalogManagedPartitionMsckRepairTest extends PaimonSparkTestWithRestCatalogBase {

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set("spark.sql.catalog.paimon", classOf[FaultInjectingFormatTableSparkCatalog].getName)

  override protected def beforeEach(): Unit = {
    MsckFaultInjection.reset()
    super.beforeEach()
  }

  override protected def afterEach(): Unit = {
    try {
      super.afterEach()
    } finally {
      MsckFaultInjection.reset()
    }
  }

  test("bare MSCK, explicit ADD, and REPAIR alias register filesystem-only partitions") {
    val tableName = "msck_add_forms"
    val first = "20260715"
    val second = "20260716"
    val third = "20260717"

    withTable(tableName) {
      createFormatTableWithCatalogManagedPartitions(tableName)

      writeCsvPartition(tableName, first, 15, "bare-msck")
      executeCatalogManagedRepair(s"MSCK REPAIR TABLE paimon.$dbName0.$tableName")
      assertPartitionState(tableName, Set(first))

      writeCsvPartition(tableName, second, 16, "explicit-add")
      executeCatalogManagedRepair(s"MSCK REPAIR TABLE paimon.$dbName0.$tableName ADD PARTITIONS")
      assertPartitionState(tableName, Set(first, second))

      writeCsvPartition(tableName, third, 17, "repair-alias")
      executeCatalogManagedRepair(s"REPAIR TABLE paimon.$dbName0.$tableName ADD PARTITIONS")
      assertPartitionState(tableName, Set(first, second, third))

      executeCatalogManagedRepair(s"REPAIR TABLE paimon.$dbName0.$tableName ADD PARTITIONS")

      assertPartitionState(tableName, Set(first, second, third))
      assert(MsckFaultInjection.createCalls == 3)
      assertRowsAndChecksum(
        tableName,
        Seq(
          Row(15, "bare-msck", first),
          Row(16, "explicit-add", second),
          Row(17, "repair-alias", third)),
        48L)
    }
  }

  test("DROP unregisters only catalog-only partitions and preserves valid files") {
    val tableName = "msck_drop_catalog_only"
    val catalogOnly = "20260714"
    val common = "20260715"

    withTable(tableName) {
      createFormatTableWithCatalogManagedPartitions(tableName)
      val commonPath = writeCsvPartition(tableName, common, 15, "common")
      registerPartitions(tableName, catalogOnly, common)

      executeCatalogManagedRepair(s"MSCK REPAIR TABLE paimon.$dbName0.$tableName DROP PARTITIONS")

      assertPartitionState(tableName, Set(common))
      assert(formatTable(tableName).fileIO().exists(commonPath))
      assertRowsAndChecksum(tableName, Seq(Row(15, "common", common)), 15L)

      executeCatalogManagedRepair(s"MSCK REPAIR TABLE paimon.$dbName0.$tableName DROP PARTITIONS")
      assertPartitionState(tableName, Set(common))
      assert(MsckFaultInjection.createCalls == 0)
      assert(MsckFaultInjection.dropCalls == 1)
    }
  }

  test("SYNC converges both directions and is idempotent") {
    val tableName = "msck_sync_both_directions"
    val catalogOnly = "20260714"
    val filesystemOnly = "20260715"
    val common = "20260716"

    withTable(tableName) {
      createFormatTableWithCatalogManagedPartitions(tableName)
      val filesystemPath =
        writeCsvPartition(tableName, filesystemOnly, 15, "filesystem-only")
      val commonPath = writeCsvPartition(tableName, common, 16, "common")
      registerPartitions(tableName, catalogOnly, common)

      executeCatalogManagedRepair(s"MSCK REPAIR TABLE paimon.$dbName0.$tableName SYNC PARTITIONS")

      assertPartitionState(tableName, Set(filesystemOnly, common))
      assert(formatTable(tableName).fileIO().exists(filesystemPath))
      assert(formatTable(tableName).fileIO().exists(commonPath))
      assertRowsAndChecksum(
        tableName,
        Seq(Row(15, "filesystem-only", filesystemOnly), Row(16, "common", common)),
        31L)
      assert(MsckFaultInjection.createCalls == 1)
      assert(MsckFaultInjection.dropCalls == 1)

      executeCatalogManagedRepair(s"MSCK REPAIR TABLE paimon.$dbName0.$tableName SYNC PARTITIONS")
      assertPartitionState(tableName, Set(filesystemOnly, common))
      assert(MsckFaultInjection.createCalls == 1)
      assert(MsckFaultInjection.dropCalls == 1)
    }
  }

  test("MSCK rejects filesystem-discovered Format Tables and native Paimon tables") {
    val filesystemPartitioned = "msck_filesystem_format_table"
    val native = "msck_native_paimon_table"
    val partition = "20260715"

    withTable(filesystemPartitioned, native) {
      createFormatTableWithFilesystemPartitions(filesystemPartitioned)
      writeCsvPartition(filesystemPartitioned, partition, 15, "filesystem")
      checkAnswer(
        sql(s"SELECT id, payload, dt FROM $filesystemPartitioned"),
        Seq(Row(15, "filesystem", partition)))

      assertMsckRejected(s"MSCK REPAIR TABLE paimon.$dbName0.$filesystemPartitioned")
      checkAnswer(
        sql(s"SELECT id, payload, dt FROM $filesystemPartitioned"),
        Seq(Row(15, "filesystem", partition)))

      sql(
        s"CREATE TABLE $native (id INT, payload STRING, dt STRING) " +
          "USING paimon PARTITIONED BY (dt)")
      sql(s"INSERT INTO $native VALUES (16, 'native', '$partition')")

      assertMsckRejected(s"MSCK REPAIR TABLE paimon.$dbName0.$native")
      checkAnswer(sql(s"SELECT id, payload, dt FROM $native"), Seq(Row(16, "native", partition)))
      assert(MsckFaultInjection.createCalls == 0)
      assert(MsckFaultInjection.dropCalls == 0)
    }
  }

  test("SYNC propagates catalog LIST failure without mutating either side") {
    val tableName = "msck_sync_list_failure"
    val catalogOnly = "20260714"
    val filesystemOnly = "20260715"

    withTable(tableName) {
      createFormatTableWithCatalogManagedPartitions(tableName)
      val filesystemPath =
        writeCsvPartition(tableName, filesystemOnly, 15, "filesystem-only")
      registerPartitions(tableName, catalogOnly)
      MsckFaultInjection.failList = true

      val error = intercept[Exception] {
        sql(s"MSCK REPAIR TABLE paimon.$dbName0.$tableName SYNC PARTITIONS").collect()
      }

      assert(causeMessages(error).contains(MsckFaultInjection.LIST_FAILURE))
      assert(MsckFaultInjection.listCalls == 1)
      assert(MsckFaultInjection.createCalls == 0)
      assert(MsckFaultInjection.dropCalls == 0)
      assert(registeredPartitions(tableName) == Set(catalogOnly))
      assert(formatTable(tableName).fileIO().exists(filesystemPath))

      MsckFaultInjection.failList = false
      executeCatalogManagedRepair(s"MSCK REPAIR TABLE paimon.$dbName0.$tableName SYNC PARTITIONS")
      assertPartitionState(tableName, Set(filesystemOnly))
      assertRowsAndChecksum(tableName, Seq(Row(15, "filesystem-only", filesystemOnly)), 15L)
    }
  }

  test("direct repair mutates nothing but still refreshes when catalog LIST fails") {
    val tableName = "msck_direct_list_failure"

    withTable(tableName) {
      createFormatTableWithCatalogManagedPartitions(tableName)
      val gateway = new StatefulFaultCatalog
      gateway.failList = true
      var refreshCalls = 0
      val command = PaimonRepairFormatTablePartitionsExec(
        sparkTable(tableName, gateway),
        addPartitions = true,
        dropPartitions = true,
        () => refreshCalls += 1)

      val error = intercept[IllegalStateException] {
        runCommand(command)
      }

      assert(error.getMessage == MsckFaultInjection.LIST_FAILURE)
      assert(gateway.createCalls == 0)
      assert(gateway.dropCalls == 0)
      // A failed response cannot prove the service did nothing, so every repair attempt
      // refreshes the Spark-side caches.
      assert(refreshCalls == 1)
    }
  }

  test("direct repair refreshes after create applies and loses its response") {
    val tableName = "msck_direct_create_lost_response"
    val filesystemOnly = "20260715"

    withTable(tableName) {
      createFormatTableWithCatalogManagedPartitions(tableName)
      writeCsvPartition(tableName, filesystemOnly, 15, "filesystem-only")
      val gateway = new StatefulFaultCatalog
      gateway.failCreateAfterApply = true
      var refreshCalls = 0
      val command = PaimonRepairFormatTablePartitionsExec(
        sparkTable(tableName, gateway),
        addPartitions = true,
        dropPartitions = true,
        () => refreshCalls += 1)

      val error = intercept[IllegalStateException] {
        runCommand(command)
      }

      assert(error.getMessage == MsckFaultInjection.CREATE_LOST_RESPONSE)
      assert(gateway.partitions == Set(Map("dt" -> filesystemOnly)))
      assert(gateway.createCalls == 1)
      assert(gateway.dropCalls == 0)
      assert(refreshCalls == 1)
    }
  }

  test("direct repair keeps the DROP failure primary when refresh also fails") {
    val tableName = "msck_direct_drop_and_refresh_failure"
    val catalogOnly = "20260714"
    val filesystemOnly = "20260715"

    withTable(tableName) {
      createFormatTableWithCatalogManagedPartitions(tableName)
      writeCsvPartition(tableName, filesystemOnly, 15, "filesystem-only")
      val gateway = new StatefulFaultCatalog(Set(Map("dt" -> catalogOnly)))
      gateway.failDrop = true
      var refreshCalls = 0
      val command = PaimonRepairFormatTablePartitionsExec(
        sparkTable(tableName, gateway),
        addPartitions = true,
        dropPartitions = true,
        () => {
          refreshCalls += 1
          throw new IllegalStateException(MsckFaultInjection.REFRESH_FAILURE)
        }
      )

      val error = intercept[IllegalStateException] {
        runCommand(command)
      }

      // The DROP failure is what explains the repair, so it propagates and the refresh
      // failure rides along; the ADD stays durable and the refresh is still attempted.
      assert(error.getMessage == MsckFaultInjection.DROP_FAILURE)
      assert(error.getSuppressed.map(_.getMessage).toSeq == Seq(MsckFaultInjection.REFRESH_FAILURE))
      assert(gateway.partitions == Set(Map("dt" -> catalogOnly), Map("dt" -> filesystemOnly)))
      assert(gateway.createCalls == 1)
      assert(gateway.dropCalls == 1)
      assert(refreshCalls == 1)
    }
  }

  test("direct repair does not suppress a DROP throwable into itself") {
    val tableName = "msck_direct_same_drop_refresh_failure"
    val catalogOnly = "20260714"
    val filesystemOnly = "20260715"

    withTable(tableName) {
      createFormatTableWithCatalogManagedPartitions(tableName)
      writeCsvPartition(tableName, filesystemOnly, 15, "filesystem-only")
      val gateway = new StatefulFaultCatalog(Set(Map("dt" -> catalogOnly)))
      val dropFailure = new IllegalStateException(MsckFaultInjection.DROP_FAILURE)
      gateway.dropFailure = dropFailure
      var refreshCalls = 0
      val command = PaimonRepairFormatTablePartitionsExec(
        sparkTable(tableName, gateway),
        addPartitions = true,
        dropPartitions = true,
        () => {
          refreshCalls += 1
          throw dropFailure
        }
      )

      val error = intercept[Throwable] {
        runCommand(command)
      }

      // Refresh rethrows the very throwable the DROP failed with. Attaching it to itself would
      // throw IllegalArgumentException, so the two must be recognised as the same failure.
      assert(error eq dropFailure)
      assert(!error.isInstanceOf[MatchError])
      assert(error.getSuppressed.isEmpty)
      assert(gateway.partitions == Set(Map("dt" -> catalogOnly), Map("dt" -> filesystemOnly)))
      assert(gateway.createCalls == 1)
      assert(gateway.dropCalls == 1)
      assert(refreshCalls == 1)
    }
  }

  test("SYNC invalidates cached data and converges after ADD succeeds but DROP fails") {
    val tableName = "msck_partial_sync_failure"
    val filesystemOnly = "20260715"
    val catalogOnly = "20260714"

    withTable(tableName) {
      createFormatTableWithCatalogManagedPartitions(tableName)
      sql(s"CACHE TABLE $tableName").collect()
      assert(spark.catalog.isCached(tableName))
      checkAnswer(sql(s"SELECT * FROM $tableName"), Seq.empty[Row])

      writeCsvPartition(tableName, filesystemOnly, 15, "filesystem-only")
      registerPartitions(tableName, catalogOnly)
      MsckFaultInjection.failDrop = true

      val error = intercept[Exception] {
        sql(s"MSCK REPAIR TABLE paimon.$dbName0.$tableName SYNC PARTITIONS").collect()
      }

      assert(causeMessages(error).contains(MsckFaultInjection.DROP_FAILURE))
      assert(MsckFaultInjection.createCalls == 1)
      assert(MsckFaultInjection.dropCalls == 1)
      assert(registeredPartitions(tableName) == Set(filesystemOnly, catalogOnly))
      assert(spark.catalog.isCached(tableName))

      // ADD is already durable even though the command failed. A stale CACHE TABLE must not hide it.
      checkAnswer(
        sql(s"SELECT id, payload, dt FROM $tableName WHERE dt = '$filesystemOnly'"),
        Seq(Row(15, "filesystem-only", filesystemOnly)))

      MsckFaultInjection.failDrop = false
      executeCatalogManagedRepair(s"MSCK REPAIR TABLE paimon.$dbName0.$tableName SYNC PARTITIONS")

      assertPartitionState(tableName, Set(filesystemOnly))
      assert(MsckFaultInjection.createCalls == 1)
      assert(MsckFaultInjection.dropCalls == 2)
      assertRowsAndChecksum(tableName, Seq(Row(15, "filesystem-only", filesystemOnly)), 15L)

      executeCatalogManagedRepair(s"MSCK REPAIR TABLE paimon.$dbName0.$tableName SYNC PARTITIONS")
      assertPartitionState(tableName, Set(filesystemOnly))
      assert(MsckFaultInjection.createCalls == 1)
      assert(MsckFaultInjection.dropCalls == 2)
    }
  }

  private def createFormatTableWithCatalogManagedPartitions(tableName: String): Unit =
    sql(s"""CREATE TABLE $tableName (id INT, payload STRING, dt STRING)
           |USING CSV
           |PARTITIONED BY (dt)
           |TBLPROPERTIES (
           |  'format-table.implementation' = 'paimon',
           |  'format-table.partition-source' = 'rest')
           |""".stripMargin)

  private def createFormatTableWithFilesystemPartitions(tableName: String): Unit =
    sql(s"""CREATE TABLE $tableName (id INT, payload STRING, dt STRING)
           |USING CSV
           |PARTITIONED BY (dt)
           |TBLPROPERTIES (
           |  'format-table.implementation' = 'paimon',
           |  'format-table.partition-source' = 'filesystem')
           |""".stripMargin)

  private def executeCatalogManagedRepair(statement: String): Unit = {
    val result = sql(statement)
    val resultPlan = result.queryExecution.executedPlan
    assert(resultPlan.isInstanceOf[CommandResultExec], resultPlan.treeString)
    val commandPlan = resultPlan.asInstanceOf[CommandResultExec].commandPhysicalPlan
    assert(commandPlan.isInstanceOf[PaimonRepairFormatTablePartitionsExec], commandPlan.treeString)
    result.collect()
  }

  private def assertMsckRejected(statement: String): Unit = {
    val error = intercept[Exception] {
      sql(statement).collect()
    }
    val messages = causeMessages(error)
    assert(messages.contains("MSCK REPAIR TABLE"), messages)
    assert(messages.toLowerCase(java.util.Locale.ROOT).contains("not supported"), messages)
  }

  private def assertPartitionState(tableName: String, expected: Set[String]): Unit = {
    assert(registeredPartitions(tableName) == expected)
    assert(
      sql(s"SHOW PARTITIONS $tableName").collect().map(_.getString(0)).toSet ==
        expected.map(value => s"dt=$value"))
  }

  private def assertRowsAndChecksum(
      tableName: String,
      expectedRows: Seq[Row],
      expectedIdSum: Long): Unit = {
    checkAnswer(sql(s"SELECT id, payload, dt FROM $tableName ORDER BY id"), expectedRows)
    checkAnswer(
      sql(s"SELECT COUNT(*), COALESCE(SUM(id), 0) FROM $tableName"),
      Seq(Row(expectedRows.size.toLong, expectedIdSum)))
  }

  private def runCommand(command: AnyRef): Unit = {
    try {
      command.getClass.getMethod("run").invoke(command)
    } catch {
      case error: InvocationTargetException => throw error.getCause
    }
  }

  private def tableIdentifier(tableName: String): Identifier =
    Identifier.create(dbName0, tableName)

  private def formatTable(tableName: String): FormatTable =
    paimonCatalog.getTable(tableIdentifier(tableName)).asInstanceOf[FormatTable]

  private def sparkTable(
      tableName: String,
      partitionManager: FormatTablePartitionManager): PaimonFormatTable =
    new PaimonFormatTable(
      MsckTestFixtures.withPartitionManager(formatTable(tableName), partitionManager))

  private def writeCsvPartition(
      tableName: String,
      partition: String,
      id: Int,
      payload: String): Path = {
    val table = formatTable(tableName)
    val partitionPath = new Path(table.location(), s"dt=$partition")
    table.fileIO().mkdirs(partitionPath)
    table.fileIO().writeFile(new Path(partitionPath, f"part-$id%05d.csv"), s"$id,$payload\n", false)
    partitionPath
  }

  private def registerPartitions(tableName: String, partitions: String*): Unit =
    paimonCatalog.createPartitions(
      tableIdentifier(tableName),
      partitions.map(value => Map("dt" -> value).asJava).asJava,
      true)

  private def registeredPartitions(tableName: String): Set[String] =
    paimonCatalog
      .listPartitions(tableIdentifier(tableName))
      .asScala
      .map(_.spec().get("dt"))
      .toSet

  private def causeMessages(error: Throwable): String =
    Iterator
      .iterate(error)(_.getCause)
      .takeWhile(_ != null)
      .flatMap(cause => Option(cause.getMessage))
      .mkString(" | ")
}

private[sql] object MsckFaultInjection {
  val DROP_FAILURE = "injected MSCK partition drop failure"
  val LIST_FAILURE = "injected MSCK partition list failure"
  val CREATE_LOST_RESPONSE = "injected MSCK create lost response"
  val REFRESH_FAILURE = "injected MSCK cache refresh failure"

  @volatile var failDrop = false
  @volatile var failList = false
  @volatile var createCalls = 0
  @volatile var dropCalls = 0
  @volatile var listCalls = 0

  def reset(): Unit = {
    failDrop = false
    failList = false
    createCalls = 0
    dropCalls = 0
    listCalls = 0
  }
}

private[sql] class StatefulFaultCatalog(initial: Set[Map[String, String]] = Set.empty)
  extends FormatTablePartitionManager {

  private val state = mutable.LinkedHashSet(initial.toSeq: _*)
  var failList = false
  var failCreateAfterApply = false
  var failDrop = false
  var dropFailure: RuntimeException = null
  var createCalls = 0
  var dropCalls = 0

  def partitions: Set[Map[String, String]] = state.toSet

  override def createPartitions(
      partitions: JList[JMap[String, String]],
      ignoreIfExists: Boolean): Unit = {
    createCalls += 1
    state ++= partitions.asScala.map(_.asScala.toMap)
    if (failCreateAfterApply) {
      throw new IllegalStateException(MsckFaultInjection.CREATE_LOST_RESPONSE)
    }
  }

  override def dropPartitions(partitions: JList[JMap[String, String]]): Unit = {
    dropCalls += 1
    if (dropFailure != null) {
      throw dropFailure
    }
    if (failDrop) {
      throw new IllegalStateException(MsckFaultInjection.DROP_FAILURE)
    }
    state --= partitions.asScala.map(_.asScala.toMap)
  }

  override def listPartitionsByNames(partitions: JList[JMap[String, String]]): JList[Partition] =
    MsckTestFixtures.toPartitions(
      partitions.asScala.map(_.asScala.toMap).filter(state.contains).toSeq)

  override def listPartitions(prefix: JMap[String, String]): JList[Partition] = {
    if (failList) {
      throw new IllegalStateException(MsckFaultInjection.LIST_FAILURE)
    }
    MsckTestFixtures.toPartitions(state.toSeq)
  }
}

/** Test helpers shared by the fault-injecting catalogs of this suite. */
private[sql] object MsckTestFixtures {

  /** Partition metadata as a catalog returns it; only the spec matters for these tests. */
  def toPartitions(specs: Seq[Map[String, String]]): JList[Partition] =
    specs.map(spec => new Partition(spec.asJava, 0L, 0L, 0L, 0L, 0, false)).asJava

  private val rebound: JMap[FormatTable, FormatTable] =
    java.util.Collections.synchronizedMap(new java.util.IdentityHashMap[FormatTable, FormatTable]())

  /** Rebind a table once per source instance, so repeated loads return the same table. */
  def rebindOnce(table: FormatTable, partitionManager: FormatTablePartitionManager): FormatTable =
    rebound.computeIfAbsent(table, _ => withPartitionManager(table, partitionManager))

  /** Rebind a Format Table to another partition catalog, as loading it from a catalog would. */
  def withPartitionManager(
      table: FormatTable,
      partitionManager: FormatTablePartitionManager): FormatTable =
    FormatTable
      .builder()
      .fileIO(table.fileIO())
      .identifier(Identifier.fromString(table.fullName()))
      .rowType(table.rowType())
      .partitionKeys(table.partitionKeys())
      .location(table.location())
      .format(table.format())
      .options(table.options())
      .comment(table.comment().orElse(null))
      .catalogContext(table.catalogContext())
      .partitionManager(partitionManager)
      .build()
}

/** Spark-loadable test catalog which delegates all state to the embedded Paimon REST catalog. */
private[sql] class FaultInjectingFormatTableSparkCatalog extends SparkCatalog {

  override def loadTable(ident: SparkIdentifier): SparkTable = {
    super.loadTable(ident) match {
      case table: PaimonFormatTable if table.partitionManager != null =>
        // The catalog hands out one table instance per table, and Spark's cached plans compare
        // that instance; rebinding must be memoized per source instance or CACHE TABLE stops
        // matching the relation it cached.
        new PaimonFormatTable(
          MsckTestFixtures.rebindOnce(
            table.table,
            new FaultInjectingFormatTablePartitionManager(table.partitionManager)))
      case table => table
    }
  }
}

private[sql] class FaultInjectingFormatTablePartitionManager(delegate: FormatTablePartitionManager)
  extends FormatTablePartitionManager {

  override def createPartitions(
      partitions: JList[JMap[String, String]],
      ignoreIfExists: Boolean): Unit = {
    delegate.createPartitions(partitions, ignoreIfExists)
    MsckFaultInjection.createCalls += 1
  }

  override def dropPartitions(partitions: JList[JMap[String, String]]): Unit = {
    MsckFaultInjection.dropCalls += 1
    if (MsckFaultInjection.failDrop) {
      throw new IllegalStateException(MsckFaultInjection.DROP_FAILURE)
    }
    delegate.dropPartitions(partitions)
  }

  override def listPartitionsByNames(partitions: JList[JMap[String, String]]): JList[Partition] =
    delegate.listPartitionsByNames(partitions)

  override def listPartitions(prefix: JMap[String, String]): JList[Partition] = {
    MsckFaultInjection.listCalls += 1
    if (MsckFaultInjection.failList) {
      throw new IllegalStateException(MsckFaultInjection.LIST_FAILURE)
    }
    delegate.listPartitions(prefix)
  }
}
