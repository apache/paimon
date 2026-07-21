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

package org.apache.paimon.spark.execution

import org.apache.paimon.catalog.{CatalogContext, Identifier}
import org.apache.paimon.fs.{FileIO, Path}
import org.apache.paimon.fs.local.LocalFileIO
import org.apache.paimon.options.Options
import org.apache.paimon.partition.Partition
import org.apache.paimon.spark.PaimonSparkTestWithRestCatalogBase
import org.apache.paimon.spark.catalyst.plans.logical.PaimonDropPartitions
import org.apache.paimon.spark.format.PaimonFormatTable
import org.apache.paimon.table.FormatTable
import org.apache.paimon.table.format.FormatTablePartitionManager
import org.apache.paimon.types.DataTypes

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{NoSuchPartitionsException, ResolvedPartitionSpec, ResolvedTable}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, GenericInternalRow}
import org.apache.spark.sql.catalyst.plans.logical.{AddPartitions, DropPartitions, RepairTable, ShowPartitions}
import org.apache.spark.sql.connector.catalog.{Identifier => SparkIdentifier, TableCatalog}
import org.apache.spark.sql.types.StringType

import java.io.IOException
import java.lang.reflect.InvocationTargetException
import java.nio.file.Files
import java.util.{Collections, List => JList, Map => JMap}
import java.util.concurrent.{Callable, Executors, TimeUnit}

import scala.collection.JavaConverters._

class FormatTablePartitionDdlPlanningTest extends PaimonSparkTestWithRestCatalogBase {

  test("strategy preserves the complete ADD batch for the catalog-managed partition command") {
    val (table, _) = formatTable(withCatalogManagedPartitions = true)
    val resolved = ResolvedTable.create(
      spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog],
      SparkIdentifier.of(Array("test"), "format_table"),
      table)
    val parts = Seq(partition(20260715, 10), partition(20260716, 11))

    val plans = PaimonStrategy(spark).apply(AddPartitions(resolved, parts, ifNotExists = true))

    assert(plans.size == 1)
    val add = plans.head.asInstanceOf[PaimonAddFormatTablePartitionsExec]
    assert(add.partSpecs == parts)
    assert(add.ignoreIfExists)
  }

  test(
    "catalog-managed ADD performs no client-side existence lookup and forwards one atomic batch") {
    val (table, gateway) = formatTable(withCatalogManagedPartitions = true)
    var refreshCalls = 0
    val parts = Seq(partition(20260715, 10), partition(20260716, 11))
    val command = PaimonAddFormatTablePartitionsExec(
      table,
      parts,
      ignoreIfExists = true,
      () => refreshCalls += 1)

    runCommand(command)

    assert(gateway.createCalls == 1)
    assert(gateway.lookupCalls == 0)
    assert(gateway.ignoreIfExists)
    assert(
      gateway.created.map(_.asScala.toMap) == Seq(
        Map("dt" -> "20260715", "hh" -> "10"),
        Map("dt" -> "20260716", "hh" -> "11")))
    assert(refreshCalls == 1)
  }

  test("mock service owns partial repeats, all repeats, atomic failure, and concurrent ADD") {
    val gateway = new AtomicGateway(Set(Map("dt" -> "20260715", "hh" -> "10")))
    val table = new PaimonFormatTable(rawFormatTable(withCatalogManagedPartitions = true, gateway))
    val existing = partition(20260715, 10)
    val added = partition(20260716, 11)

    runCommand(
      PaimonAddFormatTablePartitionsExec(
        table,
        Seq(existing, added),
        ignoreIfExists = true,
        () => ()))
    runCommand(
      PaimonAddFormatTablePartitionsExec(
        table,
        Seq(existing, added),
        ignoreIfExists = true,
        () => ()))

    assert(
      gateway.state == Set(
        Map("dt" -> "20260715", "hh" -> "10"),
        Map("dt" -> "20260716", "hh" -> "11")))
    assert(gateway.batches.take(2).forall(_.size == 2))

    val ordinaryError = intercept[IllegalStateException] {
      runCommand(
        PaimonAddFormatTablePartitionsExec(
          table,
          Seq(existing, partition(20260717, 12)),
          ignoreIfExists = false,
          () => ()))
    }
    assert(ordinaryError.getMessage.contains("already exists"))
    assert(!gateway.state.exists(_("dt") == "20260717"))

    val concurrent = partition(20260718, 13)
    val executor = Executors.newFixedThreadPool(8)
    try {
      val tasks = (1 to 20).map {
        _ =>
          executor.submit(new Callable[Unit] {
            override def call(): Unit = runCommand(
              PaimonAddFormatTablePartitionsExec(
                table,
                Seq(concurrent),
                ignoreIfExists = true,
                () => ()))
          })
      }
      tasks.foreach(_.get(30, TimeUnit.SECONDS))
    } finally {
      executor.shutdownNow()
    }
    assert(gateway.state.count(_("dt") == "20260718") == 1)
  }

  test(
    "ADD and DROP always fail with an explicit unsupported error without catalog-managed partitions") {
    val (table, gateway) = formatTable(withCatalogManagedPartitions = false)
    val part = partition(20260715, 10)

    val addError = intercept[UnsupportedOperationException] {
      runCommand(
        PaimonAddFormatTablePartitionsExec(table, Seq(part), ignoreIfExists = false, () => ()))
    }
    assert(addError.getMessage.contains("ADD PARTITION is supported only"))
    assert(addError.getMessage.contains("catalog-managed"))

    val dropError = intercept[UnsupportedOperationException] {
      runCommand(
        PaimonDropFormatTablePartitionsExec(
          table,
          Seq(part),
          ifExists = true,
          purge = false,
          () => ()))
    }
    assert(dropError.getMessage.contains("DROP PARTITION is supported only"))
    assert(dropError.getMessage.contains("catalog-managed"))
    assert(gateway.createCalls == 0)
    assert(gateway.dropCalls == 0)
    assert(gateway.lookupCalls == 0)
  }

  test("SQL ADD PARTITION registers the partition and makes it queryable as empty") {
    val tableName = "catalog_partition_format_add_sql"
    withTable(tableName) {
      sql(s"""CREATE TABLE $tableName (id INT, dt STRING, hh STRING)
             |USING CSV
             |PARTITIONED BY (dt, hh)
             |TBLPROPERTIES (
             |  'format-table.implementation' = 'paimon',
             |  'metastore.partitioned-table' = 'true')
             |""".stripMargin)

      sql(s"ALTER TABLE $tableName ADD PARTITION (dt='20260715', hh='10')").collect()

      // The catalog is what makes the partition exist, and the directory is what lets a scan
      // read it as empty instead of failing.
      assert(registeredPartitionSpecs(tableName) == Set(Map("dt" -> "20260715", "hh" -> "10")))
      assert(shownPartitions(tableName) == Set("dt=20260715/hh=10"))
      checkAnswer(sql(s"SELECT * FROM $tableName"), Seq.empty[Row])

      // Adding it again without IF NOT EXISTS is the catalog's rejection, not a local one.
      intercept[Exception] {
        sql(s"ALTER TABLE $tableName ADD PARTITION (dt='20260715', hh='10')").collect()
      }
      sql(s"ALTER TABLE $tableName ADD IF NOT EXISTS PARTITION (dt='20260715', hh='10')").collect()
      assert(registeredPartitionSpecs(tableName) == Set(Map("dt" -> "20260715", "hh" -> "10")))
    }
  }

  test("SQL partition DDL on filesystem-discovered partitions always fails with a clear error") {
    val tableName = "filesystem_format_partition_ddl"
    withTable(tableName) {
      sql(
        s"CREATE TABLE $tableName (id INT, dt INT, hh INT) USING CSV " +
          "TBLPROPERTIES ('format-table.implementation'='paimon') PARTITIONED BY (dt, hh)")

      val addError = intercept[Exception] {
        sql(s"ALTER TABLE $tableName ADD PARTITION (dt=20260715, hh=10)").collect()
      }
      assert(causeMessages(addError).contains("ADD PARTITION is supported only"))

      val dropError = intercept[Exception] {
        sql(s"ALTER TABLE $tableName DROP IF EXISTS PARTITION (dt=20260715, hh=10)").collect()
      }
      assert(causeMessages(dropError).contains("DROP PARTITION is supported only"))

      // Partial specs keep Spark's strict partition-spec resolution when partitions are
      // discovered from the filesystem.
      intercept[AnalysisException] {
        sql(s"ALTER TABLE $tableName DROP PARTITION (dt=20260715)").collect()
      }
    }
  }

  test("rewrite routes catalog-managed DROP PARTITION through PaimonDropPartitions") {
    val catalogManagedName = "catalog_partition_format_drop_rewrite"
    val filesystemName = "filesystem_format_drop_rewrite"
    withTable(catalogManagedName, filesystemName) {
      sql(s"""CREATE TABLE $catalogManagedName (id INT, dt STRING, hh STRING)
             |USING CSV
             |PARTITIONED BY (dt, hh)
             |TBLPROPERTIES (
             |  'format-table.implementation' = 'paimon',
             |  'metastore.partitioned-table' = 'true')
             |""".stripMargin)
      sql(s"""CREATE TABLE $filesystemName (id INT, dt STRING, hh STRING)
             |USING CSV
             |PARTITIONED BY (dt, hh)
             |TBLPROPERTIES ('format-table.implementation' = 'paimon')
             |""".stripMargin)

      // RewriteSparkDDLCommands runs inside the extension parser, so parsePlan already returns
      // the rewritten plan for Format Tables with catalog-managed partitions.
      val catalogManagedPlan = spark.sessionState.sqlParser.parsePlan(
        s"ALTER TABLE $catalogManagedName DROP IF EXISTS PARTITION (dt='20260715')")
      assert(catalogManagedPlan.isInstanceOf[PaimonDropPartitions])
      val paimonDrop = catalogManagedPlan.asInstanceOf[PaimonDropPartitions]
      assert(paimonDrop.ifExists)
      assert(!paimonDrop.purge)

      // Format Tables using filesystem partition discovery keep Spark's own DropPartitions and
      // strict spec resolution.
      val filesystemPlan = spark.sessionState.sqlParser.parsePlan(
        s"ALTER TABLE $filesystemName DROP IF EXISTS PARTITION (dt='20260715')")
      assert(filesystemPlan.isInstanceOf[DropPartitions])
    }
  }

  test("SQL partial DROP with catalog-managed partitions unregisters the matching partitions") {
    val tableName = "catalog_partition_format_partial_drop_sql"
    withTable(tableName) {
      sql(s"""CREATE TABLE $tableName (id INT, dt STRING, hh STRING)
             |USING CSV
             |PARTITIONED BY (dt, hh)
             |TBLPROPERTIES (
             |  'format-table.implementation' = 'paimon',
             |  'metastore.partitioned-table' = 'true')
             |""".stripMargin)
      registerPartitions(
        tableName,
        Map("dt" -> "20260715", "hh" -> "10"),
        Map("dt" -> "20260715", "hh" -> "11"),
        Map("dt" -> "20260716", "hh" -> "10"))

      // Leading partial spec expands to every registered leaf partition below it.
      sql(s"ALTER TABLE $tableName DROP PARTITION (dt='20260715')")
      assert(shownPartitions(tableName) == Set("dt=20260716/hh=10"))

      // Complete specs honor IF EXISTS and fail loudly for unregistered partitions.
      sql(s"ALTER TABLE $tableName DROP IF EXISTS PARTITION (dt='20990101', hh='00')")
      val missingError = intercept[Exception] {
        sql(s"ALTER TABLE $tableName DROP PARTITION (dt='20990101', hh='00')").collect()
      }
      assert(
        Iterator
          .iterate(missingError: Throwable)(_.getCause)
          .takeWhile(_ != null)
          .exists(_.isInstanceOf[NoSuchPartitionsException]))
      assert(shownPartitions(tableName) == Set("dt=20260716/hh=10"))

      // Non-leading partial specs resolve by partition name through the gateway.
      sql(s"ALTER TABLE $tableName DROP PARTITION (hh='10')")
      assert(shownPartitions(tableName) == Set.empty)
    }
  }

  test(
    "strategy preserves the DROP batch and catalog-managed DROP unregisters through the gateway") {
    val (table, gateway) = formatTable(withCatalogManagedPartitions = true)
    val resolved = ResolvedTable.create(
      spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog],
      SparkIdentifier.of(Array("test"), "format_table"),
      table)
    val parts = Seq(partition(20260715, 10))
    val logical = PaimonDropPartitions(resolved, parts, true, false)

    val plans = PaimonStrategy(spark).apply(logical)
    assert(plans.size == 1)
    val command = plans.head.asInstanceOf[PaimonDropFormatTablePartitionsExec]
    assert(command.partSpecs == parts)
    assert(command.ifExists)
    assert(!command.purge)

    var refreshCalls = 0
    runCommand(command.copy(refreshCache = () => refreshCalls += 1))
    assert(gateway.lookupCalls == 1)
    assert(gateway.dropCalls == 1)
    assert(gateway.dropped.map(_.asScala.toMap) == Seq(Map("dt" -> "20260715", "hh" -> "10")))
    assert(refreshCalls == 1)
  }

  test("strategy threads IF EXISTS and PURGE flags into the format-table DROP command") {
    val (table, _) = formatTable(withCatalogManagedPartitions = true)
    val resolved = ResolvedTable.create(
      spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog],
      SparkIdentifier.of(Array("test"), "format_table"),
      table)
    val parts = Seq(partition(20260715, 10))

    val plans =
      PaimonStrategy(spark).apply(DropPartitions(resolved, parts, ifExists = false, purge = true))

    assert(plans.size == 1)
    val command = plans.head.asInstanceOf[PaimonDropFormatTablePartitionsExec]
    assert(command.partSpecs == parts)
    assert(!command.ifExists)
    assert(command.purge)
  }

  test("catalog-managed DROP PARTITION PURGE is rejected before any catalog access") {
    val (table, gateway) = formatTable(withCatalogManagedPartitions = true)

    val error = intercept[UnsupportedOperationException] {
      runCommand(
        PaimonDropFormatTablePartitionsExec(
          table,
          Seq(partition(20260715, 10)),
          ifExists = false,
          purge = true,
          () => fail("PURGE must not refresh the cache")))
    }

    assert(error.getMessage.contains("PURGE"))
    assert(gateway.lookupCalls == 0)
    assert(gateway.dropCalls == 0)
  }

  test("catalog-managed complete DROP without IF EXISTS fails for unregistered partitions") {
    val fileIO = LocalFileIO.create()
    val tablePath = new Path(Files.createTempDirectory("format-table-drop-unregistered").toUri)
    val pendingDir = new Path(tablePath, "dt=20260715/hh=10")
    var dropCalls = 0
    var refreshCalls = 0
    val gateway = new FormatTablePartitionManager {
      override def createPartitions(
          partitions: JList[JMap[String, String]],
          ignoreIfExists: Boolean): Unit = {}

      override def dropPartitions(partitions: JList[JMap[String, String]]): Unit = dropCalls += 1

      override def listPartitionsByNames(
          partitions: JList[JMap[String, String]]): JList[Partition] =
        Collections.emptyList()

      override def listPartitions(prefix: JMap[String, String]): JList[Partition] =
        throw new AssertionError("Complete specs must resolve through list-by-names")
    }

    try {
      fileIO.mkdirs(pendingDir)
      val table = new PaimonFormatTable(
        rawFormatTable(withCatalogManagedPartitions = true, gateway, tablePath.toString))

      intercept[NoSuchPartitionsException] {
        runCommand(
          PaimonDropFormatTablePartitionsExec(
            table,
            Seq(partition(20260715, 10)),
            ifExists = false,
            purge = false,
            () => refreshCalls += 1))
      }

      // The unregistered directory (e.g. awaiting MSCK registration) must survive untouched.
      assert(dropCalls == 0)
      assert(refreshCalls == 0)
      assert(fileIO.exists(pendingDir))
    } finally {
      fileIO.delete(tablePath, true)
    }
  }

  test(
    "catalog-managed DROP IF EXISTS drops only registered partitions and preserves pending data") {
    val fileIO = LocalFileIO.create()
    val tablePath = new Path(Files.createTempDirectory("format-table-drop-if-exists").toUri)
    val registeredSpec = Map("dt" -> "20260716", "hh" -> "11")
    val registeredDir = new Path(tablePath, "dt=20260716/hh=11")
    val pendingDir = new Path(tablePath, "dt=20260715/hh=10")
    var lookedUp = Seq.empty[Map[String, String]]
    var dropped = Seq.empty[Map[String, String]]
    var refreshCalls = 0
    val gateway = new FormatTablePartitionManager {
      override def createPartitions(
          partitions: JList[JMap[String, String]],
          ignoreIfExists: Boolean): Unit = {}

      override def dropPartitions(partitions: JList[JMap[String, String]]): Unit = {
        dropped = partitions.asScala.map(_.asScala.toMap).toSeq
      }

      override def listPartitionsByNames(
          partitions: JList[JMap[String, String]]): JList[Partition] = {
        lookedUp = partitions.asScala.map(_.asScala.toMap).toSeq
        registeredPartitions(
          partitions.asScala.map(_.asScala.toMap).filter(_ == registeredSpec).toSeq: _*)
      }

      override def listPartitions(prefix: JMap[String, String]): JList[Partition] =
        throw new AssertionError("Complete specs must resolve through list-by-names")
    }

    try {
      fileIO.mkdirs(pendingDir)
      fileIO.mkdirs(registeredDir)
      val table = new PaimonFormatTable(
        rawFormatTable(withCatalogManagedPartitions = true, gateway, tablePath.toString))

      runCommand(
        PaimonDropFormatTablePartitionsExec(
          table,
          Seq(partition(20260715, 10), partition(20260716, 11)),
          ifExists = true,
          purge = false,
          () => refreshCalls += 1))

      assert(
        lookedUp == Seq(
          Map("dt" -> "20260715", "hh" -> "10"),
          Map("dt" -> "20260716", "hh" -> "11")))
      assert(dropped == Seq(registeredSpec))
      assert(fileIO.exists(pendingDir))
      assert(!fileIO.exists(registeredDir))
      assert(refreshCalls == 1)
    } finally {
      fileIO.delete(tablePath, true)
    }
  }

  test("catalog-managed failed directory deletion stays unregistered and preserves pending data") {
    val tablePath = new Path(Files.createTempDirectory("format-table-drop-retry-safe").toUri)
    val failedSpec = Map("dt" -> "20260715", "hh" -> "10")
    val pendingSpec = Map("dt" -> "20260716", "hh" -> "11")
    val failedDir = new Path(tablePath, "dt=20260715/hh=10")
    val pendingDir = new Path(tablePath, "dt=20260716/hh=11")
    var failFirstDelete = true
    val fileIO = new LocalFileIO {
      override def delete(path: Path, recursive: Boolean): Boolean = {
        if (path == failedDir && failFirstDelete) {
          failFirstDelete = false
          throw new IOException("injected exact partition delete failure")
        }
        super.delete(path, recursive)
      }
    }
    var registered = Set(failedSpec)
    var compensationCreates = Seq.empty[(Seq[Map[String, String]], Boolean)]
    def newGateway(): FormatTablePartitionManager = new FormatTablePartitionManager {
      override def createPartitions(
          partitions: JList[JMap[String, String]],
          ignoreIfExists: Boolean): Unit = {
        val specs = partitions.asScala.map(_.asScala.toMap).toSeq
        compensationCreates :+= ((specs, ignoreIfExists))
        registered ++= specs
      }

      override def dropPartitions(partitions: JList[JMap[String, String]]): Unit = {
        registered --= partitions.asScala.map(_.asScala.toMap)
      }

      override def listPartitionsByNames(
          partitions: JList[JMap[String, String]]): JList[Partition] =
        registeredPartitions(partitions.asScala.map(_.asScala.toMap).filter(registered).toSeq: _*)

      override def listPartitions(prefix: JMap[String, String]): JList[Partition] =
        throw new AssertionError("Exact specs must resolve through list-by-names")
    }
    var refreshCalls = 0

    try {
      fileIO.mkdirs(failedDir)
      fileIO.mkdirs(pendingDir)
      val firstTable =
        new PaimonFormatTable(
          rawFormatTable(
            withCatalogManagedPartitions = true,
            newGateway(),
            tablePath.toString,
            fileIO))

      val firstError = intercept[IOException] {
        runCommand(
          PaimonDropFormatTablePartitionsExec(
            firstTable,
            Seq(partition(20260715, 10)),
            ifExists = true,
            purge = false,
            () => refreshCalls += 1))
      }
      assert(firstError.getMessage.contains("injected exact partition delete failure"))
      // The failed partition stays unregistered and is never recreated automatically.
      assert(registered.isEmpty)
      assert(compensationCreates.isEmpty)
      assert(fileIO.exists(failedDir))
      assert(fileIO.exists(pendingDir))
      assert(refreshCalls == 1)
    } finally {
      fileIO.delete(tablePath, true)
    }
  }

  test("catalog-managed ambiguous catalog response does not recreate registration") {
    val tablePath = new Path(Files.createTempDirectory("format-table-drop-ambiguous").toUri)
    val droppedSpec = Map("dt" -> "20260715", "hh" -> "10")
    val droppedDir = new Path(tablePath, "dt=20260715/hh=10")
    val pendingDir = new Path(tablePath, "dt=20260716/hh=11")
    val lostResponse = new IOException("injected lost catalog DROP response")
    var failAfterCommit = true
    var registered = Set(droppedSpec)
    var compensationCreates = Seq.empty[(Seq[Map[String, String]], Boolean)]
    def newGateway(): FormatTablePartitionManager = new FormatTablePartitionManager {
      override def createPartitions(
          partitions: JList[JMap[String, String]],
          ignoreIfExists: Boolean): Unit = {
        val specs = partitions.asScala.map(_.asScala.toMap).toSeq
        compensationCreates :+= ((specs, ignoreIfExists))
        registered ++= specs
      }

      override def dropPartitions(partitions: JList[JMap[String, String]]): Unit = {
        registered --= partitions.asScala.map(_.asScala.toMap)
        if (failAfterCommit) {
          failAfterCommit = false
          throw lostResponse
        }
      }

      override def listPartitionsByNames(
          partitions: JList[JMap[String, String]]): JList[Partition] =
        registeredPartitions(partitions.asScala.map(_.asScala.toMap).filter(registered).toSeq: _*)

      override def listPartitions(prefix: JMap[String, String]): JList[Partition] =
        throw new AssertionError("Exact specs must resolve through list-by-names")
    }
    val firstFileIO = LocalFileIO.create
    var refreshCalls = 0

    try {
      firstFileIO.mkdirs(droppedDir)
      firstFileIO.mkdirs(pendingDir)
      val firstTable = new PaimonFormatTable(
        rawFormatTable(
          withCatalogManagedPartitions = true,
          newGateway(),
          tablePath.toString,
          firstFileIO))

      val firstError = intercept[IOException] {
        runCommand(
          PaimonDropFormatTablePartitionsExec(
            firstTable,
            Seq(partition(20260715, 10)),
            ifExists = true,
            purge = false,
            () => refreshCalls += 1))
      }

      assert(firstError eq lostResponse)
      assert(registered.isEmpty)
      assert(compensationCreates.isEmpty)
      assert(firstFileIO.exists(droppedDir))
      assert(firstFileIO.exists(pendingDir))
      assert(refreshCalls == 1)
    } finally {
      firstFileIO.delete(tablePath, true)
    }
  }

  test("catalog-managed DROP resolves a non-leading partial spec by partition name") {
    var listedPrefixes = Seq.empty[Map[String, String]]
    var dropped = Seq.empty[Map[String, String]]
    val matching = Map("dt" -> "20260715", "hh" -> "10")
    val gateway = new FormatTablePartitionManager {
      override def createPartitions(
          partitions: JList[JMap[String, String]],
          ignoreIfExists: Boolean): Unit = {}

      override def dropPartitions(partitions: JList[JMap[String, String]]): Unit = {
        dropped = partitions.asScala.map(_.asScala.toMap).toSeq
      }

      override def listPartitionsByNames(
          partitions: JList[JMap[String, String]]): JList[Partition] =
        Collections.emptyList()

      override def listPartitions(prefix: JMap[String, String]): JList[Partition] = {
        listedPrefixes :+= prefix.asScala.toMap
        registeredPartitions(matching)
      }
    }
    val table = new PaimonFormatTable(rawFormatTable(withCatalogManagedPartitions = true, gateway))
    val hhOnly = ResolvedPartitionSpec(Seq("hh"), new GenericInternalRow(Array[Any](10)))

    runCommand(
      PaimonDropFormatTablePartitionsExec(
        table,
        Seq(hhOnly),
        ifExists = false,
        purge = false,
        () => ()))

    assert(listedPrefixes == Seq(Map.empty))
    assert(dropped == Seq(matching))
  }

  test("catalog-managed multiple non-leading partial DROP specs share one catalog traversal") {
    val first = Map("dt" -> "20260715", "hh" -> "10")
    val second = Map("dt" -> "20260716", "hh" -> "11")
    val third = Map("dt" -> "20260717", "hh" -> "10")
    val unrelated = Map("dt" -> "20260718", "hh" -> "12")
    var listedPrefixes = Seq.empty[Map[String, String]]
    var dropped = Seq.empty[Map[String, String]]
    val gateway = new FormatTablePartitionManager {
      override def createPartitions(
          partitions: JList[JMap[String, String]],
          ignoreIfExists: Boolean): Unit = {}

      override def dropPartitions(partitions: JList[JMap[String, String]]): Unit = {
        dropped = partitions.asScala.map(_.asScala.toMap).toSeq
      }

      override def listPartitionsByNames(
          partitions: JList[JMap[String, String]]): JList[Partition] =
        Collections.emptyList()

      override def listPartitions(prefix: JMap[String, String]): JList[Partition] = {
        listedPrefixes :+= prefix.asScala.toMap
        registeredPartitions(first, unrelated, second, third)
      }
    }
    val table = new PaimonFormatTable(rawFormatTable(withCatalogManagedPartitions = true, gateway))
    val hhTen = ResolvedPartitionSpec(Seq("hh"), new GenericInternalRow(Array[Any](10)))
    val hhEleven = ResolvedPartitionSpec(Seq("hh"), new GenericInternalRow(Array[Any](11)))

    runCommand(
      PaimonDropFormatTablePartitionsExec(
        table,
        Seq(hhTen, hhEleven),
        ifExists = false,
        purge = false,
        () => ()))

    // Every partial spec is resolved from a single unfiltered listing.
    assert(listedPrefixes == Seq(Map.empty))
    assert(dropped == Seq(first, second, third))
    assert(dropped.distinct == dropped)
  }

  test("catalog-managed DROP with an empty batch does not refresh cache") {
    val (table, gateway) = formatTable(withCatalogManagedPartitions = true)
    var refreshCalls = 0

    runCommand(
      PaimonDropFormatTablePartitionsExec(
        table,
        Seq.empty,
        ifExists = false,
        purge = false,
        () => refreshCalls += 1))

    assert(gateway.dropCalls == 0)
    assert(refreshCalls == 0)
  }

  test("catalog-managed partial DROP rejects an incomplete catalog result before mutation") {
    var dropCalls = 0
    val gateway = new FormatTablePartitionManager {
      override def createPartitions(
          partitions: JList[JMap[String, String]],
          ignoreIfExists: Boolean): Unit = {}

      override def dropPartitions(partitions: JList[JMap[String, String]]): Unit = dropCalls += 1

      override def listPartitionsByNames(
          partitions: JList[JMap[String, String]]): JList[Partition] =
        Collections.emptyList()

      override def listPartitions(prefix: JMap[String, String]): JList[Partition] =
        registeredPartitions(Map("dt" -> "20260715"))
    }
    val table = new PaimonFormatTable(rawFormatTable(withCatalogManagedPartitions = true, gateway))
    val hhOnly = ResolvedPartitionSpec(Seq("hh"), new GenericInternalRow(Array[Any](10)))

    val error = intercept[IllegalStateException] {
      runCommand(
        PaimonDropFormatTablePartitionsExec(
          table,
          Seq(hhOnly),
          ifExists = false,
          purge = false,
          () => ()))
    }

    assert(error.getMessage.contains("complete partition spec"))
    assert(dropCalls == 0)
  }

  test("MSCK strategy intercepts only catalog-managed partitions and maps the mode flags") {
    val catalog = spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog]
    val identifier = SparkIdentifier.of(Array("test"), "format_table")
    val (catalogManaged, _) = formatTable(withCatalogManagedPartitions = true)
    val (filesystem, _) = formatTable(withCatalogManagedPartitions = false)
    val catalogManagedResolved = ResolvedTable.create(catalog, identifier, catalogManaged)

    // (plain MSCK) -> ADD; DROP PARTITIONS -> DROP; SYNC PARTITIONS -> both.
    Seq((true, false), (false, true), (true, true)).foreach {
      case (add, drop) =>
        val plans = PaimonStrategy(spark).apply(RepairTable(catalogManagedResolved, add, drop))
        assert(plans.size == 1)
        val repair = plans.head.asInstanceOf[PaimonRepairFormatTablePartitionsExec]
        assert(repair.addPartitions == add)
        assert(repair.dropPartitions == drop)
    }

    // Tables using filesystem partition discovery keep Spark's own v2 rejection: no
    // interception.
    val filesystemPlans = PaimonStrategy(spark)
      .apply(RepairTable(ResolvedTable.create(catalog, identifier, filesystem), true, false))
    assert(filesystemPlans.isEmpty)
  }

  test("MSCK repair reuses the sync engine: DROP unregisters catalog-only partitions") {
    val gateway = new AtomicGateway(Set(Map("dt" -> "20260715", "hh" -> "10")))
    val table = new PaimonFormatTable(rawFormatTable(withCatalogManagedPartitions = true, gateway))
    var refreshCalls = 0

    // The table location has no partition directories, so the registered partition is
    // catalog-only; MSCK DROP PARTITIONS must unregister it (metadata-only).
    runCommand(
      PaimonRepairFormatTablePartitionsExec(
        table,
        addPartitions = false,
        dropPartitions = true,
        () => refreshCalls += 1))

    assert(gateway.state.isEmpty)
    assert(refreshCalls == 1)
  }

  test("strategy leaves Format Table SHOW PARTITIONS to Spark's default executor") {
    val output = Seq(AttributeReference("partition", StringType, nullable = false)())
    val (catalogManaged, _) = formatTable(withCatalogManagedPartitions = true)
    val (filesystem, _) = formatTable(withCatalogManagedPartitions = false)
    val catalog = spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog]
    val identifier = SparkIdentifier.of(Array("test"), "format_table")
    val spec = Some(partition(20260715, 10))

    // SHOW PARTITIONS is served by Spark's default v2 executor via listPartitionIdentifiers
    // (backed by the catalog listing when partitions are catalog-managed), exactly like every
    // other Paimon
    // table, so the strategy must not intercept it.
    assert(
      PaimonStrategy(spark)
        .apply(
          ShowPartitions(ResolvedTable.create(catalog, identifier, catalogManaged), spec, output))
        .isEmpty)
    assert(
      PaimonStrategy(spark)
        .apply(ShowPartitions(ResolvedTable.create(catalog, identifier, filesystem), spec, output))
        .isEmpty)
  }

  private def partition(dt: Int, hh: Int): ResolvedPartitionSpec =
    ResolvedPartitionSpec(Seq("dt", "hh"), new GenericInternalRow(Array[Any](dt, hh)))

  /** Partition metadata as a catalog returns it; only the spec matters for these tests. */
  private def registeredPartitions(specs: Map[String, String]*): JList[Partition] =
    specs.map(spec => new Partition(spec.asJava, 0L, 0L, 0L, 0L, 0, false)).asJava

  private def formatTable(
      withCatalogManagedPartitions: Boolean): (PaimonFormatTable, RecordingGateway) = {
    val gateway = new RecordingGateway
    (new PaimonFormatTable(rawFormatTable(withCatalogManagedPartitions, gateway)), gateway)
  }

  private def rawFormatTable(
      withCatalogManagedPartitions: Boolean,
      gateway: FormatTablePartitionManager): FormatTable =
    rawFormatTable(
      withCatalogManagedPartitions,
      gateway,
      new Path(Files.createTempDirectory("format-table-ddl-planning-test").toUri).toString)

  private def rawFormatTable(
      withCatalogManagedPartitions: Boolean,
      gateway: FormatTablePartitionManager,
      location: String): FormatTable = {
    rawFormatTable(withCatalogManagedPartitions, gateway, location, LocalFileIO.create)
  }

  private def rawFormatTable(
      withCatalogManagedPartitions: Boolean,
      gateway: FormatTablePartitionManager,
      location: String,
      fileIO: FileIO): FormatTable = {
    val options = if (withCatalogManagedPartitions) {
      Map("metastore.partitioned-table" -> "true")
    } else {
      Map.empty[String, String]
    }
    FormatTable
      .builder()
      .fileIO(fileIO)
      .identifier(Identifier.create("test", "format_table"))
      .rowType(
        DataTypes.ROW(
          DataTypes.FIELD(0, "id", DataTypes.INT()),
          DataTypes.FIELD(1, "dt", DataTypes.INT()),
          DataTypes.FIELD(2, "hh", DataTypes.INT())))
      .partitionKeys(Seq("dt", "hh").asJava)
      .location(location)
      .format(FormatTable.Format.CSV)
      .options(options.asJava)
      .catalogContext(CatalogContext.create(new Options))
      // A table uses catalog-managed partitions exactly when it carries a partition manager,
      // so a filesystem-discovered one must not have any.
      .partitionManager(if (withCatalogManagedPartitions) gateway else null)
      .build()
  }

  private def registerPartitions(tableName: String, specs: Map[String, String]*): Unit =
    paimonCatalog.createPartitions(
      Identifier.create(dbName0, tableName),
      specs.map(_.asJava).asJava,
      true)

  private def registeredPartitionSpecs(tableName: String): Set[Map[String, String]] =
    paimonCatalog
      .listPartitions(Identifier.create(dbName0, tableName))
      .asScala
      .map(_.spec().asScala.toMap)
      .toSet

  private def shownPartitions(tableName: String): Set[String] =
    sql(s"SHOW PARTITIONS $tableName").collect().map(_.getString(0)).toSet

  private def runCommand(command: AnyRef): Unit = {
    try {
      command.getClass.getMethod("run").invoke(command)
    } catch {
      case e: InvocationTargetException => throw e.getCause
    }
  }

  private def causeMessages(error: Throwable): String = {
    Iterator
      .iterate(error)(_.getCause)
      .takeWhile(_ != null)
      .flatMap(e => Option(e.getMessage))
      .mkString(" | ")
  }

  private class RecordingGateway extends FormatTablePartitionManager {
    var createCalls = 0
    var lookupCalls = 0
    var created = Seq.empty[JMap[String, String]]
    var ignoreIfExists = false
    var dropCalls = 0
    var dropped = Seq.empty[JMap[String, String]]

    override def createPartitions(
        partitions: JList[JMap[String, String]],
        ignoreIfExists: Boolean): Unit = {
      createCalls += 1
      created = partitions.asScala.toSeq
      this.ignoreIfExists = ignoreIfExists
    }

    override def dropPartitions(partitions: JList[JMap[String, String]]): Unit = {
      dropCalls += 1
      dropped = partitions.asScala.toSeq
    }

    // Reports every requested spec as registered. ADD tests assert lookupCalls == 0 because
    // Catalog-managed ADD must never perform a client-side existence lookup.
    override def listPartitionsByNames(
        partitions: JList[JMap[String, String]]): JList[Partition] = {
      lookupCalls += 1
      registeredPartitions(partitions.asScala.map(_.asScala.toMap).toSeq: _*)
    }

    override def listPartitions(prefix: JMap[String, String]): JList[Partition] =
      Collections.emptyList()
  }

  private class AtomicGateway(initial: Set[Map[String, String]])
    extends FormatTablePartitionManager {

    private var partitions = initial
    var batches = Seq.empty[Seq[Map[String, String]]]

    def state: Set[Map[String, String]] = synchronized(partitions)

    override def createPartitions(
        partitionsToCreate: JList[JMap[String, String]],
        ignoreIfExists: Boolean): Unit = synchronized {
      val batch = partitionsToCreate.asScala.map(_.asScala.toMap).toSeq
      batches :+= batch
      val duplicates = batch.filter(partitions.contains)
      if (duplicates.nonEmpty && !ignoreIfExists) {
        throw new IllegalStateException(s"Partition already exists: ${duplicates.head}")
      }
      partitions ++= batch
    }

    override def dropPartitions(partitionsToDrop: JList[JMap[String, String]]): Unit =
      synchronized {
        partitions --= partitionsToDrop.asScala.map(_.asScala.toMap)
      }

    override def listPartitionsByNames(partitions: JList[JMap[String, String]]): JList[Partition] =
      throw new AssertionError("ADD must not perform a client-side existence lookup")

    override def listPartitions(prefix: JMap[String, String]): JList[Partition] = synchronized {
      registeredPartitions(partitions.toSeq: _*)
    }
  }
}
