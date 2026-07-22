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

package org.apache.paimon.spark.format

import org.apache.paimon.catalog.{CatalogContext, Identifier}
import org.apache.paimon.fs.{FileIO, Path}
import org.apache.paimon.fs.local.LocalFileIO
import org.apache.paimon.options.Options
import org.apache.paimon.partition.Partition
import org.apache.paimon.predicate.Predicate
import org.apache.paimon.table.FormatTable
import org.apache.paimon.table.format.FormatTablePartitionManager
import org.apache.paimon.types.DataTypes

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.unsafe.types.UTF8String

import java.lang.reflect.{InvocationHandler, InvocationTargetException, Method, Proxy}
import java.nio.file.Files
import java.util.{ArrayList, Collections, List => JList, Map => JMap}
import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.collection.JavaConverters._
import scala.collection.mutable

class FormatTablePartitionManagementTest extends SparkFunSuite {

  test(
    "catalog-managed ADD forwards the complete batch and ignoreIfExists to the partition catalog") {
    var forwardedPartitions = Seq.empty[JMap[String, String]]
    var forwardedIgnoreIfExists = false

    val gateway = new FormatTablePartitionManager {
      override def createPartitions(
          partitions: JList[JMap[String, String]],
          ignoreIfExists: Boolean): Unit = {
        forwardedPartitions = partitions.asScala.toSeq
        forwardedIgnoreIfExists = ignoreIfExists
      }

      override def dropPartitions(partitions: JList[JMap[String, String]]): Unit = {}

      override def listPartitionsByNames(
          partitions: JList[JMap[String, String]]): JList[Partition] =
        Collections.emptyList()

      override def listPartitions(
          prefix: JMap[String, String],
          filter: Predicate): JList[Partition] =
        Collections.emptyList()
    }
    val sparkTable =
      new PaimonFormatTable(formatTableWithCatalogManagedPartitions(partitionManager = gateway))

    val rows = Array[InternalRow](
      new GenericInternalRow(Array[Any](20260715, 10)),
      new GenericInternalRow(Array[Any](20260716, 11)))
    val properties = Array.fill[JMap[String, String]](2)(Collections.emptyMap())
    sparkTable.createFormatTablePartitions(rows, properties, ignoreIfExists = true)

    assert(forwardedIgnoreIfExists)
    assert(
      forwardedPartitions.map(_.asScala.toMap) == Seq(
        Map("dt" -> "20260715", "hh" -> "10"),
        Map("dt" -> "20260716", "hh" -> "11")))
  }

  test("catalog-managed ADD creates the partition directory") {
    val fileIO = LocalFileIO.create()
    val tablePath = new Path(Files.createTempDirectory("catalog-partition-format-add-mkdirs").toUri)
    val table = formatTableWithCatalogManagedPartitions(fileIO, tablePath.toString, emptyGateway)
    val partitionDir = new Path(tablePath, "dt=20260715/hh=10")
    try {
      val sparkTable = new PaimonFormatTable(table)
      sparkTable.createFormatTablePartitions(
        Array(partitionRow(20260715, 10)),
        Array[JMap[String, String]](Collections.emptyMap()),
        ignoreIfExists = true)

      // ADD PARTITION creates the directory client-side, so a scan sees an empty partition
      // rather than failing on a missing directory (Hive ADD PARTITION semantics).
      assert(fileIO.exists(partitionDir))
    } finally {
      fileIO.delete(tablePath, true)
    }
  }

  test("catalog-managed DROP rejects a partition value that would escape the table location") {
    var dropCalls = 0
    val gateway = new FormatTablePartitionManager {
      override def createPartitions(
          partitions: JList[JMap[String, String]],
          ignoreIfExists: Boolean): Unit = {}

      override def dropPartitions(partitions: JList[JMap[String, String]]): Unit = dropCalls += 1

      override def listPartitionsByNames(
          partitions: JList[JMap[String, String]]): JList[Partition] =
        Collections.emptyList()

      override def listPartitions(
          prefix: JMap[String, String],
          filter: Predicate): JList[Partition] =
        Collections.emptyList()
    }

    val sparkTable =
      new PaimonFormatTable(
        stringFormatTableWithCatalogManagedPartitions(gateway, onlyValueInPath = true))
    val error = intercept[IllegalArgumentException] {
      sparkTable.dropFormatTablePartitions(
        Array(Array("dt")),
        Array(new GenericInternalRow(Array[Any](UTF8String.fromString("..")))))
    }

    // Traversal values are rejected before any catalog mutation, so no partition is unregistered.
    assert(error.getMessage.contains(".."))
    assert(dropCalls == 0)
  }

  test("catalog-managed ADD with LOCATION is rejected before any catalog RPC") {
    var createCalls = 0
    val gateway = new FormatTablePartitionManager {
      override def createPartitions(
          partitions: JList[JMap[String, String]],
          ignoreIfExists: Boolean): Unit = createCalls += 1

      override def dropPartitions(partitions: JList[JMap[String, String]]): Unit = {}

      override def listPartitionsByNames(
          partitions: JList[JMap[String, String]]): JList[Partition] =
        Collections.emptyList()

      override def listPartitions(
          prefix: JMap[String, String],
          filter: Predicate): JList[Partition] =
        Collections.emptyList()
    }

    val sparkTable =
      new PaimonFormatTable(formatTableWithCatalogManagedPartitions(partitionManager = gateway))
    val error = intercept[UnsupportedOperationException] {
      sparkTable.createFormatTablePartitions(
        Array(new GenericInternalRow(Array[Any](20260715, 10))),
        Array(Map("location" -> "file:/tmp/custom").asJava),
        ignoreIfExists = true)
    }

    assert(error.getMessage.contains("LOCATION"))
    assert(createCalls == 0)
  }

  test("catalog-managed ADD rejects a partition value that would escape the table location") {
    var createCalls = 0
    val gateway = new FormatTablePartitionManager {
      override def createPartitions(
          partitions: JList[JMap[String, String]],
          ignoreIfExists: Boolean): Unit = createCalls += 1

      override def dropPartitions(partitions: JList[JMap[String, String]]): Unit = {}

      override def listPartitionsByNames(
          partitions: JList[JMap[String, String]]): JList[Partition] =
        Collections.emptyList()

      override def listPartitions(
          prefix: JMap[String, String],
          filter: Predicate): JList[Partition] =
        Collections.emptyList()
    }

    val sparkTable =
      new PaimonFormatTable(
        stringFormatTableWithCatalogManagedPartitions(gateway, onlyValueInPath = true))
    val error = intercept[IllegalArgumentException] {
      sparkTable.createFormatTablePartitions(
        Array(new GenericInternalRow(Array[Any](UTF8String.fromString("..")))),
        Array[JMap[String, String]](Collections.emptyMap()),
        ignoreIfExists = true)
    }

    // Symmetric with DROP: a traversal value is rejected before any catalog mutation or directory
    // creation, so the whole ADD fails without registering or creating anything.
    assert(error.getMessage.contains(".."))
    assert(createCalls == 0)
  }

  test("catalog-managed ADD keeps the partition registered when directory creation fails") {
    val delegate = LocalFileIO.create()
    val tablePath =
      new Path(Files.createTempDirectory("catalog-partition-format-add-mkdirs-failure").toUri)
    val failure = new java.io.IOException("injected mkdirs failure")
    val fileIO = throwingMkdirsFileIO(delegate, failure)
    val gateway = new InMemoryPartitionManager
    try {
      val sparkTable =
        new PaimonFormatTable(
          formatTableWithCatalogManagedPartitions(fileIO, tablePath.toString, gateway))
      val error = intercept[java.io.IOException] {
        sparkTable.createFormatTablePartitions(
          Array(partitionRow(20260715, 10)),
          Array[JMap[String, String]](Collections.emptyMap()),
          ignoreIfExists = false)
      }

      // Registration happens before the client-side mkdirs, so a mkdirs failure surfaces while the
      // partition stays registered; re-running ADD ... IF NOT EXISTS then creates the directory.
      assert(error eq failure)
      assert(gateway.partitions == Seq(Map("dt" -> "20260715", "hh" -> "10")))
    } finally {
      delegate.delete(tablePath, true)
    }
  }

  test(
    "catalog-managed DROP PARTITION unregisters first and then deletes the partition directory") {
    var dropped = Seq.empty[JMap[String, String]]
    var directoryExistedAtUnregister = false
    val fileIO = LocalFileIO.create()
    val tablePath =
      new Path(Files.createTempDirectory("catalog-partition-format-drop-ordering").toUri)
    val partitionDir = new Path(tablePath, "dt=20260715/hh=10")

    val gateway = new FormatTablePartitionManager {
      override def createPartitions(
          partitions: JList[JMap[String, String]],
          ignoreIfExists: Boolean): Unit = {}

      override def dropPartitions(partitions: JList[JMap[String, String]]): Unit = {
        // Ordering contract: the directory must still exist when the catalog unregisters.
        directoryExistedAtUnregister = fileIO.exists(partitionDir)
        dropped = partitions.asScala.toSeq
      }

      override def listPartitionsByNames(
          partitions: JList[JMap[String, String]]): JList[Partition] =
        Collections.emptyList()

      override def listPartitions(
          prefix: JMap[String, String],
          filter: Predicate): JList[Partition] =
        Collections.emptyList()
    }

    try {
      fileIO.mkdirs(partitionDir)
      assert(fileIO.exists(partitionDir))

      val sparkTable =
        new PaimonFormatTable(
          formatTableWithCatalogManagedPartitions(fileIO, tablePath.toString, gateway))
      assert(
        sparkTable.dropFormatTablePartitions(
          Array(Array("dt", "hh")),
          Array(new GenericInternalRow(Array[Any](20260715, 10)))))

      assert(dropped.map(_.asScala.toMap) == Seq(Map("dt" -> "20260715", "hh" -> "10")))
      assert(directoryExistedAtUnregister)
      assert(!fileIO.exists(partitionDir))
    } finally {
      fileIO.delete(tablePath, true)
    }
  }

  test("catalog-managed direct partial DROP expands leading values to exact leaf partitions") {
    val fileIO = LocalFileIO.create()
    val tablePath =
      new Path(Files.createTempDirectory("catalog-partition-format-direct-partial-drop").toUri)
    val dtDir = new Path(tablePath, "dt=20260715")
    val firstDir = new Path(dtDir, "hh=10")
    val secondDir = new Path(dtDir, "hh=11")
    val first = partitionSpec(20260715, 10)
    val second = partitionSpec(20260715, 11)
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

      override def listPartitions(
          prefix: JMap[String, String],
          filter: Predicate): JList[Partition] = {
        listedPrefixes :+= prefix.asScala.toMap
        registeredPartitions(first, second)
      }
    }

    try {
      fileIO.mkdirs(firstDir)
      fileIO.mkdirs(secondDir)
      val sparkTable =
        new PaimonFormatTable(
          formatTableWithCatalogManagedPartitions(fileIO, tablePath.toString, gateway))

      sparkTable.dropFormatTablePartitions(
        Array(Array("dt")),
        Array(new GenericInternalRow(Array[Any](20260715))))

      // The expansion always runs one unfiltered traversal; constraints apply client-side.
      assert(listedPrefixes == Seq(Map.empty))
      assert(dropped == Seq(first, second))
      assert(!fileIO.exists(firstDir))
      assert(!fileIO.exists(secondDir))
      assert(fileIO.exists(dtDir))
    } finally {
      fileIO.delete(tablePath, true)
    }
  }

  test("catalog-managed DROP fails when FileIO reports an undeleted partition directory") {
    val delegate = LocalFileIO.create()
    val tablePath = new Path(Files.createTempDirectory("catalog-partition-format-drop").toUri)
    val fileIO = falseDeleteFileIO(delegate)
    val partitionDir = new Path(tablePath, "dt=20260715/hh=10")
    delegate.mkdirs(partitionDir)
    val gateway = new RecordingDropCatalog
    val table = formatTableWithCatalogManagedPartitions(fileIO, tablePath.toString, gateway)

    try {
      val error = intercept[java.io.IOException] {
        new PaimonFormatTable(table)
          .dropFormatTablePartitions(
            Array(Array("dt", "hh")),
            Array(new GenericInternalRow(Array[Any](20260715, 10))))
      }

      assert(error.getMessage.contains("was not deleted"))
      assert(error.getMessage.contains("dt=20260715"))
      assert(error.getMessage.contains("hh=10"))
      assert(gateway.dropCalls == 1)
      assert(delegate.exists(partitionDir))
    } finally {
      delegate.delete(tablePath, true)
    }
  }

  test("catalog-managed DROP treats an already missing partition directory as deleted") {
    val delegate = LocalFileIO.create()
    val tablePath =
      new Path(Files.createTempDirectory("catalog-partition-format-drop-missing").toUri)
    val fileIO = falseDeleteFileIO(delegate)
    val gateway = new RecordingDropCatalog
    val table = formatTableWithCatalogManagedPartitions(fileIO, tablePath.toString, gateway)

    try {
      assert(
        new PaimonFormatTable(table)
          .dropFormatTablePartitions(
            Array(Array("dt", "hh")),
            Array(new GenericInternalRow(Array[Any](20260715, 10)))))
      assert(gateway.dropCalls == 1)
    } finally {
      delegate.delete(tablePath, true)
    }
  }

  test("catalog-managed DROP deduplicates overlapping full and partial specs before deleting") {
    val delegate = LocalFileIO.create()
    val tablePath =
      new Path(Files.createTempDirectory("catalog-partition-format-drop-overlap").toUri)
    val partitionDir = new Path(tablePath, "dt=20260715/hh=10")
    var deleteCalls = 0
    val fileIO = Proxy
      .newProxyInstance(
        classOf[FileIO].getClassLoader,
        Array(classOf[FileIO]),
        new InvocationHandler {
          override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef = {
            if (method.getName == "delete" && args(0) == partitionDir) {
              deleteCalls += 1
            }
            try {
              method.invoke(delegate, Option(args).getOrElse(Array.empty[AnyRef]): _*)
            } catch {
              case error: InvocationTargetException => throw error.getCause
            }
          }
        }
      )
      .asInstanceOf[FileIO]
    val matching = partitionSpec(20260715, 10)
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

      override def listPartitions(
          prefix: JMap[String, String],
          filter: Predicate): JList[Partition] =
        registeredPartitions(matching)
    }

    try {
      delegate.mkdirs(partitionDir)
      val sparkTable =
        new PaimonFormatTable(
          formatTableWithCatalogManagedPartitions(fileIO, tablePath.toString, gateway))
      sparkTable.dropFormatTablePartitions(
        Array(Array("dt", "hh"), Array("hh")),
        Array(partitionRow(20260715, 10), new GenericInternalRow(Array[Any](10))))

      assert(dropped == Seq(matching))
      assert(deleteCalls == 1)
      assert(!delegate.exists(partitionDir))
      assert(delegate.exists(new Path(tablePath, "dt=20260715")))
    } finally {
      delegate.delete(tablePath, true)
    }
  }

  test("catalog-managed partial DROP leaves catalog and data untouched when the listing fails") {
    val fileIO = LocalFileIO.create()
    val tablePath =
      new Path(Files.createTempDirectory("catalog-partition-format-drop-list-failure").toUri)
    val partitionDir = new Path(tablePath, "dt=20260715/hh=10")
    var dropCalls = 0
    val gateway = new FormatTablePartitionManager {
      override def createPartitions(
          partitions: JList[JMap[String, String]],
          ignoreIfExists: Boolean): Unit = {}

      override def dropPartitions(partitions: JList[JMap[String, String]]): Unit = dropCalls += 1

      override def listPartitionsByNames(
          partitions: JList[JMap[String, String]]): JList[Partition] =
        Collections.emptyList()

      override def listPartitions(
          prefix: JMap[String, String],
          filter: Predicate): JList[Partition] =
        throw new TestPartitionListException
    }

    try {
      fileIO.mkdirs(partitionDir)
      val sparkTable =
        new PaimonFormatTable(
          formatTableWithCatalogManagedPartitions(fileIO, tablePath.toString, gateway))

      intercept[TestPartitionListException] {
        sparkTable.dropFormatTablePartitions(
          Array(Array("hh")),
          Array(new GenericInternalRow(Array[Any](10))))
      }

      assert(dropCalls == 0)
      assert(fileIO.exists(partitionDir))
    } finally {
      fileIO.delete(tablePath, true)
    }
  }

  test("catalog-managed partial DROP keeps data untouched after an ambiguous catalog response") {
    val fileIO = LocalFileIO.create()
    val tablePath =
      new Path(Files.createTempDirectory("catalog-partition-format-drop-response-loss").toUri)
    val firstDir = new Path(tablePath, "dt=20260715/hh=10")
    val secondDir = new Path(tablePath, "dt=20260715/hh=11")
    val first = partitionSpec(20260715, 10)
    val second = partitionSpec(20260715, 11)
    val registered = mutable.LinkedHashSet(first, second)
    val responseFailure = new TestCatalogDropResponseException
    var dropCalls = 0
    var failDropResponse = true
    val gateway = new FormatTablePartitionManager {
      override def createPartitions(
          partitions: JList[JMap[String, String]],
          ignoreIfExists: Boolean): Unit =
        throw new AssertionError("An ambiguous DROP must not recreate catalog partitions")

      override def dropPartitions(partitions: JList[JMap[String, String]]): Unit = {
        dropCalls += 1
        registered --= partitions.asScala.map(_.asScala.toMap)
        if (failDropResponse) {
          failDropResponse = false
          throw responseFailure
        }
      }

      override def listPartitionsByNames(
          partitions: JList[JMap[String, String]]): JList[Partition] =
        Collections.emptyList()

      override def listPartitions(
          prefix: JMap[String, String],
          filter: Predicate): JList[Partition] =
        registeredPartitions(registered.toSeq: _*)
    }

    try {
      fileIO.mkdirs(firstDir)
      fileIO.mkdirs(secondDir)
      val sparkTable =
        new PaimonFormatTable(
          formatTableWithCatalogManagedPartitions(fileIO, tablePath.toString, gateway))

      val error = intercept[TestCatalogDropResponseException] {
        sparkTable.dropFormatTablePartitions(
          Array(Array("dt")),
          Array(new GenericInternalRow(Array[Any](20260715))))
      }

      assert(error eq responseFailure)
      assert(dropCalls == 1)
      assert(registered.isEmpty)
      assert(fileIO.exists(firstDir))
      assert(fileIO.exists(secondDir))
    } finally {
      fileIO.delete(tablePath, true)
    }
  }

  test("catalog-managed ADD forwards each batch whole, without deduplicating across calls") {
    val first = partitionSpec(20260715, 10)
    val second = partitionSpec(20260716, 11)
    val third = partitionSpec(20260717, 12)
    val gateway = new InMemoryPartitionManager(Seq(first, second))
    val sparkTable =
      new PaimonFormatTable(formatTableWithCatalogManagedPartitions(partitionManager = gateway))

    sparkTable.createFormatTablePartitions(
      Array(partitionRow(20260715, 10), partitionRow(20260716, 11)),
      Array.fill[JMap[String, String]](2)(Collections.emptyMap()),
      ignoreIfExists = true)
    sparkTable.createFormatTablePartitions(
      Array(partitionRow(20260716, 11), partitionRow(20260717, 12)),
      Array.fill[JMap[String, String]](2)(Collections.emptyMap()),
      ignoreIfExists = true)

    // Both batches reach the catalog exactly as asked, including the partition the previous
    // call already created: resolving duplicates is the catalog's job, not Spark's.
    assert(gateway.createRequests == Seq((Seq(first, second), true), (Seq(second, third), true)))
    assert(gateway.lookupCalls == 0)
  }

  test("catalog-managed strict ADD asks the catalog to reject duplicates, without looking first") {
    val existing = partitionSpec(20260715, 10)
    val gateway = new InMemoryPartitionManager(Seq(existing))
    val sparkTable =
      new PaimonFormatTable(formatTableWithCatalogManagedPartitions(partitionManager = gateway))

    val error = intercept[TestPartitionAlreadyExistsException] {
      sparkTable.createFormatTablePartitions(
        Array(partitionRow(20260715, 10)),
        Array[JMap[String, String]](Collections.emptyMap()),
        ignoreIfExists = false)
    }

    assert(error.partition == existing)
    // Rejecting the batch is the catalog's decision; Spark must not pre-empt it with a lookup,
    // which is what would break the batch's atomicity.
    assert(gateway.lookupCalls == 0)
  }

  test("catalog-managed ADD preserves typed special partition values") {
    val gateway = new InMemoryPartitionManager
    val sparkTable = new PaimonFormatTable(stringFormatTableWithCatalogManagedPartitions(gateway))
    val values = Seq("a/b", "a=b", "a%", "中文 value")
    val rows: Seq[InternalRow] = values
      .map(
        value => new GenericInternalRow(Array[Any](UTF8String.fromString(value))): InternalRow) :+
      new GenericInternalRow(Array[Any](null))

    sparkTable.createFormatTablePartitions(
      rows.toArray,
      Array.fill[JMap[String, String]](rows.size)(Collections.emptyMap()),
      ignoreIfExists = true)

    assert(
      gateway.createRequests == Seq(
        (values.map(value => Map("dt" -> value)) :+ Map("dt" -> "__DEFAULT_PARTITION__"), true)))
  }

  private def emptyGateway: FormatTablePartitionManager =
    new FormatTablePartitionManager {
      override def createPartitions(
          partitions: JList[JMap[String, String]],
          ignoreIfExists: Boolean): Unit = {}

      override def dropPartitions(partitions: JList[JMap[String, String]]): Unit = {}

      override def listPartitionsByNames(
          partitions: JList[JMap[String, String]]): JList[Partition] =
        Collections.emptyList()

      override def listPartitions(
          prefix: JMap[String, String],
          filter: Predicate): JList[Partition] =
        Collections.emptyList()
    }

  private class InMemoryPartitionManager(initialPartitions: Seq[Map[String, String]] = Seq.empty)
    extends FormatTablePartitionManager {

    private val storedPartitions = mutable.LinkedHashSet(initialPartitions: _*)
    private var requests = Seq.empty[(Seq[Map[String, String]], Boolean)]
    var lookupCalls = 0

    def createRequests: Seq[(Seq[Map[String, String]], Boolean)] = synchronized(requests)

    def partitions: Seq[Map[String, String]] = synchronized(storedPartitions.toSeq)

    override def createPartitions(
        partitions: JList[JMap[String, String]],
        ignoreIfExists: Boolean): Unit = synchronized {
      val requested = partitions.asScala.map(_.asScala.toMap).toSeq
      requests :+= ((requested, ignoreIfExists))
      if (!ignoreIfExists) {
        requested.find(storedPartitions.contains).foreach {
          duplicate => throw new TestPartitionAlreadyExistsException(duplicate)
        }
      }
      storedPartitions ++= requested
    }

    override def dropPartitions(partitions: JList[JMap[String, String]]): Unit = synchronized {
      storedPartitions --= partitions.asScala.map(_.asScala.toMap)
    }

    override def listPartitionsByNames(partitions: JList[JMap[String, String]]): JList[Partition] =
      synchronized {
        lookupCalls += 1
        registeredPartitions(
          partitions.asScala.map(_.asScala.toMap).filter(storedPartitions.contains).toSeq: _*)
      }

    override def listPartitions(prefix: JMap[String, String], filter: Predicate): JList[Partition] =
      throw new AssertionError("ADD must not enumerate catalog or filesystem partitions")
  }

  private class TestPartitionAlreadyExistsException(val partition: Map[String, String])
    extends RuntimeException

  private class TestPartitionListException extends RuntimeException

  private class TestCatalogDropResponseException extends RuntimeException

  private class RecordingDropCatalog extends FormatTablePartitionManager {
    var dropCalls = 0

    override def createPartitions(
        partitions: JList[JMap[String, String]],
        ignoreIfExists: Boolean): Unit = {}

    override def dropPartitions(partitions: JList[JMap[String, String]]): Unit = dropCalls += 1

    override def listPartitionsByNames(partitions: JList[JMap[String, String]]): JList[Partition] =
      Collections.emptyList()

    override def listPartitions(prefix: JMap[String, String], filter: Predicate): JList[Partition] =
      Collections.emptyList()
  }

  private def partitionRow(dt: Int, hh: Int): InternalRow =
    new GenericInternalRow(Array[Any](dt, hh))

  private def partitionSpec(dt: Int, hh: Int): Map[String, String] =
    Map("dt" -> dt.toString, "hh" -> hh.toString)

  /** Partition metadata as a catalog returns it; only the spec matters for these tests. */
  private def registeredPartitions(specs: Map[String, String]*): JList[Partition] =
    specs.map(spec => new Partition(spec.asJava, 0L, 0L, 0L, 0L, 0, false)).asJava

  private def uniqueTableLocation(prefix: String): String =
    new Path(Files.createTempDirectory(prefix).toUri).toString

  private def formatTableWithCatalogManagedPartitions(
      fileIO: FileIO = LocalFileIO.create,
      location: String = uniqueTableLocation("format_table"),
      partitionManager: FormatTablePartitionManager = null): FormatTable = {
    FormatTable
      .builder()
      .fileIO(fileIO)
      .identifier(Identifier.create("test_db", "format_table"))
      .rowType(
        DataTypes.ROW(
          DataTypes.FIELD(0, "id", DataTypes.INT()),
          DataTypes.FIELD(1, "dt", DataTypes.INT()),
          DataTypes.FIELD(2, "hh", DataTypes.INT())))
      .partitionKeys(Seq("dt", "hh").asJava)
      .location(location)
      .format(FormatTable.Format.CSV)
      .options(Map("metastore.partitioned-table" -> "true").asJava)
      .catalogContext(CatalogContext.create(new Options))
      .partitionManager(partitionManager)
      .build()
  }

  private def throwingMkdirsFileIO(delegate: FileIO, failure: Throwable): FileIO = {
    Proxy
      .newProxyInstance(
        classOf[FileIO].getClassLoader,
        Array(classOf[FileIO]),
        new InvocationHandler {
          override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef = {
            if (method.getName == "mkdirs") {
              throw failure
            } else {
              try {
                method.invoke(delegate, Option(args).getOrElse(Array.empty[AnyRef]): _*)
              } catch {
                case error: InvocationTargetException => throw error.getCause
              }
            }
          }
        }
      )
      .asInstanceOf[FileIO]
  }

  private def falseDeleteFileIO(delegate: FileIO): FileIO = {
    Proxy
      .newProxyInstance(
        classOf[FileIO].getClassLoader,
        Array(classOf[FileIO]),
        new InvocationHandler {
          override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef = {
            if (method.getName == "delete" && method.getParameterCount == 2) {
              java.lang.Boolean.FALSE
            } else {
              try {
                method.invoke(delegate, Option(args).getOrElse(Array.empty[AnyRef]): _*)
              } catch {
                case error: InvocationTargetException => throw error.getCause
              }
            }
          }
        }
      )
      .asInstanceOf[FileIO]
  }

  private def stringFormatTableWithCatalogManagedPartitions(
      partitionManager: FormatTablePartitionManager,
      onlyValueInPath: Boolean = false): FormatTable = {
    FormatTable
      .builder()
      .fileIO(LocalFileIO.create)
      .identifier(Identifier.create("test_db", "format_table_special"))
      .rowType(DataTypes.ROW(
        DataTypes.FIELD(0, "id", DataTypes.INT()),
        DataTypes.FIELD(1, "dt", DataTypes.STRING())))
      .partitionKeys(Seq("dt").asJava)
      .location(uniqueTableLocation("format_table_special"))
      .format(FormatTable.Format.CSV)
      .options(Map(
        "metastore.partitioned-table" -> "true",
        "format-table.partition-path-only-value" -> onlyValueInPath.toString).asJava)
      .catalogContext(CatalogContext.create(new Options))
      .partitionManager(partitionManager)
      .build()
  }
}
