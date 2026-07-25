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

import org.apache.paimon.CoreOptions
import org.apache.paimon.format.csv.CsvOptions
import org.apache.paimon.fs.Path
import org.apache.paimon.spark.{BaseTable, FormatTableScanBuilder}
import org.apache.paimon.spark.write.{BaseV2WriteBuilder, PaimonWriteRequirement}
import org.apache.paimon.table.FormatTable
import org.apache.paimon.table.format.FormatTablePartitionManager
import org.apache.paimon.types.RowType
import org.apache.paimon.utils.PartitionPathUtils

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, TableCapability, TableCatalog}
import org.apache.spark.sql.connector.catalog.TableCapability.{BATCH_READ, BATCH_WRITE, OVERWRITE_BY_FILTER, OVERWRITE_DYNAMIC}
import org.apache.spark.sql.connector.distributions.Distribution
import org.apache.spark.sql.connector.expressions.SortOrder
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.paimon.shims.SparkShimLoader
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import java.util.{Collections, Locale, Map => JMap, Objects}

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashSet}

case class PaimonFormatTable(table: FormatTable)
  extends BaseTable
  with SupportsRead
  with SupportsWrite {

  // A Format Table uses catalog-managed partitions exactly when the catalog gave it a partition
  // manager; tables using filesystem partition discovery return null and rely on the directory
  // layout instead.
  private[spark] def partitionManager: FormatTablePartitionManager = table.partitionManager()

  /**
   * A Format Table uses catalog-managed partitions exactly when the catalog gave it a partition
   * manager; otherwise its partitions are discovered from the filesystem directory layout.
   */
  def hasCatalogManagedPartitions: Boolean = partitionManager != null

  override def capabilities(): util.Set[TableCapability] = {
    util.EnumSet.of(BATCH_READ, BATCH_WRITE, OVERWRITE_DYNAMIC, OVERWRITE_BY_FILTER)
  }

  override def properties: util.Map[String, String] = {
    val properties = new util.HashMap[String, String](table.options())
    properties.put(TableCatalog.PROP_PROVIDER, table.format.name().toLowerCase(Locale.ROOT))
    if (table.comment.isPresent) {
      properties.put(TableCatalog.PROP_COMMENT, table.comment.get)
    }
    if (FormatTable.Format.CSV == table.format) {
      properties.put(
        "sep",
        properties.getOrDefault(
          CsvOptions.FIELD_DELIMITER.key(),
          CsvOptions.FIELD_DELIMITER.defaultValue()))
    }
    properties
  }

  override def newScanBuilder(caseInsensitiveStringMap: CaseInsensitiveStringMap): ScanBuilder = {
    val scanBuilder = FormatTableScanBuilder(table.copy(caseInsensitiveStringMap))
    scanBuilder.pruneColumns(schema)
    scanBuilder
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    PaimonFormatTableWriterBuilder(table, info.schema)
  }

  /**
   * Resolves, with a single catalog list-by-names lookup, which of the given complete partition
   * specs are registered. The result is aligned with the input arrays.
   */
  private[spark] def formatTablePartitionsRegistered(
      partitionNames: Array[Array[String]],
      rows: Array[InternalRow]): Array[Boolean] = {
    if (rows.isEmpty) {
      return Array.empty
    }
    val requested =
      rows.zip(partitionNames).map { case (row, names) => toPaimonPartition(row, names.toSeq) }
    val registered = requirePartitionManager().listPartitionsByNames(requested.toSeq.asJava)
    val registeredSpecs = registered.asScala.map(_.spec().asScala.toMap).toSet
    requested.map(spec => registeredSpecs.contains(spec.asScala.toMap))
  }

  private[spark] def createFormatTablePartitions(
      rows: Array[InternalRow],
      maps: Array[JMap[String, String]],
      ignoreIfExists: Boolean): Unit = {
    if (maps.exists(_.keySet().asScala.exists(_.equalsIgnoreCase("location")))) {
      throw new UnsupportedOperationException(
        s"ADD PARTITION with LOCATION is not supported for Format Table ${table.fullName()}.")
    }
    val onlyValueInPath =
      CoreOptions.fromMap(table.options()).formatTablePartitionOnlyValueInPath()
    val partitionKeys = table.partitionKeys().asScala.toSeq
    val specs = rows.map(row => toPaimonPartition(row, partitionKeys.take(row.numFields))).toSeq
    // Resolve (and path-safety validate) every directory before mutating anything.
    val partitionPaths =
      specs.map(spec => resolvePartitionPathWithinTable(orderedSpec(spec), onlyValueInPath))
    requirePartitionManager().createPartitions(specs.asJava, ignoreIfExists)
    // Create the partition directories client-side (symmetric with DROP deleting them), so an
    // added partition exists on the filesystem and a subsequent scan returns an empty partition
    // rather than depending on lazy directory creation, matching Hive ADD PARTITION semantics.
    val fileIO = table.fileIO()
    partitionPaths.foreach(partitionPath => fileIO.mkdirs(partitionPath))
  }

  /**
   * Drops the given partitions: complete specs are unregistered and their directories deleted
   * as-is, partial specs are expanded to the registered leaf partitions they cover. Callers are
   * responsible for resolving which complete specs are actually registered first (see
   * [[formatTablePartitionsRegistered]]), so unregistered data directories are never deleted.
   */
  private[spark] def dropFormatTablePartitions(
      partitionNames: Array[Array[String]],
      rows: Array[InternalRow]): Boolean = {
    val partitionKeyCount = table.partitionKeys().size()
    val requested =
      rows.zip(partitionNames).map { case (row, names) => toPaimonPartition(row, names.toSeq) }
    val partitions = ArrayBuffer.empty[JMap[String, String]]
    val seenPartitions = HashSet.empty[Map[String, String]]

    def addPartition(partition: JMap[String, String]): Unit = {
      if (seenPartitions.add(partition.asScala.toMap)) {
        partitions += partition
      }
    }

    // Preserve exact requests as-is and let discovery add only missing complete leaves.
    requested.filter(_.size() == partitionKeyCount).foreach(addPartition)
    val partialSpecs = requested.filter(_.size() < partitionKeyCount).toSeq.distinct
    if (partialSpecs.nonEmpty) {
      def matchesRequestedPartial(partition: JMap[String, String]): Boolean = {
        partialSpecs.exists(_.asScala.forall {
          case (key, value) => Objects.equals(value, partition.get(key))
        })
      }

      // One unfiltered traversal resolves every partial spec; the requested constraints are
      // enforced client-side.
      requirePartitionManager()
        .listPartitions(Collections.emptyMap[String, String](), null)
        .asScala
        .foreach {
          partition =>
            val validated = validateCatalogRegisteredPartition(partition.spec())
            if (matchesRequestedPartial(validated)) {
              addPartition(validated)
            }
        }
    }
    dropCatalogRegisteredPartitions(partitions.toSeq)
  }

  private def dropCatalogRegisteredPartitions(partitions: Seq[JMap[String, String]]): Boolean = {
    // Unregister first so new queries stop seeing the partition, then delete the data directory
    // with the table FileIO (client-side; the server never deletes data). A deletion failure leaves
    // the possibly incomplete directory invisible; it must not be registered again automatically.
    if (partitions.isEmpty) {
      return true
    }

    val onlyValueInPath =
      CoreOptions.fromMap(table.options()).formatTablePartitionOnlyValueInPath()
    // Resolve (and path-safety validate) every partition directory before any mutation, so a
    // traversal attempt ('.'/'..') fails the whole DROP before unregistering anything.
    val partitionPaths =
      partitions.map(spec => resolvePartitionPathWithinTable(orderedSpec(spec), onlyValueInPath))
    logInfo("Try to drop catalog-registered partitions: " + partitions.mkString(","))
    requirePartitionManager().dropPartitions(partitions.asJava)
    val fileIO = table.fileIO()
    partitionPaths.foreach {
      partitionPath =>
        val deleted = fileIO.delete(partitionPath, true)
        if (!deleted && fileIO.exists(partitionPath)) {
          throw new java.io.IOException(
            s"FileIO reported that partition directory $partitionPath was not deleted.")
        }
    }
    true
  }

  private def validateCatalogRegisteredPartition(
      partition: JMap[String, String]): JMap[String, String] = {
    val partitionKeys = table.partitionKeys().asScala
    if (partitionKeys.exists(key => partition.get(key) == null)) {
      throw new IllegalStateException(
        s"Catalog must return a complete partition spec with keys " +
          s"${partitionKeys.mkString("[", ", ", "]")} for format table " +
          s"${table.fullName()}, but returned $partition.")
    }

    val ordered = new util.LinkedHashMap[String, String]()
    partitionKeys.foreach(key => ordered.put(key, partition.get(key)))
    ordered
  }

  private def orderedSpec(spec: JMap[String, String]): util.LinkedHashMap[String, String] = {
    val ordered = new util.LinkedHashMap[String, String]()
    table.partitionKeys().asScala.foreach {
      key => if (spec.containsKey(key)) ordered.put(key, spec.get(key))
    }
    ordered
  }

  /**
   * Build the partition directory for a spec and verify it stays strictly under the table location.
   * Value-only path components are validated (including rejecting '.'/'..'), and the normalized
   * path is checked against the table location so neither DROP (recursive delete) nor ADD (mkdirs)
   * can escape the table directory via crafted or corrupt partition values.
   */
  private def resolvePartitionPathWithinTable(
      orderedSpec: util.LinkedHashMap[String, String],
      onlyValueInPath: Boolean): Path = {
    PartitionPathUtils.validatePartitionSpecForPath(orderedSpec, onlyValueInPath)
    val tablePath = new Path(table.location())
    val partitionPath = new Path(
      tablePath,
      PartitionPathUtils.generatePartitionPathUtil(orderedSpec, onlyValueInPath)
    )
    val normalizedTable = tablePath.toUri.normalize().getPath
    val tablePrefix = if (normalizedTable.endsWith("/")) normalizedTable else normalizedTable + "/"
    val normalizedPartition = partitionPath.toUri.normalize().getPath
    if (!normalizedPartition.startsWith(tablePrefix)) {
      throw new IllegalArgumentException(
        s"Resolved partition path $partitionPath escapes the table location $tablePath for " +
          s"partition spec $orderedSpec of Format Table ${table.fullName()}.")
    }
    partitionPath
  }

  private def requirePartitionManager(): FormatTablePartitionManager = {
    if (partitionManager == null) {
      throw new UnsupportedOperationException(
        s"Catalog-managed partitions are not configured for format table ${table.fullName()}.")
    }
    partitionManager
  }
}

case class PaimonFormatTableWriterBuilder(table: FormatTable, writeSchema: StructType)
  extends BaseV2WriteBuilder(table) {

  override def partitionRowType(): RowType = table.partitionType

  override def build: Write = new Write with RequiresDistributionAndOrdering {
    private val writeRequirement = PaimonWriteRequirement(table)

    override def requiredDistribution(): Distribution = writeRequirement.distribution

    override def requiredOrdering(): Array[SortOrder] = writeRequirement.ordering

    override def toBatch: BatchWrite = {
      SparkShimLoader.shim
        .createFormatTableBatchWrite(table, overwriteDynamic, overwritePartitions, writeSchema)
    }

    override def toStreaming: StreamingWrite = {
      throw new UnsupportedOperationException("FormatTable doesn't support streaming write")
    }
  }
}
