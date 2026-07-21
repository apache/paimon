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

package org.apache.paimon.spark

import org.apache.paimon.CoreOptions
import org.apache.paimon.fs.Path
import org.apache.paimon.partition.PartitionStatistics
import org.apache.paimon.table.{FileStoreTable, FormatTable, Table}
import org.apache.paimon.table.format.FormatTablePartitionManager
import org.apache.paimon.table.source.ScanMode
import org.apache.paimon.types.RowType
import org.apache.paimon.utils.{InternalRowPartitionComputer, PartitionPathUtils, TypeUtils}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.SupportsAtomicPartitionManagement
import org.apache.spark.sql.types.StructType

import java.util.{Collections, Map => JMap, Objects}

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashSet}

trait PaimonPartitionManagement extends SupportsAtomicPartitionManagement with Logging {

  val table: Table

  def partitionManager: FormatTablePartitionManager = null

  lazy val partitionRowType: RowType = TypeUtils.project(table.rowType, table.partitionKeys)

  override lazy val partitionSchema: StructType = SparkTypeUtils.fromPaimonRowType(partitionRowType)

  private def toPaimonPartitions(rows: Array[InternalRow]): Array[java.util.Map[String, String]] = {
    val partitionKeys = table.partitionKeys().asScala.toSeq

    rows.map(r => toPaimonPartition(r, partitionKeys.take(r.numFields)))
  }

  private def toPaimonPartition(
      row: InternalRow,
      partitionNames: Seq[String]): java.util.Map[String, String] = {
    val coreOptions = CoreOptions.fromMap(table.options())
    val partitionDefaultName = coreOptions.partitionDefaultName()
    val legacyPartitionName = coreOptions.legacyPartitionName
    val currentPartitionRowType = TypeUtils.project(table.rowType, partitionNames.asJava)
    val currentPartitionSchema = SparkTypeUtils.fromPaimonRowType(currentPartitionRowType)
    val rowConverter = CatalystTypeConverters.createToScalaConverter(
      CharVarcharUtils.replaceCharVarcharWithString(currentPartitionSchema))
    val rowDataPartitionComputer = new InternalRowPartitionComputer(
      partitionDefaultName,
      currentPartitionRowType,
      partitionNames.toArray,
      legacyPartitionName
    )

    rowDataPartitionComputer.generatePartValues(
      new SparkRow(currentPartitionRowType, rowConverter(row).asInstanceOf[Row]))
  }

  override def dropPartitions(rows: Array[InternalRow]): Boolean = {
    table match {
      case fileStoreTable: FileStoreTable =>
        val partitions = toPaimonPartitions(rows).toSeq.asJava
        logInfo("Try to drop partitions: " + partitions.asScala.mkString(","))
        val partitionModification = fileStoreTable.catalogEnvironment().partitionModification()
        if (partitionModification != null) {
          try {
            partitionModification.dropPartitions(partitions)
          } finally {
            partitionModification.close()
          }
        } else {
          val commit = fileStoreTable.newBatchWriteBuilder().newCommit()
          try {
            commit.truncatePartitions(partitions)
          } finally {
            commit.close()
          }
        }
        true

      case _ =>
        throw new UnsupportedOperationException("Only FileStoreTable supports drop partitions.")
    }
  }

  /**
   * Resolves, with a single catalog list-by-names lookup, which of the given complete partition
   * specs are registered for a Format Table with catalog-managed partitions. The result is aligned
   * with the input arrays.
   */
  private[spark] def formatTablePartitionsRegistered(
      partitionNames: Array[Array[String]],
      rows: Array[InternalRow]): Array[Boolean] = {
    if (rows.isEmpty) {
      return Array.empty
    }

    table match {
      case formatTable: FormatTable if partitionManager != null =>
        val requested =
          rows.zip(partitionNames).map { case (row, names) => toPaimonPartition(row, names.toSeq) }
        val registered = requirePartitionManager().listPartitionsByNames(requested.toSeq.asJava)
        val registeredSpecs = registered.asScala.map(_.spec().asScala.toMap).toSet
        requested.map(spec => registeredSpecs.contains(spec.asScala.toMap))
      case _ =>
        throw new UnsupportedOperationException(
          "Partition registration lookup is supported only for a Format Table with " +
            "catalog-managed partitions.")
    }
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
    table match {
      case formatTable: FormatTable if partitionManager != null =>
        val partitionKeyCount = formatTable.partitionKeys().size()
        val requested =
          rows.zip(partitionNames).map { case (row, names) => toPaimonPartition(row, names.toSeq) }
        val partitions = ArrayBuffer.empty[java.util.Map[String, String]]
        val seenPartitions = HashSet.empty[Map[String, String]]

        def addPartition(partition: java.util.Map[String, String]): Unit = {
          if (seenPartitions.add(partition.asScala.toMap)) {
            partitions += partition
          }
        }

        // Preserve exact requests as-is and let discovery add only missing complete leaves.
        requested.filter(_.size() == partitionKeyCount).foreach(addPartition)
        val partialSpecs = requested.filter(_.size() < partitionKeyCount).toSeq.distinct
        if (partialSpecs.nonEmpty) {
          def matchesRequestedPartial(partition: java.util.Map[String, String]): Boolean = {
            partialSpecs.exists(_.asScala.forall {
              case (key, value) => Objects.equals(value, partition.get(key))
            })
          }

          // One unfiltered traversal resolves every partial spec; the requested constraints are
          // enforced client-side.
          requirePartitionManager()
            .listPartitions(Collections.emptyMap[String, String]())
            .asScala
            .foreach {
              partition =>
                val validated = validateCatalogRegisteredPartition(formatTable, partition.spec())
                if (matchesRequestedPartial(validated)) {
                  addPartition(validated)
                }
            }
        }
        dropCatalogRegisteredPartitions(formatTable, partitions.toSeq)
      case _ =>
        throw new UnsupportedOperationException(
          "Named partition drop is supported only for a Format Table with catalog-managed " +
            "partitions.")
    }
  }

  private def dropCatalogRegisteredPartitions(
      formatTable: FormatTable,
      partitions: Seq[java.util.Map[String, String]]): Boolean = {
    // Unregister first so new queries stop seeing the partition, then delete the data directory
    // with the table FileIO (client-side; the server never deletes data). A deletion failure leaves
    // the possibly incomplete directory invisible; it must not be registered again automatically.
    if (partitions.isEmpty) {
      return true
    }

    val onlyValueInPath =
      CoreOptions.fromMap(formatTable.options()).formatTablePartitionOnlyValueInPath()
    // Resolve (and path-safety validate) every partition directory before any mutation, so a
    // traversal attempt ('.'/'..') fails the whole DROP before unregistering anything.
    val partitionPaths = partitions.map {
      spec =>
        resolvePartitionPathWithinTable(
          formatTable,
          orderedSpec(formatTable, spec),
          onlyValueInPath)
    }
    logInfo("Try to drop catalog-registered partitions: " + partitions.mkString(","))
    requirePartitionManager().dropPartitions(partitions.asJava)
    val fileIO = formatTable.fileIO()
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
      formatTable: FormatTable,
      partition: java.util.Map[String, String]): java.util.Map[String, String] = {
    val partitionKeys = formatTable.partitionKeys().asScala
    if (partitionKeys.exists(key => partition.get(key) == null)) {
      throw new IllegalStateException(
        s"Catalog must return a complete partition spec with keys " +
          s"${partitionKeys.mkString("[", ", ", "]")} for format table " +
          s"${formatTable.fullName()}, but returned $partition.")
    }

    val ordered = new java.util.LinkedHashMap[String, String]()
    partitionKeys.foreach(key => ordered.put(key, partition.get(key)))
    ordered
  }

  override def truncatePartitions(idents: Array[InternalRow]): Boolean = {
    val partitions = toPaimonPartitions(idents).toSeq.asJava
    val commit = table.newBatchWriteBuilder().newCommit()
    try {
      commit.truncatePartitions(partitions)
    } finally {
      commit.close()
    }
    true
  }

  override def truncatePartition(ident: InternalRow): Boolean = {
    truncatePartitions(Array(ident))
  }

  override def replacePartitionMetadata(
      ident: InternalRow,
      properties: JMap[String, String]): Unit = {
    throw new UnsupportedOperationException("Replace partition is not supported")
  }

  override def loadPartitionMetadata(ident: InternalRow): JMap[String, String] = {
    table match {
      case fileStoreTable: FileStoreTable =>
        val partitionSpec = toPaimonPartitions(Array(ident)).head
        val partitionEntries = fileStoreTable
          .newSnapshotReader()
          .withMode(ScanMode.ALL)
          .withPartitionFilter(partitionSpec)
          .partitionEntries()

        if (!partitionEntries.isEmpty) {
          val entry = partitionEntries.get(0)
          Map(
            PartitionStatistics.FIELD_RECORD_COUNT -> entry.recordCount().toString,
            PartitionStatistics.FIELD_FILE_SIZE_IN_BYTES -> entry.fileSizeInBytes().toString,
            PartitionStatistics.FIELD_FILE_COUNT -> entry.fileCount().toString,
            PartitionStatistics.FIELD_LAST_FILE_CREATION_TIME -> entry
              .lastFileCreationTime()
              .toString
          ).asJava
        } else {
          Map.empty[String, String].asJava
        }
      case _ =>
        Map.empty[String, String].asJava
    }
  }

  override def listPartitionIdentifiers(
      partitionCols: Array[String],
      internalRow: InternalRow): Array[InternalRow] = {
    assert(
      partitionCols.length == internalRow.numFields,
      s"Number of partition names (${partitionCols.length}) must be equal to " +
        s"the number of partition values (${internalRow.numFields})."
    )
    assert(
      partitionCols.forall(fieldName => partitionSchema.fieldNames.contains(fieldName)),
      s"Some partition names ${partitionCols.mkString("[", ", ", "]")} don't belong to " +
        s"the partition schema '${partitionSchema.sql}'."
    )
    table.newReadBuilder.newScan.listPartitions.asScala
      .map(binaryRow => DataConverter.fromPaimon(binaryRow, partitionRowType))
      .filter(
        sparkInternalRow => {
          partitionCols.zipWithIndex
            .map {
              case (partitionName, index) =>
                val internalRowIndex = partitionSchema.fieldIndex(partitionName)
                val structField = partitionSchema.fields(internalRowIndex)
                Objects.equals(
                  sparkInternalRow.get(internalRowIndex, structField.dataType),
                  internalRow.get(index, structField.dataType))
            }
            .forall(identity)
        })
      .toArray
  }

  override def createPartitions(
      rows: Array[InternalRow],
      maps: Array[JMap[String, String]]): Unit = {
    table match {
      case fileStoreTable: FileStoreTable =>
        val partitions = toPaimonPartitions(rows)
        val partitionModification = fileStoreTable.catalogEnvironment().partitionModification()
        if (partitionModification != null) {
          try {
            if (fileStoreTable.coreOptions().partitionedTableInMetastore()) {
              partitionModification.createPartitions(partitions.toSeq.asJava)
            }
          } finally {
            partitionModification.close()
          }
        }
      case _ =>
        throw new UnsupportedOperationException("Only FileStoreTable supports create partitions.")
    }
  }

  private[spark] def createFormatTablePartitions(
      rows: Array[InternalRow],
      maps: Array[JMap[String, String]],
      ignoreIfExists: Boolean): Unit = {
    if (maps.exists(_.keySet().asScala.exists(_.equalsIgnoreCase("location")))) {
      throw new UnsupportedOperationException(
        s"ADD PARTITION with LOCATION is not supported for Format Table ${table.fullName()}.")
    }
    val formatTable = table.asInstanceOf[FormatTable]
    val onlyValueInPath =
      CoreOptions.fromMap(formatTable.options()).formatTablePartitionOnlyValueInPath()
    val specs = toPaimonPartitions(rows).toSeq
    // Resolve (and path-safety validate) every directory before mutating anything.
    val partitionPaths = specs.map {
      spec =>
        resolvePartitionPathWithinTable(
          formatTable,
          orderedSpec(formatTable, spec),
          onlyValueInPath)
    }
    requirePartitionManager().createPartitions(specs.asJava, ignoreIfExists)
    // Create the partition directories client-side (symmetric with DROP deleting them), so an
    // added partition exists on the filesystem and a subsequent scan returns an empty partition
    // rather than depending on lazy directory creation, matching Hive ADD PARTITION semantics.
    val fileIO = formatTable.fileIO()
    partitionPaths.foreach(partitionPath => fileIO.mkdirs(partitionPath))
  }

  private def orderedSpec(
      formatTable: FormatTable,
      spec: java.util.Map[String, String]): java.util.LinkedHashMap[String, String] = {
    val ordered = new java.util.LinkedHashMap[String, String]()
    formatTable.partitionKeys().asScala.foreach {
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
      formatTable: FormatTable,
      orderedSpec: java.util.LinkedHashMap[String, String],
      onlyValueInPath: Boolean): Path = {
    PartitionPathUtils.validatePartitionSpecForPath(orderedSpec, onlyValueInPath)
    val tablePath = new Path(formatTable.location())
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
          s"partition spec $orderedSpec of Format Table ${formatTable.fullName()}.")
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
