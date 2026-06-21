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

package org.apache.spark.sql.execution

import org.apache.paimon.CoreOptions
import org.apache.paimon.spark.catalyst.Compatibility
import org.apache.paimon.spark.util.OptionUtils

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, BoundReference, Expression, InterpretedPredicate, Literal, Predicate => ExprPredicate}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.paimon.shims.SparkShimLoader
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._
import scala.collection.mutable

/** Factory for creating partition-aware file indexes for format tables. */
object SparkFormatTableFileIndex {

  def createFileIndex(
      options: CaseInsensitiveStringMap,
      sparkSession: SparkSession,
      paths: Seq[String],
      userSpecifiedSchema: Option[StructType],
      partitionSchema: StructType): PartitioningAwareFileIndex = {

    def globPaths: Boolean = {
      val entry = options.get(DataSource.GLOB_PATHS_KEY)
      Option(entry).forall(_ == "true")
    }

    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    if (
      SparkShimLoader.shim.hasFileStreamSinkMetadata(
        paths,
        hadoopConf,
        sparkSession.sessionState.conf)
    ) {
      SparkShimLoader.shim.createPartitionedMetadataLogFileIndex(
        sparkSession,
        new Path(paths.head),
        options.asScala.toMap,
        userSpecifiedSchema,
        partitionSchema = partitionSchema)
    } else {
      val rootPathsSpecified = DataSource.checkAndGlobPathIfNecessary(
        paths,
        hadoopConf,
        checkEmptyGlobPath = true,
        checkFilesExist = true,
        enableGlobbing = globPaths)
      val fileStatusCache = FileStatusCache.getOrCreate(sparkSession)

      val lazyPruning =
        partitionSchema.nonEmpty && OptionUtils.readFormatTableLazyPartitionPruning()

      if (lazyPruning) {
        new LazyPartitionPruningFileIndex(
          sparkSession,
          rootPathsSpecified,
          caseSensitiveMap,
          userSpecifiedSchema,
          fileStatusCache,
          partitionSchema)
      } else {
        new EagerPartitionListingFileIndex(
          sparkSession,
          rootPathsSpecified,
          caseSensitiveMap,
          userSpecifiedSchema,
          fileStatusCache,
          partitionSchema)
      }
    }
  }

  // Visible to shim-local PartitionedMetadataLogFileIndex subclasses.
  private[sql] def alignPartitionSpec(
      inferred: PartitionSpec,
      partitionSchema: StructType): PartitionSpec = {
    if (inferred.partitionColumns.isEmpty && partitionSchema.nonEmpty) {
      PartitionSpec(partitionSchema, inferred.partitions)
    } else {
      inferred
    }
  }
}

/** Eagerly lists all files at construction time, like the default [[InMemoryFileIndex]]. */
class EagerPartitionListingFileIndex(
    sparkSession: SparkSession,
    rootPathsSpecified: Seq[Path],
    parameters: Map[String, String],
    userSpecifiedSchema: Option[StructType],
    fileStatusCache: FileStatusCache,
    override val partitionSchema: StructType)
  extends InMemoryFileIndex(
    sparkSession,
    rootPathsSpecified,
    parameters,
    userSpecifiedSchema,
    fileStatusCache) {

  override def partitionSpec(): PartitionSpec =
    SparkFormatTableFileIndex.alignPartitionSpec(super.partitionSpec(), partitionSchema)
}

/**
 * A [[PartitioningAwareFileIndex]] that prunes partition directories level-by-level using partition
 * filters, similar to [[CatalogFileIndex]] but discovers partitions from the filesystem instead of
 * the metastore.
 */
class LazyPartitionPruningFileIndex(
    sparkSession: SparkSession,
    tableLocations: Seq[Path],
    parameters: Map[String, String],
    userSpecifiedSchema: Option[StructType],
    fileStatusCache: FileStatusCache,
    _partitionSchema: StructType)
  extends PartitioningAwareFileIndex(sparkSession, parameters, userSpecifiedSchema, fileStatusCache)
  with Logging {

  override val rootPaths: Seq[Path] = tableLocations

  override def partitionSchema: StructType = _partitionSchema

  override def equals(other: Any): Boolean = other match {
    case o: LazyPartitionPruningFileIndex => rootPaths.toSet == o.rootPaths.toSet
    case _ => false
  }

  override def hashCode(): Int = rootPaths.toSet.hashCode()

  @volatile private var _fullIndex: InMemoryFileIndex = _

  @volatile private var _estimatedSizeInBytes: Long = -1L

  // Leaf partitions sampled to estimate the average partition size.
  private val SAMPLE_PARTITIONS = 3

  private def fullIndex: InMemoryFileIndex = {
    if (_fullIndex == null) {
      synchronized {
        if (_fullIndex == null) {
          _fullIndex = new InMemoryFileIndex(
            sparkSession,
            tableLocations,
            parameters,
            userSpecifiedSchema,
            fileStatusCache)
        }
      }
    }
    _fullIndex
  }

  override def refresh(): Unit = {
    fileStatusCache.invalidateAll()
    synchronized {
      _fullIndex = null
      _estimatedSizeInBytes = -1L
    }
  }

  // Required by PartitioningAwareFileIndex but never accessed — all callers are overridden.
  override protected def leafFiles: mutable.LinkedHashMap[Path, FileStatus] =
    throw new IllegalStateException()
  override protected def leafDirToChildrenFiles: Map[Path, Array[FileStatus]] =
    throw new IllegalStateException()

  override def partitionSpec(): PartitionSpec =
    PartitionSpec(_partitionSchema, Seq.empty)

  // Used only by the optimizer for broadcast decisions; a full listing would defeat lazy pruning,
  // so estimate cheaply (idempotent, cached) until the full index exists, then report its exact size.
  override def sizeInBytes: Long = {
    val idx = _fullIndex
    if (idx != null) {
      idx.sizeInBytes
    } else {
      if (_estimatedSizeInBytes < 0) {
        _estimatedSizeInBytes = estimateSizeBySampling()
      }
      _estimatedSizeInBytes
    }
  }

  /**
   * Cheap whole-table size estimate without a full listing: partition count from per-level fan-outs
   * along one path, times the average size of up to [[SAMPLE_PARTITIONS]] sampled leaf partitions.
   * Falls back to `defaultSizeInBytes` (not broadcast) on empty table / empty sample / overflow, so
   * we never underestimate and wrongly broadcast a large table. Reads via `fs.listStatus`, so it
   * never touches `HiveCatalogMetrics`.
   */
  private def estimateSizeBySampling(): Long = {
    val unknownSize = sparkSession.sessionState.conf.defaultSizeInBytes
    val levels = _partitionSchema.fields.length

    var path = tableLocations.head
    var partitionCount = 1L
    var leafSiblings: Array[FileStatus] = Array.empty
    var level = 0
    while (level < levels) {
      val fs = path.getFileSystem(hadoopConf)
      val subdirs =
        try {
          fs.listStatus(path)
            .filter(s => s.isDirectory && isDataPath(s.getPath.getName))
            .filter(_.getPath.getName.contains("="))
        } catch {
          case _: java.io.FileNotFoundException => Array.empty[FileStatus]
        }
      if (subdirs.isEmpty) {
        return unknownSize
      }
      partitionCount *= subdirs.length
      leafSiblings = subdirs
      path = subdirs.head.getPath
      level += 1
    }

    // leafSiblings are the leaf partition dirs under the last visited parent.
    val samples = leafSiblings.take(SAMPLE_PARTITIONS)
    if (samples.isEmpty) {
      return unknownSize
    }
    val sampledSizes = samples.map {
      leaf =>
        val fs = leaf.getPath.getFileSystem(hadoopConf)
        try {
          fs.listStatus(leaf.getPath).filter(_.isFile).map(_.getLen).sum
        } catch {
          case _: java.io.FileNotFoundException => 0L
        }
    }
    val avgPartitionSize = sampledSizes.sum / sampledSizes.length
    if (avgPartitionSize <= 0) {
      return unknownSize
    }

    try {
      Math.multiplyExact(avgPartitionSize, partitionCount)
    } catch {
      case _: ArithmeticException => unknownSize
    }
  }

  override def inputFiles: Array[String] = fullIndex.inputFiles

  private def isDataPath(name: String): Boolean =
    !((name.startsWith("_") && !name.contains("=")) || name.startsWith("."))

  private lazy val defaultPartNames: Set[String] = Set(
    ExternalCatalogUtils.DEFAULT_PARTITION_NAME,
    parameters.getOrElse(
      CoreOptions.PARTITION_DEFAULT_NAME.key(),
      CoreOptions.PARTITION_DEFAULT_NAME.defaultValue())
  )

  override def listFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    if (_partitionSchema.isEmpty || partitionFilters.isEmpty || _fullIndex != null) {
      return fullIndex.listFiles(partitionFilters, dataFilters)
    }
    if (recursiveFileLookup) {
      throw new IllegalArgumentException(
        "Datasource with partition do not allow recursive file loading.")
    }

    val fields = _partitionSchema.fields.toSeq
    val perColumnFilters = splitFiltersByColumn(partitionFilters, fields)
    val levelPredicates =
      fields.map(f => perColumnFilters.get(f.name).map(ExprPredicate.createInterpreted))
    val tz = Some(sparkSession.sessionState.conf.sessionLocalTimeZone)
    val resolver = sparkSession.sessionState.conf.resolver

    val discovered = tableLocations.flatMap {
      root => discoverPartitions(root, 0, fields, levelPredicates, tz, resolver, Seq.empty)
    }

    logInfo(s"Discovered ${discovered.size} partitions by level-by-level pruning.")

    if (discovered.isEmpty) {
      return Seq.empty
    }

    val partitionPaths = discovered.map {
      case (values, path) => PartitionPath(InternalRow.fromSeq(values), path)
    }
    val prunedIndex = new InMemoryFileIndex(
      sparkSession,
      rootPathsSpecified = partitionPaths.map(_.path),
      parameters = parameters,
      userSpecifiedSchema = Some(_partitionSchema),
      fileStatusCache = fileStatusCache,
      userSpecifiedPartitionSpec = Some(PartitionSpec(_partitionSchema, partitionPaths))
    )

    prunedIndex.listFiles(partitionFilters, dataFilters)
  }

  private def splitFiltersByColumn(
      filters: Seq[Expression],
      fields: Seq[StructField]): Map[String, Expression] = {
    val resolver = sparkSession.sessionState.conf.resolver
    val partitionNames = fields.map(_.name).toSet
    val grouped = mutable.Map.empty[String, Seq[Expression]]

    for (filter <- filters) {
      val refs = filter.references.map(_.name).toSet
      if (refs.size == 1) {
        val refName = refs.head
        val matchedCol = partitionNames.find(n => resolver(n, refName))
        matchedCol.foreach { col => grouped(col) = grouped.getOrElse(col, Seq.empty) :+ filter }
      }
    }

    grouped.map {
      case (col, exprs) =>
        val field = fields.find(f => resolver(f.name, col)).get
        val combined = exprs.reduce(And)
        val bound = combined.transform {
          case a: AttributeReference if resolver(a.name, col) =>
            BoundReference(0, field.dataType, nullable = true)
        }
        col -> bound
    }.toMap
  }

  private def discoverPartitions(
      currentPath: Path,
      level: Int,
      fields: Seq[StructField],
      levelPredicates: Seq[Option[InterpretedPredicate]],
      tz: Option[String],
      resolver: (String, String) => Boolean,
      accumulatedValues: Seq[Any]): Seq[(Seq[Any], Path)] = {

    if (level == fields.length) {
      return Seq((accumulatedValues, currentPath))
    }

    val field = fields(level)
    val fs = currentPath.getFileSystem(hadoopConf)

    val subdirs =
      try {
        fs.listStatus(currentPath).filter(s => s.isDirectory && isDataPath(s.getPath.getName))
      } catch {
        case _: java.io.FileNotFoundException => return Seq.empty
      }

    val predicate = levelPredicates(level)

    subdirs.toSeq.flatMap {
      subdir =>
        val dirName = subdir.getPath.getName
        val eqIdx = dirName.indexOf('=')
        if (eqIdx < 0) {
          Seq.empty
        } else {
          val colName = ExternalCatalogUtils.unescapePathName(dirName.substring(0, eqIdx))
          if (!resolver(colName, field.name)) {
            Seq.empty
          } else {
            val rawValue = ExternalCatalogUtils.unescapePathName(dirName.substring(eqIdx + 1))
            val typedValue =
              if (defaultPartNames.contains(rawValue)) {
                null
              } else {
                Compatibility
                  .cast(Literal.create(rawValue, StringType), field.dataType, tz)
                  .eval(null)
              }

            if (predicate.forall(_.eval(InternalRow(typedValue)))) {
              discoverPartitions(
                subdir.getPath,
                level + 1,
                fields,
                levelPredicates,
                tz,
                resolver,
                accumulatedValues :+ typedValue)
            } else {
              Seq.empty
            }
          }
        }
    }
  }
}
