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
import org.apache.paimon.partition.Partition
import org.apache.paimon.spark.{BaseTable, FormatTableScanBuilder}
import org.apache.paimon.spark.write.{BaseV2WriteBuilder, PaimonWriteRequirement}
import org.apache.paimon.table.FormatTable
import org.apache.paimon.table.format.{FormatTablePartitionManager, FormatTablePartitionPathResolver, FormatTablePartitionRegistryValidator}
import org.apache.paimon.table.sink.BatchTableCommit
import org.apache.paimon.types.RowType
import org.apache.paimon.utils.{PartitionPathUtils, StringUtils}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{NoSuchPartitionException, NoSuchPartitionsException}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, TableCapability, TableCatalog, TruncatableTable}
import org.apache.spark.sql.connector.catalog.TableCapability.{BATCH_READ, BATCH_WRITE, OVERWRITE_BY_FILTER, OVERWRITE_DYNAMIC}
import org.apache.spark.sql.connector.distributions.Distribution
import org.apache.spark.sql.connector.expressions.SortOrder
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.paimon.shims.SparkShimLoader
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import java.util.{Collections, Locale, Map => JMap, Objects}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class PaimonFormatTable(table: FormatTable)
  extends BaseTable
  with SupportsRead
  with SupportsWrite
  with TruncatableTable {

  // A Format Table uses catalog-managed partitions exactly when the catalog gave it a partition
  // manager; tables using filesystem partition discovery return null and rely on the directory
  // layout instead.
  private[spark] def partitionManager: FormatTablePartitionManager = table.partitionManager()

  private val partitionPathOption = CoreOptions.PATH.key()

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
   * Removes the data of the whole table - of its registered partitions, when the catalog manages
   * them. The partition directories stay, and so do their catalog registrations: emptying a table
   * does not redefine which partitions it has (SPARK-34418).
   */
  override def truncateTable(): Boolean = {
    withCommit(_.truncateTable())
    true
  }

  override def truncatePartitions(idents: Array[InternalRow]): Boolean = {
    truncateFormatTablePartitions(
      idents,
      missing => new NoSuchPartitionsException(name(), missing.toSeq, partitionSchema))
  }

  override def truncatePartition(ident: InternalRow): Boolean = {
    truncateFormatTablePartitions(
      Array(ident),
      missing => new NoSuchPartitionException(name(), missing.head, partitionSchema))
  }

  /**
   * Removes the data of the given partitions, keeping the partitions themselves (see
   * [[truncateTable]]). Spark resolves a partial spec to the partitions it covers before calling
   * either entry point, so every spec arriving here is complete.
   *
   * A partition the table does not have cannot be truncated, and each entry point reports that the
   * way Spark expects it to. What the table has is answered by the catalog for catalog-managed
   * partitions and by the directory for filesystem partition discovery - each kind is asked the
   * same source it reads its partitions from, so data merely awaiting registration is never emptied
   * behind MSCK REPAIR TABLE's back.
   *
   * The partitions are truncated one after another. A Format Table has no snapshot to make that
   * atomic, so a failure part-way leaves the partitions handled before it empty - as a failing
   * `INSERT OVERWRITE` of several partitions does.
   */
  private def truncateFormatTablePartitions(
      idents: Array[InternalRow],
      noSuchPartitions: Array[InternalRow] => Throwable): Boolean = {
    if (idents.isEmpty) {
      return true
    }
    val partitionKeys = table.partitionKeys().asScala.toSeq
    idents.foreach(
      ident => requireNameablePartitionValues("TRUNCATE PARTITION", ident, partitionKeys))
    val specs = idents.map {
      ident =>
        require(
          ident.numFields == partitionKeys.size,
          s"Truncating a partition of Format Table ${table.fullName()} needs a complete spec " +
            s"for partition keys ${partitionKeys.mkString("[", ", ", "]")}, " +
            s"but got ${ident.numFields} values."
        )
        toPaimonPartition(ident, partitionKeys)
    }
    val onlyValueInPath =
      CoreOptions.fromMap(table.options()).formatTablePartitionOnlyValueInPath()
    // Resolve (and path-safety validate) every directory before deleting anything, as ADD and DROP
    // PARTITION do.
    val partitionPaths =
      specs.map(spec => resolvePartitionPathWithinTable(orderedSpec(spec), onlyValueInPath))
    val exists = if (hasCatalogManagedPartitions) {
      val partitionNames = partitionKeys.toArray
      formatTablePartitionsRegistered(idents.map(_ => partitionNames), idents)
    } else {
      val fileIO = table.fileIO()
      partitionPaths.map(fileIO.exists)
    }
    val missing = idents.zip(exists).collect { case (ident, false) => ident }
    if (missing.nonEmpty) {
      throw noSuchPartitions(missing)
    }
    withCommit(_.truncatePartitions(specs.toSeq.asJava))
    true
  }

  private def withCommit(operation: BatchTableCommit => Unit): Unit = {
    val commit = table.newBatchWriteBuilder().newCommit()
    try {
      operation(commit)
    } finally {
      commit.close()
    }
  }

  /**
   * The catalog spec of a resolved partition identifier, with each value written the way Paimon
   * writes it into a partition directory - a null becomes the default partition name.
   */
  private[spark] def toCatalogPartition(
      ident: InternalRow,
      partitionNames: Seq[String]): JMap[String, String] =
    toPaimonPartition(ident, partitionNames)

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
    val pathsBySpec = mutable.LinkedHashMap.empty[Map[String, String], String]
    registered.asScala.foreach {
      partition =>
        val spec = validateCatalogRegisteredPartition(partition.spec()).asScala.toMap
        val path = customPartitionPath(partition)
        pathsBySpec.get(spec).foreach {
          previousPath =>
            if (!Objects.equals(previousPath, path)) {
              throw new IllegalStateException(
                s"Catalog returned conflicting locations for partition $spec of Format Table " +
                  s"${table.fullName()}.")
            }
        }
        pathsBySpec.put(spec, path)
    }
    requested.map(spec => pathsBySpec.contains(spec.asScala.toMap))
  }

  /**
   * Rejects partition values the table cannot name. An empty or whitespace-only string collapses to
   * the default partition name on its way to the directory, the same name a `NULL` gets, so the
   * spec describes the null partition rather than one of its own. Adding it registers a partition
   * the value cannot round-trip to; dropping or truncating it hits the null partition instead.
   *
   * `NULL` itself keeps its defined encoding and stays on that path.
   */
  private def requireNameablePartitionValues(
      operation: String,
      row: InternalRow,
      partitionNames: Seq[String]): Unit = {
    val fields = partitionSchema.fields.map(field => field.name -> field).toMap
    partitionNames.take(row.numFields).zipWithIndex.foreach {
      case (name, index) =>
        val dataType = CharVarcharUtils.replaceCharVarcharWithString(fields(name).dataType)
        if (dataType == StringType && !row.isNullAt(index)) {
          if (StringUtils.isNullOrWhitespaceOnly(row.getString(index))) {
            val defaultPartitionName =
              CoreOptions.fromMap(table.options()).partitionDefaultName()
            throw new IllegalArgumentException(
              s"$operation does not support an empty or whitespace-only string for partition " +
                s"column $name of Format Table ${table.fullName()}. Such a value is written to " +
                s"the partition named $defaultPartitionName, name it directly to address it.")
          }
        }
    }
  }

  private[spark] def createFormatTablePartitions(
      rows: Array[InternalRow],
      maps: Array[JMap[String, String]],
      ignoreIfExists: Boolean): Unit = {
    require(
      rows.length == maps.length,
      s"Expected one option map per partition, but found ${rows.length} partitions and " +
        s"${maps.length} option maps.")
    val onlyValueInPath =
      CoreOptions.fromMap(table.options()).formatTablePartitionOnlyValueInPath()
    val partitionKeys = table.partitionKeys().asScala.toSeq
    rows.foreach(row => requireNameablePartitionValues("ADD PARTITION", row, partitionKeys))
    val partitions = rows
      .zip(maps)
      .map {
        case (row, properties) =>
          val spec = toPaimonPartition(row, partitionKeys.take(row.numFields))
          require(properties != null, s"Partition options must not be null for $spec.")
          require(
            properties
              .entrySet()
              .asScala
              .forall(entry => entry.getKey != null && entry.getValue != null),
            s"Partition options must not contain null keys or values for $spec."
          )
          val options = new util.LinkedHashMap[String, String](properties)
          val locationKeys =
            options.keySet().asScala.filter(TableCatalog.PROP_LOCATION.equalsIgnoreCase).toSeq
          require(
            locationKeys.size <= 1,
            s"Partition options contain multiple location keys for $spec: " +
              locationKeys.mkString("[", ", ", "]"))
          require(
            locationKeys.isEmpty || !options.containsKey(partitionPathOption),
            s"Partition options must not contain both ${TableCatalog.PROP_LOCATION} and " +
              s"$partitionPathOption for $spec."
          )
          locationKeys.headOption.foreach {
            key => options.put(partitionPathOption, options.remove(key))
          }
          if (options.containsKey(partitionPathOption)) {
            options.put(
              partitionPathOption,
              normalizeCustomPartitionLocation(
                options.get(partitionPathOption),
                spec,
                onlyValueInPath))
          }
          spec -> options
      }
      .toSeq
    val specs = partitions.map(_._1)
    // Resolve (and path-safety validate) every default directory before mutating anything.
    val partitionPaths =
      partitions.collect {
        case (spec, options) if customPartitionPath(options, spec) == null =>
          resolvePartitionPathWithinTable(orderedSpec(spec), onlyValueInPath)
      }
    val partitionOptions: Seq[JMap[String, String]] = partitions.map(_._2)
    if (partitionOptions.forall(_.isEmpty)) {
      requirePartitionManager().createPartitions(specs.asJava, ignoreIfExists)
    } else {
      requirePartitionManager()
        .createPartitions(specs.asJava, ignoreIfExists, null, false, partitionOptions.asJava)
    }
    // Create the partition directories client-side (symmetric with DROP deleting them), so an
    // added partition exists on the filesystem and a subsequent scan returns an empty partition
    // rather than depending on lazy directory creation, matching Hive ADD PARTITION semantics.
    val fileIO = table.fileIO()
    partitionPaths.foreach(partitionPath => fileIO.mkdirs(partitionPath))
  }

  private def normalizeCustomPartitionLocation(
      location: String,
      spec: JMap[String, String],
      onlyValueInPath: Boolean): String = {
    try {
      FormatTablePartitionPathResolver
        .resolveCustomLocation(
          new Path(table.location()),
          orderedSpec(spec),
          onlyValueInPath,
          location,
          table.catalogContext())
        .toString
    } catch {
      case error: IllegalArgumentException =>
        throw new IllegalArgumentException(
          s"ADD PARTITION ... LOCATION is invalid for partition $spec of Format Table " +
            s"${table.fullName()}.",
          error)
    }
  }

  /**
   * Drops the registered partitions covered by the given specs. Complete specs that are not
   * registered are ignored; partial specs are expanded to the registered leaf partitions they
   * cover.
   */
  private[spark] def dropFormatTablePartitions(
      partitionNames: Array[Array[String]],
      rows: Array[InternalRow]): Boolean = {
    val (_, partitions) = resolveFormatTablePartitionsForDrop(partitionNames, rows)
    dropCatalogRegisteredPartitions(partitions)
  }

  /**
   * Resolves DROP requests from one validated view of the catalog registry. The boolean array is
   * aligned with the requests and tells callers which complete specs are registered; partial-spec
   * entries are not used for existence reporting. The returned partitions are the deduplicated
   * registered leaves covered by all requests.
   */
  private[spark] def resolveFormatTablePartitionsForDrop(
      partitionNames: Array[Array[String]],
      rows: Array[InternalRow]): (Array[Boolean], Seq[Partition]) = {
    if (rows.isEmpty) {
      return (Array.empty[Boolean], Seq.empty)
    }
    val partitionKeyCount = table.partitionKeys().size()
    rows.zip(partitionNames).foreach {
      case (row, names) => requireNameablePartitionValues("DROP PARTITION", row, names.toSeq)
    }
    val requested =
      rows.zip(partitionNames).map { case (row, names) => toPaimonPartition(row, names.toSeq) }
    val onlyValueInPath =
      CoreOptions.fromMap(table.options()).formatTablePartitionOnlyValueInPath()
    // Reject invalid specs even when the catalog has no matching partition.
    requested.foreach(spec => resolvePartitionPathWithinTable(orderedSpec(spec), onlyValueInPath))
    val manager = requirePartitionManager()
    val registry = manager.listPartitions(Collections.emptyMap[String, String](), null)
    FormatTablePartitionRegistryValidator.validatePartitionLocations(
      registry,
      table.partitionKeys(),
      new Path(table.location()),
      table.fullName(),
      onlyValueInPath,
      table.catalogContext())

    val bySpec = mutable.LinkedHashMap.empty[Map[String, String], Partition]
    registry.asScala.foreach {
      partition =>
        val spec = validateCatalogRegisteredPartition(partition.spec()).asScala.toMap
        bySpec.get(spec) match {
          case Some(previous)
              if !Objects.equals(customPartitionPath(previous), customPartitionPath(partition)) =>
            throw new IllegalStateException(
              s"Catalog returned conflicting locations for partition $spec of Format Table " +
                s"${table.fullName()}.")
          case Some(_) =>
          case None => bySpec.put(spec, partition)
        }
    }

    val registered =
      requested.map(spec => spec.size() == partitionKeyCount && bySpec.contains(spec.asScala.toMap))
    val partitions = ArrayBuffer.empty[Partition]
    val requestedMaps = requested.map(_.asScala.toMap)
    // Requests with the same column set share one hash index. This keeps the common batch of
    // complete specs O(requests + registry), while preserving direct partial DROP support for
    // arbitrary column subsets without testing every request against every registered partition.
    val requestedByColumns = requestedMaps
      .groupBy(_.keySet)
      .map { case (columns, specs) => columns -> specs.toSet }
    bySpec.foreach {
      case (registeredSpec, partition) if requestedByColumns.exists {
            case (columns, specs) =>
              specs.contains(registeredSpec.filter { case (key, _) => columns.contains(key) })
          } =>
        partitions += partition
      case _ =>
    }
    (registered, partitions.toSeq)
  }

  private[spark] def dropCatalogRegisteredPartitions(partitions: Seq[Partition]): Boolean = {
    if (partitions.isEmpty) {
      return true
    }

    val onlyValueInPath =
      CoreOptions.fromMap(table.options()).formatTablePartitionOnlyValueInPath()
    val fileIO = table.fileIO()
    val resolved = partitions.map {
      partition =>
        val spec = validateCatalogRegisteredPartition(partition.spec())
        val defaultPath = if (customPartitionPath(partition) == null) {
          Some(resolvePartitionPathWithinTable(orderedSpec(spec), onlyValueInPath))
        } else {
          None
        }
        (spec, defaultPath)
    }

    val specs = resolved.map(_._1)
    logInfo("Try to drop catalog-registered partitions: " + specs.mkString(","))
    requirePartitionManager().dropPartitions(specs.asJava)
    // Default-location partitions keep the existing unregister-then-delete ordering. A deletion
    // failure leaves the incomplete directory invisible. Custom locations are never probed or
    // deleted.
    resolved
      .flatMap(_._2)
      .foreach {
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

  private def customPartitionPath(partition: Partition): String =
    customPartitionPath(partition.options(), partition.spec())

  private def customPartitionPath(
      options: JMap[String, String],
      spec: JMap[String, String]): String = {
    if (options == null || !options.containsKey(partitionPathOption)) {
      null
    } else {
      val path = options.get(partitionPathOption)
      if (path == null) {
        throw new IllegalStateException(
          s"Catalog returned a null $partitionPathOption option for partition $spec of " +
            s"Format Table ${table.fullName()}.")
      }
      path
    }
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
   * path is checked against the table location so no DROP (recursive delete), ADD (mkdirs) or
   * TRUNCATE (delete of the files below it) can escape the table directory via crafted or corrupt
   * partition values.
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

  override def build: Write = {
    // Which partitions an overwrite replaces is the table option's call, the same as for a data
    // table. Carrying the mode Spark resolved into that option is what keeps a `STATIC` overwrite
    // from being served as if it were `DYNAMIC`.
    val writeTable = overwriteDynamic match {
      case Some(dynamic) =>
        table.copy(Map(CoreOptions.DYNAMIC_PARTITION_OVERWRITE.key -> dynamic.toString).asJava)
      case None => table
    }
    new Write with RequiresDistributionAndOrdering {
      private val writeRequirement = PaimonWriteRequirement(writeTable)

      override def requiredDistribution(): Distribution = writeRequirement.distribution

      override def requiredOrdering(): Array[SortOrder] = writeRequirement.ordering

      override def toBatch: BatchWrite = {
        SparkShimLoader.shim
          .createFormatTableBatchWrite(writeTable, overwritePartitions, writeSchema)
      }

      override def toStreaming: StreamingWrite = {
        throw new UnsupportedOperationException("FormatTable doesn't support streaming write")
      }
    }
  }
}
