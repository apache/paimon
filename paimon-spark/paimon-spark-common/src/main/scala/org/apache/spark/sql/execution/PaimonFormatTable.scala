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

import org.apache.paimon.spark.{PaimonFormatTableScanBuilder, SparkTypeUtils}
import org.apache.paimon.table.FormatTable

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Literal}
import org.apache.spark.sql.connector.catalog.{SupportsPartitionManagement, SupportsRead, SupportsWrite, TableCapability}
import org.apache.spark.sql.connector.catalog.TableCapability.BATCH_READ
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.v2.csv.{CSVScanBuilder, CSVTable}
import org.apache.spark.sql.execution.datasources.v2.json.JsonTable
import org.apache.spark.sql.execution.datasources.v2.orc.OrcTable
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetTable
import org.apache.spark.sql.execution.streaming.{FileStreamSink, MetadataLogFileIndex}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

import scala.collection.JavaConverters._

object PaimonFormatTable {

  // Copy from spark and override FileIndex's partitionSchema
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
    // Hadoop Configurations are case-sensitive.
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    if (FileStreamSink.hasMetadata(paths, hadoopConf, sparkSession.sessionState.conf)) {
      // We are reading from the results of a streaming query. We will load files from
      // the metadata log instead of listing them using HDFS APIs.
      new PartitionedMetadataLogFileIndex(
        sparkSession,
        new Path(paths.head),
        options.asScala.toMap,
        userSpecifiedSchema,
        partitionSchema = partitionSchema)
    } else {
      // This is a non-streaming file based datasource.
      val rootPathsSpecified = DataSource.checkAndGlobPathIfNecessary(
        paths,
        hadoopConf,
        checkEmptyGlobPath = true,
        checkFilesExist = true,
        enableGlobbing = globPaths)
      val fileStatusCache = FileStatusCache.getOrCreate(sparkSession)

      new PartitionedInMemoryFileIndex(
        sparkSession,
        rootPathsSpecified,
        caseSensitiveMap,
        userSpecifiedSchema,
        fileStatusCache,
        partitionSchema = partitionSchema)
    }
  }

  // Extend from MetadataLogFileIndex to override partitionSchema
  private class PartitionedMetadataLogFileIndex(
      sparkSession: SparkSession,
      path: Path,
      parameters: Map[String, String],
      userSpecifiedSchema: Option[StructType],
      override val partitionSchema: StructType)
    extends MetadataLogFileIndex(sparkSession, path, parameters, userSpecifiedSchema)

  // Extend from InMemoryFileIndex to override partitionSchema
  private class PartitionedInMemoryFileIndex(
      sparkSession: SparkSession,
      rootPathsSpecified: Seq[Path],
      parameters: Map[String, String],
      userSpecifiedSchema: Option[StructType],
      fileStatusCache: FileStatusCache = NoopCache,
      userSpecifiedPartitionSpec: Option[PartitionSpec] = None,
      metadataOpsTimeNs: Option[Long] = None,
      override val partitionSchema: StructType)
    extends InMemoryFileIndex(
      sparkSession,
      rootPathsSpecified,
      parameters,
      userSpecifiedSchema,
      fileStatusCache,
      userSpecifiedPartitionSpec,
      metadataOpsTimeNs)
}

// Paimon Format Table
trait PartitionedFormatTable extends SupportsPartitionManagement {

  val partitionSchema_ : StructType

  val fileIndex: PartitioningAwareFileIndex

  override def partitionSchema(): StructType = partitionSchema_

  override def listPartitionIdentifiers(
      names: Array[String],
      ident: InternalRow): Array[InternalRow] = {
    val partitionFilters = names.zipWithIndex.map {
      case (name, index) =>
        val f = partitionSchema().apply(name)
        EqualTo(
          AttributeReference(f.name, f.dataType, f.nullable)(),
          Literal(ident.get(index, f.dataType), f.dataType))
    }.toSeq
    fileIndex.listFiles(partitionFilters, Seq.empty).map(_.values).toArray
  }

  override def createPartition(ident: InternalRow, properties: util.Map[String, String]): Unit = {
    throw new UnsupportedOperationException()
  }

  override def dropPartition(ident: InternalRow): Boolean = {
    throw new UnsupportedOperationException()
  }

  override def replacePartitionMetadata(
      ident: InternalRow,
      properties: util.Map[String, String]): Unit = {
    throw new UnsupportedOperationException()
  }

  override def loadPartitionMetadata(ident: InternalRow): util.Map[String, String] = {
    Map.empty[String, String].asJava
  }
}

case class PaimonFormatTable(
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    schema: StructType,
    override val partitionSchema_ : StructType,
    table: FormatTable,
    identName: String)
  extends org.apache.spark.sql.connector.catalog.Table
  with SupportsRead
  with SupportsWrite
  with PartitionedFormatTable {

  override lazy val fileIndex: PartitioningAwareFileIndex = {
    PaimonFormatTable.createFileIndex(
      options,
      sparkSession,
      paths,
      Option.apply(schema),
      partitionSchema())
  }

  override def name(): String = {
    identName
  }

  override def capabilities(): util.Set[TableCapability] = {
    util.EnumSet.of(BATCH_READ)
  }

  override def newScanBuilder(caseInsensitiveStringMap: CaseInsensitiveStringMap): ScanBuilder = {
    val scanBuilder = PaimonFormatTableScanBuilder(table.copy(caseInsensitiveStringMap))
    scanBuilder.pruneColumns(schema)
    scanBuilder
  }

  override def newWriteBuilder(logicalWriteInfo: LogicalWriteInfo): WriteBuilder = {
    throw new UnsupportedOperationException()
  }
}

class PartitionedCSVTable(
    name: String,
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    userSpecifiedSchema: Option[StructType],
    fallbackFileFormat: Class[_ <: FileFormat],
    override val partitionSchema_ : StructType)
  extends CSVTable(name, sparkSession, options, paths, userSpecifiedSchema, fallbackFileFormat)
  with PartitionedFormatTable {

  override def newScanBuilder(options: CaseInsensitiveStringMap): CSVScanBuilder = {
    val mergedOptions =
      this.options.asCaseSensitiveMap().asScala ++ options.asCaseSensitiveMap().asScala
    CSVScanBuilder(
      sparkSession,
      fileIndex,
      schema,
      dataSchema,
      new CaseInsensitiveStringMap(mergedOptions.asJava))
  }

  override lazy val fileIndex: PartitioningAwareFileIndex = {
    PaimonFormatTable.createFileIndex(
      options,
      sparkSession,
      paths,
      userSpecifiedSchema,
      partitionSchema())
  }

  override def newWriteBuilder(info: _root_.org.apache.spark.sql.connector.write.LogicalWriteInfo)
      : _root_.org.apache.spark.sql.connector.write.WriteBuilder = {
    super.newWriteBuilder(info)
  }
}

class PartitionedOrcTable(
    name: String,
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    userSpecifiedSchema: Option[StructType],
    fallbackFileFormat: Class[_ <: FileFormat],
    override val partitionSchema_ : StructType
) extends OrcTable(name, sparkSession, options, paths, userSpecifiedSchema, fallbackFileFormat)
  with PartitionedFormatTable {

  override lazy val fileIndex: PartitioningAwareFileIndex = {
    PaimonFormatTable.createFileIndex(
      options,
      sparkSession,
      paths,
      userSpecifiedSchema,
      partitionSchema())
  }
}

class PartitionedParquetTable(
    name: String,
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    userSpecifiedSchema: Option[StructType],
    fallbackFileFormat: Class[_ <: FileFormat],
    override val partitionSchema_ : StructType
) extends ParquetTable(name, sparkSession, options, paths, userSpecifiedSchema, fallbackFileFormat)
  with PartitionedFormatTable {

  override lazy val fileIndex: PartitioningAwareFileIndex = {
    PaimonFormatTable.createFileIndex(
      options,
      sparkSession,
      paths,
      userSpecifiedSchema,
      partitionSchema())
  }
}

class PartitionedJsonTable(
    name: String,
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    userSpecifiedSchema: Option[StructType],
    fallbackFileFormat: Class[_ <: FileFormat],
    override val partitionSchema_ : StructType)
  extends JsonTable(name, sparkSession, options, paths, userSpecifiedSchema, fallbackFileFormat)
  with PartitionedFormatTable {

  override lazy val fileIndex: PartitioningAwareFileIndex = {
    PaimonFormatTable.createFileIndex(
      options,
      sparkSession,
      paths,
      userSpecifiedSchema,
      partitionSchema())
  }
}
