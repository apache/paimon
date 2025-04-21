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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.v2.csv.CSVTable
import org.apache.spark.sql.execution.datasources.v2.json.JsonTable
import org.apache.spark.sql.execution.datasources.v2.orc.OrcTable
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetTable
import org.apache.spark.sql.execution.streaming.{FileStreamSink, MetadataLogFileIndex}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

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
      Option(entry).map(_ == "true").getOrElse(true)
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

class PartitionedCSVTable(
    name: String,
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    userSpecifiedSchema: Option[StructType],
    fallbackFileFormat: Class[_ <: FileFormat],
    partitionSchema: StructType
) extends CSVTable(name, sparkSession, options, paths, userSpecifiedSchema, fallbackFileFormat) {

  override lazy val fileIndex: PartitioningAwareFileIndex = {
    PaimonFormatTable.createFileIndex(
      options,
      sparkSession,
      paths,
      userSpecifiedSchema,
      partitionSchema)
  }
}

class PartitionedOrcTable(
    name: String,
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    userSpecifiedSchema: Option[StructType],
    fallbackFileFormat: Class[_ <: FileFormat],
    partitionSchema: StructType
) extends OrcTable(name, sparkSession, options, paths, userSpecifiedSchema, fallbackFileFormat) {

  override lazy val fileIndex: PartitioningAwareFileIndex = {
    PaimonFormatTable.createFileIndex(
      options,
      sparkSession,
      paths,
      userSpecifiedSchema,
      partitionSchema)
  }
}

class PartitionedParquetTable(
    name: String,
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    userSpecifiedSchema: Option[StructType],
    fallbackFileFormat: Class[_ <: FileFormat],
    partitionSchema: StructType
) extends ParquetTable(
    name,
    sparkSession,
    options,
    paths,
    userSpecifiedSchema,
    fallbackFileFormat) {

  override lazy val fileIndex: PartitioningAwareFileIndex = {
    PaimonFormatTable.createFileIndex(
      options,
      sparkSession,
      paths,
      userSpecifiedSchema,
      partitionSchema)
  }
}

class PartitionedJsonTable(
    name: String,
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    userSpecifiedSchema: Option[StructType],
    fallbackFileFormat: Class[_ <: FileFormat],
    partitionSchema: StructType)
  extends JsonTable(name, sparkSession, options, paths, userSpecifiedSchema, fallbackFileFormat) {

  override lazy val fileIndex: PartitioningAwareFileIndex = {
    PaimonFormatTable.createFileIndex(
      options,
      sparkSession,
      paths,
      userSpecifiedSchema,
      partitionSchema)
  }
}
