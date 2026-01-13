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

package org.apache.spark.sql.paimon.shims

import org.apache.paimon.spark.catalyst.parser.extensions.PaimonSpark40SqlExtensionsParser
import org.apache.paimon.spark.data.{Spark4ArrayData, Spark4InternalRow, Spark4InternalRowWithBlob, SparkArrayData, SparkInternalRow}
import org.apache.paimon.types.{DataType, RowType}

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, MergeRows, UpdateAction}
import org.apache.spark.sql.catalyst.plans.logical.MergeRows.Instruction
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.streaming.{FileStreamSink, MetadataLogFileIndex}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

object MinorVersionShim {

  def createSparkParser(delegate: ParserInterface): ParserInterface = {
    new PaimonSpark40SqlExtensionsParser(delegate)
  }

  def createKeep(context: String, condition: Expression, output: Seq[Expression]): Instruction = {
    MergeRows.Keep(condition, output)
  }

  def createUpdateAction(
      condition: Option[Expression],
      assignments: Seq[Assignment]): UpdateAction = {
    UpdateAction(condition, assignments)
  }

  def createDataSourceV2Relation(
      relation: DataSourceV2Relation,
      table: Table): DataSourceV2Relation = {
    relation.copy(table)
  }

  def createSparkInternalRow(rowType: RowType): SparkInternalRow = {
    new Spark4InternalRow(rowType)
  }

  def createSparkInternalRowWithBlob(
      rowType: RowType,
      blobFieldIndex: Int,
      blobAsDescriptor: Boolean): SparkInternalRow = {
    new Spark4InternalRowWithBlob(rowType, blobFieldIndex, blobAsDescriptor)
  }

  def createSparkArrayData(elementType: DataType): SparkArrayData = {
    new Spark4ArrayData(elementType)
  }

  def createFileIndex(
      options: CaseInsensitiveStringMap,
      sparkSession: SparkSession,
      paths: Seq[String],
      userSpecifiedSchema: Option[StructType],
      partitionSchema: StructType): PartitioningAwareFileIndex = {

    class PartitionedMetadataLogFileIndex(
        sparkSession: SparkSession,
        path: Path,
        parameters: Map[String, String],
        userSpecifiedSchema: Option[StructType],
        override val partitionSchema: StructType)
      extends MetadataLogFileIndex(sparkSession, path, parameters, userSpecifiedSchema)

    class PartitionedInMemoryFileIndex(
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

    def globPaths: Boolean = {
      val entry = options.get(DataSource.GLOB_PATHS_KEY)
      Option(entry).forall(_ == "true")
    }

    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    if (FileStreamSink.hasMetadata(paths, hadoopConf, sparkSession.sessionState.conf)) {
      new PartitionedMetadataLogFileIndex(
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

      new PartitionedInMemoryFileIndex(
        sparkSession,
        rootPathsSpecified,
        caseSensitiveMap,
        userSpecifiedSchema,
        fileStatusCache,
        partitionSchema = partitionSchema)
    }
  }

}
