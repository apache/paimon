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

package org.apache.paimon.spark.commands

import org.apache.paimon.fs.Path
import org.apache.paimon.table.source.DataSplit
import org.apache.paimon.utils.{FileStorePathFactory, SerializationUtils}

import scala.collection.JavaConverters._

/**
 * This class will be used as Dataset's pattern type. So here use Array[Byte] instead of BinaryRow
 * or DeletionVector.
 */
case class SparkDeletionVectors(
    partitionAndBucket: String,
    partition: Array[Byte],
    bucket: Int,
    dataFileAndDeletionVector: Seq[(String, Array[Byte])]
) {
  def relativePaths(fileStorePathFactory: FileStorePathFactory): Seq[String] = {
    val prefix = fileStorePathFactory
      .relativeBucketPath(SerializationUtils.deserializeBinaryRow(partition), bucket)
      .toUri
      .toString + "/"
    dataFileAndDeletionVector.map(prefix + _._1)
  }
}

object SparkDeletionVectors {
  def toDataSplit(
      sparkDeletionVectors: SparkDeletionVectors,
      root: Path,
      pathFactory: FileStorePathFactory,
      dataFilePathToMeta: Map[String, SparkDataFileMeta]): DataSplit = {
    val (dataFiles, deletionFiles) = sparkDeletionVectors
      .relativePaths(pathFactory)
      .map {
        dataFile =>
          val meta = dataFilePathToMeta(dataFile)
          (meta.dataFileMeta, meta.deletionFile.orNull)
      }
      .unzip
    DataSplit
      .builder()
      .withBucketPath(root + "/" + sparkDeletionVectors.partitionAndBucket)
      .withPartition(SerializationUtils.deserializeBinaryRow(sparkDeletionVectors.partition))
      .withBucket(sparkDeletionVectors.bucket)
      .withDataFiles(dataFiles.toList.asJava)
      .withDataDeletionFiles(deletionFiles.toList.asJava)
      .rawConvertible(true)
      .build()
  }
}
