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
case class SparkDeletionVector(
    partitionAndBucket: String,
    partition: Array[Byte],
    bucket: Int,
    dataFileName: String,
    deletionVector: Array[Byte]
) {
  def relativePath(pathFactory: FileStorePathFactory): String = {
    val prefix = pathFactory
      .relativeBucketPath(SerializationUtils.deserializeBinaryRow(partition), bucket)
      .toUri
      .toString + "/"
    prefix + dataFileName
  }
}

object SparkDeletionVector {
  def toDataSplit(
      deletionVector: SparkDeletionVector,
      root: Path,
      pathFactory: FileStorePathFactory,
      dataFilePathToMeta: Map[String, SparkDataFileMeta]): DataSplit = {
    val meta = dataFilePathToMeta(deletionVector.relativePath(pathFactory))
    DataSplit
      .builder()
      .withBucketPath(root + "/" + deletionVector.partitionAndBucket)
      .withPartition(SerializationUtils.deserializeBinaryRow(deletionVector.partition))
      .withBucket(deletionVector.bucket)
      .withTotalBuckets(meta.totalBuckets)
      .withDataFiles(Seq(meta.dataFileMeta).asJava)
      .withDataDeletionFiles(Seq(meta.deletionFile.orNull).asJava)
      .rawConvertible(true)
      .build()
  }
}
