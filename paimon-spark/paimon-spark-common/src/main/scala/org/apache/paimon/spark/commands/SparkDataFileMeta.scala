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

import org.apache.paimon.data.BinaryRow
import org.apache.paimon.io.DataFileMeta
import org.apache.paimon.spark.PaimonImplicits
import org.apache.paimon.table.source.{DataSplit, DeletionFile}
import org.apache.paimon.utils.FileStorePathFactory

import scala.collection.JavaConverters._

case class SparkDataFileMeta(
    partition: BinaryRow,
    bucket: Int,
    totalBuckets: Int,
    dataFileMeta: DataFileMeta,
    deletionFile: Option[DeletionFile] = None) {

  def relativePath(fileStorePathFactory: FileStorePathFactory): String = {
    fileStorePathFactory
      .relativeBucketPath(partition, bucket)
      .toUri
      .toString + "/" + dataFileMeta.fileName()
  }
}

object SparkDataFileMeta {
  def convertToSparkDataFileMeta(
      dataSplit: DataSplit,
      totalBuckets: Int): Seq[SparkDataFileMeta] = {
    import PaimonImplicits._

    val dvFactory =
      DeletionFile.factory(dataSplit.dataFiles(), dataSplit.deletionFiles().orElse(null))
    dataSplit.dataFiles().asScala.map {
      file =>
        SparkDataFileMeta(
          dataSplit.partition,
          dataSplit.bucket,
          totalBuckets,
          file,
          dvFactory.create(file.fileName()))
    }
  }.toSeq

  def convertToDataSplits(
      sparkDataFiles: Array[SparkDataFileMeta],
      rawConvertible: Boolean,
      pathFactory: FileStorePathFactory): Array[DataSplit] = {
    sparkDataFiles
      .groupBy(file => (file.partition, file.bucket))
      .map {
        case ((partition, bucket), files) =>
          val (dataFiles, deletionFiles) = files.map {
            file => (file.dataFileMeta, file.deletionFile.orNull)
          }.unzip
          new DataSplit.Builder()
            .withPartition(partition)
            .withBucket(bucket)
            .withDataFiles(dataFiles.toList.asJava)
            .withDataDeletionFiles(deletionFiles.toList.asJava)
            .rawConvertible(rawConvertible)
            .withBucketPath(pathFactory.bucketPath(partition, bucket).toString)
            .build()
      }
      .toArray
  }
}
