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
import org.apache.paimon.partition.PartitionPredicate
import org.apache.paimon.spark.read.{PaimonLocalScan, VectorSearchResultUtils}
import org.apache.paimon.table.{BucketMode, FileStoreTable, InnerTable, Table}
import org.apache.paimon.table.source.PostponeMergeReadBuilder

import org.apache.spark.sql.connector.read.Scan

import scala.collection.JavaConverters._

class PaimonScanBuilder(val table: InnerTable) extends PaimonBaseScanBuilder {

  override def build(): Scan = {
    val (actualTable, vectorSearch, hybridSearch, fullTextSearch) = table match {
      case vst: org.apache.paimon.table.VectorSearchTable =>
        (vst.origin(), Option(vst.vectorSearch()), None, None)
      case hst: org.apache.paimon.table.HybridSearchTable =>
        (hst.origin(), None, Option(hst.hybridSearch()), None)
      case ftst: org.apache.paimon.table.FullTextSearchTable =>
        (ftst.origin(), None, None, Option(ftst.fullTextSearch()))
      case _ => (table, pushedVectorSearch, None, pushedFullTextSearch)
    }
    val postponeMergeReadBuilder = actualTable match {
      case fileStoreTable: FileStoreTable
          if PaimonScanBuilder.postponeMergeOnReadEnabled(fileStoreTable) =>
        createPostponeMergeReadBuilder(fileStoreTable)
      case _ => None
    }
    if (
      postponeMergeReadBuilder.isDefined &&
      (vectorSearch.isDefined || hybridSearch.isDefined || fullTextSearch.isDefined)
    ) {
      throw new UnsupportedOperationException(
        "Option 'postpone.merge-on-read' does not support vector, hybrid or full-text search.")
    }
    if (
      vectorSearch.isDefined &&
      VectorSearchResultUtils.isVectorSearchMetaOnly(requiredSchema.fieldNames.toSeq)
    ) {
      val result = PaimonBaseScan.evalVectorSearch(
        actualTable,
        vectorSearch.get,
        pushedPartitionFilters,
        pushedDataFilters)
      return PaimonLocalScan(
        VectorSearchResultUtils.toRows(result, requiredSchema),
        requiredSchema,
        actualTable,
        pushedPartitionFilters)
    }
    val scan = PaimonScan(
      actualTable,
      requiredSchema,
      pushedPartitionFilters,
      pushedDataFilters,
      pushedLimit,
      pushedTopN,
      vectorSearch,
      hybridSearch,
      fullTextSearch)
    postponeMergeReadBuilder
      .map(builder => new PostponeMergeOnReadScan(scan, builder))
      .getOrElse(scan)
  }

  private def createPostponeMergeReadBuilder(
      table: FileStoreTable): Option[PostponeMergeReadBuilder] = {
    val partitionFilter =
      if (pushedPartitionFilters.isEmpty) null
      else PartitionPredicate.and(pushedPartitionFilters.toList.asJava)
    val result = PostponeMergeReadBuilder.create(table, partitionFilter)
    if (result.isPresent) Some(result.get) else None
  }
}

object PaimonScanBuilder {

  private[spark] def postponeMergeOnReadEnabled(table: Table): Boolean = {
    CoreOptions.fromMap(table.options()).postponeMergeOnRead() && (table match {
      case fileStoreTable: FileStoreTable =>
        fileStoreTable.bucketMode() == BucketMode.POSTPONE_MODE &&
        !fileStoreTable.primaryKeys().isEmpty
      case _ => false
    })
  }
}
