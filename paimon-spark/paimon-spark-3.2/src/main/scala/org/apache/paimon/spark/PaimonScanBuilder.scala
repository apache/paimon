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

import org.apache.paimon.spark.read.{PaimonLocalScan, VectorSearchResultUtils}
import org.apache.paimon.table.InnerTable

import org.apache.spark.sql.connector.read.Scan

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
    PaimonScan(
      actualTable,
      requiredSchema,
      pushedPartitionFilters,
      pushedDataFilters,
      pushedLimit,
      pushedTopN,
      vectorSearch,
      hybridSearch,
      fullTextSearch)
  }
}
