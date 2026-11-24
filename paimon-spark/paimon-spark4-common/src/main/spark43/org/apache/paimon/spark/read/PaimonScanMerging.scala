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

package org.apache.paimon.spark.read

import org.apache.paimon.table.{FileStoreTable, Table}

import org.apache.spark.sql.connector.catalog.TableCapability

/**
 * Spark 4.3 merges scans of a data table in `MergeSubplans`, which replaces Paimon's
 * `MergePaimonScalarSubqueries` there. System and search tables do not opt in. Neither do tables
 * whose options enable query auth, because the catalog returns row filters and masks for the
 * columns a scan reads, and a merged scan reads the columns of both.
 */
object PaimonScanMerging {
  def capabilities(table: Table): Seq[TableCapability] = table match {
    case t: FileStoreTable if !t.coreOptions().queryAuthEnabled() =>
      Seq(TableCapability.SCAN_MERGING)
    case _ => Seq.empty
  }
}
