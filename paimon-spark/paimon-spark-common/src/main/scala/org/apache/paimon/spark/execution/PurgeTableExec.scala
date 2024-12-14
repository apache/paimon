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

package org.apache.paimon.spark.execution

import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.leafnode.PaimonLeafV2CommandExec
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}

case class PurgeTableExec(catalog: TableCatalog, ident: Identifier, out: Seq[Attribute])
  extends PaimonLeafV2CommandExec {
  override protected def run(): Seq[InternalRow] = {
    val table = catalog.loadTable(ident)
    assert(table.isInstanceOf[SparkTable])

    var tags: Seq[InternalRow] = Nil
    table.asInstanceOf[SparkTable].getTable match {
      case paimonTable: FileStoreTable =>
        val fileIO = paimonTable.fileIO()
        val tablePath = paimonTable.snapshotManager().tablePath()
        fileIO
          .listStatus(tablePath)
          .filter(f => !f.getPath.getName.contains("schema"))
          .foreach(f => fileIO.delete(f.getPath, true))

      case t =>
        throw new UnsupportedOperationException(
          s"Only support purge table command for FileStoreTable: $t")
    }
    tags
  }

  override def output: Seq[Attribute] = out
}
