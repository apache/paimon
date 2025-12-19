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
import org.apache.paimon.CoreOptions.BucketFunctionType
import org.apache.paimon.options.Options
import org.apache.paimon.spark.catalog.functions.BucketFunction
import org.apache.paimon.spark.scan.PaimonCopyOnWriteScan
import org.apache.paimon.spark.schema.PaimonMetadataColumn.FILE_PATH_COLUMN
import org.apache.paimon.spark.util.OptionUtils
import org.apache.paimon.spark.write.PaimonV2WriteBuilder
import org.apache.paimon.table.{FileStoreTable, InnerTable, Table}
import org.apache.paimon.table.BucketMode.{BUCKET_UNAWARE, HASH_FIXED, POSTPONE_MODE}

import org.apache.spark.sql.connector.expressions.{Expressions, NamedReference}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, RowLevelOperation, RowLevelOperationInfo, WriteBuilder}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class PaimonSparkCopyOnWriteOperation(table: Table, info: RowLevelOperationInfo)
  extends RowLevelOperation {

  private lazy val coreOptions = new CoreOptions(table.options())

  private var copyOnWriteScan: Option[PaimonCopyOnWriteScan] = None

  private lazy val useV2Write: Boolean = {
    val v2WriteConfigured = OptionUtils.useV2Write()
    v2WriteConfigured && supportsV2Write
  }

  private def supportsV2Write: Boolean = {
    coreOptions.bucketFunctionType() == BucketFunctionType.DEFAULT && {
      table match {
        case storeTable: FileStoreTable =>
          storeTable.bucketMode() match {
            case HASH_FIXED => BucketFunction.supportsTable(storeTable)
            case BUCKET_UNAWARE | POSTPONE_MODE => true
            case _ => false
          }

        case _ => false
      }
    } && coreOptions.clusteringColumns().isEmpty
  }

  override def command(): RowLevelOperation.Command = info.command()

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    table match {
      case t: InnerTable =>
        new PaimonScanBuilder(t.copy(options.asCaseSensitiveMap).asInstanceOf[InnerTable]) {
          override def build(): Scan = {
            val scan =
              PaimonCopyOnWriteScan(t, requiredSchema, pushedPartitionFilters, pushedDataFilters)
            PaimonSparkCopyOnWriteOperation.this.copyOnWriteScan = Option(scan)
            scan
          }
        }
      case _ =>
        throw new UnsupportedOperationException(
          s"Scan is only supported for InnerTable. " +
            s"Actual table type: ${Option(table).map(_.getClass.getSimpleName).getOrElse("null")}"
        )
    }
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    table match {
      case fileStoreTable: FileStoreTable if useV2Write =>
        val options = Options.fromMap(info.options)
        val builder = new PaimonV2WriteBuilder(fileStoreTable, info.schema(), options)
        builder.overwriteFiles(copyOnWriteScan)
      case _ =>
        throw new UnsupportedOperationException(
          s"Write operation is only supported for FileStoreTable with V2 write enabled. " +
            s"Actual table type: ${table.getClass.getSimpleName}, useV2Write: $useV2Write"
        )
    }
  }

  override def requiredMetadataAttributes(): Array[NamedReference] = {
    val attributes: Seq[NamedReference] = Seq.empty
    val updatedAttributes = attributes :+ Expressions.column(FILE_PATH_COLUMN)
    updatedAttributes.toArray
  }

}
