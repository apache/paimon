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

package org.apache.paimon.spark.write

import org.apache.paimon.CoreOptions
import org.apache.paimon.Snapshot
import org.apache.paimon.options.Options
import org.apache.paimon.spark.SparkTypeUtils
import org.apache.paimon.spark.commands.SchemaEvolutionHelper
import org.apache.paimon.spark.schema.SparkSystemColumns
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.types.RowType

import org.apache.spark.sql.PaimonUtils
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.types.{DataType, StructType}

import scala.collection.JavaConverters._

class PaimonV2WriteBuilder(table: FileStoreTable, dataSchema: StructType, options: Options)
  extends BaseV2WriteBuilder(table) {

  private var _operationType: Option[Snapshot.Operation] =
    Option(options.get(PaimonWriteOptions.OPERATION_OPTION)).map(Snapshot.Operation.valueOf)

  def withOperationType(operationType: Snapshot.Operation): PaimonV2WriteBuilder = {
    _operationType = Option(operationType)
    this
  }

  override def build: PaimonV2Write = {
    validateDataSchema()
    val finalTable = overwriteDynamic match {
      case Some(o) =>
        table.copy(Map(CoreOptions.DYNAMIC_PARTITION_OVERWRITE.key -> o.toString).asJava)
      case _ => table
    }
    new PaimonV2Write(
      finalTable,
      overwritePartitions,
      copyOnWriteScan,
      dataSchema,
      options,
      _operationType)
  }

  override def partitionRowType(): RowType = table.schema().logicalPartitionType()

  /**
   * Fail fast when the query schema is binary-incompatible with the table schema.
   *
   * Paimon tables declare `ACCEPT_ANY_SCHEMA`, so Spark skips its own output resolution and the
   * query is aligned to the table schema by `PaimonAnalysis` instead, which is only injected when
   * `PaimonSparkSessionExtensions` is configured. If a write is planned without it, a
   * type-mismatched query would reach the writer as-is and be interpreted with the table's row
   * type, silently corrupting data or failing with errors like `NegativeArraySizeException` deep
   * inside the format writer.
   */
  private def validateDataSchema(): Unit = {
    // Row-level operations write the table's own rows back; schema evolution intentionally
    // diverges from the current table schema and is validated when merging.
    if (copyOnWriteScan.nonEmpty || SchemaEvolutionHelper.mergeSchemaEnabled(options)) {
      return
    }

    val expectedFields = SparkTypeUtils.fromPaimonRowType(table.rowType()).fields
    val actualFields = SparkSystemColumns.filterSparkSystemColumns(dataSchema).fields

    def fail(reason: String): Unit = {
      throw new RuntimeException(
        s"Cannot write incompatible data to table '${table.name()}': $reason. " +
          "The write was planned without Paimon's output resolution, which usually means " +
          "'org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions' is not configured " +
          "in 'spark.sql.extensions'. Please configure it, or align the query columns with the " +
          "table schema explicitly.")
    }

    if (actualFields.length != expectedFields.length) {
      fail(
        s"the number of query columns (${actualFields.length}) does not match " +
          s"the table schema's (${expectedFields.length})")
    }

    val incompatible = actualFields.zip(expectedFields).filterNot {
      case (actual, expected) =>
        val actualType = binaryCompatibleType(
          CharVarcharUtils.getRawType(actual.metadata).getOrElse(actual.dataType))
        PaimonUtils.equalsIgnoreCompatibleNullability(
          actualType,
          binaryCompatibleType(expected.dataType))
    }
    if (incompatible.nonEmpty) {
      val details = incompatible
        .map {
          case (actual, expected) =>
            s"${actual.name} ${actual.dataType.simpleString} -> " +
              s"${expected.name} ${expected.dataType.simpleString}"
        }
        .mkString("[", ", ", "]")
      fail(s"incompatible query column type(s): $details")
    }
  }

  // CHAR/VARCHAR share the string binary layout, treat them as STRING for compatibility check.
  private def binaryCompatibleType(dataType: DataType): DataType =
    CharVarcharUtils.replaceCharVarcharWithString(dataType)
}
