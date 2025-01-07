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

package org.apache.paimon.spark.catalyst.analysis

import org.apache.paimon.spark.catalyst.Compatibility

import org.apache.spark.sql.PaimonUtils.{normalizePartitionSpec, requireExactMatchedPartitionSpec}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{PartitionSpec, ResolvedPartitionSpec, UnresolvedPartitionSpec}
import org.apache.spark.sql.catalyst.analysis.ResolvePartitionSpec.conf
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object PaimonResolvePartitionSpec {

  def resolve(
      catalog: TableCatalog,
      tableIndent: Identifier,
      partitionSpec: PartitionSpec): ResolvedPartitionSpec = {
    val table = catalog.loadTable(tableIndent).asPartitionable
    partitionSpec match {
      case u: UnresolvedPartitionSpec =>
        val partitionSchema = table.partitionSchema()
        resolvePartitionSpec(table.name(), u, partitionSchema, allowPartitionSpec = false)
      case o => o.asInstanceOf[ResolvedPartitionSpec]
    }
  }

  private def resolvePartitionSpec(
      tableName: String,
      partSpec: UnresolvedPartitionSpec,
      partSchema: StructType,
      allowPartitionSpec: Boolean): ResolvedPartitionSpec = {
    val normalizedSpec = normalizePartitionSpec(partSpec.spec, partSchema, tableName, conf.resolver)
    if (!allowPartitionSpec) {
      requireExactMatchedPartitionSpec(tableName, normalizedSpec, partSchema.fieldNames)
    }
    val partitionNames = normalizedSpec.keySet
    val requestedFields = partSchema.filter(field => partitionNames.contains(field.name))
    ResolvedPartitionSpec(
      requestedFields.map(_.name),
      convertToPartIdent(normalizedSpec, requestedFields),
      partSpec.location)
  }

  def convertToPartIdent(
      partitionSpec: TablePartitionSpec,
      schema: Seq[StructField]): InternalRow = {
    val partValues = schema.map {
      part =>
        val raw = partitionSpec.get(part.name).orNull
        val dt = CharVarcharUtils.replaceCharVarcharWithString(part.dataType)
        Compatibility
          .cast(Literal.create(raw, StringType), dt, Some(conf.sessionLocalTimeZone))
          .eval()
    }
    InternalRow.fromSeq(partValues)
  }
}
