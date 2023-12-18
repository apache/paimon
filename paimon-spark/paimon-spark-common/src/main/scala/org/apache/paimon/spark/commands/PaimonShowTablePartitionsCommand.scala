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

import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.leafnode.PaimonLeafRunnableCommand
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.ResolvedPartitionSpec
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.escapePathName
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Cast, Literal}
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

case class PaimonShowTablePartitionsCommand(
    v2Table: SparkTable,
    specOpt: Option[ResolvedPartitionSpec])
  extends PaimonLeafRunnableCommand
  with PaimonCommand {

  override def output: Seq[Attribute] = {
    AttributeReference("partition", StringType, nullable = false)() :: Nil
  }

  override def table: FileStoreTable = v2Table.getTable.asInstanceOf[FileStoreTable]

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val (names, ident) = specOpt
      .map(spec => (spec.names, spec.ident))
      .getOrElse((Seq.empty[String], InternalRow.empty))
    val schema = v2Table.partitionSchema()
    val partitionIdentifiers = v2Table.listPartitionIdentifiers(names.toArray, ident)
    val len = schema.length
    val partitions = new Array[String](len)
    val timeZoneId = conf.sessionLocalTimeZone
    val output = partitionIdentifiers.map {
      row =>
        var i = 0
        while (i < len) {
          val dataType = schema(i).dataType
          val partValueUTF8String =
            Cast(Literal(row.get(i, dataType), dataType), StringType, Some(timeZoneId)).eval()
          val partValueStr =
            if (partValueUTF8String == null) "null" else partValueUTF8String.toString
          partitions(i) = escapePathName(schema(i).name) + "=" + escapePathName(partValueStr)
          i += 1
        }
        partitions.mkString("/")
    }
    output.sorted.map(p => Row(UTF8String.fromString(p)))
  }
}
