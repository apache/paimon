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

import org.apache.paimon.CoreOptions
import org.apache.paimon.spark.{SparkRow, SparkTypeUtils}
import org.apache.paimon.spark.format.PaimonFormatTable
import org.apache.paimon.spark.leafnode.PaimonLeafV2CommandExec
import org.apache.paimon.utils.{InternalRowPartitionComputer, TypeUtils}

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.analysis.ResolvedPartitionSpec
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.escapePathName
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.unsafe.types.UTF8String

import java.util.{Collections, Map => JMap}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/** Physical command for bounded managed Format Table partition listing. */
case class PaimonShowFormatTablePartitionsExec(
    output: Seq[Attribute],
    table: PaimonFormatTable,
    partitionSpec: Option[ResolvedPartitionSpec],
    maxResults: Int)
  extends PaimonLeafV2CommandExec {

  override protected def run(): Seq[InternalRow] = {
    if (table.partitionCatalog == null) {
      throw new UnsupportedOperationException(
        s"Partition catalog is not configured for managed Format Table ${table.name()}.")
    }

    val prefix = toCatalogPrefix
    val partitions = ArrayBuffer.empty[JMap[String, String]]
    var pageToken: String = null
    do {
      val page =
        table.partitionCatalog.listPartitions(prefix, pageToken, maxResults + 1 - partitions.size)
      if (page.partitions != null) {
        partitions ++= page.partitions.asScala
      }
      // A null or empty next-page token is terminal, matching PaimonPartitionManagement.
      pageToken = Option(page.nextPageToken).filter(_.nonEmpty).orNull
    } while (pageToken != null && partitions.size < maxResults + 1)

    if (partitions.size > maxResults) {
      throw new IllegalStateException(
        s"SHOW PARTITIONS for managed Format Table ${table.name()} exceeded the configured " +
          s"result limit $maxResults. Use CALL sys.list_format_table_partitions for paginated " +
          "partition listing.")
    }

    val defaultPartitionName = table.table.defaultPartName()
    partitions
      .map(partition => formatPartition(partition, defaultPartitionName))
      .sorted
      .map(value => InternalRow(UTF8String.fromString(value)))
      .toSeq
  }

  private def toCatalogPrefix: JMap[String, String] = {
    partitionSpec match {
      case None => Collections.emptyMap[String, String]()
      case Some(spec) =>
        val partitionNames = spec.names
        val rowType = TypeUtils.project(table.table.rowType(), partitionNames.asJava)
        val sparkSchema = SparkTypeUtils.fromPaimonRowType(rowType)
        val rowConverter = CatalystTypeConverters.createToScalaConverter(
          CharVarcharUtils.replaceCharVarcharWithString(sparkSchema))
        val coreOptions = CoreOptions.fromMap(table.table.options())
        val partitionComputer = new InternalRowPartitionComputer(
          coreOptions.partitionDefaultName(),
          rowType,
          partitionNames.toArray,
          coreOptions.legacyPartitionName)
        partitionComputer.generatePartValues(
          new SparkRow(rowType, rowConverter(spec.ident).asInstanceOf[Row]))
    }
  }

  private def formatPartition(
      partition: JMap[String, String],
      defaultPartitionName: String): String = {
    table.table
      .partitionKeys()
      .asScala
      .map {
        name =>
          val catalogValue = partition.get(name)
          val displayValue = if (defaultPartitionName == catalogValue) "null" else catalogValue
          escapePathName(name) + "=" + escapePathName(displayValue)
      }
      .mkString("/")
  }
}

object PaimonShowFormatTablePartitionsExec {

  val DEFAULT_MAX_RESULTS: Int = 10000
}
