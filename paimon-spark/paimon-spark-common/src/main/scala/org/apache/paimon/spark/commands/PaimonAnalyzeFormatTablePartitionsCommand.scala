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

import org.apache.paimon.spark.catalyst.analysis.PaimonResolvePartitionSpec
import org.apache.paimon.spark.format.PaimonFormatTable
import org.apache.paimon.spark.leafnode.PaimonLeafRunnableCommand
import org.apache.paimon.spark.util.OptionUtils
import org.apache.paimon.table.format.FormatTablePartitionStatsCollector

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.PaimonUtils.normalizePartitionSpec
import org.apache.spark.sql.catalyst.analysis.NoSuchPartitionException
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import java.util.{Map => JMap}

import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap

/**
 * Recomputes the catalog statistics of a Format Table with catalog-managed partitions, backing
 * `ANALYZE TABLE t [PARTITION(...)] COMPUTE STATISTICS [NOSCAN]`.
 *
 * The partitions are measured from storage and each measured field replaces what the catalog holds,
 * so this is how a table catches up with writers the catalog never saw. `NOSCAN` stops at what a
 * directory listing gives — file count, byte size, last file creation time — while a full ANALYZE
 * also reads each file footer for its row count. A format that carries no footer (CSV, TEXT, JSON)
 * leaves the row count as it was rather than guessing one.
 *
 * Analyzing is not a way to add or remove partitions: it measures the ones registered at the time
 * of the listing and re-registers exactly those. There is no lock between the listing and the
 * write, so a partition dropped concurrently can be re-registered with its last measurement — the
 * same last-writer-wins window every lock-free partition operation on these tables has. A
 * `PARTITION(...)` clause selects the partitions whose leading values it fixes.
 */
case class PaimonAnalyzeFormatTablePartitionsCommand(
    v2Table: PaimonFormatTable,
    partitionSpec: Map[String, Option[String]],
    noScan: Boolean)
  extends PaimonLeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val prefix = leadingPrefix(sparkSession)
    val partitions = v2Table.partitionManager
      .listPartitions(prefix.asJava, null)
      .asScala
      .map(_.spec())
      .toList

    if (partitions.isEmpty && prefix.nonEmpty) {
      throw new NoSuchPartitionException(
        v2Table.name(),
        new GenericInternalRow(prefix.values.map(UTF8String.fromString(_): Any).toArray),
        StructType(prefix.keys.map(StructField(_, StringType)).toSeq)
      )
    }

    if (partitions.nonEmpty) {
      val collector = new FormatTablePartitionStatsCollector(
        v2Table.table,
        !noScan,
        OptionUtils.formatTableStatisticsParallelism())
      val statistics = collector.collect(partitions.asJava)
      v2Table.partitionManager
        .createPartitions(partitions.asJava, true, statistics, true)
    }
    Seq.empty[Row]
  }

  /**
   * The values the `PARTITION(...)` clause fixes, as a leading prefix of the partition keys — the
   * shape the catalog can select on.
   *
   * This follows what Spark does with the same clause on a metastore table: names are resolved by
   * the same helper its own commands use, a column named without a value means every value of it,
   * and the columns that do carry a value have to be a leading run. `PARTITION (dt = 'x', hour)`
   * therefore selects every hour of that day and `PARTITION (dt, hour)` selects everything, while
   * `PARTITION (hour = '00')` is rejected: the catalog cannot select on a non-leading key, and
   * quietly widening it would measure more partitions than were asked for.
   */
  private def leadingPrefix(sparkSession: SparkSession): Map[String, String] = {
    if (partitionSpec.isEmpty) {
      return Map.empty
    }
    val partitionKeys = v2Table.table.partitionKeys().asScala.toSeq
    val normalized = normalizePartitionSpec(
      partitionSpec,
      v2Table.partitionSchema,
      v2Table.name(),
      sparkSession.sessionState.conf.resolver)
    val valueByKey = partitionKeys.map(key => key -> normalized.get(key).flatten)
    val prefix = valueByKey.takeWhile(_._2.isDefined)
    if (valueByKey.drop(prefix.size).exists(_._2.isDefined)) {
      throw new IllegalArgumentException(
        s"ANALYZE TABLE ${v2Table.name()} PARTITION must give values for a leading run of its " +
          s"partition columns ${partitionKeys.mkString("[", ", ", "]")}, but got values for " +
          valueByKey.filter(_._2.isDefined).map(_._1).mkString("[", ", ", "]"))
    }
    if (prefix.isEmpty) {
      return Map.empty
    }
    // The catalog holds the values the way Paimon writes them, so what the parser handed over is
    // read as the partition column type first: PARTITION (p = '01') selects the INT partition
    // registered as 1, and a null value selects the default partition.
    val names = prefix.map { case (key, _) => key }
    val ident = PaimonResolvePartitionSpec.convertToPartIdent(
      prefix.map { case (key, value) => key -> value.get }.toMap,
      names.map(v2Table.partitionSchema.apply))
    // Kept in partition-key order, so a message built from it reads in that order too.
    ListMap(v2Table.toCatalogPartition(ident, names).asScala.toSeq: _*)
  }
}
