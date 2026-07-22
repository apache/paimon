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

import org.apache.paimon.spark.format.{FormatTablePartitionRepair, PaimonFormatTable}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{NoSuchPartitionsException, ResolvedPartitionSpec}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec

import java.util.{Map => JMap}

import scala.collection.JavaConverters._

object PaimonFormatTablePartitionDdlExec {

  /**
   * Run an operation and refresh Spark's cached plans afterwards, whether it succeeded or not. A
   * refresh failure never replaces the operation's own failure — that one explains what went wrong
   * — and is attached to it instead, unless the two are the same throwable.
   */
  private[execution] def refreshingCache[T](refreshCache: () => Unit)(operation: => T): T = {
    var operationFailure: Throwable = null
    try {
      operation
    } catch {
      case failure: Throwable =>
        operationFailure = failure
        throw failure
    } finally {
      try {
        refreshCache()
      } catch {
        case refreshFailure: Throwable =>
          if (operationFailure == null) {
            throw refreshFailure
          } else if (refreshFailure ne operationFailure) {
            operationFailure.addSuppressed(refreshFailure)
          }
      }
    }
  }

  private[execution] def unsupportedWithoutCatalogManagedPartitions(
      operation: String,
      table: PaimonFormatTable): UnsupportedOperationException =
    new UnsupportedOperationException(
      s"$operation PARTITION is supported only for a Format Table with catalog-managed " +
        s"partitions, but partitions of ${table.name()} are discovered from the filesystem.")
}

/**
 * Registers catalog-managed partitions of a Format Table without Spark's client-side existence
 * precheck. The whole batch and IF NOT EXISTS flag are forwarded to the catalog so it can apply
 * them atomically.
 */
case class PaimonAddFormatTablePartitionsExec(
    table: PaimonFormatTable,
    partSpecs: Seq[ResolvedPartitionSpec],
    ignoreIfExists: Boolean,
    refreshCache: () => Unit)
  extends LeafV2CommandExec {

  override protected def run(): Seq[InternalRow] = {
    if (!table.hasCatalogManagedPartitions) {
      throw PaimonFormatTablePartitionDdlExec.unsupportedWithoutCatalogManagedPartitions(
        "ADD",
        table)
    }

    if (partSpecs.nonEmpty) {
      val properties: Array[JMap[String, String]] =
        partSpecs.map(spec => spec.location.map("location" -> _).toMap.asJava).toArray
      PaimonFormatTablePartitionDdlExec.refreshingCache(refreshCache) {
        table.createFormatTablePartitions(
          partSpecs.map(_.ident).toArray,
          properties,
          ignoreIfExists)
      }
    }
    Seq.empty
  }

  override def output: Seq[Attribute] = Seq.empty
}

/**
 * Physical command providing explicit DROP semantics for Format Tables with catalog-managed
 * partitions (tables using filesystem partition discovery always fail with a clear error). Complete
 * specs are resolved against the catalog registration first: missing specs fail with
 * [[NoSuchPartitionsException]] unless IF EXISTS was given, and only registered specs are
 * unregistered and have their directories deleted, so data that is merely awaiting registration
 * (e.g. by MSCK REPAIR TABLE) is never removed. Partial specs resolve to the registered leaf
 * partitions they cover.
 */
case class PaimonDropFormatTablePartitionsExec(
    table: PaimonFormatTable,
    partSpecs: Seq[ResolvedPartitionSpec],
    ifExists: Boolean,
    purge: Boolean,
    refreshCache: () => Unit)
  extends LeafV2CommandExec {

  override protected def run(): Seq[InternalRow] = {
    if (!table.hasCatalogManagedPartitions) {
      throw PaimonFormatTablePartitionDdlExec.unsupportedWithoutCatalogManagedPartitions(
        "DROP",
        table)
    }
    if (purge) {
      // Match Spark's v2 default for purgePartitions. DROP PARTITION on a Format Table with
      // catalog-managed partitions already removes the partition data, so PURGE adds nothing.
      throw new UnsupportedOperationException(
        s"DROP PARTITION ... PURGE is not supported for Format Table ${table.name()}. " +
          "DROP PARTITION already removes the partition data.")
    }
    if (partSpecs.isEmpty) {
      return Seq.empty
    }

    val partitionKeyCount = table.table.partitionKeys().size()
    val (completeSpecs, partialSpecs) =
      partSpecs.partition(_.ident.numFields == partitionKeyCount)
    val registration = table
      .formatTablePartitionsRegistered(
        completeSpecs.map(_.names.toArray).toArray,
        completeSpecs.map(_.ident).toArray)
      .toSeq
    val missingSpecs = completeSpecs.zip(registration).collect { case (spec, false) => spec }
    if (missingSpecs.nonEmpty && !ifExists) {
      throw new NoSuchPartitionsException(
        table.name(),
        missingSpecs.map(_.ident),
        table.partitionSchema)
    }

    val specsToDrop =
      completeSpecs.zip(registration).collect { case (spec, true) => spec } ++ partialSpecs
    if (specsToDrop.nonEmpty) {
      // Catalog-managed semantics (PaimonPartitionManagement#dropFormatTablePartitions): resolve
      // partial specs, unregister the exact catalog partitions, then delete their directories
      // with the table FileIO client-side. A directory-deletion failure stays unregistered so
      // partially deleted data is not exposed again.
      PaimonFormatTablePartitionDdlExec.refreshingCache(refreshCache) {
        table.dropFormatTablePartitions(
          specsToDrop.map(_.names.toArray).toArray,
          specsToDrop.map(_.ident).toArray)
      }
    }
    Seq.empty
  }

  override def output: Seq[Attribute] = Seq.empty
}

/**
 * MSCK REPAIR TABLE for Format Tables with catalog-managed partitions. Spark rejects RepairTable
 * for v2 tables in DataSourceV2Strategy, so PaimonStrategy intercepts first; plain MSCK means ADD.
 * Tables using filesystem partition discovery are not intercepted and keep Spark's own v2
 * rejection.
 */
case class PaimonRepairFormatTablePartitionsExec(
    table: PaimonFormatTable,
    addPartitions: Boolean,
    dropPartitions: Boolean,
    refreshCache: () => Unit)
  extends LeafV2CommandExec {

  override protected def run(): Seq[InternalRow] = {
    PaimonFormatTablePartitionDdlExec.refreshingCache(refreshCache) {
      FormatTablePartitionRepair.repair(table, addPartitions, dropPartitions)
    }
    Seq.empty
  }

  override def output: Seq[Attribute] = Seq.empty
}
