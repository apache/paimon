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

import org.apache.paimon.CoreOptions
import org.apache.paimon.spark.format.PaimonFormatTable

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{NoSuchPartitionsException, ResolvedPartitionSpec}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec

import java.util.{Map => JMap}

import scala.collection.JavaConverters._

object PaimonFormatTablePartitionDdlExec {

  def isManaged(table: PaimonFormatTable): Boolean =
    CoreOptions.fromMap(table.table.options()).partitionedTableInMetastore()

  private[execution] def unsupportedUnmanaged(
      operation: String,
      table: PaimonFormatTable): UnsupportedOperationException =
    new UnsupportedOperationException(
      s"$operation PARTITION is not supported for unmanaged Format Table ${table.name()}.")
}

/**
 * Adds managed Format Table partitions without Spark's client-side existence precheck. The whole
 * batch and IF NOT EXISTS flag are forwarded to the catalog so it can apply them atomically.
 */
case class PaimonAddFormatTablePartitionsExec(
    table: PaimonFormatTable,
    partSpecs: Seq[ResolvedPartitionSpec],
    ignoreIfExists: Boolean,
    refreshCache: () => Unit)
  extends LeafV2CommandExec {

  override protected def run(): Seq[InternalRow] = {
    if (!PaimonFormatTablePartitionDdlExec.isManaged(table)) {
      throw PaimonFormatTablePartitionDdlExec.unsupportedUnmanaged("ADD", table)
    }

    if (partSpecs.nonEmpty) {
      val properties: Array[JMap[String, String]] =
        partSpecs.map(spec => spec.location.map("location" -> _).toMap.asJava).toArray
      try {
        table.createFormatTablePartitions(
          partSpecs.map(_.ident).toArray,
          properties,
          ignoreIfExists)
      } finally {
        refreshCache()
      }
    }
    Seq.empty
  }

  override def output: Seq[Attribute] = Seq.empty
}

/**
 * Physical command providing explicit DROP semantics for managed Format Tables (unmanaged Format
 * Tables always fail with a clear error). Complete specs are resolved against the catalog
 * registration first: missing specs fail with [[NoSuchPartitionsException]] unless IF EXISTS was
 * given, and only registered specs are unregistered and have their directories deleted, so data
 * that is merely awaiting registration is never removed. Partial specs resolve to the registered
 * leaf partitions they cover.
 */
case class PaimonDropFormatTablePartitionsExec(
    table: PaimonFormatTable,
    partSpecs: Seq[ResolvedPartitionSpec],
    ifExists: Boolean,
    purge: Boolean,
    refreshCache: () => Unit)
  extends LeafV2CommandExec {

  override protected def run(): Seq[InternalRow] = {
    if (!PaimonFormatTablePartitionDdlExec.isManaged(table)) {
      throw PaimonFormatTablePartitionDdlExec.unsupportedUnmanaged("DROP", table)
    }
    if (purge) {
      // Match Spark's v2 default for purgePartitions. Managed Format Table DROP PARTITION
      // already removes the partition data, so PURGE adds nothing.
      throw new UnsupportedOperationException(
        s"DROP PARTITION ... PURGE is not supported for managed Format Table ${table.name()}. " +
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
      // Managed semantics (PaimonPartitionManagement#dropFormatTablePartitions): resolve partial
      // specs, unregister the exact catalog partitions, then delete their directories with the
      // table FileIO client-side. A directory-deletion failure stays unregistered so partially
      // deleted data is not exposed again.
      try {
        table.dropFormatTablePartitions(
          specsToDrop.map(_.names.toArray).toArray,
          specsToDrop.map(_.ident).toArray)
      } finally {
        refreshCache()
      }
    }
    Seq.empty
  }

  override def output: Seq[Attribute] = Seq.empty
}
