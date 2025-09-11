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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{NoSuchPartitionsException, ResolvedPartitionSpec}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits.TableHelper
import org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec

case class PaimonDropPartitionsExec(
    table: SparkTable,
    partSpecs: Seq[ResolvedPartitionSpec],
    ignoreIfNotExists: Boolean,
    purge: Boolean,
    refreshCache: () => Unit)
  extends LeafV2CommandExec
  with Logging {
  override protected def run(): Seq[InternalRow] = {
    val partitionSchema = table.asPartitionable.partitionSchema()
    val (partialPartSpecs, fullPartSpecs) =
      partSpecs.partition(_.ident.numFields != partitionSchema.length)

    val (existsPartIdents, notExistsPartIdents) =
      fullPartSpecs.map(_.ident).partition(table.partitionExists)
    if (notExistsPartIdents.nonEmpty && !ignoreIfNotExists) {
      throw new NoSuchPartitionsException(
        table.name(),
        notExistsPartIdents,
        table.asPartitionable.partitionSchema())
    }
    val allExistsPartIdents = existsPartIdents ++ partialPartSpecs.flatMap(expendPartialSpec)
    logInfo("Try to drop partitions: " + allExistsPartIdents.mkString(","))
    val isTableAltered = if (allExistsPartIdents.nonEmpty) {
      allExistsPartIdents
        .map(
          partIdent => {
            if (purge) table.purgePartition(partIdent) else table.dropPartition(partIdent)
          })
        .reduce(_ || _)
    } else false

    if (isTableAltered) refreshCache()
    Seq.empty
  }

  private def expendPartialSpec(partialSpec: ResolvedPartitionSpec): Seq[InternalRow] = {
    assertSpec(partialSpec)
    table.listPartitionIdentifiers(partialSpec.names.toArray, partialSpec.ident).toSeq
  }

  private def assertSpec(partialSpec: ResolvedPartitionSpec): Unit = {
    val partitionSchema = table.asPartitionable.partitionSchema();
    if (partialSpec.ident.numFields > partitionSchema.length) {
      throw new IllegalArgumentException(
        s"Partial partition spec should be part of $partitionSchema fields, but got $partialSpec")
    }
    if (!partitionSchema.names.toSeq.startsWith(partialSpec.names)) {
      throw new IllegalArgumentException(
        s"Partial partition spec should be prefix of $partitionSchema fields, but got $partialSpec")
    }
  }

  override def output: Seq[Attribute] = Seq.empty
}
