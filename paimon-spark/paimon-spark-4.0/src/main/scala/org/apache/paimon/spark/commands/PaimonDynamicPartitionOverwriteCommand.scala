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

import org.apache.paimon.options.Options
import org.apache.paimon.spark.DynamicOverWrite
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.PaimonUtils.{createDataset, createNewDataFrame}
import org.apache.spark.sql.catalyst.analysis.NamedRelation
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan, V2WriteCommand}
import org.apache.spark.sql.connector.catalog.TableWritePrivilege
import org.apache.spark.sql.execution.command.RunnableCommand

import scala.collection.convert.ImplicitConversions._

/**
 * Spark 4.0/4.1 flavour of the class with the same FQCN in `paimon-spark-common`, which is compiled
 * against the profile baseline (Spark 4.2).
 *
 * The body is identical; only the compilation target differs. Spark 4.2 mixes the new
 * `WriteWithSchemaEvolution` trait into `V2WriteCommand`, so a 4.2-compiled subclass calls that
 * trait's `$init$` from its constructor. The trait does not exist before 4.2, so on a 4.0/4.1
 * runtime every instantiation dies with `NoClassDefFoundError` — `PaimonAnalysis` builds one for
 * any dynamic partition overwrite, making it unreachable to write such a table at all.
 *
 * This cannot be fixed with a `SparkShim` method: the incompatibility is in the supertype, not in a
 * call. Dropping `extends V2WriteCommand` is not an option either — Spark matches on that type to
 * transform the plan, which is the whole reason this class exists.
 *
 * A `RunnableCommand` that will execute dynamic partition overwrite using [[WriteIntoPaimonTable]].
 *
 * This is a workaround of Spark not supporting V1 fallback for dynamic partition overwrite. Note
 * the following details:
 *   - Extends [[V2WriteCommand]] so that Spark can transform this plan.
 *   - Exposes the query as a child so that the Spark optimizer can optimize it.
 */
case class PaimonDynamicPartitionOverwriteCommand(
    table: NamedRelation,
    fileStoreTable: FileStoreTable,
    query: LogicalPlan,
    writeOptions: Map[String, String],
    isByName: Boolean)
  extends RunnableCommand
  with V2WriteCommand {

  override def child: LogicalPlan = query

  override def withNewQuery(newQuery: LogicalPlan): PaimonDynamicPartitionOverwriteCommand = {
    copy(query = newQuery)
  }

  override def withNewTable(newTable: NamedRelation): PaimonDynamicPartitionOverwriteCommand = {
    copy(table = newTable)
  }

  override protected def withNewChildInternal(
      newChild: LogicalPlan): PaimonDynamicPartitionOverwriteCommand = copy(query = newChild)

  // Declared without `override` so this source stays identical to the `paimon-spark-common` copy,
  // which must also compile on 4.2 where `WriteWithSchemaEvolution` declares these members.
  // Dynamic partition overwrite never evolves the target schema.
  def withSchemaEvolution: Boolean = false

  // Dynamic partition overwrite replaces whole partitions, so it deletes as well as inserts —
  // the same privilege set Spark's own `OverwritePartitionsDynamic` requests.
  def writePrivileges: Set[TableWritePrivilege] =
    Set(TableWritePrivilege.INSERT, TableWritePrivilege.DELETE)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    WriteIntoPaimonTable(
      fileStoreTable,
      DynamicOverWrite,
      createNewDataFrame(createDataset(sparkSession, query)),
      Options.fromMap(fileStoreTable.options() ++ writeOptions)
    ).run(sparkSession)
  }

  // Do not annotate with override here to maintain compatibility with Spark 3.3-.
  def storeAnalyzedQuery(): Command = copy(query = query)
}
