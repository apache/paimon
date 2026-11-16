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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.paimon.spark.PaimonTableAsSelectWrite

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.{Identifier, StagingTableCatalog, Table, TableCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** Keeps Spark's table creation and rollback semantics while preparing the write distribution. */
private[v2] trait PaimonTableAsSelectWriteExec extends TableWriteExecHelper {
  def plan: LogicalPlan

  override protected def writeToTable(
      catalog: TableCatalog,
      table: Table,
      options: CaseInsensitiveStringMap,
      ident: Identifier): Seq[InternalRow] = {
    PaimonTableAsSelectWrite
      .writeIfSupported(session, catalog, table, ident, plan, options)
      .getOrElse(super.writeToTable(catalog, table, options, ident))
  }
}

object PaimonTableAsSelectWriteExec {

  def prepare(exec: SparkPlan): SparkPlan = exec match {
    case create: CreateTableAsSelectExec => prepare(create)
    case create: AtomicCreateTableAsSelectExec => prepare(create)
    case replace: AtomicReplaceTableAsSelectExec => prepare(replace)
    case replace: ReplaceTableAsSelectExec => prepare(replace)
    case other => other
  }

  def prepare(exec: CreateTableAsSelectExec): CreateTableAsSelectExec = {
    new PaimonCreateTableAsSelectExec(
      exec.catalog,
      exec.ident,
      exec.partitioning,
      exec.plan,
      exec.query,
      exec.properties,
      exec.writeOptions,
      exec.ifNotExists)
  }

  def prepare(exec: AtomicCreateTableAsSelectExec): AtomicCreateTableAsSelectExec = {
    new PaimonAtomicCreateTableAsSelectExec(
      exec.catalog,
      exec.ident,
      exec.partitioning,
      exec.plan,
      exec.query,
      exec.properties,
      exec.writeOptions,
      exec.ifNotExists)
  }

  def prepare(exec: AtomicReplaceTableAsSelectExec): AtomicReplaceTableAsSelectExec = {
    new PaimonAtomicReplaceTableAsSelectExec(
      exec.catalog,
      exec.ident,
      exec.partitioning,
      exec.plan,
      exec.query,
      exec.properties,
      exec.writeOptions,
      exec.orCreate,
      exec.invalidateCache)
  }

  def prepare(exec: ReplaceTableAsSelectExec): ReplaceTableAsSelectExec = {
    new PaimonReplaceTableAsSelectExec(
      exec.catalog,
      exec.ident,
      exec.partitioning,
      exec.plan,
      exec.query,
      exec.properties,
      exec.writeOptions,
      exec.orCreate,
      exec.invalidateCache)
  }
}

// Match the native constructor so Spark's reflective TreeNode.clone also retains this wrapper.
private[v2] class PaimonCreateTableAsSelectExec(
    catalog: TableCatalog,
    ident: Identifier,
    partitioning: Seq[Transform],
    plan: LogicalPlan,
    query: SparkPlan,
    properties: Map[String, String],
    writeOptions: CaseInsensitiveStringMap,
    ifNotExists: Boolean)
  extends CreateTableAsSelectExec(
    catalog,
    ident,
    partitioning,
    plan,
    query,
    properties,
    writeOptions,
    ifNotExists)
  with PaimonTableAsSelectWriteExec {

  override protected def withNewChildInternal(newChild: SparkPlan): CreateTableAsSelectExec = {
    PaimonTableAsSelectWriteExec.prepare(copy(query = newChild))
  }
}

// Match the native constructor so Spark's reflective TreeNode.clone also retains this wrapper.
private[v2] class PaimonAtomicCreateTableAsSelectExec(
    catalog: StagingTableCatalog,
    ident: Identifier,
    partitioning: Seq[Transform],
    plan: LogicalPlan,
    query: SparkPlan,
    properties: Map[String, String],
    writeOptions: CaseInsensitiveStringMap,
    ifNotExists: Boolean)
  extends AtomicCreateTableAsSelectExec(
    catalog,
    ident,
    partitioning,
    plan,
    query,
    properties,
    writeOptions,
    ifNotExists)
  with PaimonTableAsSelectWriteExec {

  override protected def withNewChildInternal(
      newChild: SparkPlan): AtomicCreateTableAsSelectExec = {
    PaimonTableAsSelectWriteExec.prepare(copy(query = newChild))
  }
}

// Match the native constructor so Spark's reflective TreeNode.clone also retains this wrapper.
private[v2] class PaimonAtomicReplaceTableAsSelectExec(
    catalog: StagingTableCatalog,
    ident: Identifier,
    partitioning: Seq[Transform],
    plan: LogicalPlan,
    query: SparkPlan,
    properties: Map[String, String],
    writeOptions: CaseInsensitiveStringMap,
    orCreate: Boolean,
    invalidateCache: (TableCatalog, Table, Identifier) => Unit)
  extends AtomicReplaceTableAsSelectExec(
    catalog,
    ident,
    partitioning,
    plan,
    query,
    properties,
    writeOptions,
    orCreate,
    invalidateCache)
  with PaimonTableAsSelectWriteExec {

  override protected def withNewChildInternal(
      newChild: SparkPlan): AtomicReplaceTableAsSelectExec = {
    PaimonTableAsSelectWriteExec.prepare(copy(query = newChild))
  }
}

// Match the native constructor so Spark's reflective TreeNode.clone also retains this wrapper.
private[v2] class PaimonReplaceTableAsSelectExec(
    catalog: TableCatalog,
    ident: Identifier,
    partitioning: Seq[Transform],
    plan: LogicalPlan,
    query: SparkPlan,
    properties: Map[String, String],
    writeOptions: CaseInsensitiveStringMap,
    orCreate: Boolean,
    invalidateCache: (TableCatalog, Table, Identifier) => Unit)
  extends ReplaceTableAsSelectExec(
    catalog,
    ident,
    partitioning,
    plan,
    query,
    properties,
    writeOptions,
    orCreate,
    invalidateCache)
  with PaimonTableAsSelectWriteExec {

  override protected def withNewChildInternal(newChild: SparkPlan): ReplaceTableAsSelectExec = {
    PaimonTableAsSelectWriteExec.prepare(copy(query = newChild))
  }
}
