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

package org.apache.paimon.spark.catalyst.plans.logical

import org.apache.paimon.spark.leafnode.{PaimonBinaryCommand, PaimonUnaryCommand}

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, ShowViews, Statistics}
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier}

case class CreatePaimonView(
    child: LogicalPlan,
    queryText: String,
    query: LogicalPlan,
    columnAliases: Seq[String],
    columnComments: Seq[Option[String]],
    queryColumnNames: Seq[String] = Seq.empty,
    comment: Option[String],
    properties: Map[String, String],
    allowExisting: Boolean,
    replace: Boolean)
  extends PaimonBinaryCommand {

  override def left: LogicalPlan = child

  override def right: LogicalPlan = query

  override protected def withNewChildrenInternal(
      newLeft: LogicalPlan,
      newRight: LogicalPlan): LogicalPlan =
    copy(child = newLeft, query = newRight)
}

case class DropPaimonView(child: LogicalPlan, ifExists: Boolean) extends PaimonUnaryCommand {

  override protected def withNewChildInternal(newChild: LogicalPlan): DropPaimonView =
    copy(child = newChild)
}

case class ShowPaimonViews(
    namespace: LogicalPlan,
    pattern: Option[String],
    override val output: Seq[Attribute] = ShowViews.getOutputAttrs)
  extends PaimonUnaryCommand {

  override def child: LogicalPlan = namespace

  override protected def withNewChildInternal(newChild: LogicalPlan): ShowPaimonViews =
    copy(namespace = newChild)
}

/** Copy from spark 3.4+ */
case class ResolvedIdentifier(catalog: CatalogPlugin, identifier: Identifier) extends LeafNode {

  override def output: Seq[Attribute] = Nil

  override def stats: Statistics = Statistics.DUMMY
}
