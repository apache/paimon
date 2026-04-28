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

import org.apache.paimon.spark.SparkTable
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, CurrentRow, Descending, EqualNullSafe, EqualTo, LessThanOrEqual, Literal, RowFrame, RowNumber, SortOrder, SpecifiedWindowFrame, UnboundedPreceding, WindowExpression, WindowSpecDefinition}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.paimon.shims.SparkShimLoader

import scala.collection.JavaConverters._

/** Rewrite upsert table to merge into. */
case class RewriteUpsertTable(spark: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsDown {
    case p @ PaimonV2WriteCommand(table) =>
      val (usingUpsert, upsertKey, sequenceField) = usingUpsertTable(table)
      if (!usingUpsert) {
        return p
      }

      p match {
        case AppendData(target, source, _, _, _, _) =>
          val deduplicatedSource = if (sequenceField.nonEmpty) {
            deduplicateBySequenceField(source, upsertKey, sequenceField)
          } else {
            source
          }

          rewriteToMergeInto(target, deduplicatedSource, upsertKey, sequenceField)
        case _ => p
      }
  }

  private def usingUpsertTable(table: DataSourceV2Relation): (Boolean, Seq[String], Seq[String]) = {
    table.table match {
      case SparkTable(fileStoreTable: FileStoreTable) =>
        val coreOptions = fileStoreTable.coreOptions()
        val upsertKey = coreOptions.upsertKey().asScala.toSeq
        val sequenceField = coreOptions.sequenceField().asScala.toSeq
        if (fileStoreTable.primaryKeys().isEmpty && upsertKey.nonEmpty) {
          (true, upsertKey, sequenceField)
        } else {
          (false, Seq.empty, Seq.empty)
        }
      case _ => (false, Seq.empty, Seq.empty)
    }
  }

  private def deduplicateBySequenceField(
      source: LogicalPlan,
      upsertKey: Seq[String],
      sequenceField: Seq[String]): LogicalPlan = {
    val winSpec = WindowSpecDefinition(
      cols(source.output, upsertKey),
      cols(source.output, sequenceField).map(SortOrder(_, Descending)),
      SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow)
    )
    val rnAlias = Alias(WindowExpression(RowNumber(), winSpec), "__rn__")()
    val withRN = Project(UnresolvedStar(None) :: rnAlias :: Nil, source)
    val filtered = Filter(EqualTo(UnresolvedAttribute("__rn__"), Literal(1)), withRN)
    Project(source.output, filtered)
  }

  private def rewriteToMergeInto(
      target: LogicalPlan,
      source: LogicalPlan,
      upsertKey: Seq[String],
      sequenceField: Seq[String]
  ): MergeIntoTable = {
    val mergeCondition = upsertKey
      .map(k => EqualNullSafe(col(target.output, k), col(source.output, k)))
      .reduce(And)

    val updateCondiction = if (sequenceField.nonEmpty) {
      Option.apply(
        sequenceField
          .map(s => LessThanOrEqual(col(target.output, s), col(source.output, s)))
          .reduce(And))
    } else {
      Option.empty
    }

    val assignments: Seq[Assignment] =
      target.output.zip(source.output).map(a => Assignment(a._1, a._2))

    val mergeActions = Seq(UpdateAction(updateCondiction, assignments))
    val notMatchedActions = Seq(InsertAction(None, assignments))

    SparkShimLoader.shim.createMergeIntoTable(
      target,
      source,
      mergeCondition,
      mergeActions,
      notMatchedActions,
      Seq.empty,
      withSchemaEvolution = false)
  }

  private def cols(input: Seq[Attribute], colsNames: Seq[String]): Seq[Attribute] = {
    colsNames.map(c => col(input, c))
  }

  private def col(input: Seq[Attribute], colsName: String): Attribute = {
    input.find(_.name == colsName).get
  }
}
