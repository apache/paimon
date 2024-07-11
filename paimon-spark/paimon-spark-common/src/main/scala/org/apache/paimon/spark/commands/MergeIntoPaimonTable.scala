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
import org.apache.paimon.spark.{InsertInto, SparkTable}
import org.apache.paimon.spark.leafnode.PaimonLeafRunnableCommand
import org.apache.paimon.spark.schema.SparkSystemColumns
import org.apache.paimon.spark.util.EncoderUtils
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.types.RowKind

import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.sql.PaimonUtils._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, BasePredicate, Expression, Literal, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.expressions.codegen.GeneratePredicate
import org.apache.spark.sql.catalyst.plans.logical.{DeleteAction, Filter, InsertAction, LogicalPlan, MergeAction, UpdateAction}
import org.apache.spark.sql.functions.{col, lit, monotonically_increasing_id, sum}
import org.apache.spark.sql.types.{ByteType, StructField, StructType}

/** Command for Merge Into. */
case class MergeIntoPaimonTable(
    v2Table: SparkTable,
    targetTable: LogicalPlan,
    sourceTable: LogicalPlan,
    mergeCondition: Expression,
    matchedActions: Seq[MergeAction],
    notMatchedActions: Seq[MergeAction],
    notMatchedBySourceActions: Seq[MergeAction])
  extends PaimonLeafRunnableCommand
  with PaimonCommand {

  import MergeIntoPaimonTable._

  override val table: FileStoreTable = v2Table.getTable.asInstanceOf[FileStoreTable]

  lazy val tableSchema: StructType = v2Table.schema

  lazy val filteredTargetPlan: LogicalPlan = {
    val filtersOnlyTarget = getExpressionOnlyRelated(mergeCondition, targetTable)
    filtersOnlyTarget
      .map(Filter.apply(_, targetTable))
      .getOrElse(targetTable)
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {

    // Avoid that more than one source rows match the same target row.
    checkMatchRationality(sparkSession)

    val changed = constructChangedRows(sparkSession)

    WriteIntoPaimonTable(
      table,
      InsertInto,
      changed,
      new Options()
    ).run(sparkSession)

    Seq.empty[Row]
  }

  /** Get a Dataset where each of Row has an additional column called _row_kind_. */
  private def constructChangedRows(sparkSession: SparkSession): Dataset[Row] = {
    val targetDS = createDataset(sparkSession, filteredTargetPlan)
      .withColumn(TARGET_ROW_COL, lit(true))

    val sourceDS = createDataset(sparkSession, sourceTable)
      .withColumn(SOURCE_ROW_COL, lit(true))

    val joinedDS = sourceDS.join(targetDS, new Column(mergeCondition), "fullOuter")
    val joinedPlan = joinedDS.queryExecution.analyzed

    def resolveOnJoinedPlan(exprs: Seq[Expression]): Seq[Expression] = {
      resolveExpressions(sparkSession)(exprs, joinedPlan)
    }

    val targetOutput = filteredTargetPlan.output
    val targetRowNotMatched = resolveOnJoinedPlan(Seq(col(SOURCE_ROW_COL).isNull.expr)).head
    val sourceRowNotMatched = resolveOnJoinedPlan(Seq(col(TARGET_ROW_COL).isNull.expr)).head
    val matchedExprs = matchedActions.map(_.condition.getOrElse(TrueLiteral))
    val notMatchedExprs = notMatchedActions.map(_.condition.getOrElse(TrueLiteral))
    val notMatchedBySourceExprs = notMatchedBySourceActions.map(_.condition.getOrElse(TrueLiteral))
    val matchedOutputs = matchedActions.map {
      case UpdateAction(_, assignments) =>
        assignments.map(_.value) :+ Literal(RowKind.UPDATE_AFTER.toByteValue)
      case DeleteAction(_) =>
        targetOutput :+ Literal(RowKind.DELETE.toByteValue)
      case _ =>
        throw new RuntimeException("should not be here.")
    }
    val notMatchedBySourceOutputs = notMatchedBySourceActions.map {
      case UpdateAction(_, assignments) =>
        assignments.map(_.value) :+ Literal(RowKind.UPDATE_AFTER.toByteValue)
      case DeleteAction(_) =>
        targetOutput :+ Literal(RowKind.DELETE.toByteValue)
      case _ =>
        throw new RuntimeException("should not be here.")
    }
    val notMatchedOutputs = notMatchedActions.map {
      case InsertAction(_, assignments) =>
        assignments.map(_.value) :+ Literal(RowKind.INSERT.toByteValue)
      case _ =>
        throw new RuntimeException("should not be here.")
    }
    val noopOutput = targetOutput :+ Alias(Literal(NOOP_ROW_KIND_VALUE), ROW_KIND_COL)()
    val outputSchema = StructType(tableSchema.fields :+ StructField(ROW_KIND_COL, ByteType))

    val joinedRowEncoder = EncoderUtils.encode(joinedPlan.schema)
    val outputEncoder = EncoderUtils.encode(outputSchema).resolveAndBind()

    val processor = MergeIntoProcessor(
      joinedPlan.output,
      targetRowNotMatched,
      sourceRowNotMatched,
      matchedExprs,
      matchedOutputs,
      notMatchedBySourceExprs,
      notMatchedBySourceOutputs,
      notMatchedExprs,
      notMatchedOutputs,
      noopOutput,
      joinedRowEncoder,
      outputEncoder
    )
    joinedDS.mapPartitions(processor.processPartition)(outputEncoder).toDF()
  }

  private def checkMatchRationality(sparkSession: SparkSession): Unit = {
    if (matchedActions.nonEmpty) {
      val targetDS = createDataset(sparkSession, filteredTargetPlan)
        .withColumn(ROW_ID_COL, monotonically_increasing_id())
      val sourceDS = createDataset(sparkSession, sourceTable)
      val count = sourceDS
        .join(targetDS, new Column(mergeCondition), "inner")
        .select(col(ROW_ID_COL), lit(1).as("one"))
        .groupBy(ROW_ID_COL)
        .agg(sum("one").as("count"))
        .filter("count > 1")
        .count()
      if (count > 0) {
        throw new RuntimeException(
          "Can't execute this MergeInto when there are some target rows that each of them match more then one source rows. It may lead to an unexpected result.")
      }
    }
  }
}

object MergeIntoPaimonTable {
  val ROW_ID_COL = "_row_id_"
  val SOURCE_ROW_COL = "_source_row_"
  val TARGET_ROW_COL = "_target_row_"
  // +I, +U, -U, -D
  val ROW_KIND_COL: String = SparkSystemColumns.ROW_KIND_COL
  val NOOP_ROW_KIND_VALUE: Byte = "-1".toByte

  case class MergeIntoProcessor(
      joinedAttributes: Seq[Attribute],
      targetRowHasNoMatch: Expression,
      sourceRowHasNoMatch: Expression,
      matchedConditions: Seq[Expression],
      matchedOutputs: Seq[Seq[Expression]],
      notMatchedBySourceConditions: Seq[Expression],
      notMatchedBySourceOutputs: Seq[Seq[Expression]],
      notMatchedConditions: Seq[Expression],
      notMatchedOutputs: Seq[Seq[Expression]],
      noopCopyOutput: Seq[Expression],
      joinedRowEncoder: ExpressionEncoder[Row],
      outputRowEncoder: ExpressionEncoder[Row]
  ) extends Serializable {

    private def generateProjection(exprs: Seq[Expression]): UnsafeProjection = {
      UnsafeProjection.create(exprs, joinedAttributes)
    }

    private def generatePredicate(expr: Expression): BasePredicate = {
      GeneratePredicate.generate(expr, joinedAttributes)
    }

    private def unusedRow(row: InternalRow): Boolean = {
      row.getByte(outputRowEncoder.schema.fieldIndex(ROW_KIND_COL)) == NOOP_ROW_KIND_VALUE
    }

    def processPartition(rowIterator: Iterator[Row]): Iterator[Row] = {
      val targetRowHasNoMatchPred = generatePredicate(targetRowHasNoMatch)
      val sourceRowHasNoMatchPred = generatePredicate(sourceRowHasNoMatch)
      val matchedPreds = matchedConditions.map(generatePredicate)
      val matchedProjs = matchedOutputs.map(generateProjection)
      val notMatchedBySourcePreds = notMatchedBySourceConditions.map(generatePredicate)
      val notMatchedBySourceProjs = notMatchedBySourceOutputs.map(generateProjection)
      val notMatchedPreds = notMatchedConditions.map(generatePredicate)
      val notMatchedProjs = notMatchedOutputs.map(generateProjection)
      val noopCopyProj = generateProjection(noopCopyOutput)
      val outputProj = UnsafeProjection.create(outputRowEncoder.schema)

      def processRow(inputRow: InternalRow): InternalRow = {
        if (targetRowHasNoMatchPred.eval(inputRow)) {
          val pair = notMatchedBySourcePreds.zip(notMatchedBySourceProjs).find {
            case (predicate, _) => predicate.eval(inputRow)
          }

          pair match {
            case Some((_, projections)) =>
              projections.apply(inputRow)
            case None => noopCopyProj.apply(inputRow)
          }
        } else if (sourceRowHasNoMatchPred.eval(inputRow)) {
          val pair = notMatchedPreds.zip(notMatchedProjs).find {
            case (predicate, _) => predicate.eval(inputRow)
          }

          pair match {
            case Some((_, projections)) =>
              projections.apply(inputRow)
            case None => noopCopyProj.apply(inputRow)
          }
        } else {
          val pair =
            matchedPreds.zip(matchedProjs).find { case (predicate, _) => predicate.eval(inputRow) }

          pair match {
            case Some((_, projections)) =>
              projections.apply(inputRow)
            case None => noopCopyProj.apply(inputRow)
          }
        }
      }

      val toRow = joinedRowEncoder.createSerializer()
      val fromRow = outputRowEncoder.createDeserializer()
      rowIterator
        .map(toRow)
        .map(processRow)
        .filterNot(unusedRow)
        .map(notDeletedInternalRow => fromRow(outputProj(notDeletedInternalRow)))
    }
  }
}
