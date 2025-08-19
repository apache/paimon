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

import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.catalyst.analysis.PaimonRelation
import org.apache.paimon.spark.leafnode.PaimonLeafRunnableCommand
import org.apache.paimon.spark.schema.PaimonMetadataColumn
import org.apache.paimon.spark.schema.PaimonMetadataColumn.FILE_PATH_COLUMN
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.CommitMessage
import org.apache.paimon.table.source.DataSplit

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.PaimonUtils._
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer.resolver
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftOuter}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.logical.MergeRows.Keep
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.StructType

import scala.collection.{immutable, mutable, Seq}
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/** Command for Merge Into for Data Evolution paimom table. */
case class MergeIntoPaimonDataEvolutionTable(
    v2Table: SparkTable,
    targetTable: LogicalPlan,
    sourceTable: LogicalPlan,
    matchedCondition: Expression,
    matchedActions: Seq[MergeAction],
    notMatchedActions: Seq[MergeAction],
    notMatchedBySourceActions: Seq[MergeAction])
  extends PaimonLeafRunnableCommand
  with PaimonCommand {

  assert(
    notMatchedBySourceActions.isEmpty,
    "notMatchedBySourceActions is not supported in MergeIntoPaimonDataEvolutionTable.")
  assert(
    matchedActions.forall(x => x.isInstanceOf[UpdateAction]),
    "Only SET clause is supported in MergeIntoPaimonDataEvolutionTable for SQL: WHEN MATCHED.")
  assert(
    notMatchedActions.forall(x => x.isInstanceOf[InsertAction]),
    "Only INSERT clause is supported in MergeIntoPaimonDataEvolutionTable for SQL: WHEN NOT MATCHED."
  )

  import MergeIntoPaimonDataEvolutionTable._

  override val table: FileStoreTable = v2Table.getTable.asInstanceOf[FileStoreTable]

  lazy val targetRelation: DataSourceV2Relation = PaimonRelation.getPaimonRelation(targetTable)
  lazy val sourceRelation: DataSourceV2Relation = PaimonRelation.getPaimonRelation(sourceTable)

  lazy val tableSchema: StructType = v2Table.schema

  override def run(sparkSession: SparkSession): scala.collection.Seq[Row] = {
    // Avoid that more than one source rows match the same target row.
    val commitMessages = invokeMergeInto(sparkSession)
    dvSafeWriter.commit(commitMessages.toList)
    scala.collection.Seq.empty[Row]
  }

  private def invokeMergeInto(sparkSession: SparkSession): Seq[CommitMessage] = {
    // step 1: find the related data split, make it target file plan
    val dataSplits: Seq[DataSplit] = targetRelatedSplits(sparkSession)
    val touchedFileTargetRelation =
      createNewPlan(dataSplits.toList, targetRelation)

    // step 2: invoke update action
    val updateCommit =
      if (matchedActions.nonEmpty)
        updateActionInvoke(sparkSession, touchedFileTargetRelation)
      else Nil

    // step 3: invoke insert action
    val insertCommit =
      if (notMatchedActions.nonEmpty)
        insertActionInvoke(sparkSession, touchedFileTargetRelation)
      else Nil

    updateCommit ++ insertCommit
  }

  private def targetRelatedSplits(sparkSession: SparkSession): Seq[DataSplit] = {
    val mergeFields = extractFields(matchedCondition)
    val mergeFieldsOnTarget =
      mergeFields.filter(field => targetTable.output.exists(attr => attr.equals(field)))
    val mergeFieldsOnSource =
      mergeFields.filter(field => sourceTable.output.exists(attr => attr.equals(field)))

    val targetDss = createDataset(
      sparkSession,
      targetRelation.copy(
        targetRelation.table,
        mergeFieldsOnTarget.toList
      ))

    val sourceDss = createDataset(
      sparkSession,
      sourceRelation.copy(
        sourceRelation.table,
        mergeFieldsOnSource.toList
      ))

    val firstRowIdsTouched = mutable.Set.empty[Long]

    firstRowIdsTouched ++= findRelatedFirstRowIds(
      targetDss.alias("_left").join(sourceDss, toColumn(matchedCondition), "inner"),
      sparkSession,
      "_left." + FIRST_ROW_ID_NAME)

    table
      .newSnapshotReader()
      .withManifestEntryFilter(
        entry =>
          entry.file().firstRowId() != null && firstRowIdsTouched.contains(
            entry.file().firstRowId()))
      .read()
      .splits()
      .asScala
      .map(_.asInstanceOf[DataSplit])
  }

  private def updateActionInvoke(
      sparkSession: SparkSession,
      touchedFileTargetRelation: DataSourceV2Relation): Seq[CommitMessage] = {
    val mergeFields = extractFields(matchedCondition)
    val allFields = mutable.SortedSet.empty[AttributeReference](
      (o1, o2) => {
        o1.toString().compareTo(o2.toString())
      }) ++ mergeFields
    val updateColumns = mutable.Set[AttributeReference]()
    for (action <- matchedActions) {
      action match {
        case updateAction: UpdateAction =>
          for (assignment <- updateAction.assignments) {
            if (!assignment.key.equals(assignment.value)) {
              val key = assignment.key.asInstanceOf[AttributeReference]
              updateColumns ++= Seq(key)
            }
          }
      }
    }

    val assignments = redundantColumns.map(column => Assignment(column, column))
    val output = updateColumns.toSeq ++ redundantColumns
    val realUpdateActions = matchedActions
      .map(s => s.asInstanceOf[UpdateAction])
      .map(
        update =>
          UpdateAction.apply(
            update.condition,
            update.assignments.filter(
              a => updateColumns.contains(a.key.asInstanceOf[AttributeReference])) ++ assignments))

    for (action <- realUpdateActions) {
      allFields ++= action.references.flatMap(r => extractFields(r)).seq
    }

    val allReadFieldsOnTarget = allFields.filter(
      field =>
        targetTable.output.exists(
          attr => attr.toString().equals(field.toString()))) ++ redundantColumns
    val allReadFieldsOnSource = allFields.filter(
      field => sourceTable.output.exists(attr => attr.toString().equals(field.toString())))

    val targetReadPlan =
      touchedFileTargetRelation.copy(targetRelation.table, allReadFieldsOnTarget.toSeq)
    val targetTableProjExprs = targetReadPlan.output :+ Alias(TrueLiteral, ROW_FROM_TARGET)()
    val targetTableProj = Project(targetTableProjExprs, targetReadPlan)

    val sourceReadPlan = sourceRelation.copy(sourceRelation.table, allReadFieldsOnSource.toSeq)
    val sourceTableProjExprs = sourceReadPlan.output :+ Alias(TrueLiteral, ROW_FROM_SOURCE)()
    val sourceTableProj = Project(sourceTableProjExprs, sourceReadPlan)

    val joinPlan =
      Join(targetTableProj, sourceTableProj, LeftOuter, Some(matchedCondition), JoinHint.NONE)

    val rowFromSourceAttr = attribute(ROW_FROM_SOURCE, joinPlan)
    val rowFromTargetAttr = attribute(ROW_FROM_TARGET, joinPlan)

    val mergeRows = MergeRows(
      isSourceRowPresent = rowFromSourceAttr,
      isTargetRowPresent = rowFromTargetAttr,
      matchedInstructions = realUpdateActions
        .map(
          action => {
            Keep(action.condition.getOrElse(TrueLiteral), action.assignments.map(a => a.value))
          })
        .toList,
      notMatchedInstructions = Nil,
      notMatchedBySourceInstructions = immutable.Seq(Keep(TrueLiteral, output)),
      checkCardinality = false,
      output = output,
      child = joinPlan
    )

    val toWrite = createDataset(sparkSession, mergeRows)
    assert(toWrite.schema.fields.length == updateColumns.size + redundantColumns.size)
    val sortedDs = toWrite
      .repartitionByRange(toColumn(attribute(FIRST_ROW_ID_NAME, joinPlan)))
      .sortWithinPartitions(FIRST_ROW_ID_NAME, ROW_ID_NAME)
    val writer = dvSafeWriter.withDataEvolutionMergeWrite(updateColumns.map(_.name).toSeq)
    writer.write(sortedDs)
  }

  private def insertActionInvoke(
      sparkSession: SparkSession,
      touchedFileTargetRelation: DataSourceV2Relation): Seq[CommitMessage] = {
    val mergeFields = extractFields(matchedCondition)
    val allReadFieldsOnTarget =
      mergeFields.filter(field => targetTable.output.exists(attr => attr.equals(field)))

    val targetReadPlan =
      touchedFileTargetRelation.copy(targetRelation.table, allReadFieldsOnTarget.toList)

    val joinPlan =
      Join(sourceRelation, targetReadPlan, LeftAnti, Some(matchedCondition), JoinHint.NONE)

    // merge rows as there are multiple not matched actions
    val mergeRows = MergeRows(
      isSourceRowPresent = TrueLiteral,
      isTargetRowPresent = FalseLiteral,
      matchedInstructions = Nil,
      notMatchedInstructions = notMatchedActions.map {
        case insertAction: InsertAction =>
          Keep(
            insertAction.condition.getOrElse(TrueLiteral),
            insertAction.assignments.map(a => a.value))
      }.toList,
      notMatchedBySourceInstructions = Nil,
      checkCardinality = false,
      output = targetTable.output,
      child = joinPlan
    )

    val toWrite = createDataset(sparkSession, mergeRows)
    val writer = dvSafeWriter.disableDataEvolutionMergeWrite()
    writer.write(toWrite)
  }

  private def findRelatedFirstRowIds(
      dataset: Dataset[Row],
      sparkSession: SparkSession,
      identifier: String = FILE_PATH_COLUMN): Array[Long] = {
    import sparkSession.implicits._
    dataset
      .select(identifier)
      .distinct()
      .as[Long]
      .collect()
  }

  private def extractFields(expression: Expression): Seq[AttributeReference] = {
    val fields = new ListBuffer[AttributeReference]()

    def traverse(expr: Expression): Unit = {
      expr match {
        case attr: AttributeReference =>
          fields += attr
        case other =>
          other.children.foreach(traverse)
      }
    }

    traverse(expression)
    fields.distinct
  }

  private def attribute(name: String, plan: LogicalPlan) =
    plan.output.find(attr => resolver(name, attr.name)).get
}

object MergeIntoPaimonDataEvolutionTable {
  final private val ROW_FROM_SOURCE = "__row_from_source"
  final private val ROW_FROM_TARGET = "__row_from_target"
  final private val ROW_ID_NAME = "_ROW_ID"
  final private val FIRST_ROW_ID_NAME = "_FIRST_ROW_ID";
  final private val redundantColumns =
    Seq(PaimonMetadataColumn.FIRST_ROW_ID.toAttribute, PaimonMetadataColumn.ROW_ID.toAttribute)
}
