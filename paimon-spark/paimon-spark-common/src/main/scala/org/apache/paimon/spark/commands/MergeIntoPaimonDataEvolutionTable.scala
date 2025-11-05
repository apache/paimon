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

import org.apache.paimon.format.blob.BlobFileFormat.isBlobFile
import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.catalyst.analysis.PaimonRelation
import org.apache.paimon.spark.catalyst.analysis.PaimonUpdateTable.toColumn
import org.apache.paimon.spark.leafnode.PaimonLeafRunnableCommand
import org.apache.paimon.spark.schema.PaimonMetadataColumn
import org.apache.paimon.spark.util.ScanPlanHelper.createNewScanPlan
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.CommitMessage
import org.apache.paimon.table.source.DataSplit

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.PaimonUtils._
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer.resolver
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftOuter}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.logical.MergeRows.Keep
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._
import scala.collection.Searching.{search, Found, InsertionPoint}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/** Command for Merge Into for Data Evolution paimon table. */
case class MergeIntoPaimonDataEvolutionTable(
    v2Table: SparkTable,
    targetTable: LogicalPlan,
    sourceTable: LogicalPlan,
    matchedCondition: Expression,
    matchedActions: Seq[MergeAction],
    notMatchedActions: Seq[MergeAction],
    notMatchedBySourceActions: Seq[MergeAction])
  extends PaimonLeafRunnableCommand
  with WithFileStoreTable {

  private lazy val partialColumnWriter = DataEvolutionPaimonWriter(table)
  private lazy val writer = PaimonSparkWriter(table)

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
  private val firstRowIds: Seq[Long] = table
    .store()
    .newScan()
    .withManifestEntryFilter(
      entry =>
        entry.file().firstRowId() != null && (!isBlobFile(
          entry
            .file()
            .fileName())))
    .plan()
    .files()
    .asScala
    .map(file => file.file().firstRowId().asInstanceOf[Long])
    .distinct
    .sorted
    .toSeq

  private val firstRowIdToBlobFirstRowIds = {
    val map = new mutable.HashMap[Long, List[Long]]()
    val files = table
      .store()
      .newScan()
      .withManifestEntryFilter(entry => isBlobFile(entry.file().fileName()))
      .plan()
      .files()
      .asScala
      .sortBy(f => f.file().firstRowId())

    for (file <- files) {
      val firstRowId = file.file().firstRowId().asInstanceOf[Long]
      val firstIdInNormalFile = floorBinarySearch(firstRowIds, firstRowId)
      map.update(
        firstIdInNormalFile,
        map.getOrElseUpdate(firstIdInNormalFile, List.empty[Long]) :+ firstRowId
      )
    }
    map
  }

  lazy val targetRelation: DataSourceV2Relation = PaimonRelation.getPaimonRelation(targetTable)
  lazy val sourceRelation: DataSourceV2Relation = PaimonRelation.getPaimonRelation(sourceTable)

  lazy val tableSchema: StructType = v2Table.schema

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // Avoid that more than one source rows match the same target row.
    val commitMessages = invokeMergeInto(sparkSession)
    writer.commit(commitMessages.toSeq)
    Seq.empty[Row]
  }

  private def invokeMergeInto(sparkSession: SparkSession): Seq[CommitMessage] = {
    // step 1: find the related data split, make it target file plan
    val dataSplits: Seq[DataSplit] = targetRelatedSplits(sparkSession)
    val touchedFileTargetRelation =
      createNewScanPlan(dataSplits.toSeq, targetRelation)

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
    val targetDss = createDataset(
      sparkSession,
      targetRelation
    )
    val sourceDss = createDataset(sparkSession, sourceRelation)

    val firstRowIdsTouched = mutable.Set.empty[Long]

    firstRowIdsTouched ++= findRelatedFirstRowIds(
      targetDss.alias("_left").join(sourceDss, toColumn(matchedCondition), "inner"),
      sparkSession,
      "_left." + ROW_ID_NAME)

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
      .toSeq
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

    val updateColumnsSorted = updateColumns.toSeq.sortBy(
      s => targetTable.output.map(x => x.toString()).indexOf(s.toString()))

    val assignments = redundantColumns.map(column => Assignment(column, column))
    val output = updateColumnsSorted ++ redundantColumns
    val realUpdateActions = matchedActions
      .map(s => s.asInstanceOf[UpdateAction])
      .map(
        update =>
          UpdateAction.apply(
            update.condition,
            update.assignments.filter(
              a =>
                updateColumnsSorted.contains(
                  a.key.asInstanceOf[AttributeReference])) ++ assignments))

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
          }) ++ Seq(Keep(TrueLiteral, output)),
      notMatchedInstructions = Nil,
      notMatchedBySourceInstructions = Seq(Keep(TrueLiteral, output)).toSeq,
      checkCardinality = false,
      output = output,
      child = joinPlan
    )

    val firstRowIdsFinal = firstRowIds
    val firstRowIdUdf = udf((rowId: Long) => floorBinarySearch(firstRowIdsFinal, rowId))
    val firstRowIdColumn = firstRowIdUdf(col(ROW_ID_NAME))
    val toWrite =
      createDataset(sparkSession, mergeRows).withColumn(FIRST_ROW_ID_NAME, firstRowIdColumn)
    assert(toWrite.schema.fields.length == updateColumnsSorted.size + 2)
    val sortedDs = toWrite
      .repartitionByRange(firstRowIdColumn)
      .sortWithinPartitions(FIRST_ROW_ID_NAME, ROW_ID_NAME)
    partialColumnWriter.writePartialFields(sortedDs, updateColumnsSorted.map(_.name))
  }

  private def insertActionInvoke(
      sparkSession: SparkSession,
      touchedFileTargetRelation: DataSourceV2Relation): Seq[CommitMessage] = {
    val mergeFields = extractFields(matchedCondition)
    val allReadFieldsOnTarget =
      mergeFields.filter(field => targetTable.output.exists(attr => attr.equals(field)))

    val targetReadPlan =
      touchedFileTargetRelation.copy(targetRelation.table, allReadFieldsOnTarget.toSeq)

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
            insertAction.assignments.map(
              a =>
                if (
                  !a.value.isInstanceOf[AttributeReference] || joinPlan.output.exists(
                    attr => attr.toString().equals(a.value.toString()))
                ) a.value
                else Literal(null))
          )
      }.toSeq,
      notMatchedBySourceInstructions = Nil,
      checkCardinality = false,
      output = targetTable.output,
      child = joinPlan
    )

    val toWrite = createDataset(sparkSession, mergeRows)
    writer.write(toWrite)
  }

  private def findRelatedFirstRowIds(
      dataset: Dataset[Row],
      sparkSession: SparkSession,
      identifier: String): Array[Long] = {
    import sparkSession.implicits._
    val firstRowIdsFinal = firstRowIds
    val firstRowIdToBlobFirstRowIdsFinal = firstRowIdToBlobFirstRowIds
    val firstRowIdUdf = udf((rowId: Long) => floorBinarySearch(firstRowIdsFinal, rowId))
    dataset
      .select(firstRowIdUdf(col(identifier)))
      .distinct()
      .as[Long]
      .flatMap(
        f => {
          if (firstRowIdToBlobFirstRowIdsFinal.contains(f)) {
            firstRowIdToBlobFirstRowIdsFinal(f)
          } else {
            Seq(f)
          }
        })
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
    fields.distinct.toSeq
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
    Seq(PaimonMetadataColumn.ROW_ID.toAttribute)

  def floorBinarySearch(sortedSeq: Seq[Long], value: Long): Long = {
    val indexed = sortedSeq.toIndexedSeq

    if (indexed.isEmpty) {
      throw new IllegalArgumentException("The input sorted sequence is empty.")
    }

    indexed.search(value) match {
      case Found(foundIndex) => indexed(foundIndex)
      case InsertionPoint(insertionIndex) => {
        if (insertionIndex == 0) {
          throw new IllegalArgumentException(
            s"Value $value is less than the first element in the sorted sequence.")
        } else {
          indexed(insertionIndex - 1)
        }
      }
    }
  }
}
