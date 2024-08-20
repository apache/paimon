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
import org.apache.paimon.spark.schema.{PaimonMetadataColumn, SparkSystemColumns}
import org.apache.paimon.spark.schema.PaimonMetadataColumn.{FILE_PATH, FILE_PATH_COLUMN, ROW_INDEX, ROW_INDEX_COLUMN}
import org.apache.paimon.spark.util.{EncoderUtils, SparkRowUtils}
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.CommitMessage
import org.apache.paimon.types.RowKind

import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.sql.PaimonUtils._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, BasePredicate, EqualTo, Expression, Literal, Or, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.expressions.codegen.GeneratePredicate
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.functions.{col, lit, monotonically_increasing_id, sum}
import org.apache.spark.sql.types.{ByteType, StructField, StructType}

import scala.collection.mutable

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

  lazy val relation: DataSourceV2Relation = PaimonRelation.getPaimonRelation(targetTable)

  lazy val tableSchema: StructType = v2Table.schema

  private lazy val writer = PaimonSparkWriter(table)

  private lazy val (targetOnlyCondition, filteredTargetPlan): (Option[Expression], LogicalPlan) = {
    val filtersOnlyTarget = getExpressionOnlyRelated(mergeCondition, targetTable)
    (
      filtersOnlyTarget,
      filtersOnlyTarget
        .map(Filter.apply(_, targetTable))
        .getOrElse(targetTable))
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // Avoid that more than one source rows match the same target row.
    checkMatchRationality(sparkSession)
    val commitMessages = if (withPrimaryKeys) {
      performMergeForPkTable(sparkSession)
    } else {
      performMergeForNonPkTable(sparkSession)
    }
    writer.commit(commitMessages)
    Seq.empty[Row]
  }

  private def performMergeForPkTable(sparkSession: SparkSession): Seq[CommitMessage] = {
    writer.write(
      constructChangedRows(
        sparkSession,
        createDataset(sparkSession, filteredTargetPlan),
        remainDeletedRow = true))
  }

  private def performMergeForNonPkTable(sparkSession: SparkSession): Seq[CommitMessage] = {
    val targetDS = createDataset(sparkSession, filteredTargetPlan)
    val sourceDS = createDataset(sparkSession, sourceTable)

    // Step1: get the candidate data splits which are filtered by Paimon Predicate.
    val candidateDataSplits =
      findCandidateDataSplits(targetOnlyCondition.getOrElse(TrueLiteral), relation.output)
    val dataFilePathToMeta = candidateFileMap(candidateDataSplits)

    if (deletionVectorsEnabled) {
      // Step2: generate dataset that should contains ROW_KIND, FILE_PATH, ROW_INDEX columns
      val metadataCols = Seq(FILE_PATH, ROW_INDEX)
      val filteredRelation = createDataset(
        sparkSession,
        createNewScanPlan(
          candidateDataSplits,
          targetOnlyCondition.getOrElse(TrueLiteral),
          relation,
          metadataCols))
      val ds = constructChangedRows(
        sparkSession,
        filteredRelation,
        remainDeletedRow = true,
        metadataCols = metadataCols)

      ds.cache()
      try {
        val rowKindAttribute = ds.queryExecution.analyzed.output
          .find(attr => sparkSession.sessionState.conf.resolver(attr.name, ROW_KIND_COL))
          .getOrElse(throw new RuntimeException("Can not find _row_kind_ column."))

        // Step3: filter rows that should be marked as DELETED in Deletion Vector mode.
        val toDeleteRowsFilter = Or(
          EqualTo(rowKindAttribute, Literal(RowKind.DELETE.toByteValue)),
          EqualTo(rowKindAttribute, Literal(RowKind.UPDATE_AFTER.toByteValue)))
        val dvDS = ds.where(new Column(toDeleteRowsFilter))
        val deletionVectors = collectDeletionVectors(dataFilePathToMeta, dvDS, sparkSession)
        val indexCommitMsg = writer.persistDeletionVectors(deletionVectors)

        // Step4: filter rows that should be written as the inserted/updated data.
        val toWriteRowsFilter = Or(
          EqualTo(rowKindAttribute, Literal(RowKind.INSERT.toByteValue)),
          EqualTo(rowKindAttribute, Literal(RowKind.UPDATE_AFTER.toByteValue)))
        val toWriteDS = ds
          .filter(new Column(toWriteRowsFilter))
          .drop(FILE_PATH_COLUMN, ROW_INDEX_COLUMN)
        val addCommitMessage = writer.write(toWriteDS)

        // Step5: commit index and data commit messages
        addCommitMessage ++ indexCommitMsg
      } finally {
        ds.unpersist()
      }
    } else {
      val touchedFilePathsSet = mutable.Set.empty[String]
      def hasUpdate(actions: Seq[MergeAction]): Boolean = {
        actions.exists {
          case _: UpdateAction | _: DeleteAction => true
          case _ => false
        }
      }
      if (hasUpdate(matchedActions)) {
        touchedFilePathsSet ++= findTouchedFiles(
          targetDS.join(sourceDS, new Column(mergeCondition), "inner"),
          sparkSession)
      }
      if (hasUpdate(notMatchedBySourceActions)) {
        touchedFilePathsSet ++= findTouchedFiles(
          targetDS.join(sourceDS, new Column(mergeCondition), "left_anti"),
          sparkSession)
      }

      val targetFilePaths: Array[String] = findTouchedFiles(targetDS, sparkSession)
      val touchedFilePaths: Array[String] = touchedFilePathsSet.toArray
      val unTouchedFilePaths = targetFilePaths.filterNot(touchedFilePaths.contains)

      val (touchedFiles, touchedFileRelation) =
        createNewRelation(touchedFilePaths, dataFilePathToMeta, relation)
      val (_, unTouchedFileRelation) =
        createNewRelation(unTouchedFilePaths, dataFilePathToMeta, relation)

      // Add FILE_TOUCHED_COL to mark the row as coming from the touched file, if the row has not been
      // modified and was from touched file, it should be kept too.
      val targetDSWithFileTouchedCol = createDataset(sparkSession, touchedFileRelation)
        .withColumn(FILE_TOUCHED_COL, lit(true))
        .union(createDataset(sparkSession, unTouchedFileRelation)
          .withColumn(FILE_TOUCHED_COL, lit(false)))

      val toWriteDS =
        constructChangedRows(sparkSession, targetDSWithFileTouchedCol).drop(ROW_KIND_COL)
      val addCommitMessage = writer.write(toWriteDS)
      val deletedCommitMessage = buildDeletedCommitMessage(touchedFiles)

      addCommitMessage ++ deletedCommitMessage
    }
  }

  /** Get a Dataset where each of Row has an additional column called _row_kind_. */
  private def constructChangedRows(
      sparkSession: SparkSession,
      targetDataset: Dataset[Row],
      remainDeletedRow: Boolean = false,
      deletionVectorEnabled: Boolean = false,
      metadataCols: Seq[PaimonMetadataColumn] = Seq.empty): Dataset[Row] = {
    val targetDS = targetDataset
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
    val noopOutput = targetOutput :+ Alias(Literal(NOOP_ROW_KIND_VALUE), ROW_KIND_COL)()
    val keepOutput = targetOutput :+ Alias(Literal(RowKind.INSERT.toByteValue), ROW_KIND_COL)()

    val resolver = sparkSession.sessionState.conf.resolver
    val metadataAttributes = metadataCols.flatMap {
      metadataCol => joinedPlan.output.find(attr => resolver(metadataCol.name, attr.name))
    }
    def processMergeActions(actions: Seq[MergeAction]): Seq[Seq[Expression]] = {
      val columnExprs = actions.map {
        case UpdateAction(_, assignments) =>
          assignments.map(_.value) :+ Literal(RowKind.UPDATE_AFTER.toByteValue)
        case DeleteAction(_) =>
          if (remainDeletedRow || deletionVectorEnabled) {
            targetOutput :+ Literal(RowKind.DELETE.toByteValue)
          } else {
            // If RowKind = NOOP_ROW_KIND_VALUE, then these rows will be dropped in MergeIntoProcessor.processPartition by default.
            // If these rows still need to be remained, set MergeIntoProcessor.remainNoopRow true.
            noopOutput
          }
        case InsertAction(_, assignments) =>
          assignments.map(_.value) :+ Literal(RowKind.INSERT.toByteValue)
      }
      columnExprs.map(exprs => exprs ++ metadataAttributes)
    }

    val matchedOutputs = processMergeActions(matchedActions)
    val notMatchedBySourceOutputs = processMergeActions(notMatchedBySourceActions)
    val notMatchedOutputs = processMergeActions(notMatchedActions)
    val outputFields = mutable.ArrayBuffer(tableSchema.fields: _*)
    outputFields += StructField(ROW_KIND_COL, ByteType)
    outputFields ++= metadataCols.map(_.toStructField)
    val outputSchema = StructType(outputFields)

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
      keepOutput,
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
          "Can't execute this MergeInto when there are some target rows that each of " +
            "them match more then one source rows. It may lead to an unexpected result.")
      }
    }
  }
}

object MergeIntoPaimonTable {
  private val ROW_ID_COL = "_row_id_"
  private val SOURCE_ROW_COL = "_source_row_"
  private val TARGET_ROW_COL = "_target_row_"
  private val FILE_TOUCHED_COL = "_file_touched_col_"
  // +I, +U, -U, -D
  private val ROW_KIND_COL: String = SparkSystemColumns.ROW_KIND_COL
  private val NOOP_ROW_KIND_VALUE: Byte = "-1".toByte

  private case class MergeIntoProcessor(
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
      keepOutput: Seq[Expression],
      joinedRowEncoder: ExpressionEncoder[Row],
      outputRowEncoder: ExpressionEncoder[Row]
  ) extends Serializable {

    private val rowKindColumnIndex: Int = outputRowEncoder.schema.fieldIndex(ROW_KIND_COL)

    private val fileTouchedColumnIndex: Int =
      SparkRowUtils.getFieldIndex(joinedRowEncoder.schema, FILE_TOUCHED_COL)

    private def generateProjection(exprs: Seq[Expression]): UnsafeProjection = {
      UnsafeProjection.create(exprs, joinedAttributes)
    }

    private def generatePredicate(expr: Expression): BasePredicate = {
      GeneratePredicate.generate(expr, joinedAttributes)
    }

    private def fromTouchedFile(row: InternalRow): Boolean = {
      fileTouchedColumnIndex != -1 && row.getBoolean(fileTouchedColumnIndex)
    }

    private def unusedRow(row: InternalRow): Boolean = {
      row.getByte(rowKindColumnIndex) == NOOP_ROW_KIND_VALUE
    }

    def processPartition(rowIterator: Iterator[Row]): Iterator[Row] = {
      val targetRowHasNoMatchPred = generatePredicate(targetRowHasNoMatch)
      val sourceRowHasNoMatchPred = generatePredicate(sourceRowHasNoMatch)
      val matchedPreds = matchedConditions.map(generatePredicate)
      val matchedProjs = matchedOutputs.map(generateProjection)
      val notMatchedBySourcePreds: Seq[BasePredicate] =
        notMatchedBySourceConditions.map(generatePredicate)
      val notMatchedBySourceProjs = notMatchedBySourceOutputs.map(generateProjection)
      val notMatchedPreds = notMatchedConditions.map(generatePredicate)
      val notMatchedProjs = notMatchedOutputs.map(generateProjection)
      val noopCopyProj = generateProjection(noopCopyOutput)
      val keepProj = generateProjection(keepOutput)
      val outputProj = UnsafeProjection.create(outputRowEncoder.schema)

      def processRow(inputRow: InternalRow): InternalRow = {
        def applyPreds(preds: Seq[BasePredicate], projs: Seq[UnsafeProjection]): InternalRow = {
          preds.zip(projs).find { case (predicate, _) => predicate.eval(inputRow) } match {
            case Some((_, projections)) =>
              projections.apply(inputRow)
            case None =>
              // keep the row if it is from touched file and not be matched
              if (fromTouchedFile(inputRow)) {
                keepProj.apply(inputRow)
              } else {
                noopCopyProj.apply(inputRow)
              }
          }
        }

        if (targetRowHasNoMatchPred.eval(inputRow)) {
          applyPreds(notMatchedBySourcePreds, notMatchedBySourceProjs)
        } else if (sourceRowHasNoMatchPred.eval(inputRow)) {
          applyPreds(notMatchedPreds, notMatchedProjs)
        } else {
          applyPreds(matchedPreds, matchedProjs)
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
