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
import org.apache.paimon.table.{FileStoreTable, SpecialFields}
import org.apache.paimon.table.sink.CommitMessage
import org.apache.paimon.types.RowKind

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.PaimonUtils._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, BasePredicate, Expression, Literal, UnsafeProjection}
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
    dvSafeWriter.commit(commitMessages)
    Seq.empty[Row]
  }

  private def performMergeForPkTable(sparkSession: SparkSession): Seq[CommitMessage] = {
    dvSafeWriter.write(
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
        extraMetadataCols = metadataCols)

      ds.cache()
      try {
        // Step3: filter rows that should be marked as DELETED in Deletion Vector mode.
        val dvDS = ds.where(
          s"$ROW_KIND_COL = ${RowKind.DELETE.toByteValue} or $ROW_KIND_COL = ${RowKind.UPDATE_AFTER.toByteValue}")
        val deletionVectors = collectDeletionVectors(dataFilePathToMeta, dvDS, sparkSession)
        val indexCommitMsg = dvSafeWriter.persistDeletionVectors(deletionVectors)

        // Step4: filter rows that should be written as the inserted/updated data.
        val toWriteDS = ds
          .where(
            s"$ROW_KIND_COL = ${RowKind.INSERT.toByteValue} or $ROW_KIND_COL = ${RowKind.UPDATE_AFTER.toByteValue}")
          .drop(FILE_PATH_COLUMN, ROW_INDEX_COLUMN)
        val addCommitMessage = dvSafeWriter.write(toWriteDS)

        // Step5: commit index and data commit messages
        addCommitMessage ++ indexCommitMsg
      } finally {
        ds.unpersist()
      }
    } else {
      // Files need to be rewritten
      val filePathsToRewritten = mutable.Set.empty[String]
      // Files need to be read, but not rewritten
      val filePathsToRead = mutable.Set.empty[String]

      def hasUpdate(actions: Seq[MergeAction]): Boolean = {
        actions.exists {
          case _: UpdateAction | _: DeleteAction => true
          case _ => false
        }
      }

      def findTouchedFiles0(joinType: String): Array[String] = {
        findTouchedFiles(
          targetDS.alias("_left").join(sourceDS, toColumn(mergeCondition), joinType),
          sparkSession,
          "_left." + FILE_PATH_COLUMN)
      }

      if (hasUpdate(matchedActions)) {
        filePathsToRewritten ++= findTouchedFiles0("inner")
      } else if (notMatchedActions.nonEmpty) {
        filePathsToRead ++= findTouchedFiles0("inner")
      }

      if (hasUpdate(notMatchedBySourceActions)) {
        val noMatchedBySourceFilePaths = findTouchedFiles0("left_anti")
        filePathsToRewritten ++= noMatchedBySourceFilePaths
        filePathsToRead --= noMatchedBySourceFilePaths
      }

      val (filesToRewritten, touchedFileRelation) =
        createNewRelation(filePathsToRewritten.toArray, dataFilePathToMeta, relation)
      val (_, unTouchedFileRelation) =
        createNewRelation(filePathsToRead.toArray, dataFilePathToMeta, relation)

      // Add FILE_TOUCHED_COL to mark the row as coming from the touched file, if the row has not been
      // modified and was from touched file, it should be kept too.
      val targetDSWithFileTouchedCol = createDataset(sparkSession, touchedFileRelation)
        .withColumn(FILE_TOUCHED_COL, lit(true))
        .union(createDataset(sparkSession, unTouchedFileRelation)
          .withColumn(FILE_TOUCHED_COL, lit(false)))

      // If no files need to be rewritten, no need to write row lineage
      val writeRowLineage = coreOptions.rowTrackingEnabled() && filesToRewritten.nonEmpty

      val toWriteDS = constructChangedRows(
        sparkSession,
        targetDSWithFileTouchedCol,
        writeRowLineage = writeRowLineage).drop(ROW_KIND_COL)

      val writer = if (writeRowLineage) {
        dvSafeWriter.withRowLineage()
      } else {
        dvSafeWriter
      }
      val addCommitMessage = writer.write(toWriteDS)
      val deletedCommitMessage = buildDeletedCommitMessage(filesToRewritten)

      addCommitMessage ++ deletedCommitMessage
    }
  }

  /** Get a Dataset where each of Row has an additional column called _row_kind_. */
  private def constructChangedRows(
      sparkSession: SparkSession,
      targetDataset: Dataset[Row],
      remainDeletedRow: Boolean = false,
      deletionVectorEnabled: Boolean = false,
      extraMetadataCols: Seq[PaimonMetadataColumn] = Seq.empty,
      writeRowLineage: Boolean = false): Dataset[Row] = {
    val targetDS = targetDataset
      .withColumn(TARGET_ROW_COL, lit(true))

    val sourceDS = createDataset(sparkSession, sourceTable)
      .withColumn(SOURCE_ROW_COL, lit(true))

    val joinedDS = sourceDS.join(targetDS, toColumn(mergeCondition), "fullOuter")
    val joinedPlan = joinedDS.queryExecution.analyzed

    def resolveOnJoinedPlan(exprs: Seq[Expression]): Seq[Expression] = {
      resolveExpressions(sparkSession)(exprs, joinedPlan)
    }

    val targetRowNotMatched = resolveOnJoinedPlan(
      Seq(toExpression(sparkSession, col(SOURCE_ROW_COL).isNull))).head
    val sourceRowNotMatched = resolveOnJoinedPlan(
      Seq(toExpression(sparkSession, col(TARGET_ROW_COL).isNull))).head
    val matchedExprs = matchedActions.map(_.condition.getOrElse(TrueLiteral))
    val notMatchedExprs = notMatchedActions.map(_.condition.getOrElse(TrueLiteral))
    val notMatchedBySourceExprs = notMatchedBySourceActions.map(_.condition.getOrElse(TrueLiteral))

    val resolver = sparkSession.sessionState.conf.resolver
    def attribute(name: String) = joinedPlan.output.find(attr => resolver(name, attr.name))
    val extraMetadataAttributes =
      extraMetadataCols.flatMap(metadataCol => attribute(metadataCol.name))
    val (rowIdAttr, sequenceNumberAttr) = if (writeRowLineage) {
      (
        attribute(SpecialFields.ROW_ID.name()).get,
        attribute(SpecialFields.SEQUENCE_NUMBER.name()).get)
    } else {
      (null, null)
    }

    val targetOutput = if (writeRowLineage) {
      filteredTargetPlan.output ++ Seq(rowIdAttr, sequenceNumberAttr)
    } else {
      filteredTargetPlan.output
    }
    val noopOutput = targetOutput :+ Alias(Literal(NOOP_ROW_KIND_VALUE), ROW_KIND_COL)()
    val keepOutput = targetOutput :+ Alias(Literal(RowKind.INSERT.toByteValue), ROW_KIND_COL)()

    def processMergeActions(actions: Seq[MergeAction]): Seq[Seq[Expression]] = {
      val columnExprs = actions.map {
        case UpdateAction(_, assignments) =>
          var exprs = assignments.map(_.value)
          if (writeRowLineage) {
            exprs ++= Seq(rowIdAttr, Literal(null))
          }
          exprs :+ Literal(RowKind.UPDATE_AFTER.toByteValue)
        case DeleteAction(_) =>
          if (remainDeletedRow || deletionVectorEnabled) {
            targetOutput :+ Literal(RowKind.DELETE.toByteValue)
          } else {
            // If RowKind = NOOP_ROW_KIND_VALUE, then these rows will be dropped in MergeIntoProcessor.processPartition by default.
            // If these rows still need to be remained, set MergeIntoProcessor.remainNoopRow true.
            noopOutput
          }
        case InsertAction(_, assignments) =>
          var exprs = assignments.map(_.value)
          if (writeRowLineage) {
            exprs ++= Seq(rowIdAttr, sequenceNumberAttr)
          }
          exprs :+ Literal(RowKind.INSERT.toByteValue)
      }

      columnExprs.map(exprs => exprs ++ extraMetadataAttributes)
    }

    val matchedOutputs = processMergeActions(matchedActions)
    val notMatchedBySourceOutputs = processMergeActions(notMatchedBySourceActions)
    val notMatchedOutputs = processMergeActions(notMatchedActions)
    val outputFields = mutable.ArrayBuffer(tableSchema.fields: _*)
    if (writeRowLineage) {
      outputFields += PaimonMetadataColumn.ROW_ID.toStructField
      outputFields += PaimonMetadataColumn.SEQUENCE_NUMBER.toStructField
    }
    outputFields += StructField(ROW_KIND_COL, ByteType)
    outputFields ++= extraMetadataCols.map(_.toStructField)
    val outputSchema = StructType(outputFields.toSeq)

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
        .join(targetDS, toColumn(mergeCondition), "inner")
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
      val notMatchedBySourcePreds = notMatchedBySourceConditions.map(generatePredicate)
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
