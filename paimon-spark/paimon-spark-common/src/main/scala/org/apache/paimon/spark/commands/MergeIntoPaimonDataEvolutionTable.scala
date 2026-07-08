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

import org.apache.paimon.CoreOptions
import org.apache.paimon.CoreOptions.GlobalIndexColumnUpdateAction
import org.apache.paimon.Snapshot
import org.apache.paimon.data.BinaryRow
import org.apache.paimon.deletionvectors.DeletionVector
import org.apache.paimon.format.blob.BlobFileFormat.isBlobFile
import org.apache.paimon.index.GlobalIndexMeta
import org.apache.paimon.io.{CompactIncrement, DataIncrement}
import org.apache.paimon.manifest.IndexManifestEntry
import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.catalyst.analysis.PaimonRelation
import org.apache.paimon.spark.catalyst.analysis.PaimonRelation.isPaimonTable
import org.apache.paimon.spark.catalyst.analysis.PaimonUpdateTable.toColumn
import org.apache.paimon.spark.catalyst.analysis.expressions.ExpressionHelper
import org.apache.paimon.spark.leafnode.PaimonLeafRunnableCommand
import org.apache.paimon.spark.util.ScanPlanHelper.createNewScanPlan
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.{CommitMessage, CommitMessageImpl}
import org.apache.paimon.table.source.DataSplit
import org.apache.paimon.table.source.snapshot.SnapshotReader
import org.apache.paimon.table.source.snapshot.TimeTravelUtil
import org.apache.paimon.types.DataTypeRoot.BLOB
import org.apache.paimon.types.RowType
import org.apache.paimon.types.VectorType.isVectorStoreFile

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.PaimonUtils._
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer.resolver
import org.apache.spark.sql.catalyst.expressions.{Alias, And, AttributeReference, EqualTo, Expression, ExprId, Literal, Or, PythonUDF, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftOuter}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.paimon.shims.SparkShimLoader
import org.apache.spark.sql.types.{BooleanType, StructType}

import java.util.{HashMap => JHashMap}

import scala.collection.{immutable, mutable}
import scala.collection.JavaConverters._
import scala.collection.Searching.{search, Found, InsertionPoint}
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/** Command for Merge Into for Data Evolution paimon table. */
case class MergeIntoPaimonDataEvolutionTable(
    v2Table: SparkTable,
    targetTable: LogicalPlan,
    sourceTable: LogicalPlan,
    matchedCondition: Expression,
    matchedActions: Seq[MergeAction],
    notMatchedActions: Seq[MergeAction],
    notMatchedBySourceActions: Seq[MergeAction])
  extends PaimonRowLevelCommand
  with Logging {

  assert(
    matchedActions.forall {
      case _: UpdateAction | _: DeleteAction => true
      case _ => false
    },
    "Only SET and DELETE clauses are supported in MergeIntoPaimonDataEvolutionTable for SQL: " +
      "WHEN MATCHED."
  )
  assert(
    notMatchedActions.forall(x => x.isInstanceOf[InsertAction]),
    "Only INSERT clause is supported in MergeIntoPaimonDataEvolutionTable for SQL: WHEN NOT MATCHED."
  )
  assert(
    notMatchedBySourceActions.forall(x => x.isInstanceOf[DeleteAction]),
    "Only DELETE clause is supported in MergeIntoPaimonDataEvolutionTable for SQL: WHEN NOT " +
      "MATCHED BY SOURCE."
  )

  import MergeIntoPaimonDataEvolutionTable._

  private lazy val originalTargetRelation: DataSourceV2Relation =
    PaimonRelation.getPaimonRelation(targetTable)

  private lazy val matchedUpdateScanTarget: (SparkTable, DataSourceV2Relation) =
    if (matchedActions.nonEmpty) {
      withMatchedUpdateScanOptions(v2Table, originalTargetRelation)
    } else {
      (v2Table, originalTargetRelation)
    }

  private lazy val targetSparkTable: SparkTable = matchedUpdateScanTarget._1

  override lazy val table: FileStoreTable = targetSparkTable.getTable.asInstanceOf[FileStoreTable]

  private lazy val hasTargetSideDeleteActions: Boolean =
    matchedActions.exists(_.isInstanceOf[DeleteAction]) ||
      notMatchedBySourceActions.exists(_.isInstanceOf[DeleteAction])

  private lazy val hasMatchedDeleteActions: Boolean =
    matchedActions.exists(_.isInstanceOf[DeleteAction])

  assert(
    !hasTargetSideDeleteActions || table.coreOptions().deletionVectorsEnabled(),
    "MERGE INTO DELETE on DataEvolution table requires deletion-vectors.enabled=true."
  )

  private val updateColumns: Set[AttributeReference] = {
    val columns = mutable.Set[AttributeReference]()
    for (action <- matchedActions) {
      action match {
        case updateAction: UpdateAction =>
          for (assignment <- updateAction.assignments) {
            if (isModifiedAssignment(assignment)) {
              columns += assignmentKeyAttribute(assignment)
            }
          }
        case _ =>
      }
    }
    columns.toSet
  }

  /**
   * Self-Merge pattern:
   * {{{
   * MERGE INTO T AS t
   * USING T AS s
   * ON t._ROW_ID = s._ROW_ID
   * WHEN MATCHED THEN UPDATE ... SET ...
   * }}}
   * For this pattern, the execution can be optimized to:
   *
   * `Scan -> MergeRows -> Write`
   *
   * without any extra shuffle, join, or sort.
   */
  private lazy val isSelfMergeOnRowId: Boolean = {
    if (!isPaimonTable(sourceTable)) {
      false
    } else if (
      !originalTargetRelation.name.equals(PaimonRelation.getPaimonRelation(sourceTable).name)
    ) {
      false
    } else {
      matchedCondition match {
        case EqualTo(left: AttributeReference, right: AttributeReference)
            if left.name == ROW_ID_NAME && right.name == ROW_ID_NAME =>
          true
        case _ => false
      }
    }
  }

  assert(
    !(isSelfMergeOnRowId && (notMatchedActions.nonEmpty || notMatchedBySourceActions.nonEmpty)),
    "Self-Merge on _ROW_ID only supports WHEN MATCHED actions. WHEN NOT MATCHED and " +
      "WHEN NOT MATCHED BY SOURCE are not supported."
  )

  private lazy val targetRelation: DataSourceV2Relation = matchedUpdateScanTarget._2

  lazy val tableSchema: StructType = targetSparkTable.schema

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // Persist the schema that the analyzer evolved in memory (commit deferred to execution).
    SchemaEvolutionHelper.commitEvolvedSchemaAtExecution(table, targetRelation, sparkSession)
    invokeMergeInto(sparkSession)
    Seq.empty[Row]
  }

  private def invokeMergeInto(sparkSession: SparkSession): Unit = {
    val readSnapshot = table.snapshotManager().latestSnapshot()
    val snapshotReader = table.newSnapshotReader()
    if (readSnapshot != null) {
      snapshotReader.withSnapshot(readSnapshot)
    }
    pushDownMergePartitionFilter(snapshotReader)
    val plan = snapshotReader.read()
    val tableSplits: Seq[DataSplit] = plan
      .splits()
      .asScala
      .map(_.asInstanceOf[DataSplit])
      .toSeq

    val firstRowIds: immutable.IndexedSeq[Long] = tableSplits
      .flatMap(_.dataFiles().asScala)
      .filter {
        file =>
          file.firstRowId() != null &&
          !isBlobFile(file.fileName()) &&
          !isVectorStoreFile(file.fileName())
      }
      .map(file => file.firstRowId().asInstanceOf[Long])
      .distinct
      .sorted
      .toIndexedSeq

    val firstRowIdToBlobFirstRowIds: Map[Long, List[Long]] = {
      val map = new mutable.HashMap[Long, List[Long]]()
      val files = tableSplits
        .flatMap(_.dataFiles().asScala)
        .filter(file => isBlobFile(file.fileName()))
        .sortBy(f => f.firstRowId())

      for (file <- files) {
        val firstRowId = file.firstRowId().asInstanceOf[Long]
        val firstIdInNormalFile = floorBinarySearch(firstRowIds, firstRowId)
        map.update(
          firstIdInNormalFile,
          map.getOrElseUpdate(firstIdInNormalFile, List.empty[Long]) :+ firstRowId
        )
      }
      map.toMap
    }

    val persistSourceDss: Option[Dataset[Row]] =
      if (
        table.coreOptions().dataEvolutionMergeIntoSourcePersist()
        && (matchedActions.nonEmpty || notMatchedActions.nonEmpty || notMatchedBySourceActions.nonEmpty)
      ) {
        val dss = createDataset(sparkSession, sourceTable)
        dss.persist()
        Some(dss)
      } else {
        None
      }

    var targetActionCleanup = () => ()
    try {
      // step 1: find the related data splits for matched / insert actions.
      lazy val matchedOrInsertDataSplits: Seq[DataSplit] = targetRelatedSplits(
        sparkSession,
        tableSplits,
        firstRowIds,
        firstRowIdToBlobFirstRowIds,
        persistSourceDss)
      lazy val touchedFileTargetRelation =
        createNewScanPlan(matchedOrInsertDataSplits, targetRelation)

      // step 2: invoke target-side action
      val matchedResult =
        if (matchedActions.nonEmpty) {
          val result = targetActionInvoke(
            matchedOrInsertDataSplits,
            sparkSession,
            touchedFileTargetRelation,
            firstRowIds,
            persistSourceDss)
          targetActionCleanup = result.cleanup
          result
        } else {
          TargetActionResult(Nil, None, () => ())
        }
      val notMatchedBySourceDvs =
        if (notMatchedBySourceActions.nonEmpty) {
          notMatchedBySourceDeletionVectors(sparkSession, readSnapshot, persistSourceDss)
        } else None

      // Merge deletion vectors generated from NMBS and matched conditions
      // These two statement branch could generate deletion vectors for same files!
      val deleteCommit = persistMergedDeletionVectors(
        Seq(matchedResult.deletionVectors, notMatchedBySourceDvs).flatten,
        readSnapshot)

      // step 3: invoke insert action
      val insertCommit =
        if (notMatchedActions.nonEmpty)
          insertActionInvoke(sparkSession, touchedFileTargetRelation, persistSourceDss)
        else Nil

      if (readSnapshot != null) {
        writer.rowIdCheckConflict(readSnapshot.id())
      }
      writer.commit(
        matchedResult.commitMessages ++ deleteCommit ++ insertCommit,
        Snapshot.Operation.MERGE)
    } finally {
      targetActionCleanup()
      if (persistSourceDss.isDefined) {
        persistSourceDss.get.unpersist(blocking = false)
      }
    }
  }

  private def pushDownMergePartitionFilter(snapshotReader: SnapshotReader): Unit = {
    val partitionRowType = table.schema().logicalPartitionType()
    if (partitionRowType.getFieldCount == 0) {
      return
    }

    // matchedCondition comes from MergeIntoTable.mergeCondition, which is the MERGE ON condition.
    val partitionPredicates = getExpressionOnlyRelated(matchedCondition, targetTable)
      .map(splitConjunctivePredicates)
      .map(extractMergePartitionFilters(_, partitionRowType))
      .getOrElse(Seq.empty)

    if (partitionPredicates.nonEmpty) {
      val filter = convertConditionToPaimonPredicate(
        partitionPredicates.reduce(And),
        targetRelation.output,
        rowType,
        ignorePartialFailure = true)
      filter.foreach(snapshotReader.withFilter)
    }
  }

  private def extractMergePartitionFilters(
      filters: Seq[Expression],
      partitionRowType: RowType): Seq[Expression] = {
    val partitionColumns = partitionRowType.getFieldNames.asScala.toSet
    filters.filter {
      f =>
        f.deterministic &&
        f.references.forall(attr => partitionColumns.exists(_.equalsIgnoreCase(attr.name))) &&
        !SubqueryExpression.hasSubquery(f) &&
        f.collect { case _: PythonUDF => true }.isEmpty
    }
  }

  private def targetRelatedSplits(
      sparkSession: SparkSession,
      tableSplits: Seq[DataSplit],
      firstRowIds: immutable.IndexedSeq[Long],
      firstRowIdToBlobFirstRowIds: Map[Long, List[Long]],
      persistSourceDss: Option[Dataset[Row]]): Seq[DataSplit] = {
    // Self-Merge shortcut:
    // In Self-Merge mode, every row in the table may be updated, so we scan all splits.
    if (isSelfMergeOnRowId) {
      return tableSplits
    }

    if (!table.coreOptions().dataEvolutionMergeIntoFilePruning()) {
      logInfo(
        "Skip file-level pruning for MergeInto partial column update on data-evolution table " +
          s"${table.name()}.")
      return tableSplits
    }

    val sourceDss = persistSourceDss.getOrElse(createDataset(sparkSession, sourceTable))

    val firstRowIdsTouched = extractSourceRowIdMapping match {
      case Some(sourceRowIdAttr) =>
        // Shortcut: Directly get _FIRST_ROW_IDs from the source table.
        findRelatedFirstRowIds(
          sourceDss,
          sparkSession,
          firstRowIds,
          firstRowIdToBlobFirstRowIds,
          sourceRowIdAttr.name).toSet

      case None =>
        // Perform the full join to find related _FIRST_ROW_IDs.
        val targetDss = createDataset(sparkSession, targetRelation)
        findRelatedFirstRowIds(
          targetDss.alias("_left").join(sourceDss, toColumn(matchedCondition), "inner"),
          sparkSession,
          firstRowIds,
          firstRowIdToBlobFirstRowIds,
          "_left." + ROW_ID_NAME
        ).toSet
    }

    tableSplits
      .map(
        split =>
          split.filterDataFile(
            file => file.firstRowId() != null && firstRowIdsTouched.contains(file.firstRowId())))
      .filter(optional => optional.isPresent)
      .map(_.get())
  }

  private case class TargetActionResult(
      commitMessages: Seq[CommitMessage],
      deletionVectors: Option[Dataset[SparkDeletionVector]],
      cleanup: () => Unit)

  private def targetActionInvoke(
      dataSplits: Seq[DataSplit],
      sparkSession: SparkSession,
      touchedFileTargetRelation: DataSourceV2Relation,
      firstRowIds: immutable.IndexedSeq[Long],
      persistSourceDss: Option[Dataset[Row]]): TargetActionResult = {
    if (dataSplits.isEmpty) {
      return TargetActionResult(Nil, None, () => ())
    }

    val conditionFields = extractFields(matchedCondition)
    val allFields = mutable.SortedSet.empty[AttributeReference](
      (o1, o2) => {
        o1.toString().compareTo(o2.toString())
      }) ++ conditionFields

    val updateColumnsSorted = updateColumns.toSeq.sortBy(
      s => targetTable.output.map(x => x.toString()).indexOf(s.toString()))

    // Different Spark versions might produce duplicate attributes between `output` and
    // `metadataOutput`, so manually deduplicate by `exprId`.
    val metadataColumns = (targetRelation.output ++ targetRelation.metadataOutput)
      .filter(attr => attr.name.equals(ROW_ID_NAME))
      .groupBy(_.exprId)
      .map { case (_, attrs) => attrs.head }
      .toSeq

    // Find raw blob update columns and avoid reading them from target table
    val blobInlineFields = table.coreOptions().blobInlineField().asScala.toSet
    val rawBlobFieldNames = table
      .rowType()
      .getFields
      .asScala
      .filter(
        field =>
          field.`type`().is(BLOB) &&
            !blobInlineFields.exists(inlineField => resolver(inlineField, field.name())))
      .map(_.name())
      .toSet

    def isRawBlobUpdateColumn(attr: AttributeReference): Boolean = {
      rawBlobFieldNames.exists(rawBlobFieldName => resolver(rawBlobFieldName, attr.name))
    }

    // The final output is composed by updated columns, metadata columns and blob marker columns.
    // Marker columns are used to mark whether a blob field should be written with placeholder
    val rawBlobUpdateColumns = updateColumnsSorted.filter(isRawBlobUpdateColumn)
    val rawBlobMarkerNames =
      rawBlobMarkerNamesAvoiding(
        rawBlobUpdateColumns.size,
        updateColumnsSorted.map(_.name) ++ sourceTable.output.map(_.name))
    val rawBlobMarkerNamesByColumn = rawBlobUpdateColumns
      .zip(rawBlobMarkerNames)
      .map { case (attr, markerName) => attr.name -> markerName }
      .toMap
    val rawBlobMarkerAttributes = rawBlobUpdateColumns.map(
      attr =>
        AttributeReference(rawBlobMarkerNamesByColumn(attr.name), BooleanType, nullable = false)())
    val deletedAttribute =
      AttributeReference(MERGE_DELETED_NAME, BooleanType, nullable = false)()
    // Row metadata, _FIRST_ROW_ID added by addFirstRowId, and the delete marker.
    val fixedMergeOutputColumnCount = metadataColumns.size + 2
    val mergeOutput = (updateColumnsSorted ++ metadataColumns ++ rawBlobMarkerAttributes) :+
      deletedAttribute

    val targetMatchedActions = matchedActions.map {
      case update: UpdateAction =>
        UpdateAction.apply(
          update.condition,
          update.assignments.filter(a => updateColumnsSorted.contains(assignmentKeyAttribute(a))))
      case delete: DeleteAction => delete
      case other =>
        throw new UnsupportedOperationException(s"Unsupported matched action: $other.")
    }

    // All fields are composed by:
    // 1. Match condition fields
    // 2. For each target-side action, the condition fields and the assignment value fields
    // 3. All updated fields exclude raw blob fields
    for (action <- targetMatchedActions) {
      action.condition.foreach(condition => allFields ++= extractFields(condition))
      action match {
        case update: UpdateAction =>
          for (assignment <- update.assignments) {
            if (isModifiedAssignment(assignment)) {
              allFields ++= extractFields(assignment.value)
            }
          }
        case _ =>
      }
    }
    allFields ++= updateColumnsSorted.filterNot(isRawBlobUpdateColumn)

    def modifiedRawBlobNames(action: UpdateAction): Set[String] = {
      action.assignments.flatMap {
        assignment =>
          if (isModifiedAssignment(assignment)) {
            val key = assignmentKeyAttribute(assignment)
            rawBlobUpdateColumns.find(_.sameRef(key)).map(_.name)
          } else {
            None
          }
      }.toSet
    }

    def assignmentValue(action: UpdateAction, attr: AttributeReference): Expression = {
      action.assignments
        .find(assignment => assignmentKeyAttribute(assignment).sameRef(attr))
        .map(_.value)
        .getOrElse(attr)
    }

    // the output projection for update from source table
    def updateOutput(action: UpdateAction, rawBlobModified: Set[String]): Seq[Expression] = {
      val updatedColumns = updateColumnsSorted.map {
        attr =>
          if (
            rawBlobUpdateColumns.exists(_.sameRef(attr)) && !rawBlobModified.contains(attr.name)
          ) {
            Literal(null, attr.dataType)
          } else {
            assignmentValue(action, attr)
          }
      }
      val metadata = metadataColumns.map(attr => assignmentValue(action, attr))
      val markers = rawBlobUpdateColumns.map {
        attr =>
          if (rawBlobModified.contains(attr.name)) {
            FalseLiteral
          } else {
            TrueLiteral
          }
      }
      updatedColumns ++ metadata ++ markers :+ FalseLiteral
    }

    // the output projection for target table copy
    def copyOutput(deleted: Boolean): Seq[Expression] = {
      val copiedColumns = updateColumnsSorted.map {
        attr =>
          if (rawBlobUpdateColumns.exists(_.sameRef(attr))) {
            Literal(null, attr.dataType)
          } else {
            attr
          }
      }
      val deletedLiteral = if (deleted) TrueLiteral else FalseLiteral
      copiedColumns ++ metadataColumns ++ rawBlobUpdateColumns.map(_ => TrueLiteral) :+
        deletedLiteral
    }

    def reorderPartialWriteColumns(dataset: Dataset[Row]): Dataset[Row] = {
      val columns =
        updateColumnsSorted.map(attr => quotedColumn(attr.name)) ++
          Seq(quotedColumn(ROW_ID_NAME), quotedColumn(FIRST_ROW_ID_NAME)) ++
          rawBlobMarkerAttributes.map(attr => quotedColumn(attr.name))
      dataset.select(columns: _*)
    }

    val keepCopyInstructions = Seq(
      SparkShimLoader.shim
        .mergeRowsKeepCopy(TrueLiteral, copyOutput(deleted = false))
        .asInstanceOf[MergeRows.Instruction])

    def matchedActionInstruction(action: MergeAction): MergeRows.Instruction = {
      action match {
        case update: UpdateAction =>
          SparkShimLoader.shim
            .mergeRowsKeepUpdate(
              update.condition.getOrElse(TrueLiteral),
              updateOutput(update, modifiedRawBlobNames(update)))
            .asInstanceOf[MergeRows.Instruction]
        case DeleteAction(condition) =>
          SparkShimLoader.shim
            .mergeRowsKeepUpdate(condition.getOrElse(TrueLiteral), copyOutput(deleted = true))
            .asInstanceOf[MergeRows.Instruction]
        case other =>
          throw new UnsupportedOperationException(s"Unsupported matched action: $other.")
      }
    }

    def matchedActionInstructions(actions: Seq[MergeAction]): Seq[MergeRows.Instruction] =
      actions.map(matchedActionInstruction) ++ keepCopyInstructions

    val targetActionOutput: Dataset[Row] = if (isSelfMergeOnRowId) {
      // Self-Merge shortcut:
      // - Scan the target table only (no source scan, no join), and read all columns required by
      //   merge condition and update expressions.
      // - Rewrite all source-side AttributeReferences to the corresponding target attributes.
      // - The scan output already satisfies the required partitioning and ordering for partial
      //   updates, so no extra shuffle or sort is needed.

      val targetAttrsDedup: Seq[AttributeReference] =
        (targetRelation.output ++ targetRelation.metadataOutput)
          .groupBy(_.exprId)
          .map { case (_, attrs) => attrs.head }
          .toSeq

      val neededNames: Set[String] = (allFields ++ metadataColumns).map(_.name).toSet
      val allReadFieldsOnTarget: Seq[AttributeReference] =
        targetAttrsDedup.filter(a => neededNames.exists(n => resolver(n, a.name)))
      val readPlan = touchedFileTargetRelation.copy(output = allReadFieldsOnTarget)

      // Build mapping: source exprId -> target attr (matched by column name).
      val sourceToTarget = {
        val targetAttrs = targetRelation.output ++ targetRelation.metadataOutput
        val sourceAttrs = sourceTable.output ++ sourceTable.metadataOutput
        sourceAttrs.flatMap {
          s => targetAttrs.find(t => resolver(t.name, s.name)).map(t => s.exprId -> t)
        }.toMap
      }

      def rewriteSourceToTarget(
          expr: Expression,
          m: Map[ExprId, AttributeReference]): Expression = {
        expr.transform {
          case a: AttributeReference if m.contains(a.exprId) => m(a.exprId)
        }
      }

      val rewrittenMatchedActions: Seq[MergeAction] = targetMatchedActions.map {
        case action: UpdateAction =>
          val newCond = action.condition.map(c => rewriteSourceToTarget(c, sourceToTarget))
          val newAssignments = action.assignments.map {
            a => Assignment(a.key, rewriteSourceToTarget(a.value, sourceToTarget))
          }
          action.copy(condition = newCond, assignments = newAssignments)
        case DeleteAction(condition) =>
          DeleteAction(condition.map(c => rewriteSourceToTarget(c, sourceToTarget)))
        case other =>
          throw new UnsupportedOperationException(s"Unsupported matched action: $other.")
      }

      val mergeRows = MergeRows(
        isSourceRowPresent = TrueLiteral,
        isTargetRowPresent = TrueLiteral,
        matchedInstructions = matchedActionInstructions(rewrittenMatchedActions),
        notMatchedInstructions = Nil,
        notMatchedBySourceInstructions = keepCopyInstructions,
        checkCardinality = false,
        output = mergeOutput,
        child = readPlan
      )

      val withFirstRowId = addFirstRowId(sparkSession, mergeRows, firstRowIds)
      val expectedOutputColumnCount =
        updateColumnsSorted.size + fixedMergeOutputColumnCount + rawBlobUpdateColumns.size
      assert(withFirstRowId.schema.fields.length == expectedOutputColumnCount)
      withFirstRowId
    } else {
      val allReadFieldsOnTarget = allFields.filter(
        field =>
          targetTable.output.exists(attr => attr.exprId.equals(field.exprId))) ++ metadataColumns
      val allReadFieldsOnSource =
        allFields.filter(
          field => sourceTable.output.exists(attr => attr.exprId.equals(field.exprId)))

      val targetReadPlan =
        touchedFileTargetRelation.copy(output = allReadFieldsOnTarget.toSeq)
      val targetTableProjExprs = targetReadPlan.output :+ Alias(TrueLiteral, ROW_FROM_TARGET)()
      val targetTableProj = Project(targetTableProjExprs, targetReadPlan)

      val sourceTableProjExprs =
        allReadFieldsOnSource.toSeq :+ Alias(TrueLiteral, ROW_FROM_SOURCE)()
      val sourceChild = persistSourceDss.map(_.queryExecution.logical).getOrElse(sourceTable)
      val sourceTableProj = Project(sourceTableProjExprs, sourceChild)

      val joinPlan =
        Join(targetTableProj, sourceTableProj, LeftOuter, Some(matchedCondition), JoinHint.NONE)
      val rowFromSourceAttr = attribute(ROW_FROM_SOURCE, joinPlan)
      val rowFromTargetAttr = attribute(ROW_FROM_TARGET, joinPlan)

      val mergeRows = MergeRows(
        isSourceRowPresent = rowFromSourceAttr,
        isTargetRowPresent = rowFromTargetAttr,
        matchedInstructions = matchedActionInstructions(targetMatchedActions),
        notMatchedInstructions = Nil,
        notMatchedBySourceInstructions = keepCopyInstructions,
        checkCardinality = false,
        output = mergeOutput,
        child = joinPlan
      )
      val withFirstRowId = addFirstRowId(sparkSession, mergeRows, firstRowIds)
      val expectedOutputColumnCount =
        updateColumnsSorted.size + fixedMergeOutputColumnCount + rawBlobUpdateColumns.size
      assert(withFirstRowId.schema.fields.length == expectedOutputColumnCount)
      withFirstRowId
        .repartition(col(FIRST_ROW_ID_NAME))
        .sortWithinPartitions(FIRST_ROW_ID_NAME, ROW_ID_NAME)
    }

    val writePartialFields = updateColumnsSorted.nonEmpty
    val shouldPersistOutput = writePartialFields && hasMatchedDeleteActions
    val output: Dataset[Row] =
      if (shouldPersistOutput) targetActionOutput.persist()
      else targetActionOutput
    var success = false
    try {
      val updateCommit =
        if (writePartialFields) {
          val dataEvolutionWriter = DataEvolutionPaimonWriter(table, dataSplits)
          val partialCommit = dataEvolutionWriter.writePartialFields(
            reorderPartialWriteColumns(output),
            updateColumnsSorted.map(_.name),
            rawBlobUpdateColumns
              .map(attr => attr.name -> rawBlobMarkerNamesByColumn(attr.name))
              .toMap
          )
          checkUpdateResult(partialCommit)
        } else {
          Nil
        }

      val deletionVectors =
        if (hasMatchedDeleteActions) {
          val dataFilePathToMeta = candidateFileMap(dataSplits)
          Some(
            collectDataEvolutionDeletionVectors(
              dataSplits,
              dataFilePathToMeta,
              output.filter(quotedColumn(MERGE_DELETED_NAME)).select(quotedColumn(ROW_ID_NAME)),
              sparkSession))
        } else {
          None
        }

      success = true
      TargetActionResult(
        updateCommit,
        deletionVectors,
        if (shouldPersistOutput) () => output.unpersist(blocking = false) else () => ())
    } finally {
      if (!success && shouldPersistOutput) {
        output.unpersist(blocking = false)
      }
    }
  }

  private def notMatchedBySourceDeletionVectors(
      sparkSession: SparkSession,
      readSnapshot: Snapshot,
      persistSourceDss: Option[Dataset[Row]]): Option[Dataset[SparkDeletionVector]] = {
    // Do not reuse tableSplits here because they may have been pruned by the MERGE ON
    // partition filter. NOT MATCHED BY SOURCE must scan target rows independently.
    val snapshotReader = table.newSnapshotReader()
    if (readSnapshot != null) {
      snapshotReader.withSnapshot(readSnapshot)
    }
    if (table.coreOptions().manifestDeleteFileDropStats()) {
      snapshotReader.dropStats()
    }
    val dataSplits =
      snapshotReader.read().splits().asScala.collect { case split: DataSplit => split }.toSeq
    if (dataSplits.isEmpty) {
      return None
    }

    val rowIdMetadataColumn = (targetRelation.output ++ targetRelation.metadataOutput)
      .filter(attr => attr.name.equals(ROW_ID_NAME))
      .groupBy(_.exprId)
      .map { case (_, attrs) => attrs.head }
      .head

    // No need to project source/target, spark optimizer will take care of it
    val deleteCondition = notMatchedBySourceDeleteCondition
    val targetReadPlan = createNewScanPlan(dataSplits, targetRelation)
      .copy(output = targetRelation.output :+ rowIdMetadataColumn)
    val targetFilteredPlan =
      if (deleteCondition == TrueLiteral) targetReadPlan
      else Filter(deleteCondition, targetReadPlan)
    val sourceChild = persistSourceDss.map(_.queryExecution.logical).getOrElse(sourceTable)
    val antiJoinPlan =
      Join(targetFilteredPlan, sourceChild, LeftAnti, Some(matchedCondition), JoinHint.NONE)
    val deletePlan = Project(Seq(rowIdMetadataColumn), antiJoinPlan)

    val dataFilePathToMeta = candidateFileMap(dataSplits)
    val deletionVectors = collectDataEvolutionDeletionVectors(
      dataSplits,
      dataFilePathToMeta,
      createDataset(sparkSession, deletePlan).select(quotedColumn(ROW_ID_NAME)),
      sparkSession)
    Some(deletionVectors)
  }

  private def persistMergedDeletionVectors(
      deletionVectorDatasets: Seq[Dataset[SparkDeletionVector]],
      readSnapshot: Snapshot): Seq[CommitMessage] = {
    if (deletionVectorDatasets.isEmpty) {
      return Nil
    }

    val sparkSession = deletionVectorDatasets.head.sparkSession
    import sparkSession.implicits._
    val deletionVectors = deletionVectorDatasets
      .reduce(_.union(_))
      .groupByKey(_.dataFilePath)
      .mapGroups {
        (_, iter) =>
          var first: SparkDeletionVector = null
          var merged: DeletionVector = null
          while (iter.hasNext) {
            val current = iter.next()
            val deletionVector = DeletionVector.deserializeFromBytes(current.deletionVector)
            if (first == null) {
              first = current
              merged = deletionVector
            } else {
              merged.merge(deletionVector)
            }
          }

          SparkDeletionVector(
            first.bucketPath,
            first.partition,
            first.bucket,
            first.dataFilePath,
            DeletionVector.serializeToBytes(merged))
      }

    writer.persistDeletionVectors(deletionVectors, readSnapshot)
  }

  private def notMatchedBySourceDeleteCondition: Expression = {
    if (notMatchedBySourceActions.exists(_.condition.isEmpty)) {
      TrueLiteral
    } else {
      notMatchedBySourceActions.flatMap(_.condition).reduce(Or)
    }
  }

  private def insertActionInvoke(
      sparkSession: SparkSession,
      touchedFileTargetRelation: DataSourceV2Relation,
      persistSourceDss: Option[Dataset[Row]]): Seq[CommitMessage] = {
    val mergeFields = extractFields(matchedCondition)
    val allReadFieldsOnTarget =
      mergeFields.filter(field => targetTable.output.exists(attr => attr.equals(field)))

    val targetReadPlan =
      touchedFileTargetRelation.copy(targetRelation.table, allReadFieldsOnTarget.toSeq)
    val sourceReadPlan = persistSourceDss.map(_.queryExecution.logical).getOrElse(sourceTable)

    val joinPlan =
      Join(sourceReadPlan, targetReadPlan, LeftAnti, Some(matchedCondition), JoinHint.NONE)

    // merge rows as there are multiple not matched actions
    val mergeRows = MergeRows(
      isSourceRowPresent = TrueLiteral,
      isTargetRowPresent = FalseLiteral,
      matchedInstructions = Nil,
      notMatchedInstructions = notMatchedActions.map {
        case insertAction: InsertAction =>
          SparkShimLoader.shim
            .mergeRowsKeepInsert(
              insertAction.condition.getOrElse(TrueLiteral),
              insertAction.assignments.map(
                a =>
                  if (
                    !a.value.isInstanceOf[AttributeReference] || joinPlan.output.exists(
                      attr => attr.toString().equals(a.value.toString()))
                  ) a.value
                  else Literal(null))
            )
            .asInstanceOf[MergeRows.Instruction]
        case other =>
          throw new UnsupportedOperationException(s"Unsupported not matched action: $other.")
      }.toSeq,
      notMatchedBySourceInstructions = Nil,
      checkCardinality = false,
      output = targetTable.output,
      child = joinPlan
    )

    val toWrite = createDataset(sparkSession, mergeRows)
    writer.write(toWrite)
  }

  /**
   * Attempts to identify a direct mapping from sourceTable's attribute to the target table's
   * `_ROW_ID`.
   *
   * This is a shortcut optimization for `MERGE INTO` to avoid a full, expensive join when the merge
   * condition is a simple equality on the target's `_ROW_ID`.
   *
   * @return
   *   An `Option` containing the sourceTable's attribute if a pattern like
   *   `target._ROW_ID = source.col` (or its reverse) is found, otherwise `None`.
   */
  private def extractSourceRowIdMapping: Option[AttributeReference] = {

    // Helper to check if an attribute is the target's _ROW_ID
    def isTargetRowId(attr: AttributeReference): Boolean = {
      attr.name == ROW_ID_NAME && (targetRelation.output ++ targetRelation.metadataOutput)
        .exists(_.exprId.equals(attr.exprId))
    }

    // Helper to check if an attribute belongs to the source table
    def isSourceAttribute(attr: AttributeReference): Boolean = {
      (sourceTable.output ++ sourceTable.metadataOutput).exists(_.exprId.equals(attr.exprId))
    }

    matchedCondition match {
      // Case 1: target._ROW_ID = source.col
      case EqualTo(left: AttributeReference, right: AttributeReference)
          if isTargetRowId(left) && isSourceAttribute(right) =>
        Some(right)
      // Case 2: source.col = target._ROW_ID
      case EqualTo(left: AttributeReference, right: AttributeReference)
          if isSourceAttribute(left) && isTargetRowId(right) =>
        Some(left)
      case _ => None
    }
  }

  private def checkUpdateResult(updateCommit: Seq[CommitMessage]): Seq[CommitMessage] = {
    val affectedParts: Set[BinaryRow] = updateCommit.map(_.partition()).toSet
    val rowType = table.rowType()

    // find all global index files of affected partitions and updated columns
    val latestSnapshot = table.latestSnapshot()
    if (!latestSnapshot.isPresent) {
      return updateCommit
    }

    val filter: org.apache.paimon.utils.Filter[IndexManifestEntry] =
      (entry: IndexManifestEntry) => {
        val globalIndexMeta = entry.indexFile().globalIndexMeta()
        if (globalIndexMeta == null) {
          false
        } else {
          val indexedNames = globalIndexMeta.getIndexedFieldNames(rowType).asScala
          affectedParts.contains(entry.partition()) && updateColumns.exists(
            col => indexedNames.contains(col.name))
        }
      }

    val affectedIndexEntries = table
      .store()
      .newIndexFileHandler()
      .scan(latestSnapshot.get(), filter)
      .asScala

    if (affectedIndexEntries.isEmpty) {
      updateCommit
    } else {
      table.coreOptions().globalIndexColumnUpdateAction() match {
        case GlobalIndexColumnUpdateAction.THROW_ERROR =>
          val updatedColNames = updateColumns.map(_.name)
          val conflicted = affectedIndexEntries
            .flatMap(e => e.indexFile().globalIndexMeta().getIndexedFieldNames(rowType).asScala)
            .toSet
          throw new RuntimeException(
            s"""MergeInto: update columns contain globally indexed columns, not supported now.
               |Updated columns: ${updatedColNames.toSeq.sorted.mkString("[", ", ", "]")}
               |Conflicted columns: ${conflicted.toSeq.sorted.mkString("[", ", ", "]")}
               |""".stripMargin)
        case GlobalIndexColumnUpdateAction.DROP_PARTITION_INDEX =>
          val grouped = affectedIndexEntries.groupBy(_.partition())
          val deleteCommitMessages = ArrayBuffer.empty[CommitMessage]
          grouped.foreach {
            case (part, entries) =>
              deleteCommitMessages += new CommitMessageImpl(
                part,
                0,
                null,
                DataIncrement.deleteIndexIncrement(entries.map(_.indexFile()).asJava),
                CompactIncrement.emptyIncrement())
          }
          updateCommit ++ deleteCommitMessages
      }
    }
  }

  private def findRelatedFirstRowIds(
      dataset: Dataset[Row],
      sparkSession: SparkSession,
      firstRowIds: immutable.IndexedSeq[Long],
      firstRowIdToBlobFirstRowIds: Map[Long, List[Long]],
      identifier: String): Array[Long] = {
    import sparkSession.implicits._
    val firstRowIdUdf = udf((rowId: Long) => floorBinarySearch(firstRowIds, rowId))
    dataset
      .select(firstRowIdUdf(col(identifier)))
      .distinct()
      .as[Long]
      .flatMap(
        f => {
          if (firstRowIdToBlobFirstRowIds.contains(f)) {
            firstRowIdToBlobFirstRowIds(f)
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

  private def addFirstRowId(
      sparkSession: SparkSession,
      plan: LogicalPlan,
      firstRowIds: immutable.IndexedSeq[Long]): Dataset[Row] = {
    assert(plan.output.exists(_.name.equals(ROW_ID_NAME)))
    val firstRowIdUdf = udf((rowId: Long) => floorBinarySearch(firstRowIds, rowId))
    val firstRowIdColumn = firstRowIdUdf(col(ROW_ID_NAME))
    createDataset(sparkSession, plan).withColumn(FIRST_ROW_ID_NAME, firstRowIdColumn)
  }
}

object MergeIntoPaimonDataEvolutionTable {

  final private val ROW_FROM_SOURCE = "__row_from_source"
  final private val ROW_FROM_TARGET = "__row_from_target"
  final private val ROW_ID_NAME = "_ROW_ID"
  final private val FIRST_ROW_ID_NAME = "_FIRST_ROW_ID";
  final private val MERGE_DELETED_NAME = "_PAIMON_MERGE_INTO_DELETED"
  final private val RAW_BLOB_PLACEHOLDER_MARKER_PREFIX = "__paimon_raw_blob_placeholder_"

  private[commands] def withMatchedUpdateScanOptions(
      v2Table: SparkTable,
      relation: DataSourceV2Relation): (SparkTable, DataSourceV2Relation) = {
    val table = v2Table.getTable.asInstanceOf[FileStoreTable]
    val fullSearchMode = CoreOptions.GlobalIndexSearchMode.FULL.toString
    Option(TimeTravelUtil.tryTravelOrLatest(table)) match {
      case None =>
        (v2Table, relation)
      case Some(snapshot) =>
        val configuredSnapshotId =
          Option(table.options().get(CoreOptions.SCAN_SNAPSHOT_ID.key()))
        val snapshotId = snapshot.id().toString
        if (
          configuredSnapshotId.contains(snapshotId) &&
          fullSearchMode.equalsIgnoreCase(
            table.options().get(CoreOptions.GLOBAL_INDEX_SEARCH_MODE.key()))
        ) {
          (v2Table, relation)
        } else {
          val dynamicOptions = new JHashMap[String, String]()
          timeTravelOptionKeys.foreach(dynamicOptions.put(_, null))
          dynamicOptions.put(CoreOptions.GLOBAL_INDEX_SEARCH_MODE.key(), fullSearchMode)
          dynamicOptions.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), snapshotId)

          val scanTable = SparkTable.of(table.copy(dynamicOptions))
          val scanRelation =
            SparkShimLoader.shim.copyDataSourceV2Relation(relation, scanTable, relation.output)
          (scanTable, scanRelation)
        }
    }
  }

  private def timeTravelOptionKeys: Seq[String] = Seq(
    CoreOptions.SCAN_SNAPSHOT_ID.key(),
    CoreOptions.SCAN_TAG_NAME.key(),
    CoreOptions.SCAN_WATERMARK.key(),
    CoreOptions.SCAN_TIMESTAMP.key(),
    CoreOptions.SCAN_TIMESTAMP_MILLIS.key(),
    CoreOptions.SCAN_VERSION.key()
  )

  private[commands] def isModifiedAssignment(assignment: Assignment): Boolean = {
    !sameAttributeReference(assignment.key, assignment.value)
  }

  private[commands] def assignmentKeyAttribute(assignment: Assignment): AttributeReference = {
    assignment.key match {
      case key: AttributeReference => key
      case other =>
        throw new UnsupportedOperationException(
          s"Unsupported update assignment key: $other. Only top-level attributes are supported.")
    }
  }

  private[commands] def rawBlobMarkerName(index: Int): String = {
    RAW_BLOB_PLACEHOLDER_MARKER_PREFIX + index
  }

  private[commands] def rawBlobMarkerNamesAvoiding(
      count: Int,
      reservedNames: Seq[String]): Seq[String] = {
    var nextIndex = 0
    (0 until count).map {
      _ =>
        var markerName = rawBlobMarkerName(nextIndex)
        while (reservedNames.exists(reservedName => resolver(reservedName, markerName))) {
          nextIndex += 1
          markerName = rawBlobMarkerName(nextIndex)
        }
        nextIndex += 1
        markerName
    }
  }

  private def quotedColumn(name: String) = {
    col("`" + name.replace("`", "``") + "`")
  }

  private def sameAttributeReference(left: Expression, right: Expression): Boolean = {
    (left, right) match {
      case (leftAttr: AttributeReference, rightAttr: AttributeReference) =>
        leftAttr.sameRef(rightAttr)
      case _ => false
    }
  }

  private def floorBinarySearch(indexed: immutable.IndexedSeq[Long], value: Long): Long = {
    if (indexed.isEmpty) {
      throw new IllegalArgumentException("The input sorted sequence is empty.")
    }

    indexed.search(value) match {
      case Found(foundIndex) => indexed(foundIndex)
      case InsertionPoint(insertionIndex) =>
        if (insertionIndex == 0) {
          throw new IllegalArgumentException(
            s"Value $value is less than the first element in the sorted sequence.")
        } else {
          indexed(insertionIndex - 1)
        }
    }
  }
}
