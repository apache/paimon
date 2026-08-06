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

import org.apache.paimon.CoreOptions.{ChangelogProducer, MergeEngine}
import org.apache.paimon.Snapshot
import org.apache.paimon.index.pk.PrimaryKeyIndexDefinitions
import org.apache.paimon.spark.SparkConnectorOptions
import org.apache.paimon.spark.catalyst.analysis.expressions.ExpressionHelper
import org.apache.paimon.spark.schema.SparkSystemColumns.ROW_KIND_COL
import org.apache.paimon.spark.util.OptionUtils
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.PrimaryKeyTableUtils.validatePKUpsertDeletable
import org.apache.paimon.table.sink.CommitMessage
import org.apache.paimon.types.RowKind

import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.PaimonUtils.createDataset
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, EqualNullSafe, EqualTo, Expression, In, InSet, InSubquery, ListQuery, Literal, Not}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, SupportsSubquery}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import scala.collection.JavaConverters._
import scala.collection.mutable

case class DeleteFromPaimonTableCommand(
    relation: DataSourceV2Relation,
    override val table: FileStoreTable,
    condition: Expression)
  extends PaimonRowLevelCommand
  with ExpressionHelper
  with SupportsSubquery {

  // Guards cartesian blow-up of multi-column IN lists on the driver.
  private def pointDeleteMaxRows: Long =
    OptionUtils.getOptionString(SparkConnectorOptions.DELETE_POINT_DELETE_MAX_ROWS).toLong

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val commitMessages = if (usePKUpsertDelete()) {
      performPrimaryKeyDelete(sparkSession)
    } else {
      performNonPrimaryKeyDelete(sparkSession)
    }
    writer.commit(commitMessages, Snapshot.Operation.DELETE)
    Seq.empty[Row]
  }

  private def usePKUpsertDelete(): Boolean = {
    try {
      validatePKUpsertDeletable(table)
      true
    } catch {
      case _: UnsupportedOperationException => false
    }
  }

  /**
   * The fast path fills non-pk columns of the -D rows with NULL, which is only safe for DEDUPLICATE
   * (the merge never reads fields of a delete row). Other engines (e.g. partial-update with
   * remove-record-on-sequence-group) rely on field values of the delete row. Fall back to scan
   * otherwise.
   *
   * It also requires that nothing else depends on those NULLs:
   *   - `delete.force-produce-changelog`: the user asks for a faithful changelog, so the -D rows
   *     have to carry the old field values (same option Flink SQL and the metadata only delete use
   *     to opt out of their own shortcut).
   *   - `changelog-producer`: a streaming consumer would retract with NULL field values instead of
   *     the old ones.
   *   - NOT NULL non-pk columns: the write would be rejected.
   *   - `sequence.field`: a NULL sequence value sorts oldest, so the delete would lose the merge.
   *   - partition columns outside the pk (cross partition update): the -D row would carry a NULL
   *     partition.
   *   - sorted/global pk indexes: they are built from real field values, so a NULL-filled -D row
   *     would leave the deletion invisible to index lookups.
   *
   * Incremental/audit_log reads still see NULL instead of the old field values of the deleted rows
   * until compaction removes the -D rows.
   */
  private def fastPathEligible: Boolean = {
    coreOptions.mergeEngine() == MergeEngine.DEDUPLICATE &&
    !coreOptions.deleteForceProduceChangelog() &&
    coreOptions.changelogProducer() == ChangelogProducer.NONE &&
    coreOptions.sequenceField().isEmpty &&
    !table.schema().crossPartitionUpdate() && {
      val pkSet = table.primaryKeys().asScala.toSet
      table
        .rowType()
        .getFields
        .asScala
        .forall(f => pkSet.contains(f.name()) || f.`type`().isNullable)
    } && {
      PrimaryKeyIndexDefinitions.create(table.schema()).definitions().isEmpty
    }
  }

  private def performPrimaryKeyDelete(sparkSession: SparkSession): Seq[CommitMessage] = {
    // Fast path: when the matched keys are fully described by the condition itself — literals
    // (pk = v / pk IN (...)) or a pk IN (subquery) — build -D rows without scanning the target
    // table. Keys that do not exist only add a -D record that compaction removes later (it can
    // however materialize a partition that did not exist before). Otherwise fall back to scan.
    val keyDf = if (!fastPathEligible) {
      None
    } else {
      extractPointDeleteKeys()
        .map(literalKeyDataFrame(sparkSession, _))
        .orElse(extractSubqueryKeyDataFrame(sparkSession))
    }
    keyDf match {
      case Some(keys) =>
        writer.write(buildDeleteDataFrame(keys))
      case None =>
        val df = createDataset(sparkSession, Filter(condition, relation))
          .withColumn(ROW_KIND_COL, lit(RowKind.DELETE.toByteValue))
        writer.write(df)
    }
  }

  /** pk IN (subquery) covering all pk columns -> key DataFrame from the subquery only. */
  private def extractSubqueryKeyDataFrame(sparkSession: SparkSession): Option[DataFrame] = {
    val primaryKeys = table.primaryKeys().asScala.toSeq
    condition match {
      // `listQuery.children` are the outer references: a correlated subquery can not be planned on
      // its own, so leave it to the scan path. Decorrelation also widens the subquery output, hence
      // the output size check.
      case InSubquery(values, listQuery: ListQuery)
          if listQuery.children.isEmpty && primaryKeys.nonEmpty &&
            values.size == primaryKeys.size && values.forall(_.isInstanceOf[Attribute]) &&
            listQuery.plan.output.size == values.size =>
        val resolver = conf.resolver
        val valueNames = values.map(_.asInstanceOf[Attribute].name)
        val coversAllPks = primaryKeys.forall(pk => valueNames.exists(resolver(_, pk))) &&
          valueNames.forall(n => primaryKeys.exists(resolver(n, _)))
        if (coversAllPks) {
          // Subquery output columns correspond positionally to `values`; rename to pk names.
          val keyDf = createDataset(sparkSession, listQuery.plan).toDF(valueNames: _*)
          // NULL never matches under SQL three-valued logic; dropping these rows keeps the fast
          // path in sync with the scan path and avoids writing -D rows with NULL primary keys.
          Some(keyDf.filter(valueNames.map(n => quotedCol(n).isNotNull).reduce(_ && _)))
        } else {
          None
        }
      case _ => None
    }
  }

  /**
   * One single column DataFrame per pk column, cross joined into the key DataFrame. Only the
   * literals of the condition itself live on the driver (they are part of the plan anyway); the
   * cartesian product is left to Spark instead of being materialized here.
   */
  private def literalKeyDataFrame(
      sparkSession: SparkSession,
      keyValues: Seq[(String, Seq[Any])]): DataFrame = {
    val attributeByName = relation.output.map(a => a.name -> a).toMap
    keyValues
      .map {
        case (pk, values) =>
          val attribute = attributeByName(pk)
          val schema = StructType(Seq(StructField(pk, attribute.dataType, attribute.nullable)))
          val rows = values.map(v => Row(convertLiteral(v, attribute.dataType)))
          sparkSession.createDataFrame(rows.asJava, schema)
      }
      .reduce(_.crossJoin(_))
  }

  /**
   * Extracts the per pk column literal values when the condition is a conjunction of pk = literal /
   * pk IN (literals) covering all pk columns; None -> fall back to the scan path.
   */
  private def extractPointDeleteKeys(): Option[Seq[(String, Seq[Any])]] = {
    val primaryKeys = table.primaryKeys().asScala.toSeq
    if (condition == null || primaryKeys.isEmpty) {
      None
    } else {
      val resolver = conf.resolver
      // pk column -> distinct literal values; matched stays true only if every conjunct fits
      val keyValues = mutable.LinkedHashMap.empty[String, Seq[Any]]
      var matched = true

      def pkName(attr: Attribute): Option[String] =
        primaryKeys.find(pk => resolver(attr.name, pk))

      def splitAnd(e: Expression): Seq[Expression] = e match {
        case And(l, r) => splitAnd(l) ++ splitAnd(r)
        case other => Seq(other)
      }

      def record(attr: Attribute, values: Seq[Any]): Unit = {
        // NULL literals never match under SQL three-valued logic; let the scan path handle them.
        if (values.exists(_ == null)) {
          matched = false
        } else {
          pkName(attr) match {
            case Some(pk) if !keyValues.contains(pk) => keyValues(pk) = values
            case _ => matched = false
          }
        }
      }

      splitAnd(condition).foreach {
        case _ if !matched => // short-circuit remaining conjuncts
        case EqualTo(attr: Attribute, Literal(v, _)) => record(attr, Seq(v))
        case EqualTo(Literal(v, _), attr: Attribute) => record(attr, Seq(v))
        case In(attr: Attribute, values) if values.forall(_.isInstanceOf[Literal]) =>
          record(attr, values.map(_.asInstanceOf[Literal].value).distinct)
        case InSet(attr: Attribute, values) => record(attr, values.toSeq)
        case _ => matched = false
      }

      if (!matched || primaryKeys.exists(pk => !keyValues.contains(pk))) {
        None
      } else {
        // Multiply with a division based check: it bounds the number of -D rows and can not
        // overflow, unlike computing the product first.
        var totalRows = 1L
        val withinLimit = keyValues.values.forall {
          values =>
            values.nonEmpty && totalRows <= pointDeleteMaxRows / values.size && {
              totalRows *= values.size
              true
            }
        }
        if (withinLimit) {
          Some(keyValues.toSeq)
        } else {
          logInfo(
            s"Point-delete rows exceed ${SparkConnectorOptions.DELETE_POINT_DELETE_MAX_ROWS.key()}" +
              s"=$pointDeleteMaxRows, falling back to scan-based delete; consider IN (subquery).")
          None
        }
      }
    }
  }

  /** `col` parses dots as nested field access, so column names have to be quoted. */
  private def quotedCol(name: String): Column = col("`" + name.replace("`", "``") + "`")

  /** Builds the -D DataFrame from a key DataFrame without reading the target table. */
  private def buildDeleteDataFrame(keyDf: DataFrame): DataFrame = {
    val keyCols = keyDf.schema.fieldNames.toSet
    val projected = relation.output.map {
      a =>
        if (keyCols.contains(a.name)) {
          quotedCol(a.name).cast(a.dataType).as(a.name)
        } else {
          lit(null).cast(a.dataType).as(a.name)
        }
    }
    keyDf
      .select(projected: _*)
      .withColumn(ROW_KIND_COL, lit(RowKind.DELETE.toByteValue))
  }

  /** Catalyst literal internal values -> external row values accepted by createDataFrame. */
  private def convertLiteral(v: Any, dataType: DataType): Any =
    CatalystTypeConverters.convertToScala(v, dataType)

  private def performNonPrimaryKeyDelete(sparkSession: SparkSession): Seq[CommitMessage] = {
    val readSnapshot = table.snapshotManager().latestSnapshot()
    // Step1: the candidate data splits which are filtered by Paimon Predicate.
    val candidateDataSplits = findCandidateDataSplits(condition, relation.output)
    val dataFilePathToMeta = candidateFileMap(candidateDataSplits)

    if (deletionVectorsEnabled) {
      // Step2: collect all the deletion vectors that marks the deleted rows.
      val deletionVectors = collectDeletionVectors(
        candidateDataSplits,
        dataFilePathToMeta,
        condition,
        relation,
        sparkSession,
        coreOptions.dataEvolutionEnabled())

      // Step3: update the touched deletion vectors and index files
      writer.persistDeletionVectors(deletionVectors, readSnapshot)
    } else {
      // Step2: extract out the exactly files, which must have at least one record to be updated.
      val touchedFilePaths =
        findTouchedFiles(candidateDataSplits, condition, relation, sparkSession)

      // Step3: the smallest range of data files that need to be rewritten.
      val (touchedFiles, newRelation) =
        extractFilesAndCreateNewScan(touchedFilePaths, dataFilePathToMeta, relation)

      // Step4: build a dataframe that contains the unchanged data, and write out them.
      // Use Not(EqualNullSafe(condition, true)) instead of Not(condition) to correctly
      // handle NULL values. Not(NULL) evaluates to NULL (filtered out), which would
      // incorrectly delete rows where the condition column is NULL.
      val toRewriteScanRelation =
        Filter(Not(EqualNullSafe(condition, Literal.TrueLiteral)), newRelation)
      var data = createDataset(sparkSession, toRewriteScanRelation)
      if (coreOptions.rowTrackingEnabled()) {
        data = selectWithRowTracking(data)
      }

      // only write new files, should have no compaction
      val addCommitMessage = writer.writeOnly().withRowTracking().write(data)

      // Step5: convert the deleted files that need to be written to commit message.
      val deletedCommitMessage = buildDeletedCommitMessage(touchedFiles)

      addCommitMessage ++ deletedCommitMessage
    }
  }
}
