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

import org.apache.paimon.Snapshot
import org.apache.paimon.spark.SparkConnectorOptions
import org.apache.paimon.spark.catalyst.analysis.expressions.ExpressionHelper
import org.apache.paimon.spark.schema.SparkSystemColumns.ROW_KIND_COL
import org.apache.paimon.spark.util.OptionUtils
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.PrimaryKeyTableUtils.validatePKUpsertDeletable
import org.apache.paimon.table.sink.CommitMessage
import org.apache.paimon.types.RowKind

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
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

  private def performPrimaryKeyDelete(sparkSession: SparkSession): Seq[CommitMessage] = {
    // Fast path: when the matched keys are fully described by the condition itself — literals
    // (pk = v / pk IN (...)) or a pk IN (subquery) — build -D rows without scanning the target
    // table. Absent keys are harmless (merged away in compaction). Otherwise fall back to scan.
    val keyDf = extractPointDeleteKeys()
      .map(literalKeyDataFrame(sparkSession, _))
      .orElse(extractSubqueryKeyDataFrame(sparkSession))
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
      case InSubquery(values, ListQuery(plan, _, _, _, _, _))
          if primaryKeys.nonEmpty && values.size == primaryKeys.size &&
            values.forall(_.isInstanceOf[Attribute]) =>
        val resolver = conf.resolver
        val valueNames = values.map(_.asInstanceOf[Attribute].name)
        val coversAllPks = primaryKeys.forall(pk => valueNames.exists(resolver(_, pk))) &&
          valueNames.forall(n => primaryKeys.exists(resolver(n, _)))
        if (coversAllPks) {
          // Subquery output columns correspond positionally to `values`; rename to pk names.
          val keyDf = createDataset(sparkSession, plan)
          Some(keyDf.toDF(valueNames: _*))
        } else {
          None
        }
      case _ => None
    }
  }

  /** Materializes extracted literal key rows into a small local DataFrame. */
  private def literalKeyDataFrame(
      sparkSession: SparkSession,
      keyRows: Seq[Map[String, Any]]): DataFrame = {
    val tableSchema = StructType(
      relation.output.map(a => StructField(a.name, a.dataType, a.nullable)))
    val pkSchema = StructType(tableSchema.fields.filter(f => keyRows.head.contains(f.name)))
    val sparkRows =
      keyRows.map(m => Row.fromSeq(pkSchema.fields.map(f => convertLiteral(m(f.name), f.dataType))))
    sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(sparkRows, 1), pkSchema)
  }

  /**
   * Extracts key rows when the condition is a conjunction of pk = literal / pk IN (literals)
   * covering all pk columns; None -> fall back to the scan path.
   */
  private def extractPointDeleteKeys(): Option[Seq[Map[String, Any]]] = {
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
        pkName(attr) match {
          case Some(pk) if !keyValues.contains(pk) => keyValues(pk) = values
          case _ => matched = false
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

      lazy val totalRows = keyValues.values.map(_.size.toLong).product
      if (!matched || primaryKeys.exists(pk => !keyValues.contains(pk)) || totalRows <= 0) {
        None
      } else if (totalRows > pointDeleteMaxRows) {
        logInfo(
          s"Point-delete rows $totalRows exceeds ${SparkConnectorOptions.DELETE_POINT_DELETE_MAX_ROWS.key()}" +
            s"=$pointDeleteMaxRows, falling back to scan-based delete; consider IN (subquery).")
        None
      } else {
        // cartesian product of per-column value lists
        var rows: Seq[Map[String, Any]] = Seq(Map.empty)
        for ((pk, values) <- keyValues) {
          rows = for (row <- rows; v <- values) yield row + (pk -> v)
        }
        Some(rows)
      }
    }
  }

  /** Builds the -D DataFrame from a key DataFrame without reading the target table. */
  private def buildDeleteDataFrame(keyDf: DataFrame): DataFrame = {
    val keyCols = keyDf.schema.fieldNames.toSet
    val projected = relation.output.map {
      a =>
        if (keyCols.contains(a.name)) {
          col(a.name).cast(a.dataType).as(a.name)
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
