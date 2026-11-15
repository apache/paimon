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

import org.apache.paimon.{CoreOptions, Snapshot}
import org.apache.paimon.spark.{PaimonScanBuilder, SparkTable}
import org.apache.paimon.spark.catalyst.analysis.PaimonOutputResolver
import org.apache.paimon.table.{FileStoreTable, KnownSplitsTable}
import org.apache.paimon.table.sink.{CommitMessage, CommitMessageImpl}
import org.apache.paimon.table.source.{DataSplit, Split}
import org.apache.paimon.table.source.snapshot.TimeTravelUtil

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.PaimonUtils.createDataset
import org.apache.spark.sql.catalyst.expressions.{Attribute, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.optimizer.ComputeCurrentTime
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Command, CTERelationRef, Deduplicate, GlobalLimit, LocalLimit, LocalRelation, LogicalPlan, OneRowRelation, ParsedStatement, Range, Sample, Tail, Window}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.paimon.shims.SparkShimLoader
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.{Collections, TreeMap}

import scala.collection.JavaConverters._
import scala.collection.mutable

/** Prepare a key-based delete and append, publishing both in one data commit. */
case class ReplaceByKeysCommand(
    override val table: FileStoreTable,
    relation: DataSourceV2Relation,
    sourceQuery: String,
    keyColumns: Seq[String],
    targetFilter: Option[String])
  extends PaimonRowLevelCommand {

  override def run(spark: SparkSession): Seq[Row] = {
    require(coreOptions.dataEvolutionEnabled(), "replace_by_keys requires a Data Evolution table")
    require(
      coreOptions.deletionVectorsEnabled(),
      "replace_by_keys requires deletion-vectors.enabled=true")
    val (messages, readSnapshotId) = prepare(spark)
    val checked =
      if (readSnapshotId < 0) messages
      else messages.map(_.asInstanceOf[CommitMessageImpl].withCheckFromSnapshot(readSnapshotId))
    // Once publication is attempted an exception can have an unknown outcome. Do not abort it.
    writer.commit(checked, Snapshot.Operation.MERGE)
    Seq.empty
  }

  private[spark] def prepare(spark: SparkSession): (Seq[CommitMessage], Long) = {
    val snapshot = table.snapshotManager().latestSnapshot()
    def byName(attributes: Seq[Attribute]): TreeMap[String, Attribute] = {
      val result =
        if (spark.sessionState.conf.caseSensitiveAnalysis) new TreeMap[String, Attribute]()
        else new TreeMap[String, Attribute](String.CASE_INSENSITIVE_ORDER)
      attributes.foreach {
        attribute =>
          require(!result.containsKey(attribute.name), s"Ambiguous column name: ${attribute.name}")
          result.put(attribute.name, attribute)
      }
      result
    }
    val fields = byName(relation.output)
    require(keyColumns.nonEmpty, "key_columns must not be empty")
    val keys = keyColumns.map {
      key =>
        require(fields.containsKey(key), s"Unknown target key column: $key")
        fields.get(key).name
    }
    require(keys.distinct.size == keyColumns.size, "Duplicate key columns")
    val source = stableSource(spark, snapshot)
    val sourceFields = byName(source.output)
    require(
      sourceFields.keySet() == fields.keySet(),
      "Source query must supply all target columns by name")
    // Reuse INSERT's assignment policy, nested/nullability checks and CHAR/VARCHAR handling.
    val aligned = PaimonOutputResolver.resolveOutputColumns(
      table.name(),
      relation.output,
      source,
      byName = true,
      mergeSchemaEnabled = false)
    val input = createDataset(spark, aligned)
    // Match original query keys: an INSERT cast must not change which old rows are deleted.
    val sourceKeys = createDataset(spark, source)
      .select(keys.map(k => col(quoted(sourceFields.get(k).name)).as(k)): _*)
      .alias("source_keys")

    var staged = Seq.empty[CommitMessage]
    try {
      staged = prepareDeletion(spark, snapshot, sourceKeys, keys)
      staged = staged ++ writer.write(input)
      (staged, if (snapshot == null) -1L else snapshot.id())
    } catch {
      case failure: Throwable =>
        if (staged.nonEmpty) {
          try {
            val cleanup = writer.writeBuilder.newCommit()
            try cleanup.abort(staged.asJava)
            finally cleanup.close()
          } catch {
            case cleanupFailure: Throwable =>
              if (cleanupFailure ne failure) failure.addSuppressed(cleanupFailure)
          }
        }
        throw failure
    }
  }

  // Release the target scan and full file inventory before starting the append job.
  private def prepareDeletion(
      spark: SparkSession,
      snapshot: Snapshot,
      sourceKeys: DataFrame,
      keys: Seq[String]): Seq[CommitMessage] = {
    val reader = table.newSnapshotReader()
    if (snapshot != null) reader.withSnapshot(snapshot)
    val predicate = targetFilter
      .map {
        text =>
          val parsed = spark.sessionState.sqlParser.parseExpression(text)
          require(
            !SubqueryExpression.hasSubquery(parsed),
            "target_filter must be a deterministic predicate over target columns")
          val resolved = createDataset(spark, relation)
            .filter(expr(text))
            .queryExecution
            .analyzed
            .collect { case f: org.apache.spark.sql.catalyst.plans.logical.Filter => f.condition }
            .head
          require(resolved.deterministic, "target_filter must be deterministic")
          resolved
      }
      .getOrElse(TrueLiteral)
    convertConditionToPaimonPredicate(
      predicate,
      relation.output,
      rowType,
      ignorePartialFailure = true)
      .foreach(reader.withFilter)
    if (coreOptions.manifestDeleteFileDropStats()) reader.dropStats()
    val splits = reader.read().splits().asScala.collect { case d: DataSplit => d }.toSeq
    if (splits.nonEmpty) {
      val rowId = (relation.output ++ relation.metadataOutput).find(_.name == "_ROW_ID").get
      val scan = createNewScanPlan(splits, relation).copy(output = relation.output :+ rowId)
      val target = createDataset(spark, scan).filter(toColumn(predicate)).alias("target")
      val matches = keys
        .map(key => col("target." + quoted(key)) === col("source_keys." + quoted(key)))
        .reduce(_ && _)
      // Semi join has the same TRUE-only membership semantics as DELETE WHERE key IN source.
      val rowIds = target.join(sourceKeys, matches, "left_semi").select(col("target._ROW_ID"))
      val vectors =
        collectDataEvolutionDeletionVectors(splits, candidateFileMap(splits), rowIds, spark)
      writer.persistDeletionVectors(vectors, snapshot)
    } else Seq.empty
  }

  private def stableSource(spark: SparkSession, targetSnapshot: Snapshot): LogicalPlan = {
    require(sourceQuery != null && sourceQuery.trim.nonEmpty, "source_query must not be empty")
    val parsed = spark.sessionState.sqlParser.parsePlan(sourceQuery)
    parsed.foreach {
      case _: Command | _: ParsedStatement =>
        throw new IllegalArgumentException("source_query must be a query, not a command")
      case _ =>
    }
    // Dataset construction eagerly executes commands. Analyze without creating one first.
    val analyzed = spark.sessionState.executePlan(parsed).analyzed
    analyzed.foreach {
      case _: Command =>
        throw new IllegalArgumentException("source_query must be a query, not a command")
      case _ =>
    }
    require(!analyzed.isStreaming, "Streaming source queries are not supported")
    val selectors = Seq(
      CoreOptions.SCAN_SNAPSHOT_ID.key(),
      CoreOptions.SCAN_TAG_NAME.key(),
      CoreOptions.SCAN_WATERMARK.key(),
      CoreOptions.SCAN_TIMESTAMP.key(),
      CoreOptions.SCAN_TIMESTAMP_MILLIS.key(),
      CoreOptions.SCAN_VERSION.key()
    )
    val snapshots = mutable.Map
      .empty[(String, String, Seq[Option[String]], CoreOptions.StartupMode), Option[Snapshot]]
    snapshots.put(
      (
        table.location().toString,
        table.coreOptions().branch(),
        selectors.map(k => Option(table.options().get(k))),
        CoreOptions.StartupMode.LATEST_FULL),
      Option(targetSnapshot)
    )
    // Query-time values must not be evaluated afresh by the two preparation jobs.
    ComputeCurrentTime(analyzed).transformUpWithSubqueries {
      case r: DataSourceV2Relation =>
        require(r.expressions.forall(_.deterministic), "Source query must be deterministic")
        r.table match {
          case s @ SparkTable(fileTable: FileStoreTable) =>
            val effective = fileTable.copyWithoutTimeTravel(r.options.asCaseSensitiveMap())
            val mode = effective.coreOptions().startupMode()
            require(
              Set(
                CoreOptions.StartupMode.LATEST_FULL,
                CoreOptions.StartupMode.LATEST,
                CoreOptions.StartupMode.FROM_TIMESTAMP,
                CoreOptions.StartupMode.FROM_SNAPSHOT,
                CoreOptions.StartupMode.FROM_SNAPSHOT_FULL
              ).contains(mode),
              "Source tables must use a full snapshot scan, not an incremental or file-time scan"
            )
            require(
              effective.coreOptions().toConfiguration.get(CoreOptions.BATCH_SCAN_MODE) ==
                CoreOptions.BatchScanMode.NONE,
              "Source monitoring scans are not supported"
            )
            val normalizedMode = mode match {
              case CoreOptions.StartupMode.LATEST => CoreOptions.StartupMode.LATEST_FULL
              case CoreOptions.StartupMode.FROM_SNAPSHOT_FULL =>
                CoreOptions.StartupMode.FROM_SNAPSHOT
              case other => other
            }
            val identity = (
              effective.location().toString,
              effective.coreOptions().branch(),
              selectors.map(k => Option(effective.options().get(k))),
              normalizedMode)
            val historicalKeys =
              if (normalizedMode == CoreOptions.StartupMode.FROM_TIMESTAMP)
                Seq(CoreOptions.SCAN_TIMESTAMP.key(), CoreOptions.SCAN_TIMESTAMP_MILLIS.key())
              else
                selectors.filterNot(
                  k =>
                    k == CoreOptions.SCAN_TIMESTAMP.key() ||
                      k == CoreOptions.SCAN_TIMESTAMP_MILLIS.key())
            require(
              normalizedMode == CoreOptions.StartupMode.LATEST_FULL ||
                historicalKeys.exists(k => effective.options().get(k) != null),
              "Historical source mode requires an explicit matching snapshot selector"
            )
            val selected = snapshots.getOrElseUpdate(
              identity,
              if (normalizedMode == CoreOptions.StartupMode.LATEST_FULL)
                Option(effective.snapshotManager().latestSnapshot())
              else {
                val historical = TimeTravelUtil.tryTravelToSnapshot(effective)
                require(historical.isPresent, "Could not bind the requested source snapshot")
                Some(historical.get())
              }
            )
            val scanTable = selected match {
              case Some(snapshot) =>
                val options = new java.util.HashMap[String, String]()
                selectors.foreach(k => options.put(k, null))
                options.put(
                  CoreOptions.SCAN_MODE.key(),
                  CoreOptions.StartupMode.FROM_SNAPSHOT.toString)
                options.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), snapshot.id().toString)
                new ReplaceByKeysCommand.PinnedSourceTable(effective.copyWithoutTimeTravel(options))
              case None => s.copy(table = KnownSplitsTable.create(effective, Array.empty[Split]))
            }
            SparkShimLoader.shim.copyDataSourceV2Relation(r, scanTable, r.output)
          case _ =>
            throw new IllegalArgumentException("Source tables must support Paimon snapshots")
        }
      case p =>
        require(p.expressions.forall(_.deterministic), "Source query must be deterministic")
        val unstable = p match {
          case _: Aggregate | _: Deduplicate | _: GlobalLimit | _: LocalLimit | _: Sample |
              _: Tail | _: Window =>
            true
          // OFFSET is unavailable in older Spark compilation targets.
          case _ => p.nodeName == "Offset"
        }
        require(
          !unstable,
          "Source query must be repeatable across projections; materialize aggregate, window, " +
            "limit, offset, deduplication or sampled batches into a Paimon table first"
        )
        require(
          p.children.nonEmpty || p.isInstanceOf[LocalRelation] || p.isInstanceOf[CTERelationRef] ||
            p.isInstanceOf[OneRowRelation] || p.isInstanceOf[Range],
          "Source leaves must be Paimon tables or immutable SQL values"
        )
        p
    }
  }

  private def quoted(name: String): String = "`" + name.replace("`", "``") + "`"
}

object ReplaceByKeysCommand {

  // Keep the analyzed schema while pinning data. A normal scan builder copies the table again,
  // which both reapplies relation options and travels to the snapshot's historical schema.
  private class PinnedSourceTable(pinned: FileStoreTable) extends SparkTable(pinned) {
    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
      new PaimonScanBuilder(pinned)
  }

  /** Java procedure bridge; no table configuration is persisted. */
  def execute(
      spark: SparkSession,
      relation: DataSourceV2Relation,
      query: String,
      keys: java.util.List[String],
      filter: String): Unit = {
    val table = relation.table.asInstanceOf[SparkTable].getTable.asInstanceOf[FileStoreTable]
    val writeOnly = table.copy(Collections.singletonMap(CoreOptions.WRITE_ONLY.key(), "true"))
    ReplaceByKeysCommand(writeOnly, relation, query, keys.asScala.toSeq, Option(filter)).run(spark)
  }
}
