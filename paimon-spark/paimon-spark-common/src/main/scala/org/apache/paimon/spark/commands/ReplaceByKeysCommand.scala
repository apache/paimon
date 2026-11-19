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
import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.catalyst.analysis.PaimonOutputResolver
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.{CommitMessage, CommitMessageImpl}
import org.apache.paimon.table.source.DataSplit

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.PaimonUtils.createDataset
import org.apache.spark.sql.catalyst.expressions.{Attribute, InSubquery, ListQuery, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.functions.{col, expr}

import java.util.{Collections, TreeMap}

import scala.collection.JavaConverters._

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
    val source = ReplaceByKeysSource.bind(spark, table, sourceQuery, snapshot)
    val sourceFields = byName(source.output)
    require(
      sourceFields.keySet() == fields.keySet(),
      "Source query must supply all target columns by name")
    // Reuse INSERT's casts, nested/nullability checks and CHAR/VARCHAR handling.
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
  private[spark] def prepareDeletion(
      spark: SparkSession,
      snapshot: Snapshot,
      sourceKeys: DataFrame,
      keys: Seq[String]): Seq[CommitMessage] = {
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
    // An empty baseline must not start reading a concurrent writer's first snapshot.
    if (snapshot == null) return Seq.empty
    val reader = table.newSnapshotReader().withSnapshot(snapshot)
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
      val targetPlan = target.queryExecution.analyzed
      val values = keys.map(key => targetPlan.output.find(_.name == key).get)
      // Let Catalyst apply IN coercion before rewriting membership as a semi join.
      val matches = InSubquery(values, ListQuery(sourceKeys.queryExecution.analyzed))
      val rowIds = createDataset(spark, Filter(matches, targetPlan)).select(col("target._ROW_ID"))
      val vectors =
        collectDataEvolutionDeletionVectors(splits, candidateFileMap(splits), rowIds, spark)
      writer.persistDeletionVectors(vectors, snapshot)
    } else Seq.empty
  }

  private def quoted(name: String): String = "`" + name.replace("`", "``") + "`"
}

object ReplaceByKeysCommand {

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
