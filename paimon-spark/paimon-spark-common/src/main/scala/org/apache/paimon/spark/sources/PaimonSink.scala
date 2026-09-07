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

package org.apache.paimon.spark.sources

import org.apache.paimon.options.Options
import org.apache.paimon.spark.{InsertInto, Overwrite, SparkConnectorOptions}
import org.apache.paimon.spark.commands.{SchemaEvolutionHelper, WriteIntoPaimonTable}
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, PaimonUtils, SQLContext}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.AlwaysTrue
import org.apache.spark.sql.streaming.OutputMode

import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID

import scala.collection.JavaConverters._

class PaimonSink(
    sqlContext: SQLContext,
    override val originTable: FileStoreTable,
    partitionColumns: Seq[String],
    outputMode: OutputMode,
    options: Options)
  extends Sink
  with SchemaEvolutionHelper
  with Logging {

  /**
   * Structured Streaming replays a micro-batch with its original batch id when a query is restarted
   * after failing between this sink returning from [[addBatch]] and Spark recording the batch as
   * completed. Committing every batch under a commit user that is stable across restarts lets
   * Paimon skip such a replay instead of committing its data twice.
   *
   * Resolved lazily: neither the checkpoint location nor the query id is available on the thread
   * that constructs the sink.
   */
  private lazy val commitUser: String = {
    configuredCommitUser.getOrElse {
      checkpointLocation
        .map(derivedCommitUser("checkpoint", _))
        .orElse(queryId.map(derivedCommitUser("query", _)))
        .getOrElse {
          logWarning(
            "This streaming write has neither a checkpoint location nor a query id to derive a " +
              "stable commit user from, so a replayed micro-batch cannot be recognised and may " +
              s"be committed twice. Set '${SparkConnectorOptions.STREAM_WRITE_COMMIT_USER.key}' " +
              "to make the write idempotent.")
          UUID.randomUUID().toString
        }
    }
  }

  // Like the read side, which takes its 'read.stream.*' options from the table, so that a
  // 'spark.paimon.<key>' session conf works the same as an option of the writer.
  private def configuredCommitUser: Option[String] = {
    val fromWriter = options.get(SparkConnectorOptions.STREAM_WRITE_COMMIT_USER)
    val fromTable =
      Options.fromMap(originTable.options()).get(SparkConnectorOptions.STREAM_WRITE_COMMIT_USER)
    Seq(fromWriter, fromTable).find(user => user != null && user.nonEmpty)
  }

  // Spark hands the sink its options case-insensitively, but keeps whatever case the user wrote.
  private def checkpointLocation: Option[String] =
    options.toMap.asScala.collectFirst {
      case (key, value)
          if key.equalsIgnoreCase(PaimonSink.CHECKPOINT_LOCATION) && value != null &&
            value.nonEmpty =>
        value
    }

  /**
   * The id Spark persists in the checkpoint metadata, hence stable across restarts of the same
   * query. It covers the case of a checkpoint location that never reaches the sink options, for
   * example one taken from `spark.sql.streaming.checkpointLocation`. It is a thread local of the
   * stream execution thread, so it can only be read from within [[addBatch]].
   */
  private def queryId: Option[String] =
    Option(sqlContext.sparkContext.getLocalProperty(PaimonSink.QUERY_ID_KEY)).filter(_.nonEmpty)

  private def derivedCommitUser(kind: String, value: String): String = {
    val user = s"spark-$kind-${UUID.nameUUIDFromBytes(value.getBytes(UTF_8))}"
    logInfo(s"Streaming writes to ${originTable.name()} commit as '$user'.")
    user
  }

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    val saveMode = if (outputMode == OutputMode.Complete()) {
      Overwrite(Some(AlwaysTrue))
    } else {
      InsertInto
    }
    val newData = PaimonUtils.createNewDataFrame(data)
    WriteIntoPaimonTable(originTable, saveMode, newData, options, Some(batchId), Some(commitUser))
      .run(sqlContext.sparkSession)
  }
}

object PaimonSink {

  private val CHECKPOINT_LOCATION = "checkpointLocation"

  /**
   * `org.apache.spark.sql.execution.streaming.StreamExecution.QUERY_ID_KEY`, inlined because that
   * class is not in the same package across all supported Spark versions.
   */
  private val QUERY_ID_KEY = "sql.streaming.queryId"
}
