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

import org.apache.paimon.CoreOptions
import org.apache.paimon.options.Options
import org.apache.paimon.spark.{InsertInto, Overwrite}
import org.apache.paimon.spark.commands.{SchemaEvolutionHelper, WriteIntoPaimonTable}
import org.apache.paimon.spark.write.{CommitMarker, StreamingWrite, StreamingWriteContext}
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, PaimonUtils, SQLContext}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.AlwaysTrue
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryListener}

import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID

import scala.collection.JavaConverters._
import scala.util.Try

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
   * What the commit user has to identify is one incarnation of a checkpoint, since the
   * micro-batches it numbers are only unique within one. Paimon skips a batch whose id a previous
   * run committed under the same user, so reusing a user across two checkpoints drops the data of
   * the second one, while changing it within one brings back the duplicate. The query id Spark
   * persists in the checkpoint metadata is exactly that identity: it is new when a checkpoint is
   * recreated, unchanged when a query resumes from one, and independent of how the location is
   * spelled. It is therefore not configurable; 'commit.user-prefix' can name it.
   *
   * Resolved lazily: neither the query id nor the checkpoint location is available on the thread
   * that constructs the sink.
   */
  private lazy val commitUser: String = {
    queryId
      .map(derivedCommitUser("query", _))
      // Only reachable outside a stream execution, e.g. a direct addBatch call. A location cannot
      // tell a recreated checkpoint from a resumed one, so it is a last resort.
      .orElse(checkpointLocation.map(derivedCommitUser("checkpoint", _)))
      .getOrElse {
        logWarning(
          "This streaming write has neither a query id nor a checkpoint location to derive a " +
            "stable commit user from, so a replayed micro-batch cannot be recognised and may " +
            "be committed twice.")
        withPrefix(UUID.randomUUID().toString)
      }
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
   * The id Spark persists in the checkpoint metadata. It is a thread local of the stream execution
   * thread, so it can only be read from within [[addBatch]].
   */
  private def queryId: Option[String] =
    Option(sqlContext.sparkContext.getLocalProperty(PaimonSink.QUERY_ID_KEY)).filter(_.nonEmpty)

  private def derivedCommitUser(kind: String, value: String): String = {
    val user = withPrefix(s"spark-$kind-${UUID.nameUUIDFromBytes(value.getBytes(UTF_8))}")
    logInfo(s"Streaming writes to ${originTable.name()} commit as '$user'.")
    user
  }

  /**
   * 'commit.user-prefix' names the writers of a table, the way
   * [[org.apache.paimon.CoreOptions#createCommitUser]] prefixes the random user of a batch write.
   * It prefixes a derived user here too, so that a streaming job keeps the name its table was
   * configured with. It cannot be the identity by itself, since every writer of the table shares
   * it, so what follows it still has to be unique to the query.
   */
  private def withPrefix(user: String): String =
    Seq(
      options.get(CoreOptions.COMMIT_USER_PREFIX),
      originTable.options().get(CoreOptions.COMMIT_USER_PREFIX.key))
      .find(prefix => prefix != null && prefix.nonEmpty)
      .fold(user)(prefix => s"${prefix}_$user")

  /**
   * The writer and the committer of this run of the query, from its first micro-batch until it
   * terminates. Spark creates a sink per run, so a restarted query starts a new context and looks
   * up once what the previous run committed.
   */
  private lazy val context: StreamingWriteContext =
    new StreamingWriteContext(
      commitUser,
      markerLocation.map(new CommitMarker(_, sqlContext.sparkSession.sessionState.newHadoopConf())))

  /**
   * Where the commit marker is kept: the checkpoint of the query, which the writer names only when
   * it passes 'checkpointLocation' as an option. A checkpoint configured through
   * 'spark.sql.streaming.checkpointLocation' and a query name never reaches the options, so it is
   * taken from the running query instead. A query whose checkpoint cannot be found fails rather
   * than write without a marker, which would let a replay after snapshot expiration be committed
   * twice. Outside a stream execution there is no replay to guard.
   */
  private def markerLocation: Option[String] =
    checkpointLocation.orElse {
      queryId.map {
        id =>
          runningQueryCheckpoint(id).getOrElse {
            throw new IllegalStateException(
              s"Cannot find the checkpoint location of streaming query $id writing to " +
                s"${originTable.name()}, which is needed to recognise a replayed micro-batch " +
                s"after snapshot expiration. Set '${PaimonSink.CHECKPOINT_LOCATION}' as an " +
                "option of the writer.")
          }
      }
    }

  /**
   * The checkpoint root Spark resolved for the running query. `StreamingQueryWrapper` and
   * `StreamExecution` are internal and not in the same package across the supported Spark versions,
   * so their public accessors are called by name.
   */
  private def runningQueryCheckpoint(id: String): Option[String] =
    Try {
      val query = sqlContext.sparkSession.streams.get(UUID.fromString(id))
      val execution = query.getClass.getMethod("streamingQuery").invoke(query)
      execution.getClass.getMethod("resolvedCheckpointRoot").invoke(execution).asInstanceOf[String]
    }.toOption.filter(location => location != null && location.nonEmpty)

  private var closeRegistered = false

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    val saveMode = if (outputMode == OutputMode.Complete()) {
      Overwrite(Some(AlwaysTrue))
    } else {
      InsertInto
    }
    val newData = PaimonUtils.createNewDataFrame(data)
    val closeAfterBatch = !registerCloseOnTermination()
    try {
      WriteIntoPaimonTable(
        originTable,
        saveMode,
        newData,
        options,
        Some(StreamingWrite(context, batchId)))
        .run(sqlContext.sparkSession)
    } finally {
      // Outside a stream execution nothing ever terminates the query, so the committer of a batch
      // cannot be kept for the next one.
      if (closeAfterBatch) {
        context.close()
      }
    }
  }

  /**
   * Closes the committer of this run when the query terminates, whether it completes, fails or is
   * stopped. Returns whether the query is one whose termination Spark reports; it is not when
   * [[addBatch]] is called outside a stream execution.
   */
  private def registerCloseOnTermination(): Boolean = synchronized {
    if (closeRegistered) {
      return true
    }
    // Spark's own query id is a uuid; anything else cannot be matched against a termination.
    queryId.flatMap(id => Try(UUID.fromString(id)).toOption) match {
      case Some(id) =>
        val streams = sqlContext.sparkSession.streams
        streams.addListener(new StreamingQueryListener {
          override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

          override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {}

          override def onQueryTerminated(
              event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
            if (event.id == id) {
              try {
                context.close()
              } finally {
                streams.removeListener(this)
              }
            }
          }
        })
        closeRegistered = true
        true
      case None => false
    }
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
