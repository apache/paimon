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

package org.apache.paimon.spark.write

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import java.io.IOException
import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID

import scala.io.Source
import scala.util.Try

/**
 * What a streaming query was about to commit, kept under its checkpoint location.
 *
 * A replayed micro-batch is recognised by the snapshots of its commit user, which snapshot
 * expiration eventually removes. Before committing a micro-batch the sink records its batch id and
 * the latest snapshot of the table at that moment: whatever that commit produced is newer than that
 * snapshot, so as long as the table still retains the snapshot after it, a lookup of the commit
 * user sees everything the micro-batch may have committed. Once expiration has gone past it, the
 * lookup can no longer tell, and the replay must not be committed blindly.
 *
 * A marker is published before committing and is never replaced by a retry of the same batch. Only
 * a later batch (or a new query) replaces it, after writing its complete contents to a temp file. A
 * missing marker therefore means this batch never reached its commit.
 */
class CommitMarker(checkpointLocation: String, hadoopConf: Configuration) {

  val path: Path = new Path(new Path(checkpointLocation, "paimon"), "commit-marker")

  def write(commitUser: String, batchId: Long, latestSnapshotId: Long): Unit = {
    if (latestSnapshotIdBefore(commitUser, batchId).isDefined) {
      return
    }
    val fs = path.getFileSystem(hadoopConf)
    val temporary = new Path(path.getParent, s".commit-marker-${UUID.randomUUID()}")
    try {
      val out = fs.create(temporary, false)
      try {
        out.write(s"$commitUser\n$batchId\n$latestSnapshotId\n".getBytes(UTF_8))
      } finally {
        out.close()
      }
      // Spark has only one writer per checkpoint. The previous marker belongs to a different
      // batch/query, so losing it here is safe: this batch has not reached its commit yet. Never
      // delete a marker of this same batch, even if its first attempt has already committed.
      if (fs.exists(path) && !fs.delete(path, false)) {
        throw new IOException(s"Cannot replace commit marker $path")
      }
      if (!fs.rename(temporary, path)) {
        throw new IOException(s"Cannot publish commit marker $path")
      }
    } finally {
      fs.delete(temporary, false)
    }
  }

  /**
   * The boundary of the first attempt of `batchId` of `commitUser`. A marker of another batch or
   * query says nothing about this one. A corrupt marker, however, might have been truncated by an
   * older sink retrying an already committed batch, so it must not be treated as a missing marker.
   */
  def latestSnapshotIdBefore(commitUser: String, batchId: Long): Option[Long] = {
    val fs = path.getFileSystem(hadoopConf)
    if (!fs.exists(path)) {
      return None
    }
    val in = fs.open(path)
    val lines =
      try {
        Source.fromInputStream(in, UTF_8.name()).getLines().toList
      } finally {
        in.close()
      }
    val entry = lines match {
      case user :: batch :: snapshot :: Nil =>
        for {
          id <- Try(batch.toLong).toOption.filter(_ >= 0)
          before <- Try(snapshot.toLong).toOption.filter(_ >= 0)
          if user.nonEmpty
        } yield (user, id, before)
      case _ => None
    }
    entry match {
      case Some((user, batch, snapshot)) =>
        if (user == commitUser && batch == batchId) Some(snapshot) else None
      case None =>
        throw new IllegalStateException(
          s"Cannot read commit marker $path. It may belong to an already committed micro-batch; " +
            "restore a valid marker or verify the table before deleting it and restarting.")
    }
  }
}
