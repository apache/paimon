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

import java.nio.charset.StandardCharsets.UTF_8

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
 * The marker is written before the commit, so a micro-batch without one never reached its commit.
 */
class CommitMarker(checkpointLocation: String, hadoopConf: Configuration) {

  val path: Path = new Path(new Path(checkpointLocation, "paimon"), "commit-marker")

  def write(commitUser: String, batchId: Long, latestSnapshotId: Long): Unit = {
    val fs = path.getFileSystem(hadoopConf)
    val out = fs.create(path, true)
    try {
      out.write(s"$commitUser\n$batchId\n$latestSnapshotId\n".getBytes(UTF_8))
    } finally {
      out.close()
    }
  }

  /**
   * The latest snapshot id recorded before `batchId` of `commitUser` was committed, if that batch
   * got as far as its commit. A marker left by another batch or another query, or one that was not
   * written completely, says nothing about this batch.
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
    lines match {
      case user :: batch :: snapshot :: Nil
          if user == commitUser && Try(batch.toLong).toOption.contains(batchId) =>
        Try(snapshot.toLong).toOption
      case _ => None
    }
  }
}
