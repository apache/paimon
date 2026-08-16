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

package org.apache.paimon.spark.copyinto

import org.apache.paimon.fs.{FileIO, Path}
import org.apache.paimon.utils.JsonSerdeUtil

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.{LinkedHashMap => JLinkedHashMap}

case class CopyLoadRecord(
    filePath: String,
    fileSize: Long,
    lastModified: Long,
    loadedAt: Long,
    snapshotId: Long,
    rowsLoaded: Long) {

  def toJson: String = {
    val map = new JLinkedHashMap[String, AnyRef]()
    map.put("filePath", filePath)
    map.put("fileSize", java.lang.Long.valueOf(fileSize))
    map.put("lastModified", java.lang.Long.valueOf(lastModified))
    map.put("loadedAt", java.lang.Long.valueOf(loadedAt))
    map.put("snapshotId", java.lang.Long.valueOf(snapshotId))
    map.put("rowsLoaded", java.lang.Long.valueOf(rowsLoaded))
    JsonSerdeUtil.toJson(map)
  }
}

object CopyLoadRecord {
  def fromJson(json: String): CopyLoadRecord = {
    val map = JsonSerdeUtil.parseJsonMap(json, classOf[AnyRef])
    CopyLoadRecord(
      filePath = map.get("filePath").toString,
      fileSize = map.get("fileSize").toString.toLong,
      lastModified = map.get("lastModified").toString.toLong,
      loadedAt = map.get("loadedAt").toString.toLong,
      snapshotId = map.get("snapshotId").toString.toLong,
      rowsLoaded = map.get("rowsLoaded").toString.toLong
    )
  }
}

class CopyLoadHistoryManager(fileIO: FileIO, tablePath: Path) {

  private val LOG = org.slf4j.LoggerFactory.getLogger(classOf[CopyLoadHistoryManager])

  private val historyDir = new Path(tablePath, "copy-into/history")

  def isLoaded(filePath: String, fileSize: Long, lastModified: Long): Boolean = {
    val prefix = s"load-${sha256(filePath)}-"
    if (!fileIO.exists(historyDir)) return false
    try {
      val files = fileIO.listStatus(historyDir)
      files.exists {
        status =>
          val name = status.getPath.getName
          if (name.startsWith(prefix)) {
            try {
              val content = fileIO.readFileUtf8(status.getPath)
              val record = CopyLoadRecord.fromJson(content)
              record.fileSize == fileSize && record.lastModified == lastModified
            } catch {
              case e: Exception =>
                LOG.warn(s"Failed to read load history record ${status.getPath}: ${e.getMessage}")
                false
            }
          } else {
            false
          }
      }
    } catch {
      case e: Exception =>
        LOG.warn(s"Failed to list load history directory $historyDir: ${e.getMessage}")
        false
    }
  }

  def recordLoaded(record: CopyLoadRecord): Unit = {
    fileIO.mkdirs(historyDir)
    val hash = sha256(record.filePath)
    val recordPath = new Path(historyDir, s"load-$hash-${record.loadedAt}")
    fileIO.overwriteFileUtf8(recordPath, record.toJson)
  }

  private def sha256(input: String): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    val hash = digest.digest(input.getBytes(StandardCharsets.UTF_8))
    hash.map("%02x".format(_)).mkString.take(16)
  }
}
