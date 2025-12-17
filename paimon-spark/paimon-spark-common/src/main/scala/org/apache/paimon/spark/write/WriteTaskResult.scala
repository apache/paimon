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

import org.apache.paimon.table.sink.{CommitMessage, CommitMessageSerializer}

import org.apache.spark.sql.connector.write.WriterCommitMessage

/**
 * Basic task [[WriterCommitMessage]] which should be serializable and will be sent back to driver
 * side.
 */
trait WriteTaskResult extends WriterCommitMessage {
  def commitMessages(): Seq[CommitMessage]
}

object WriteTaskResult {

  /** Merge all taskMessage, usually called in driver side. */
  def merge(taskMessages: Seq[WriterCommitMessage]): Seq[CommitMessage] = {
    taskMessages.collect { case t: WriteTaskResult => t.commitMessages() }.flatten
  }
}

/** WriteTaskResult created by paimon inner table. */
case class InnerTableWriteTaskResult private (serializedMessageBytes: Seq[Array[Byte]])
  extends WriteTaskResult {
  def commitMessages(): Seq[CommitMessage] = {
    val deserializer = new CommitMessageSerializer()
    serializedMessageBytes.map(deserializer.deserialize(deserializer.getVersion, _))
  }
}

object InnerTableWriteTaskResult {
  def fromCommitMessages(commitMessages: Seq[CommitMessage]): InnerTableWriteTaskResult = {
    val serializer = new CommitMessageSerializer()
    InnerTableWriteTaskResult(commitMessages.map(serializer.serialize))
  }
}

/** WriteTaskResult created by paimon format table. */
case class FormatTableWriteTaskResult(commitMessages: Seq[CommitMessage]) extends WriteTaskResult
