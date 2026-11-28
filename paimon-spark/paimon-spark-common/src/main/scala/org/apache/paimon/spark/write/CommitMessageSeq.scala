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

import org.apache.paimon.table.sink.{CommitMessage, CommitMessageList}

import scala.collection.JavaConverters._

/** Keeps the byte-backed list intact across Scala's collection and Java commit interfaces. */
private[spark] class CommitMessageSeq(val messages: java.util.List[CommitMessage])
  extends IndexedSeq[CommitMessage] {
  override def length: Int = messages.size()
  override def apply(index: Int): CommitMessage = messages.get(index)
  override def iterator: Iterator[CommitMessage] = messages.iterator().asScala
}

private[spark] object CommitMessageSeq {
  def asJava(messages: Seq[CommitMessage]): java.util.List[CommitMessage] = messages match {
    case lazyMessages: CommitMessageSeq => lazyMessages.messages
    case _ => messages.asJava
  }

  def concat(children: Seq[Seq[CommitMessage]]): Seq[CommitMessage] =
    new CommitMessageSeq(CommitMessageList.concat(children.map(asJava).asJava))

  def map(messages: Seq[CommitMessage])(
      mapper: CommitMessage => CommitMessage): Seq[CommitMessage] =
    new CommitMessageSeq(CommitMessageList.map(asJava(messages), m => mapper(m)))
}
