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

package org.apache.paimon.spark.catalyst.plans.logical

import org.apache.paimon.spark.leafnode.PaimonLeafCommand

import org.apache.spark.sql.catalyst.expressions.Attribute

/** Logical command for ALTER TABLE ... ARCHIVE operations. */
case class AlterTableArchiveCommand(
    table: Seq[String],
    partitionSpec: Map[String, String],
    operation: ArchiveOperation)
  extends PaimonLeafCommand {

  override def output: Seq[Attribute] = Nil

  override def simpleString(maxFields: Int): String = {
    s"Alter table archive: ${operation.name} for table: $table, partition: $partitionSpec"
  }
}

/** Archive operation type. */
sealed trait ArchiveOperation {
  def name: String
}

case object ArchiveStandard extends ArchiveOperation {
  override def name: String = "ARCHIVE"
}

case object ArchiveCold extends ArchiveOperation {
  override def name: String = "COLD ARCHIVE"
}

case class RestoreArchive(duration: Option[java.time.Duration]) extends ArchiveOperation {
  override def name: String = "RESTORE ARCHIVE"
}

case object Unarchive extends ArchiveOperation {
  override def name: String = "UNARCHIVE"
}

