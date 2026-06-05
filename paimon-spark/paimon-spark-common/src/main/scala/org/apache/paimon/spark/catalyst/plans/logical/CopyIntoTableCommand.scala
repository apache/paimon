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

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.types.{LongType, StringType}

case class CopyIntoTableCommand(
    table: Seq[String],
    columns: Option[Seq[String]],
    sourcePath: String,
    fileFormat: CopyFileFormat,
    pattern: Option[String],
    force: Boolean,
    onError: OnErrorMode = OnErrorMode.AbortStatement)
  extends PaimonLeafCommand {

  override def output: Seq[Attribute] = Seq(
    AttributeReference("file_name", StringType, nullable = false)(),
    AttributeReference("status", StringType, nullable = false)(),
    AttributeReference("rows_loaded", LongType, nullable = false)(),
    AttributeReference("rows_parsed", LongType, nullable = false)(),
    AttributeReference("errors_seen", LongType, nullable = false)(),
    AttributeReference("first_error", StringType, nullable = true)()
  )

  override def simpleString(maxFields: Int): String = {
    s"CopyIntoTable: table=$table, source=$sourcePath"
  }
}
