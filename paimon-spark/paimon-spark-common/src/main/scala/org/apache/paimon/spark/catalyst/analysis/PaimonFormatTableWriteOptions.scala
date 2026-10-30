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

package org.apache.paimon.spark.catalyst.analysis

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.FormatTableFileIndex
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.text.TextFileFormat

import java.util.Locale

/**
 * Spark writes an INSERT into an engine format table through the V1 path, with the options of the
 * statement only. For CSV and TEXT tables, whose reads use the options of the table, add them, so
 * the files are written with the same format options.
 */
object PaimonFormatTableWriteOptions extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case c: InsertIntoHadoopFsRelationCommand
        if c.fileFormat.isInstanceOf[CSVFileFormat] || c.fileFormat.isInstanceOf[TextFileFormat] =>
      c.fileIndex match {
        case Some(index: FormatTableFileIndex) =>
          // a statement option overrides the table option of the same key, case-insensitively
          val statementKeys = c.options.keySet.map(_.toLowerCase(Locale.ROOT))
          val tableOptions = index.tableOptions.filter {
            case (k, _) => !statementKeys.contains(k.toLowerCase(Locale.ROOT))
          }
          if (tableOptions.isEmpty) c else c.copy(options = tableOptions ++ c.options)
        case _ => c
      }
  }
}
