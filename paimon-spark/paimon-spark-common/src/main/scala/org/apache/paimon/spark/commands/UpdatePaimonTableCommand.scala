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
package org.apache.paimon.spark.commands

import org.apache.paimon.options.Options
import org.apache.paimon.spark.{InsertInto, SparkTable}
import org.apache.paimon.spark.schema.SparkSystemColumns.ROW_KIND_COL
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.types.RowKind

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.Utils.createDataset
import org.apache.spark.sql.catalyst.analysis.{AssignmentAlignmentHelper, EliminateSubqueryAliases}
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Project, UpdateTable}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.functions.lit

case class UpdatePaimonTableCommand(u: UpdateTable)
  extends LeafRunnableCommand
  with AssignmentAlignmentHelper {

  override def run(sparkSession: SparkSession): Seq[Row] = {

    val relation = EliminateSubqueryAliases(u.table).asInstanceOf[DataSourceV2Relation]

    val updatedExprs: Seq[Alias] =
      alignUpdateAssignments(relation.output, u.assignments).zip(relation.output).map {
        case (expr, attr) => Alias(expr, attr.name)()
      }

    val updatedPlan = Project(updatedExprs, Filter(u.condition.getOrElse(TrueLiteral), relation))

    val df = createDataset(sparkSession, updatedPlan)
      .withColumn(ROW_KIND_COL, lit(RowKind.UPDATE_AFTER.toByteValue))

    WriteIntoPaimonTable(
      relation.table.asInstanceOf[SparkTable].getTable.asInstanceOf[FileStoreTable],
      InsertInto,
      df,
      Options.fromMap(relation.options)).run(sparkSession)

    Seq.empty[Row]
  }
}
