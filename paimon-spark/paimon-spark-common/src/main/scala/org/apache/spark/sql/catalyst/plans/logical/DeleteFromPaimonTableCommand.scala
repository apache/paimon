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
package org.apache.spark.sql.catalyst.plans.logical

import org.apache.paimon.options.Options
import org.apache.paimon.spark.{InsertInto, SparkTable}
import org.apache.paimon.spark.commands.WriteIntoPaimonTable
import org.apache.paimon.spark.schema.SparkSystemColumns.ROW_KIND_COL
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.types.RowKind

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.functions.lit

case class DeleteFromPaimonTableCommand(relation: DataSourceV2Relation, condition: Expression)
  extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val filteredPlan = if (condition != null) {
      Filter(condition, relation)
    } else {
      relation
    }
    val df = Dataset
      .ofRows(sparkSession, filteredPlan)
      .withColumn(ROW_KIND_COL, lit(RowKind.DELETE.toByteValue))

    WriteIntoPaimonTable(
      relation.table.asInstanceOf[SparkTable].getTable.asInstanceOf[FileStoreTable],
      InsertInto,
      df,
      Options.fromMap(relation.options)).run(sparkSession)

    Seq.empty[Row]
  }
}
