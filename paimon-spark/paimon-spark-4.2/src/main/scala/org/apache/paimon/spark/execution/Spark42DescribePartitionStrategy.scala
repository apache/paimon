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

package org.apache.paimon.spark.execution

import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.catalog.SparkBaseCatalog

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{ResolvedPartitionSpec, ResolvedTable}
import org.apache.spark.sql.catalyst.plans.logical.{DescribeTablePartition, LogicalPlan}
import org.apache.spark.sql.execution.{PaimonDescribeTableExec, SparkPlan, SparkStrategy}

/**
 * Plans Spark 4.2's `DescribeTablePartition` on a Paimon table with [[PaimonDescribeTableExec]],
 * which `PaimonStrategy` uses for DESCRIBE ... PARTITION on earlier Spark versions.
 */
case class Spark42DescribePartitionStrategy(spark: SparkSession) extends SparkStrategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case DescribeTablePartition(
          r @ ResolvedTable(catalog: SparkBaseCatalog, _, table: SparkTable, _),
          spec: ResolvedPartitionSpec,
          isExtended,
          output) =>
      PaimonDescribeTableExec(output, catalog, r.identifier, table, Some(spec), isExtended) :: Nil
    case _ => Nil
  }
}
