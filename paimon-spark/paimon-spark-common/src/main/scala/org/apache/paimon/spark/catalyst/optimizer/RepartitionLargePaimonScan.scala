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

package org.apache.paimon.spark.catalyst.optimizer

import org.apache.paimon.spark.{PaimonScan, SparkConnectorOptions}
import org.apache.paimon.spark.read.BinPackingSplits
import org.apache.paimon.spark.util.{OptionUtils, SplitUtils}
import org.apache.paimon.types.BlobType

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Repartition, RepartitionOperation}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.internal.SQLConf

import scala.collection.JavaConverters._

/** Redistributes oversized scan partitions for downstream operators, preserving the scan itself. */
object RepartitionLargePaimonScan extends Rule[LogicalPlan] {

  // injectOptimizerRule runs before V2 scan pushdown. The final user-provided optimizer batch
  // runs after it, so register there as well when Spark initializes its optimizer rules.
  def register(spark: SparkSession): Unit = {
    val experimental = spark.experimental
    if (!experimental.extraOptimizations.contains(this)) {
      experimental.extraOptimizations = experimental.extraOptimizations :+ this
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (
      !OptionUtils
        .getOptionString(SparkConnectorOptions.READ_REPARTITION_LARGE_SCAN_ENABLED)
        .toBoolean
    ) {
      return plan
    }

    def rewrite(node: LogicalPlan): LogicalPlan = node match {
      // Preserve an explicit shuffle and make repeated applications of this rule idempotent.
      case repartition: RepartitionOperation
          if repartition.shuffle && repartition.child.isInstanceOf[DataSourceV2ScanRelation] =>
        repartition
      case relation: DataSourceV2ScanRelation if !relation.isStreaming =>
        relation.scan match {
          case scan: PaimonScan
              if scan.table
                .rowType()
                .getFields
                .asScala
                .exists(field => BlobType.isBlobFileField(field.`type`())) =>
            val partitions = scan.inputPartitions
            val targetSize = BinPackingSplits.filesMaxPartitionBytes(scan.coreOptions, SQLConf.get)
            val threshold = BigInt(targetSize) * 2
            val sizes = partitions.map {
              partition => partition.splits.map(split => BigInt(SplitUtils.splitSize(split))).sum
            }
            if (targetSize > 0 && sizes.exists(_ > threshold)) {
              val numPartitions = (sizes.sum + targetSize - 1) / targetSize
              require(
                numPartitions <= Int.MaxValue,
                s"Scan repartition count $numPartitions exceeds ${Int.MaxValue}; " +
                  "increase the scan split target size"
              )
              Repartition(numPartitions.toInt, shuffle = true, relation)
            } else {
              relation
            }
          case _ => relation
        }
      case other => other.mapChildren(rewrite)
    }

    rewrite(plan)
  }
}
