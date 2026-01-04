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

import org.apache.paimon.spark.SparkTable

import org.apache.spark.sql.{types, PaimonUtils}
import org.apache.spark.sql.catalyst.analysis.{PartitionSpec, ResolvedPartitionSpec}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, V2PartitionCommand}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits.TableHelper
import org.apache.spark.sql.types.StructType;

/** Drop partitions command. */
case class PaimonDropPartitions(
    table: LogicalPlan,
    parts: Seq[PartitionSpec],
    ifExists: Boolean,
    purge: Boolean)
  extends V2PartitionCommand {

  override def allowPartialPartitionSpec: Boolean = true
  override protected def withNewChildInternal(newChild: LogicalPlan): PaimonDropPartitions =
    copy(table = newChild)
}

/**
 * If a paimon table has partition spec like (pt1, pt2, pt3), then the drop partition command must
 * provide all fields or a prefix subset of partition fields. For example, (pt1 = 'v1', pt2 = 'v2')
 * is valid, but (pt2 = 'v2') is not.
 */
object PaimonDropPartitions {
  def validate(table: SparkTable, partialSpecs: Seq[ResolvedPartitionSpec]): Unit = {
    val partitionSchema = table.asPartitionable.partitionSchema();
    partialSpecs.foreach {
      partialSpec =>
        if (!partitionSchema.names.toSeq.startsWith(partialSpec.names)) {
          val values = partialSpec.names.zipWithIndex.map {
            case (name, ordinal) =>
              partialSpec.ident.get(ordinal, partitionSchema.apply(name).dataType).toString
          }
          val spec = partialSpec.names
            .zip(values)
            .map { case (name, value) => s"$name = '$value'" }
            .mkString(",")
          throw PaimonUtils.invalidPartitionSpecError(spec, partitionSchema.fieldNames, table.name)
        }
    }
  }
}
