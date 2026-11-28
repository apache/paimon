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

import org.apache.paimon.CoreOptions
import org.apache.paimon.CoreOptions.PartitionSinkStrategy
import org.apache.paimon.spark.catalyst.Compatibility
import org.apache.paimon.spark.commands.BucketExpression.quote
import org.apache.paimon.spark.commands.FixedBucketExpression
import org.apache.paimon.spark.util.OptionUtils.writeParallelism
import org.apache.paimon.table.{FileStoreTable, FormatTable}
import org.apache.paimon.table.BucketMode._

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{Expression => CatalystExpression, Literal => CatalystLiteral}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.distributions.{ClusteredDistribution, Distribution, Distributions}
import org.apache.spark.sql.connector.expressions.{Expression, Expressions, Literal, NamedReference, SortOrder, Transform}

import scala.collection.JavaConverters._

/** Distribution requirements of Spark write. */
case class PaimonWriteRequirement(distribution: Distribution, ordering: Array[SortOrder])

object PaimonWriteRequirement extends SQLConfHelper {

  private val EMPTY_ORDERING: Array[SortOrder] = Array.empty
  private val EMPTY: PaimonWriteRequirement =
    PaimonWriteRequirement(Distributions.unspecified(), EMPTY_ORDERING)

  // Spark 3.2/3.3 do not pass the FunctionCatalog when preparing V2 write distributions.
  // Bind the same distribution with Paimon's bucket expression before Spark prepares the write.
  private[spark] def requiresQueryPreparation: Boolean = org.apache.spark.SPARK_VERSION < "3.4"

  def prepareQuery(table: FileStoreTable, query: LogicalPlan): LogicalPlan = {
    if (!requiresQueryPreparation) {
      return query
    }

    def bind(expression: Expression): CatalystExpression = expression match {
      case reference: NamedReference =>
        query.output.find(a => conf.resolver(a.name, reference.fieldNames().head)).get
      case literal: Literal[_] => CatalystLiteral(literal.value(), literal.dataType())
      case transform: Transform if transform.name() == "identity" =>
        bind(transform.arguments().head)
      case transform: Transform if transform.name() == "bucket" =>
        FixedBucketExpression(
          CatalystLiteral(table.coreOptions().bucketFunctionType().toString) +:
            transform.arguments().map(bind).toSeq)
      case other =>
        throw new UnsupportedOperationException(s"Unsupported write distribution: $other")
    }

    apply(table).distribution match {
      case clustered: ClusteredDistribution =>
        val numPartitions = if (table.bucketMode() == HASH_FIXED) {
          writeParallelism(table.coreOptions())
        } else {
          None
        }
        // Leave the count unspecified so AQE can coalesce the bucket shuffle on Spark 3.2/3.3.
        Compatibility.repartitionByExpression(
          clustered.clustering().map(bind).toSeq,
          query,
          numPartitions)
      case _ => query
    }
  }

  def apply(table: FileStoreTable): PaimonWriteRequirement = {
    val bucketSpec = table.bucketSpec()
    val bucketTransforms: Seq[Expression] = bucketSpec.getBucketMode match {
      case HASH_FIXED =>
        Seq(
          Expressions.bucket(
            bucketSpec.getNumBuckets,
            bucketSpec.getBucketKeys.asScala.map(quote).toArray: _*))
      case BUCKET_UNAWARE | POSTPONE_MODE =>
        Seq.empty
      case _ =>
        throw new UnsupportedOperationException(
          s"Unsupported bucket mode ${bucketSpec.getBucketMode}")
    }

    create(
      table.schema().partitionKeys().asScala.toSeq,
      bucketTransforms,
      table.coreOptions().partitionSinkStrategy())
  }

  def apply(table: FormatTable): PaimonWriteRequirement = {
    create(
      table.partitionKeys().asScala.toSeq,
      Seq.empty,
      CoreOptions.fromMap(table.options()).partitionSinkStrategy())
  }

  private def create(
      partitionKeys: Seq[String],
      bucketTransforms: Seq[Expression],
      partitionSinkStrategy: PartitionSinkStrategy): PaimonWriteRequirement = {
    val partitionTransforms =
      partitionKeys.map(key => Expressions.identity(quote(key)))
    val clusteringExpressions =
      (partitionTransforms ++ bucketTransforms).map(identity[Expression]).toArray

    if (
      clusteringExpressions.isEmpty || (bucketTransforms.isEmpty && partitionSinkStrategy
        .equals(PartitionSinkStrategy.NONE))
    ) {
      EMPTY
    } else {
      val distribution: ClusteredDistribution =
        Distributions.clustered(clusteringExpressions)
      PaimonWriteRequirement(distribution, EMPTY_ORDERING)
    }
  }
}
