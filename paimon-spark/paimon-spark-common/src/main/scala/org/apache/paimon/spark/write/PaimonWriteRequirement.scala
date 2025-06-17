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

import org.apache.paimon.spark.commands.BucketExpression.quote
import org.apache.paimon.table.BucketMode._
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.connector.distributions.{ClusteredDistribution, Distribution, Distributions}
import org.apache.spark.sql.connector.expressions.{Expression, Expressions, SortOrder}

import scala.collection.JavaConverters._

/** Distribution requirements of Spark write. */
case class PaimonWriteRequirement(distribution: Distribution, ordering: Array[SortOrder])

object PaimonWriteRequirement {

  private val EMPTY_ORDERING: Array[SortOrder] = Array.empty
  private val EMPTY: PaimonWriteRequirement =
    PaimonWriteRequirement(Distributions.unspecified(), EMPTY_ORDERING)

  def apply(table: FileStoreTable): PaimonWriteRequirement = {
    val bucketSpec = table.bucketSpec()
    val bucketTransforms = bucketSpec.getBucketMode match {
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

    val partitionTransforms =
      table.schema().partitionKeys().asScala.map(key => Expressions.identity(quote(key)))
    val clusteringExpressions =
      (partitionTransforms ++ bucketTransforms).map(identity[Expression]).toArray

    if (clusteringExpressions.isEmpty) {
      EMPTY
    } else {
      val distribution: ClusteredDistribution =
        Distributions.clustered(clusteringExpressions)
      PaimonWriteRequirement(distribution, EMPTY_ORDERING)
    }
  }
}
