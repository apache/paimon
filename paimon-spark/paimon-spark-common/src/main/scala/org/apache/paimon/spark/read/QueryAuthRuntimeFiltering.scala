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

package org.apache.paimon.spark.read

import org.apache.paimon.data.BinaryRow
import org.apache.paimon.partition.PartitionPredicate
import org.apache.paimon.table.source.{ChainSplit, DataSplit, Split, Splits}

import scala.collection.JavaConverters._

/**
 * Identifies unmasked partition fields for runtime filtering and filters the existing authorized
 * split plan. Retained splits are returned unchanged, including their authorization and fallback
 * wrappers and all files needed for chain-table reads.
 */
private[read] object QueryAuthRuntimeFiltering {

  def filterableFields(splits: Array[Split], partitionFields: Set[String]): Set[String] = {
    // An empty or unrecognized plan cannot establish a useful partition filtering capability.
    if (splits.isEmpty || splits.exists(partition(_).isEmpty)) {
      Set.empty
    } else {
      val maskedFields = splits.iterator.flatMap {
        split =>
          Option(Splits.authResult(split)).iterator
            .flatMap(_.extractColumnMasking().keySet().asScala)
      }.toSet
      partitionFields -- maskedFields
    }
  }

  def filter(splits: Array[Split], predicates: Seq[PartitionPredicate]): Array[Split] = {
    // Keep the original split, including its authorization and fallback wrappers. A chain split
    // is indivisible: its older physical partitions may be needed to reconstruct the logical one.
    splits.filter(split => partition(split).forall(p => predicates.forall(_.test(p))))
  }

  private def partition(split: Split): Option[BinaryRow] = {
    Splits.underlying(split) match {
      case data: DataSplit => Option(data.partition())
      case chain: ChainSplit => Option(chain.logicalPartition())
      case _ => None
    }
  }
}
