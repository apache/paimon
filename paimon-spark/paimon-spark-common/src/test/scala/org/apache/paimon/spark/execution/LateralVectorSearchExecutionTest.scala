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

import org.apache.paimon.table.source.{IndexVectorSearchSplit, RawVectorSearchSplit}
import org.apache.paimon.utils.Range

import org.scalatest.funsuite.AnyFunSuite

import java.util.Collections

class LateralVectorSearchExecutionTest extends AnyFunSuite {

  test("balance expensive splits across groups") {
    val groups = LateralVectorSearchExecution.groupByCost(
      Seq(
        LateralVectorSearchExecution.WeightedSplit("a", 8L),
        LateralVectorSearchExecution.WeightedSplit("b", 7L),
        LateralVectorSearchExecution.WeightedSplit("c", 6L),
        LateralVectorSearchExecution.WeightedSplit("d", 5L)
      ),
      2
    )

    assert(groups.map(_.map(_.value)) == Seq(Seq("a", "d"), Seq("b", "c")))
    assert(groups.map(_.map(_.cost).sum) == Seq(13L, 13L))
  }

  test("merge partial results with deterministic bounded top k") {
    val merged = LateralVectorSearchExecution
      .ScoredCandidates(Array(4L, 2L, 1L), Array(0.8f, 0.9f, 0.9f))
      .merge(LateralVectorSearchExecution.ScoredCandidates(Array(3L, 5L), Array(0.95f, 0.7f)), 3)

    assert(merged.rowIds.sameElements(Array(3L, 1L, 2L)))
    assert(merged.scores.sameElements(Array(0.95f, 0.9f, 0.9f)))
  }

  test("merge partial results deduplicates row IDs and uses total float ordering") {
    val merged = LateralVectorSearchExecution
      .ScoredCandidates(Array(4L, 2L), Array(0.8f, 0.7f))
      .merge(
        LateralVectorSearchExecution.ScoredCandidates(Array(2L, 1L), Array(0.9f, Float.NaN)),
        3)

    assert(merged.rowIds.sameElements(Array(1L, 2L, 4L)))
    assert(merged.scores(0).isNaN)
    assert(merged.scores.drop(1).sameElements(Array(0.9f, 0.8f)))
  }

  test("merge large top k matches full deduplication and sorting") {
    val left = LateralVectorSearchExecution.ScoredCandidates(
      (0L until 100L).toArray,
      (0 until 100).map(i => if (i == 3) Float.NaN else (i % 17).toFloat).toArray)
    val right = LateralVectorSearchExecution.ScoredCandidates(
      (50L until 180L).toArray,
      (50 until 180)
        .map(i => if (i == 75) Float.NaN else if (i == 76) -0.0f else (i % 19).toFloat)
        .toArray)

    val bestScores = scala.collection.mutable.LongMap.empty[Float]
    (left.rowIds.zip(left.scores) ++ right.rowIds.zip(right.scores)).foreach {
      case (rowId, score) =>
        bestScores.get(rowId) match {
          case Some(existing) if java.lang.Float.compare(score, existing) <= 0 =>
          case _ => bestScores.update(rowId, score)
        }
    }
    val expected = bestScores.iterator.toArray
      .sortWith {
        (left: (Long, Float), right: (Long, Float)) =>
          val comparison = java.lang.Float.compare(left._2, right._2)
          comparison > 0 || (comparison == 0 && left._1 < right._1)
      }
      .take(140)

    val merged = left.merge(right, 140)

    assert(merged.rowIds.sameElements(expected.map(_._1)))
    assert(
      merged.scores
        .map(java.lang.Float.floatToIntBits)
        .sameElements(expected.map(value => java.lang.Float.floatToIntBits(value._2))))
  }

  test("merge partial results keeps the outer-row anchor from an empty group") {
    val anchor = LateralVectorSearchExecution.PartialBatchResult(
      Array(Array[Byte](1, 2, 3)),
      Array(LateralVectorSearchExecution.ScoredCandidates.empty))
    val candidates = LateralVectorSearchExecution.PartialBatchResult(
      null,
      Array(LateralVectorSearchExecution.ScoredCandidates(Array(7L), Array(0.8f))))

    val merged = candidates.merge(anchor, 1)

    assert(merged.outerRowBytes.head.sameElements(Array[Byte](1, 2, 3)))
    assert(merged.candidates.head.rowIds.sameElements(Array(7L)))
    assert(merged.candidates.head.scores.sameElements(Array(0.8f)))
  }

  test("combine search batches across input partitions up to the vector limit") {
    def batch(ordinal: Long, size: Int) =
      LateralVectorSearchExecution.SearchQueryBatch(
        LateralVectorSearchExecution.QueryBatchId(0, ordinal),
        Array.fill(size)(Array(1.0f)),
        null)

    val grouped = LateralVectorSearchExecution
      .groupSearchBatches(Iterator(batch(0, 3), batch(1, 3), batch(2, 2)), 5)
      .toSeq

    assert(grouped.map(_.map(_.vectors.length)) == Seq(Seq(3), Seq(3, 2)))
  }

  test("distribute query batches evenly across merge partitions") {
    val partitioner = new LateralVectorSearchExecution.QueryBatchPartitioner(16)

    val firstBatchPartitions = (0 until 16).map {
      inputPartition =>
        partitioner.getPartition(LateralVectorSearchExecution.QueryBatchId(inputPartition, 0L))
    }

    assert(firstBatchPartitions.sorted == (0 until 16))

    val repeatedBatchPartitioner = new LateralVectorSearchExecution.QueryBatchPartitioner(4)
    val repeatedBatchCounts = (for {
      inputPartition <- 0 until 4
      ordinal <- 0L until 12L
    } yield {
      repeatedBatchPartitioner
        .getPartition(LateralVectorSearchExecution.QueryBatchId(inputPartition, ordinal))
    }).groupBy(identity).map { case (partition, batches) => partition -> batches.size }

    assert(repeatedBatchCounts == (0 until 4).map(_ -> 12).toMap)
  }

  test("only distribute complete indexed plans without refine") {
    val indexedSplits = Seq(
      new IndexVectorSearchSplit(0L, 9L, Collections.emptyList(), Collections.emptyList()),
      new IndexVectorSearchSplit(10L, 19L, Collections.emptyList(), Collections.emptyList())
    )
    val rawSplit = new RawVectorSearchSplit(
      Collections.singletonList(new Range(20L, 29L)),
      Collections.emptyList(),
      "ivf-pq")

    assert(LateralVectorSearchExecution.canDistribute(indexedSplits, Map.empty, Map.empty))
    assert(!LateralVectorSearchExecution.canDistribute(indexedSplits.take(1), Map.empty, Map.empty))
    assert(
      !LateralVectorSearchExecution.canDistribute(indexedSplits :+ rawSplit, Map.empty, Map.empty))
    assert(
      !LateralVectorSearchExecution
        .canDistribute(indexedSplits, Map("ivf.refine_factor" -> "2"), Map.empty))
    assert(
      !LateralVectorSearchExecution
        .canDistribute(indexedSplits, Map.empty, Map("fields.embedding.ivf.rerank-factor" -> "2")))

    val overlappingSplits = Seq(
      new IndexVectorSearchSplit(0L, 10L, Collections.emptyList(), Collections.emptyList()),
      new IndexVectorSearchSplit(10L, 19L, Collections.emptyList(), Collections.emptyList())
    )
    assert(!LateralVectorSearchExecution.canDistribute(overlappingSplits, Map.empty, Map.empty))
  }
}
