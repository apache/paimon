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

import org.apache.paimon.globalindex.ScoredGlobalIndexResult
import org.apache.paimon.globalindex.ScoreGetter
import org.apache.paimon.table.source.{IndexVectorSearchSplit, VectorSearchSplit}
import org.apache.paimon.utils.RoaringNavigableMap64

import org.apache.spark.Partitioner

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.LongMap

private[execution] object LateralVectorSearchExecution {

  private val MaxMergeBatchesPerSearch = 16
  private val MaxMaterializationCandidatesPerBatch = 64L * 1024

  case class QueryBatchId(inputPartition: Int, ordinal: Long)

  final case class QueryBatchPartitioner(override val numPartitions: Int) extends Partitioner {

    require(numPartitions > 0, "numPartitions must be positive")

    override def getPartition(key: Any): Int = key match {
      case batchId: QueryBatchId =>
        Math.floorMod(batchId.inputPartition.toLong + batchId.ordinal, numPartitions.toLong).toInt
      case _ =>
        throw new IllegalArgumentException(
          s"Expected ${classOf[QueryBatchId].getName}, but found ${key.getClass.getName}.")
    }
  }

  case class QueryPayload(vector: Array[Float], outerRowBytes: Array[Byte])

  case class QueryBatch(id: QueryBatchId, queries: Array[QueryPayload])

  case class SearchQueryBatch(
      id: QueryBatchId,
      vectors: Array[Array[Float]],
      outerRowBytes: Array[Array[Byte]])

  case class WeightedSplit[T](value: T, cost: Long)

  case class SplitGroup(id: Int, splits: Seq[VectorSearchSplit])

  case class PartialBatchResult(
      outerRowBytes: Array[Array[Byte]],
      candidates: Array[ScoredCandidates]) {

    require(candidates.nonEmpty, "candidates must not be empty")
    require(
      outerRowBytes == null || outerRowBytes.length == candidates.length,
      "outer rows and candidates must have the same length")

    def merge(other: PartialBatchResult, limit: Int): PartialBatchResult = {
      require(
        candidates.length == other.candidates.length,
        "partial batch results must have the same length")
      val outer = if (outerRowBytes != null) outerRowBytes else other.outerRowBytes
      val mergedCandidates = new Array[ScoredCandidates](candidates.length)
      var index = 0
      while (index < candidates.length) {
        mergedCandidates(index) = candidates(index).merge(other.candidates(index), limit)
        index += 1
      }
      PartialBatchResult(outer, mergedCandidates)
    }
  }

  case class ScoredCandidates(rowIds: Array[Long], scores: Array[Float]) {

    require(rowIds.length == scores.length, "rowIds and scores must have the same length")

    def isEmpty: Boolean = rowIds.isEmpty

    def merge(other: ScoredCandidates, limit: Int): ScoredCandidates = {
      require(limit > 0, "limit must be positive")
      if (other.isEmpty && rowIds.length <= limit) {
        return this
      }
      if (isEmpty && other.rowIds.length <= limit) {
        return other
      }

      if (rowIds.length + other.rowIds.length > ScoredCandidates.SmallTopKMergeThreshold) {
        return mergeLargeTopK(other, limit)
      }

      // Each partial result is already bounded by TopK. Avoid allocating a HashMap and sorting
      // boxed tuples for every query and every merge level; the working set is at most 2 * limit.
      val mergedRowIds = new Array[Long](rowIds.length + other.rowIds.length)
      val mergedScores = new Array[Float](mergedRowIds.length)
      var mergedSize = appendDistinct(mergedRowIds, mergedScores, 0, this)
      mergedSize = appendDistinct(mergedRowIds, mergedScores, mergedSize, other)

      // Insertion sort is efficient for the small bounded TopK arrays used by vector search.
      var index = 1
      while (index < mergedSize) {
        val rowId = mergedRowIds(index)
        val score = mergedScores(index)
        var insertion = index
        while (
          insertion > 0 &&
          stronger(score, rowId, mergedScores(insertion - 1), mergedRowIds(insertion - 1))
        ) {
          mergedRowIds(insertion) = mergedRowIds(insertion - 1)
          mergedScores(insertion) = mergedScores(insertion - 1)
          insertion -= 1
        }
        mergedRowIds(insertion) = rowId
        mergedScores(insertion) = score
        index += 1
      }

      val resultSize = Math.min(limit, mergedSize)
      ScoredCandidates(
        java.util.Arrays.copyOf(mergedRowIds, resultSize),
        java.util.Arrays.copyOf(mergedScores, resultSize))
    }

    private def mergeLargeTopK(other: ScoredCandidates, limit: Int): ScoredCandidates = {
      val scoreByRowId = LongMap.empty[Float]
      appendBestScores(scoreByRowId, this)
      appendBestScores(scoreByRowId, other)

      val sorted = scoreByRowId.iterator.toArray
      scala.util.Sorting.stableSort(
        sorted,
        (left: (Long, Float), right: (Long, Float)) =>
          stronger(left._2, left._1, right._2, right._1))

      val resultSize = Math.min(limit, sorted.length)
      val resultRowIds = new Array[Long](resultSize)
      val resultScores = new Array[Float](resultSize)
      var index = 0
      while (index < resultSize) {
        resultRowIds(index) = sorted(index)._1
        resultScores(index) = sorted(index)._2
        index += 1
      }
      ScoredCandidates(resultRowIds, resultScores)
    }

    private def appendBestScores(
        scoreByRowId: LongMap[Float],
        candidates: ScoredCandidates): Unit = {
      var index = 0
      while (index < candidates.rowIds.length) {
        val rowId = candidates.rowIds(index)
        val score = candidates.scores(index)
        scoreByRowId.get(rowId) match {
          case Some(existing) if java.lang.Float.compare(score, existing) <= 0 =>
          case _ => scoreByRowId.update(rowId, score)
        }
        index += 1
      }
    }

    def toResult: ScoredGlobalIndexResult = {
      val bitmap = new RoaringNavigableMap64
      val scoreMap = new java.util.HashMap[java.lang.Long, java.lang.Float]()
      var index = 0
      while (index < rowIds.length) {
        bitmap.add(rowIds(index))
        scoreMap.put(rowIds(index), scores(index))
        index += 1
      }
      ScoredGlobalIndexResult.create(
        bitmap,
        new ScoreGetter {
          override def score(rowId: Long): Float = scoreMap.get(rowId)
        })
    }

    private def appendDistinct(
        targetRowIds: Array[Long],
        targetScores: Array[Float],
        initialSize: Int,
        candidates: ScoredCandidates): Int = {
      var size = initialSize
      var index = 0
      while (index < candidates.rowIds.length) {
        val rowId = candidates.rowIds(index)
        val score = candidates.scores(index)
        var existing = 0
        while (existing < size && targetRowIds(existing) != rowId) {
          existing += 1
        }
        if (existing == size) {
          targetRowIds(size) = rowId
          targetScores(size) = score
          size += 1
        } else if (java.lang.Float.compare(score, targetScores(existing)) > 0) {
          targetScores(existing) = score
        }
        index += 1
      }
      size
    }

    private def stronger(
        leftScore: Float,
        leftRowId: Long,
        rightScore: Float,
        rightRowId: Long): Boolean = {
      val scoreComparison = java.lang.Float.compare(leftScore, rightScore)
      scoreComparison > 0 || (scoreComparison == 0 && leftRowId < rightRowId)
    }
  }

  object ScoredCandidates {

    private val SmallTopKMergeThreshold = 128

    val empty: ScoredCandidates = ScoredCandidates(Array.emptyLongArray, Array.emptyFloatArray)

    def from(result: ScoredGlobalIndexResult): ScoredCandidates = {
      val rowIds = new Array[Long](result.results().getIntCardinality)
      val scores = new Array[Float](rowIds.length)
      val scoreGetter = result.scoreGetter()
      val iterator = result.results().iterator()
      var index = 0
      while (iterator.hasNext) {
        val rowId = iterator.next()
        rowIds(index) = rowId
        scores(index) = scoreGetter.score(rowId)
        index += 1
      }
      ScoredCandidates(rowIds, scores)
    }
  }

  def groupByCost[T](splits: Seq[WeightedSplit[T]], maxGroups: Int): Seq[Seq[WeightedSplit[T]]] = {
    require(maxGroups > 0, "maxGroups must be positive")
    if (splits.isEmpty) {
      return Seq.empty
    }

    val groupCount = Math.min(maxGroups, splits.size)
    val groups = Array.fill(groupCount)(ArrayBuffer[WeightedSplit[T]]())
    val groupCosts = Array.fill(groupCount)(0L)
    val ordered = splits.zipWithIndex.sortWith {
      case ((left, leftIndex), (right, rightIndex)) =>
        left.cost > right.cost || (left.cost == right.cost && leftIndex < rightIndex)
    }

    ordered.foreach {
      case (split, _) =>
        var target = 0
        var index = 1
        while (index < groupCount) {
          if (groupCosts(index) < groupCosts(target)) {
            target = index
          }
          index += 1
        }
        groups(target) += split
        groupCosts(target) += Math.max(1L, split.cost)
    }

    groups.map(_.toSeq).toSeq
  }

  def canDistribute(
      splits: Seq[VectorSearchSplit],
      queryOptions: Map[String, String],
      tableOptions: Map[String, String]): Boolean = {
    splits.size > 1 &&
    splits.forall(_.isInstanceOf[IndexVectorSearchSplit]) &&
    hasNonOverlappingRowRanges(splits.map(_.asInstanceOf[IndexVectorSearchSplit])) &&
    !hasRefineOption(queryOptions) &&
    !hasRefineOption(tableOptions)
  }

  def splitGroups(splits: Seq[VectorSearchSplit], maxGroups: Int): Seq[SplitGroup] = {
    groupByCost(splits.map(split => WeightedSplit(split, splitCost(split))), maxGroups).zipWithIndex
      .map { case (group, index) => SplitGroup(index, group.map(_.value)) }
  }

  def groupSearchBatches(
      batches: Iterator[SearchQueryBatch],
      maxVectors: Int): Iterator[Seq[SearchQueryBatch]] = {
    require(maxVectors > 0, "maxVectors must be positive")
    val pendingBatches = batches.buffered
    new Iterator[Seq[SearchQueryBatch]] {

      override def hasNext: Boolean = pendingBatches.hasNext

      override def next(): Seq[SearchQueryBatch] = {
        if (!hasNext) {
          throw new NoSuchElementException("next on empty search-batch iterator")
        }
        val grouped = ArrayBuffer[SearchQueryBatch]()
        var vectorCount = 0
        while (
          pendingBatches.hasNext &&
          (grouped.isEmpty || vectorCount + pendingBatches.head.vectors.length <= maxVectors)
        ) {
          val batch = pendingBatches.next()
          grouped += batch
          vectorCount += batch.vectors.length
        }
        grouped.toSeq
      }
    }
  }

  def queryMergeBatchSize(searchBatchSize: Int, mergeParallelism: Int): Int = {
    // Create enough merge keys to utilize reducers without growing per-batch shuffle metadata
    // unboundedly. Search batches are regrouped to searchBatchSize before entering native search.
    val batchesPerSearch =
      Math.min(Math.max(1, mergeParallelism), MaxMergeBatchesPerSearch)
    Math.max(1, (searchBatchSize + batchesPerSearch - 1) / batchesPerSearch)
  }

  def groupMergedResults(
      results: Iterator[PartialBatchResult],
      maxQueries: Int,
      maxCandidates: Long = MaxMaterializationCandidatesPerBatch)
      : Iterator[Seq[PartialBatchResult]] = {
    require(maxQueries > 0, "maxQueries must be positive")
    require(maxCandidates > 0, "maxCandidates must be positive")
    val pendingResults = results.buffered
    new Iterator[Seq[PartialBatchResult]] {

      override def hasNext: Boolean = pendingResults.hasNext

      override def next(): Seq[PartialBatchResult] = {
        if (!hasNext) {
          throw new NoSuchElementException("next on empty merged-result iterator")
        }
        val grouped = ArrayBuffer[PartialBatchResult]()
        var queryCount = 0
        var candidateCount = 0L
        var grouping = true
        while (pendingResults.hasNext && grouping) {
          val next = pendingResults.head
          val nextQueryCount = next.candidates.length
          val nextCandidateCount =
            next.candidates.iterator.map(_.rowIds.length.toLong).sum
          if (
            grouped.nonEmpty &&
            (queryCount.toLong + nextQueryCount > maxQueries ||
              candidateCount + nextCandidateCount > maxCandidates)
          ) {
            grouping = false
          } else {
            grouped += pendingResults.next()
            queryCount += nextQueryCount
            candidateCount += nextCandidateCount
          }
        }
        grouped.toSeq
      }
    }
  }

  private def splitCost(split: VectorSearchSplit): Long = split match {
    case indexed: IndexVectorSearchSplit =>
      val fileBytes = indexed
        .vectorIndexFiles()
        .asScala
        .map(_.fileSize())
        .foldLeft(0L)(saturatedAdd)
      if (fileBytes > 0) {
        fileBytes
      } else {
        saturatedRangeSize(indexed.rowRangeStart(), indexed.rowRangeEnd())
      }
    case _ => 1L
  }

  private def saturatedRangeSize(from: Long, to: Long): Long = {
    if (to < from) {
      1L
    } else if (to == Long.MaxValue && from == 0L) {
      Long.MaxValue
    } else {
      val difference = to - from
      if (difference < 0 || difference == Long.MaxValue) Long.MaxValue else difference + 1L
    }
  }

  private def saturatedAdd(left: Long, right: Long): Long = {
    if (right > 0 && left > Long.MaxValue - right) Long.MaxValue else left + right
  }

  private def hasRefineOption(options: Map[String, String]): Boolean = {
    options.keys.exists {
      key =>
        val normalized = key.toLowerCase
        normalized.endsWith("refine_factor") ||
        normalized.endsWith("refine-factor") ||
        normalized.endsWith("rerank_factor") ||
        normalized.endsWith("rerank-factor")
    }
  }

  private def hasNonOverlappingRowRanges(splits: Seq[IndexVectorSearchSplit]): Boolean = {
    val ordered = splits.sortBy(split => (split.rowRangeStart(), split.rowRangeEnd()))
    ordered
      .sliding(2)
      .forall {
        case Seq(left, right) => left.rowRangeEnd() < right.rowRangeStart()
        case _ => true
      }
  }
}
