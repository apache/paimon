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

import org.apache.paimon.catalog.TableQueryAuthResult
import org.apache.paimon.data.{BinaryRow, BinaryString, GenericRow}
import org.apache.paimon.data.serializer.InternalRowSerializer
import org.apache.paimon.partition.PartitionPredicate
import org.apache.paimon.predicate.{ConcatTransform, PredicateBuilder}
import org.apache.paimon.spark.{PaimonInputPartition, SparkTypeUtils}
import org.apache.paimon.table.FallbackReadFileStoreTable.{toFallbackSplit, FallbackSplitImpl}
import org.apache.paimon.table.Table
import org.apache.paimon.table.source.{ChainSplit, DataSplit, QueryAuthSplit, Split}
import org.apache.paimon.types.{DataType, DataTypes, RowType}
import org.apache.paimon.utils.JsonSerdeUtil

import org.apache.spark.sql.connector.expressions.{Expression, Expressions}
import org.apache.spark.sql.connector.expressions.filter.{Or, Predicate => SparkPredicate}
import org.apache.spark.sql.types.StructType
import org.mockito.Mockito.{mock, never, verify, when}
import org.scalatest.funsuite.AnyFunSuite

import java.util.{Collections, OptionalLong}

import scala.collection.JavaConverters._

class QueryAuthRuntimeFilteringTest extends AnyFunSuite {

  private val rowType = RowType.of(
    Array[DataType](DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING()),
    Array("region", "dt", "payload"))
  private val partitionType = rowType.project(java.util.Arrays.asList("region", "dt"))
  private val fields = Set("region", "dt")

  private def partition(region: String, dt: String): BinaryRow = {
    new InternalRowSerializer(partitionType).toBinaryRow(
      GenericRow.of(
        Option(region).map(BinaryString.fromString).orNull,
        Option(dt).map(BinaryString.fromString).orNull))
  }

  private def data(region: String, dt: String): DataSplit = {
    DataSplit
      .builder()
      .withPartition(partition(region, dt))
      .withBucket(0)
      .withBucketPath("bucket-0")
      .withTotalBuckets(1)
      .withDataFiles(Collections.emptyList())
      .build()
  }

  private def authorized(split: Split, masked: String): Split = {
    val mask = new ConcatTransform(Collections.singletonList(BinaryString.fromString("***")))
    new QueryAuthSplit(
      split,
      new TableQueryAuthResult(
        null,
        Collections.singletonMap(masked, JsonSerdeUtil.toFlatJson(mask))))
  }

  private def runtimeIn(field: String, values: String*): SparkPredicate = {
    new SparkPredicate(
      "IN",
      (Seq[Expression](Expressions.column(field)) ++ values.map(Expressions.literal(_))).toArray)
  }

  private def scan(splits: Split*): TestScan = {
    val table = mock(classOf[Table])
    when(table.options()).thenReturn(Collections.singletonMap("query-auth.enabled", "true"))
    when(table.rowType()).thenReturn(rowType)
    when(table.partitionKeys()).thenReturn(java.util.Arrays.asList("region", "dt"))
    new TestScan(table, SparkTypeUtils.fromPaimonRowType(rowType), splits.toArray)
  }

  private class TestScan(val table: Table, val requiredSchema: StructType, planned: Array[Split])
    extends PaimonSupportsRuntimeFiltering {

    override val pushedPartitionFilters: Seq[PartitionPredicate] = Seq.empty
    override val pushedDataFilters: Seq[org.apache.paimon.predicate.Predicate] = Seq.empty
    var plans = 0

    override protected def getInputSplits: Array[Split] = {
      plans += 1
      assert(plans == 1, "runtime filtering must reuse the authorized plan")
      planned
    }

    override protected def getInputPartitions(splits: Array[Split]): Seq[PaimonInputPartition] = {
      splits.map(split => PaimonInputPartition(split)).toSeq
    }

    def attributes: Set[String] = filterAttributes().map(_.fieldNames().mkString(".")).toSet
  }

  test("runtime capabilities exclude mask targets across all authorized branches") {
    val plain = data("CN", "20250810")
    val payloadMask = authorized(plain, "payload")
    assert(QueryAuthRuntimeFiltering.filterableFields(Array(payloadMask), fields) == fields)
    val dtMask = new FallbackSplitImpl(authorized(plain, "dt"), true)
    val regionMask = authorized(new FallbackSplitImpl(plain, false), "region")
    assert(
      QueryAuthRuntimeFiltering.filterableFields(Array(plain, dtMask), fields) == Set("region"))
    assert(QueryAuthRuntimeFiltering.filterableFields(Array(dtMask, regionMask), fields).isEmpty)
    // FallbackDataSplit wraps itself; inspecting it must terminate and retain both fields.
    assert(
      QueryAuthRuntimeFiltering
        .filterableFields(Array(toFallbackSplit(plain, true)), fields) == fields)
  }

  test("filter chain logical partitions without changing wrappers or physical dependencies") {
    val anchor = data("CN", "20250810")
    val chain = new ChainSplit(
      partition("CN", "20250811"),
      anchor.dataFiles(),
      Collections.singletonMap("snapshot-file", "snapshot"),
      Collections.singletonMap("snapshot-file", "dt=20250810/bucket-0"),
      null
    )
    val wrapped = new FallbackSplitImpl(authorized(chain, "payload"), true)
    val builder = new PredicateBuilder(partitionType)
    val predicate = PartitionPredicate.fromPredicate(
      partitionType,
      builder.equal(1, BinaryString.fromString("20250811")))
    val result = QueryAuthRuntimeFiltering.filter(Array(anchor, wrapped), Seq(predicate))
    assert(result.length == 1)
    assert(result.head eq wrapped)
    assert(chain.fileBucketPathMapping().get("snapshot-file") == "dt=20250810/bucket-0")
  }

  test("successive runtime filters narrow cached splits and rebuild input partitions") {
    val first = authorized(data("CN", "20250810"), "payload")
    val second = authorized(data("CN", "20250811"), "payload")
    val third = authorized(data("US", "20250810"), "payload")
    val s = scan(first, second, third)
    assert(s.inputPartitions.size == 3)
    assert(s.attributes == fields)
    s.filter(Array(runtimeIn("region", "CN")))
    assert(s.inputSplits.toSeq == Seq(first, second))
    assert(s.inputPartitions.size == 2)
    s.filter(Array(runtimeIn("dt", "20250811")))
    assert(s.inputSplits.toSeq == Seq(second))
    assert(s.inputPartitions.size == 1)
    s.filter(Array(runtimeIn("region", "US")))
    assert(s.inputSplits.isEmpty)
    assert(s.inputPartitions.isEmpty)
    assert(s.attributes == fields)
    assert(s.plans == 1)
    verify(s.table, never()).newReadBuilder()
  }

  test("filtering cannot expand the initial authorization capability") {
    val masked = authorized(data("CN", "20250810"), "dt")
    val plain = data("US", "20250811")
    val s = scan(masked, plain)
    assert(s.attributes == Set("region"))
    s.filter(Array(runtimeIn("region", "US")))
    assert(s.inputSplits.toSeq == Seq(plain))
    assert(s.attributes == Set("region"))
    s.filter(Array(runtimeIn("dt", "absent")))
    assert(s.inputSplits.toSeq == Seq(plain))
    assert(s.plans == 1)
  }

  test("reject predicates on masked fields including mixed disjunctions") {
    val first = authorized(data("CN", "20250810"), "dt")
    val second = authorized(data("US", "20250811"), "dt")
    val s = scan(first, second)
    s.filter(Array(new Or(runtimeIn("region", "absent"), runtimeIn("dt", "***"))))
    assert(s.inputSplits.toSeq == Seq(first, second))
    assert(s.pushedRuntimePartitionFilters.isEmpty)
    s.filter(Array(runtimeIn("region", "CN"), runtimeIn("dt", "***")))
    assert(s.inputSplits.toSeq == Seq(first))
    assert(s.pushedRuntimePartitionFilters.size == 1)
  }

  test("unconvertible predicates do not alter authorized splits") {
    val split = authorized(data("CN", "20250810"), "payload")
    val s = scan(split)
    s.filter(Array(new SparkPredicate("UNSUPPORTED", Array.empty[Expression])))
    assert(s.inputSplits.toSeq == Seq(split))
    assert(s.pushedRuntimePartitionFilters.isEmpty)
  }

  test("runtime filters preserve null partition semantics and constant false results") {
    val nullPartition = data("CN", null)
    val other = data("CN", "20250810")
    val s = scan(nullPartition, other)
    s.filter(Array(new SparkPredicate("IS_NULL", Array[Expression](Expressions.column("dt")))))
    assert(s.inputSplits.toSeq == Seq(nullPartition))
    s.filter(Array(new SparkPredicate("ALWAYS_FALSE", Array.empty[Expression])))
    assert(s.inputSplits.isEmpty)
  }

  test("unknown and empty plans do not advertise filtering capabilities") {
    val unknown = new Split {
      override def rowCount(): Long = 0L
      override def mergedRowCount(): OptionalLong = OptionalLong.empty()
    }
    assert(scan(unknown).attributes.isEmpty)
    assert(scan().attributes.isEmpty)
    assert(
      QueryAuthRuntimeFiltering
        .filter(Array(unknown), Seq(PartitionPredicate.ALWAYS_FALSE))
        .head eq unknown)
  }

  test("partition columns outside the scan output are not runtime filter attributes") {
    val base = scan(authorized(data("CN", "20250810"), "payload"))
    val projection =
      SparkTypeUtils.fromPaimonRowType(rowType.project(Collections.singletonList("payload")))
    val s = new TestScan(base.table, projection, Array(data("CN", "20250810")))
    assert(s.attributes.isEmpty)
    assert(s.plans == 0)
    s.filter(Array(runtimeIn("dt", "absent")))
    assert(s.inputSplits.length == 1)
  }
}
