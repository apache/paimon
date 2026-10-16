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

package org.apache.paimon.spark

import org.apache.paimon.CoreOptions
import org.apache.paimon.predicate.PredicateBuilder
import org.apache.paimon.spark.schema.PaimonMetadataColumn.BUCKET_COLUMN
import org.apache.paimon.table.{BucketMode, FileStoreTable, InnerTable}
import org.apache.paimon.table.source.ReadBuilder
import org.apache.paimon.types.{DataField, DataTypes, RowType}
import org.apache.paimon.utils.InstantiationUtil

import org.apache.spark.sql.connector.expressions.{Expression, Expressions, Literal}
import org.apache.spark.sql.connector.expressions.filter.{And, Not, Or, Predicate => SparkPredicate}
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.mockito.ArgumentMatchers.anyInt
import org.mockito.Mockito.{mock, never, verify, when}
import org.scalatest.funsuite.AnyFunSuite

import java.util.Collections

import scala.collection.JavaConverters._

class PhysicalBucketFilterTest extends AnyFunSuite {

  private val bucket = Expressions.column(BUCKET_COLUMN)
  private val converter = SparkV2FilterConverter(PhysicalBucketFilter.rowType)

  private def predicate(name: String, values: Integer*): SparkPredicate = {
    val literals = values.map {
      v =>
        new Literal[Integer] {
          override def value(): Integer = v
          override def dataType(): DataType = IntegerType
          override def children(): Array[Expression] = Array.empty
          override def describe(): String = String.valueOf(v)
        }
    }
    new SparkPredicate(name, Array[Expression](bucket) ++ literals)
  }

  test("preserve NULL semantics and serialize compound bucket filters") {
    val in = predicate("IN", 1, null)
    val equalNull = predicate("=", null)
    val nullSafe = predicate("<=>", null)
    Seq(
      in -> Set(1),
      new Not(in) -> Set.empty[Int],
      equalNull -> Set.empty[Int],
      new Not(equalNull) -> Set.empty[Int],
      nullSafe -> Set.empty[Int],
      new Not(nullSafe) -> Set(-1, 0, 1, 2, 3),
      new Or(in, predicate("=", 3)) -> Set(1, 3),
      new Not(new Or(in, predicate("=", 3))) -> Set.empty[Int],
      new Not(new And(in, predicate("=", 3))) -> Set(-1, 0, 1, 2)
    ).foreach {
      case (source, expected) =>
        val filter = PhysicalBucketFilter(converter.convert(source).get)
        assert((-1 to 3).filter(b => filter.test(b)).toSet == expected, source.toString)
    }

    val filter = PhysicalBucketFilter(converter.convert(new Or(in, predicate("=", 3))).get)
    val restored = InstantiationUtil.deserializeObject[PhysicalBucketFilter](
      InstantiationUtil.serializeObject(filter),
      getClass.getClassLoader)
    assert(restored == filter)
    assert((-1 to 3).filter(b => restored.test(b)).toSet == Set(1, 3))
  }

  test("support negative and boundary bucket IDs in IN filters") {
    val filter =
      PhysicalBucketFilter(
        converter.convert(predicate("IN", Int.MinValue, -2, 3, Int.MaxValue)).get)
    Seq(Int.MinValue, -2, 3, Int.MaxValue).foreach(b => assert(filter.test(b)))
    Seq(-1, 0, 2, 4).foreach(b => assert(!filter.test(b)))
  }

  test("use withBucket only for compatible equality predicates and retain the full filter") {
    // (predicates, configured scan.bucket, expected withBucket argument)
    Seq(
      (Seq(predicate("=", 1)), None, Some(1)),
      (Seq(predicate("=", 1)), Some(1), Some(1)),
      (Seq(predicate("<=>", 1)), None, Some(1)),
      (Seq(new And(predicate(">=", 0), predicate("=", 1))), None, Some(1)),
      (Seq(predicate(">=", 0), predicate("=", 1)), None, Some(1)),
      (Seq(predicate("IN", 1, 3)), None, None),
      (Seq(new And(predicate(">=", 1), predicate("<", 3))), None, None),
      (Seq(new Or(predicate("=", 1), predicate("=", 3))), None, None),
      (Seq(predicate("=", null)), None, None),
      (Seq(predicate("<=>", null)), None, None),
      (Seq(predicate("=", 1)), Some(0), None),
      (Seq(predicate("<=>", 1)), Some(0), None)
    ).foreach {
      case (sources, scanBucket, expectedBucket) =>
        withClue(s"$sources, scan.bucket=$scanBucket: ") {
          val readBuilder = mock(classOf[ReadBuilder])
          val predicates = sources.map(source => converter.convert(source).get)
          PhysicalBucketFilter.push(readBuilder, predicates, scanBucket)

          expectedBucket match {
            case Some(bucket) => verify(readBuilder).withBucket(bucket)
            case None => verify(readBuilder, never()).withBucket(anyInt())
          }
          verify(readBuilder)
            .withBucketFilter(PhysicalBucketFilter(PredicateBuilder.and(predicates.asJava)))
        }
    }
  }

  test("leave bucket filters unpushed for non-file-store tables") {
    val builder = scanBuilder(mock(classOf[InnerTable]), Map.empty)
    val predicates = Array(predicate("=", 1), predicate("IN", 1, 3), predicate(">=", 1))
    assert(builder.pushPredicates(predicates).sameElements(predicates))
    assert(builder.pushedPredicates.isEmpty)
    val scan = builder.build().asInstanceOf[PaimonScan]
    assert(scan.pushedBucketFilters.isEmpty)
    assert(scan.pushedDataFilters.isEmpty)
  }

  test("push bucket filters only when postpone merge uses the standard scan") {
    Seq(
      (false, "latest-full", true),
      (true, "latest-full", false),
      (true, "compacted-full", true)
    ).foreach {
      case (mergeOnRead, scanMode, canPush) =>
        val options = Map(
          "bucket" -> "-2",
          "postpone.merge-on-read" -> mergeOnRead.toString,
          "scan.mode" -> scanMode)
        val table = mock(classOf[FileStoreTable])
        val builder = scanBuilder(table, options)
        when(table.coreOptions()).thenReturn(CoreOptions.fromMap(options.asJava))
        when(table.bucketMode()).thenReturn(BucketMode.POSTPONE_MODE)
        when(table.primaryKeys()).thenReturn(Collections.singletonList("id"))

        val predicates = Array(predicate("=", 1), predicate("IN", 1, 3), predicate(">=", 1))
        assert(builder.pushPredicates(predicates).sameElements(predicates))
        val scan = builder.build().asInstanceOf[PaimonScan]
        assert(scan.pushedDataFilters.isEmpty)
        if (canPush) {
          assert(builder.pushedPredicates.sameElements(predicates))
          assert(scan.pushedBucketFilters.size == predicates.length)
        } else {
          assert(builder.pushedPredicates.isEmpty)
          assert(scan.pushedBucketFilters.isEmpty)
        }
    }
  }

  private def scanBuilder(table: InnerTable, options: Map[String, String]): PaimonScanBuilder = {
    when(table.options()).thenReturn(options.asJava)
    when(table.rowType()).thenReturn(RowType.of(new DataField(0, "id", DataTypes.INT())))
    when(table.partitionKeys()).thenReturn(Collections.emptyList[String]())
    when(table.primaryKeys()).thenReturn(Collections.emptyList[String]())
    new PaimonScanBuilder(table)
  }
}
