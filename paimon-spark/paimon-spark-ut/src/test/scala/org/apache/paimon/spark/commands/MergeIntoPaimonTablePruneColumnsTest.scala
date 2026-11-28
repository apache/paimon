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

package org.apache.paimon.spark.commands

import org.apache.spark.sql.catalyst.expressions.{Add, AttributeReference, EqualTo, GreaterThan, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, DeleteAction, InsertAction, UpdateAction}
import org.apache.spark.sql.types.IntegerType
import org.scalatest.funsuite.AnyFunSuite

/** Unit tests for the pure column-collection logic behind MERGE INTO join pruning. */
class MergeIntoPaimonTablePruneColumnsTest extends AnyFunSuite {

  private def attr(name: String): AttributeReference =
    AttributeReference(name, IntegerType, nullable = true)()

  test("collect referenced columns from conditions and assignment values on both sides") {
    val tId = attr("id")
    val tV1 = attr("v1")
    val tV2 = attr("v2")
    val tV3 = attr("v3")
    val sId = attr("s_id")
    val sV1 = attr("s_v1")

    val (neededTarget, neededSource) = MergeIntoPaimonTable.pruneJoinColumnsForMerge(
      EqualTo(tId, sId),
      // SET v1 = target.v1 + source.s_v1
      Seq(UpdateAction(None, Seq(Assignment(tV1, Add(tV1, sV1))))),
      // INSERT (id, v2) VALUES (s_id, literal)
      Seq(InsertAction(None, Seq(Assignment(tId, sId), Assignment(tV2, Literal(1))))),
      Seq.empty,
      Seq(tId, tV1, tV2, tV3),
      Seq(sId, sV1),
      remainDeletedRow = true
    )

    // v3 is never referenced; assignment keys are not collected, so `v2` (a literal value) drops.
    assert(neededTarget == Seq(tId, tV1))
    assert(neededSource == Seq(sId, sV1))
  }

  test("delete clause keeps the full target side") {
    val tId = attr("id")
    val tV1 = attr("v1")
    val sId = attr("s_id")
    val fullTarget = Seq(tId, tV1)

    // A matched DELETE materializes a -D record carrying the full target row.
    val (matchedDelete, _) = MergeIntoPaimonTable.pruneJoinColumnsForMerge(
      EqualTo(tId, sId),
      Seq(DeleteAction(None)),
      Seq.empty,
      Seq.empty,
      fullTarget,
      Seq(sId),
      remainDeletedRow = true
    )
    assert(matchedDelete == fullTarget)

    // Same for a NOT MATCHED BY SOURCE DELETE; once un-gated, its condition reference (`v1`)
    // is collected alongside the merge condition's (`id`).
    val nmsDelete = Seq(DeleteAction(Some(GreaterThan(tV1, Literal(5)))))
    val (nmsGated, _) = MergeIntoPaimonTable.pruneJoinColumnsForMerge(
      EqualTo(tId, sId),
      Seq.empty,
      Seq.empty,
      nmsDelete,
      fullTarget,
      Seq(sId),
      remainDeletedRow = true
    )
    assert(nmsGated == fullTarget)
    val (nmsUngated, _) = MergeIntoPaimonTable.pruneJoinColumnsForMerge(
      EqualTo(tId, sId),
      Seq.empty,
      Seq.empty,
      nmsDelete,
      fullTarget,
      Seq(sId),
      remainDeletedRow = false
    )
    assert(nmsUngated == Seq(tId, tV1))

    // remainDeletedRow = false does not gate.
    val (ungated, _) = MergeIntoPaimonTable.pruneJoinColumnsForMerge(
      EqualTo(tId, sId),
      Seq(DeleteAction(None)),
      Seq.empty,
      Seq.empty,
      fullTarget,
      Seq(sId),
      remainDeletedRow = false
    )
    assert(ungated == Seq(tId))

    // A DELETE under WHEN NOT MATCHED never materializes -D records, so it does not gate.
    val (notMatchedDelete, _) = MergeIntoPaimonTable.pruneJoinColumnsForMerge(
      EqualTo(tId, sId),
      Seq.empty,
      Seq(DeleteAction(None)),
      Seq.empty,
      fullTarget,
      Seq(sId),
      remainDeletedRow = true
    )
    assert(notMatchedDelete == Seq(tId))
  }

  test("no referenced columns yields empty sets") {
    val tV1 = attr("v1")

    val (neededTarget, neededSource) = MergeIntoPaimonTable.pruneJoinColumnsForMerge(
      Literal.TrueLiteral,
      Seq.empty,
      Seq(InsertAction(None, Seq(Assignment(tV1, Literal(1))))),
      Seq.empty,
      Seq(tV1),
      Seq.empty,
      remainDeletedRow = true
    )

    assert(neededTarget.isEmpty)
    assert(neededSource.isEmpty)
  }
}
