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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualTo, GreaterThanOrEqual, LessThanOrEqual, Literal, Or}
import org.apache.spark.sql.types.LongType

class PaimonRowLevelCommandTest extends SparkFunSuite {

  test("extract row id bounds only from guaranteed conjuncts") {
    val rowId = AttributeReference("_ROW_ID", LongType)()
    val other = AttributeReference("other", LongType)()
    val condition = And(
      And(GreaterThanOrEqual(rowId, Literal(100L)), LessThanOrEqual(rowId, Literal(200L))),
      EqualTo(other, Literal(1L)))

    val range = PaimonRowLevelCommand.conjunctiveRowIdRange(condition, Seq(rowId, other)).get
    assert(range.from == 100L && range.to == 200L)
    assert(
      PaimonRowLevelCommand
        .conjunctiveRowIdRange(
          Or(EqualTo(rowId, Literal(100L)), EqualTo(other, Literal(1L))),
          Seq(rowId, other))
        .isEmpty)
  }

  test("ignore contradictory row id bounds rather than construct an invalid range") {
    val rowId = AttributeReference("_ROW_ID", LongType)()
    val condition =
      And(GreaterThanOrEqual(rowId, Literal(200L)), LessThanOrEqual(rowId, Literal(100L)))
    assert(PaimonRowLevelCommand.conjunctiveRowIdRange(condition, Seq(rowId)).isEmpty)
  }

  test("do not prune on user columns or unrelated row id attributes") {
    val metadataRowId = AttributeReference("_ROW_ID", LongType)()
    val userRowId = AttributeReference("_row_id", LongType)()
    val unrelatedRowId = AttributeReference("_ROW_ID", LongType)()
    val targetAttributes = Seq(metadataRowId, userRowId)

    assert(
      PaimonRowLevelCommand
        .conjunctiveRowIdRange(EqualTo(userRowId, Literal(100L)), targetAttributes)
        .isEmpty)
    assert(
      PaimonRowLevelCommand
        .conjunctiveRowIdRange(EqualTo(unrelatedRowId, Literal(100L)), targetAttributes)
        .isEmpty)
    assert(
      PaimonRowLevelCommand
        .conjunctiveRowIdRange(EqualTo(metadataRowId, Literal(100L)), targetAttributes)
        .nonEmpty)
  }
}
