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
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, GetStructField, If, IsNull, Literal}
import org.apache.spark.sql.catalyst.plans.logical.Assignment
import org.apache.spark.sql.types.{BinaryType, IntegerType, StringType, StructField, StructType}

class MergeIntoPaimonDataEvolutionTableTest extends SparkFunSuite {

  test("update column detection ignores target self-assignment with different qualifiers") {
    val targetPicture = AttributeReference("picture", BinaryType)()
    val qualifiedTargetPicture = targetPicture.withQualifier(Seq("t"))

    assert(!targetPicture.equals(qualifiedTargetPicture))
    assert(targetPicture.sameRef(qualifiedTargetPicture))
    assert(
      !MergeIntoPaimonDataEvolutionTable.isModifiedAssignment(
        Assignment(targetPicture, qualifiedTargetPicture)))
  }

  test("update column detection includes source assignment with same column name") {
    val targetFileType = AttributeReference("file_type", StringType)()
    val sourceFileType = AttributeReference("file_type", StringType)().withQualifier(Seq("s"))

    assert(!targetFileType.sameRef(sourceFileType))
    assert(
      MergeIntoPaimonDataEvolutionTable.isModifiedAssignment(
        Assignment(targetFileType, sourceFileType)))
  }

  test("update column detection rejects non top-level assignment key") {
    val targetStruct =
      AttributeReference("metadata", StructType(Seq(StructField("name", StringType))))()
    val nestedKey = GetStructField(targetStruct, 0, Some("name"))

    intercept[UnsupportedOperationException] {
      MergeIntoPaimonDataEvolutionTable.assignmentKeyAttribute(
        Assignment(nestedKey, Literal("new-name")))
    }
  }

  // JingsongLi's review on apache/paimon#8334 found that prunedStructType / buildPrunedStruct
  // lay out fields in table-schema order while the action-ordered path list was used verbatim for
  // writePaths, so a reversed SET clause order could make the persisted writeType disagree with
  // the physical column order. The fix canonicalizes the path list to schema order before it's
  // used for either the output struct or writePaths.
  test("pruned struct type uses canonical schema order regardless of input path order") {
    val nestType =
      StructType(
        Seq(
          StructField("a", IntegerType),
          StructField("b", IntegerType),
          StructField("c", IntegerType)))

    // MATCHED clause 1 sets c, clause 2 sets a: action order is [c, a].
    val actionOrderedPaths: Seq[Seq[String]] = Seq(Seq("c"), Seq("a"))
    val fieldOrder = nestType.fieldNames.zipWithIndex.toMap
    val canonicalPaths = actionOrderedPaths.sortBy(p => fieldOrder(p.head))

    assert(canonicalPaths.map(_.mkString(".")) == Seq("a", "c"))
    val outputStructType =
      MergeIntoPaimonDataEvolutionTable.prunedStructType(nestType, canonicalPaths)
    assert(outputStructType.fieldNames.toSeq == Seq("a", "c"))
    assert(canonicalPaths.map(_.mkString(".")) == outputStructType.fieldNames.toSeq)
  }

  // JingsongLi's review also found that buildPrunedStruct has no null-guard, so copying a NULL
  // struct through the pruned path silently turned it into a non-null struct of null leaves. The
  // call sites now wrap the built expression with If(IsNull(attr), Literal(null, ...), built).
  test("guarding buildPrunedStruct with IsNull preserves a NULL source struct") {
    val nestType =
      StructType(Seq(StructField("a", IntegerType), StructField("b", IntegerType)))
    val prunedType = StructType(Seq(StructField("b", IntegerType)))
    val nullNest = Literal(null, nestType)

    val built = MergeIntoPaimonDataEvolutionTable.buildPrunedStruct(
      nestType,
      Nil,
      Seq(Seq("b")),
      p => MergeIntoPaimonDataEvolutionTable.passthroughExpr(nullNest, nestType, p))

    // Unguarded, this still reproduces the bug: a non-null struct from a NULL source.
    assert(built.eval() != null)

    val guarded = If(IsNull(nullNest), Literal(null, prunedType), built)
    assert(guarded.eval() == null)
  }
}
