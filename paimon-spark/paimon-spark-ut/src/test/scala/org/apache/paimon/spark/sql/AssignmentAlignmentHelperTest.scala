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

package org.apache.paimon.spark.sql

import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.spark.catalyst.analysis.AssignmentAlignmentHelper

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, DeleteAction, InsertAction, MergeIntoTable, UpdateAction}
import org.apache.spark.sql.types.{IntegerType, StringType}

/**
 * Test suite for [[AssignmentAlignmentHelper]] methods:
 *   - alignMergeAction (with v2Write parameter)
 */
class AssignmentAlignmentHelperTest extends PaimonSparkTestBase with AssignmentAlignmentHelper {

  test("alignMergeAction: DeleteAction with v2Write=false should remain unchanged") {
    val condition = Some(Literal(true))
    val deleteAction = DeleteAction(condition)

    val targetOutput = Seq(
      AttributeReference("a", IntegerType)(),
      AttributeReference("b", IntegerType)(),
      AttributeReference("c", StringType)()
    )

    val aligned = alignMergeAction(deleteAction, targetOutput, v2Write = false)

    assert(aligned.isInstanceOf[DeleteAction])
    assert(aligned.asInstanceOf[DeleteAction].condition == condition)
  }

  test("alignMergeAction: DeleteAction with v2Write=true should remain unchanged") {
    val condition = Some(Literal(true))
    val deleteAction = DeleteAction(condition)

    val targetOutput = Seq(
      AttributeReference("a", IntegerType)(),
      AttributeReference("b", IntegerType)(),
      AttributeReference("c", StringType)()
    )

    val aligned = alignMergeAction(deleteAction, targetOutput, v2Write = true)

    assert(aligned.isInstanceOf[DeleteAction])
    assert(aligned.asInstanceOf[DeleteAction].condition == condition)
  }

  test("alignMergeAction: UpdateAction with v2Write=false should keep missing columns as-is") {
    val targetA = AttributeReference("a", IntegerType)()
    val targetB = AttributeReference("b", IntegerType)()
    val targetC = AttributeReference("c", StringType)()
    val targetOutput = Seq(targetA, targetB, targetC)

    // Only update column 'a', 'b' and 'c' should be kept as is
    val assignments = Seq(Assignment(targetA, Literal(100)))
    val updateAction = UpdateAction(None, assignments)

    val aligned = alignMergeAction(updateAction, targetOutput, v2Write = false)

    assert(aligned.isInstanceOf[UpdateAction])
    val alignedAssignments = aligned.asInstanceOf[UpdateAction].assignments
    assert(alignedAssignments.size == 3)
    // a = 100
    assert(alignedAssignments(0).key.asInstanceOf[AttributeReference].name == "a")
    assert(alignedAssignments(0).value.isInstanceOf[Literal])
    // b = b (unchanged, keeps original attribute)
    assert(alignedAssignments(1).key.asInstanceOf[AttributeReference].name == "b")
    assert(alignedAssignments(1).value.isInstanceOf[AttributeReference])
    // c = c (unchanged, keeps original attribute)
    assert(alignedAssignments(2).key.asInstanceOf[AttributeReference].name == "c")
    assert(alignedAssignments(2).value.isInstanceOf[AttributeReference])
  }

  test("alignMergeAction: UpdateAction with v2Write=true should use NULL for missing columns") {
    val targetA = AttributeReference("a", IntegerType)()
    val targetB = AttributeReference("b", IntegerType)()
    val targetC = AttributeReference("c", StringType)()
    val targetOutput = Seq(targetA, targetB, targetC)

    // Only update column 'a', 'b' and 'c' should be NULL
    val assignments = Seq(Assignment(targetA, Literal(100)))
    val updateAction = UpdateAction(None, assignments)

    val aligned = alignMergeAction(updateAction, targetOutput, v2Write = true)

    assert(aligned.isInstanceOf[UpdateAction])
    val alignedAssignments = aligned.asInstanceOf[UpdateAction].assignments
    assert(alignedAssignments.size == 3)
    // a = 100
    assert(alignedAssignments(0).key.asInstanceOf[AttributeReference].name == "a")
    assert(alignedAssignments(0).value.isInstanceOf[Literal])
    assert(alignedAssignments(0).value.asInstanceOf[Literal].value == 100)
    // b = NULL (v2Write mode)
    assert(alignedAssignments(1).key.asInstanceOf[AttributeReference].name == "b")
    assert(alignedAssignments(1).value.isInstanceOf[Literal])
    assert(alignedAssignments(1).value.asInstanceOf[Literal].value == null)
    // c = NULL (v2Write mode)
    assert(alignedAssignments(2).key.asInstanceOf[AttributeReference].name == "c")
    assert(alignedAssignments(2).value.isInstanceOf[Literal])
    assert(alignedAssignments(2).value.asInstanceOf[Literal].value == null)
  }

  test("alignMergeAction: InsertAction with v2Write=false should keep missing columns as-is") {
    val targetA = AttributeReference("a", IntegerType)()
    val targetB = AttributeReference("b", IntegerType)()
    val targetC = AttributeReference("c", StringType)()
    val targetOutput = Seq(targetA, targetB, targetC)

    val sourceA = AttributeReference("a", IntegerType)()
    val assignments = Seq(Assignment(targetA, sourceA))
    val insertAction = InsertAction(None, assignments)

    val aligned = alignMergeAction(insertAction, targetOutput, v2Write = false)

    assert(aligned.isInstanceOf[InsertAction])
    val alignedAssignments = aligned.asInstanceOf[InsertAction].assignments
    assert(alignedAssignments.size == 3)
    // a = source.a
    assert(alignedAssignments(0).key.asInstanceOf[AttributeReference].name == "a")
    // b = b (unchanged, keeps original attribute)
    assert(alignedAssignments(1).key.asInstanceOf[AttributeReference].name == "b")
    assert(alignedAssignments(1).value.isInstanceOf[AttributeReference])
    // c = c (unchanged, keeps original attribute)
    assert(alignedAssignments(2).key.asInstanceOf[AttributeReference].name == "c")
    assert(alignedAssignments(2).value.isInstanceOf[AttributeReference])
  }

  test("alignMergeAction: InsertAction with v2Write=true should use NULL for missing columns") {
    val targetA = AttributeReference("a", IntegerType)()
    val targetB = AttributeReference("b", IntegerType)()
    val targetC = AttributeReference("c", StringType)()
    val targetOutput = Seq(targetA, targetB, targetC)

    // Only insert column 'a', 'b' and 'c' should be NULL
    val sourceA = AttributeReference("a", IntegerType)()
    val assignments = Seq(Assignment(targetA, sourceA))
    val insertAction = InsertAction(None, assignments)

    val aligned = alignMergeAction(insertAction, targetOutput, v2Write = true)

    assert(aligned.isInstanceOf[InsertAction])
    val alignedAssignments = aligned.asInstanceOf[InsertAction].assignments
    assert(alignedAssignments.size == 3)
    // a = source.a
    assert(alignedAssignments(0).key.asInstanceOf[AttributeReference].name == "a")
    // b = NULL (v2Write mode)
    assert(alignedAssignments(1).key.asInstanceOf[AttributeReference].name == "b")
    assert(alignedAssignments(1).value.isInstanceOf[Literal])
    assert(alignedAssignments(1).value.asInstanceOf[Literal].value == null)
    // c = NULL (v2Write mode)
    assert(alignedAssignments(2).key.asInstanceOf[AttributeReference].name == "c")
    assert(alignedAssignments(2).value.isInstanceOf[Literal])
    assert(alignedAssignments(2).value.asInstanceOf[Literal].value == null)
  }
}
