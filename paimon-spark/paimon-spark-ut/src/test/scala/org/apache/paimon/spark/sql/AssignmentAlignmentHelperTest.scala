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
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, DeleteAction, InsertAction, MergeIntoTable, UpdateAction}
import org.apache.spark.sql.types.{IntegerType, Metadata, MetadataBuilder, StringType}

import scala.reflect.ClassTag

/**
 * Test suite for [[AssignmentAlignmentHelper]] methods:
 *   - alignMergeAction (with isInsert parameter)
 */
class AssignmentAlignmentHelperTest extends PaimonSparkTestBase with AssignmentAlignmentHelper {

  /** Assert assignment key name and value type. */
  private def assertAssignment[T <: Expression: ClassTag](
      assignment: Assignment,
      expectedKeyName: String): Unit = {
    assert(assignment.key.asInstanceOf[AttributeReference].name == expectedKeyName)
    val expectedType = implicitly[ClassTag[T]].runtimeClass
    assert(
      expectedType.isInstance(assignment.value),
      s"Expected value type ${expectedType.getSimpleName}, " +
        s"but got ${assignment.value.getClass.getSimpleName}"
    )
  }

  /** Assert assignment key name and literal value. */
  private def assertLiteralValue(
      assignment: Assignment,
      expectedKeyName: String,
      expectedValue: Any): Unit = {
    assertAssignment[Literal](assignment, expectedKeyName)
    assert(
      assignment.value.asInstanceOf[Literal].value == expectedValue,
      s"Expected literal value $expectedValue, but got ${assignment.value.asInstanceOf[Literal].value}"
    )
  }

  test("alignMergeAction: DeleteAction should remain unchanged") {
    val condition = Some(Literal(true))
    val deleteAction = DeleteAction(condition)

    val targetOutput = Seq(
      AttributeReference("a", IntegerType)(),
      AttributeReference("b", IntegerType)(),
      AttributeReference("c", StringType)()
    )

    val aligned = alignMergeAction(deleteAction, targetOutput)

    assert(aligned.isInstanceOf[DeleteAction])
    assert(aligned.asInstanceOf[DeleteAction].condition == condition)
  }

  test("alignMergeAction: UpdateAction should keep missing columns as-is") {
    val targetA = AttributeReference("a", IntegerType)()
    val targetB = AttributeReference("b", IntegerType)()
    val targetC = AttributeReference("c", StringType)()
    val targetOutput = Seq(targetA, targetB, targetC)

    // Only update column 'a', 'b' and 'c' should be kept as is
    val assignments = Seq(Assignment(targetA, Literal(100)))
    val updateAction = UpdateAction(None, assignments)

    val aligned = alignMergeAction(updateAction, targetOutput)

    assert(aligned.isInstanceOf[UpdateAction])
    val alignedAssignments = aligned.asInstanceOf[UpdateAction].assignments
    assert(alignedAssignments.size == 3)
    assertAssignment[Literal](alignedAssignments(0), "a") // a = 100
    assertAssignment[AttributeReference](alignedAssignments(1), "b") // b = b (unchanged)
    assertAssignment[AttributeReference](alignedAssignments(2), "c") // c = c (unchanged)
  }

  test("alignMergeAction: InsertAction should use NULL for missing columns") {
    val targetA = AttributeReference("a", IntegerType)()
    val targetB = AttributeReference("b", IntegerType)()
    val targetC = AttributeReference("c", StringType)()
    val targetOutput = Seq(targetA, targetB, targetC)

    // Only insert column 'a', 'b' and 'c' should be NULL
    val sourceA = AttributeReference("a", IntegerType)()
    val assignments = Seq(Assignment(targetA, sourceA))
    val insertAction = InsertAction(None, assignments)

    val aligned = alignMergeAction(insertAction, targetOutput)

    assert(aligned.isInstanceOf[InsertAction])
    val alignedAssignments = aligned.asInstanceOf[InsertAction].assignments
    assert(alignedAssignments.size == 3)
    assertAssignment[AttributeReference](alignedAssignments(0), "a") // a = source.a
    assertLiteralValue(alignedAssignments(1), "b", null) // b = NULL (isInsert mode)
    assertLiteralValue(alignedAssignments(2), "c", null) // c = NULL (isInsert mode)
  }

  test("alignMergeAction: InsertAction should use default value for missing columns") {
    val targetA = AttributeReference("a", IntegerType)()
    // Column 'b' has default value 100
    val metadataWithDefault = new MetadataBuilder()
      .putString("CURRENT_DEFAULT", "100")
      .build()
    val targetB = AttributeReference("b", IntegerType, nullable = true, metadataWithDefault)()
    val targetC = AttributeReference("c", StringType)()
    val targetOutput = Seq(targetA, targetB, targetC)

    // Only insert column 'a', 'b' should use default value 100, 'c' should be NULL
    val sourceA = AttributeReference("a", IntegerType)()
    val assignments = Seq(Assignment(targetA, sourceA))
    val insertAction = InsertAction(None, assignments)

    val aligned = alignMergeAction(insertAction, targetOutput)

    assert(aligned.isInstanceOf[InsertAction])
    val alignedAssignments = aligned.asInstanceOf[InsertAction].assignments
    assert(alignedAssignments.size == 3)
    assertAssignment[AttributeReference](alignedAssignments(0), "a") // a = source.a
    assertLiteralValue(alignedAssignments(1), "b", 100) // b = 100 (default value)
    assertLiteralValue(alignedAssignments(2), "c", null) // c = NULL (no default)
  }
}
