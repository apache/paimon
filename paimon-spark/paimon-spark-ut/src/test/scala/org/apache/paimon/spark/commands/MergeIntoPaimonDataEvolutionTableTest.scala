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
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, GetStructField, Literal}
import org.apache.spark.sql.catalyst.plans.logical.Assignment
import org.apache.spark.sql.types.{BinaryType, StringType, StructField, StructType}

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
}
