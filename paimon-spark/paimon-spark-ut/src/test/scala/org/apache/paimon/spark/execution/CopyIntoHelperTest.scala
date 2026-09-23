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

import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.types.{DataField, DataTypes}

class CopyIntoHelperTest extends PaimonSparkTestBase {

  test("resolveTargetColumns: use all writable columns when columns is None") {
    val writableColumns = Seq("id", "name", "age")
    val result = CopyIntoHelper.resolveTargetColumns(spark, None, writableColumns)
    assert(result == writableColumns)
  }

  test("resolveTargetColumns: resolve specified columns case-insensitively") {
    val writableColumns = Seq("id", "name", "age")
    val columns = Some(Seq("ID", "Name"))
    val result = CopyIntoHelper.resolveTargetColumns(spark, columns, writableColumns)
    assert(result == Seq("id", "name"))
  }

  test("resolveTargetColumns: throw exception for non-existent column") {
    val writableColumns = Seq("id", "name", "age")
    val columns = Some(Seq("id", "invalid_col"))
    val exception = intercept[IllegalArgumentException] {
      CopyIntoHelper.resolveTargetColumns(spark, columns, writableColumns)
    }
    assert(exception.getMessage.contains("invalid_col"))
    assert(exception.getMessage.contains("does not exist"))
  }

  test("resolveTargetColumns: throw exception for duplicate columns") {
    val writableColumns = Seq("id", "name", "age")
    val columns = Some(Seq("id", "ID"))
    val exception = intercept[IllegalArgumentException] {
      CopyIntoHelper.resolveTargetColumns(spark, columns, writableColumns)
    }
    assert(exception.getMessage.contains("Duplicate columns"))
  }

  test("validateNonNullableDefaults: pass when all non-nullable columns are mapped") {
    val writableColumns = Seq("id", "name", "age")
    val targetColumns = Seq("id", "name", "age")
    val fields = Seq(
      new DataField(0, "id", DataTypes.INT().notNull()),
      new DataField(1, "name", DataTypes.STRING()),
      new DataField(2, "age", DataTypes.INT())
    )
    // Should not throw
    CopyIntoHelper.validateNonNullableDefaults(
      Some(targetColumns),
      writableColumns,
      targetColumns,
      fields)
  }

  test("validateNonNullableDefaults: pass when unmapped non-nullable column has default") {
    val writableColumns = Seq("id", "name", "status")
    val targetColumns = Seq("id", "name")
    val fields = Seq(
      new DataField(0, "id", DataTypes.INT().notNull()),
      new DataField(1, "name", DataTypes.STRING()),
      new DataField(2, "status", DataTypes.STRING().notNull(), null, "default value")
    )
    // Should not throw
    CopyIntoHelper.validateNonNullableDefaults(
      Some(targetColumns),
      writableColumns,
      targetColumns,
      fields)
  }

  test("validateNonNullableDefaults: throw when unmapped non-nullable column has no default") {
    val writableColumns = Seq("id", "name", "status")
    val targetColumns = Seq("id", "name")
    val fields = Seq(
      new DataField(0, "id", DataTypes.INT().notNull()),
      new DataField(1, "name", DataTypes.STRING()),
      new DataField(2, "status", DataTypes.STRING().notNull())
    )
    val exception = intercept[IllegalArgumentException] {
      CopyIntoHelper.validateNonNullableDefaults(
        Some(targetColumns),
        writableColumns,
        targetColumns,
        fields)
    }
    assert(exception.getMessage.contains("status"))
    assert(exception.getMessage.contains("not in the column list"))
    assert(exception.getMessage.contains("no default value"))
  }

  test("safeTempCol: generate unique column name") {
    val existingColumns = Set("col1", "col2", "__temp")
    val result = CopyIntoHelper.safeTempCol(spark, "__new", existingColumns)
    assert(result == "__new")
  }

  test("safeTempCol: add prefix when name conflicts") {
    val existingColumns = Set("col1", "col2", "__temp")
    val result = CopyIntoHelper.safeTempCol(spark, "__temp", existingColumns)
    assert(result == "___temp")
  }

  test("safeTempCol: handle case-insensitive conflicts") {
    val existingColumns = Set("Col1", "COL2")
    val result = CopyIntoHelper.safeTempCol(spark, "col1", existingColumns)
    assert(result == "_col1")
  }

  test("safeTempCol: add multiple prefixes for multiple conflicts") {
    val existingColumns = Set("__temp", "___temp")
    val result = CopyIntoHelper.safeTempCol(spark, "__temp", existingColumns)
    assert(result == "____temp")
  }
}
