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

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class CopyIntoCastValidatorTest extends PaimonSparkTestBase {

  private lazy val validator = new CopyIntoCastValidator(spark)

  test("buildParquetCastValidation: no validation needed when all columns are compatible") {
    val schema = StructType(Seq(StructField("id", IntegerType), StructField("name", StringType)))
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1, "Alice"), Row(2, "Bob"))),
      schema)

    val targetColumns = Seq("id", "name")
    val writableColumns = Seq("id", "name")
    val fields = Seq(
      new DataField(0, "id", DataTypes.INT()),
      new DataField(1, "name", DataTypes.STRING())
    )

    val setup = validator.buildParquetCastValidation(df, targetColumns, writableColumns, fields)
    assert(setup.castColMapping.nonEmpty)
    assert(setup.badCastFilter.isDefined)
  }

  test("buildParquetCastValidation: exclude specified columns") {
    val schema = StructType(
      Seq(
        StructField("id", IntegerType),
        StructField("name", StringType),
        StructField("__file__", StringType)))
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1, "Alice", "file1.csv"))),
      schema)

    val targetColumns = Seq("id", "name")
    val writableColumns = Seq("id", "name")
    val fields = Seq(
      new DataField(0, "id", DataTypes.INT()),
      new DataField(1, "name", DataTypes.STRING())
    )

    val setup = validator.buildParquetCastValidation(
      df,
      targetColumns,
      writableColumns,
      fields,
      excludeCols = Set("__file__"))
    assert(!setup.castColMapping.contains("__file__"))
  }

  test("buildTextCastValidation: no validation for string columns") {
    val schema = StructType(Seq(StructField("id", StringType), StructField("name", StringType)))
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("1", "Alice"), Row("2", "Bob"))),
      schema)

    val writableColumns = Seq("id", "name")
    val fields = Seq(
      new DataField(0, "id", DataTypes.STRING()),
      new DataField(1, "name", DataTypes.STRING())
    )

    val setup = validator.buildTextCastValidation(df, writableColumns, fields)
    assert(setup.castColMapping.isEmpty)
    assert(setup.badCastFilter.isEmpty)
  }

  test("buildTextCastValidation: add validation columns for non-string types") {
    val schema = StructType(Seq(StructField("id", StringType), StructField("age", StringType)))
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("1", "30"), Row("2", "25"))),
      schema)

    val writableColumns = Seq("id", "age")
    val fields = Seq(
      new DataField(0, "id", DataTypes.INT()),
      new DataField(1, "age", DataTypes.INT())
    )

    val setup = validator.buildTextCastValidation(df, writableColumns, fields)
    assert(setup.castColMapping.size == 2)
    assert(setup.badCastFilter.isDefined)
    assert(setup.castColMapping.contains("id"))
    assert(setup.castColMapping.contains("age"))
  }

  test("validateParquetCast: pass when all casts are valid") {
    val schema = StructType(Seq(StructField("id", IntegerType), StructField("name", StringType)))
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1, "Alice"), Row(2, "Bob"))),
      schema)

    val targetColumns = Seq("id", "name")
    val writableColumns = Seq("id", "name")
    val fields = Seq(
      new DataField(0, "id", DataTypes.INT()),
      new DataField(1, "name", DataTypes.STRING())
    )

    // Should not throw
    validator.validateParquetCast(df, targetColumns, writableColumns, fields)
  }

  test("validateParquetCast: throw exception when cast fails") {
    val schema = StructType(Seq(StructField("id", StringType), StructField("name", StringType)))
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("abc", "Alice"), Row("2", "Bob"))),
      schema)

    val targetColumns = Seq("id", "name")
    val writableColumns = Seq("id", "name")
    val fields = Seq(
      new DataField(0, "id", DataTypes.INT()),
      new DataField(1, "name", DataTypes.STRING())
    )

    val exception = intercept[IllegalArgumentException] {
      validator.validateParquetCast(df, targetColumns, writableColumns, fields)
    }
    assert(exception.getMessage.contains("ON_ERROR = ABORT_STATEMENT"))
    assert(exception.getMessage.contains("Cast failure"))
  }

  test("castColumns: cast all columns to target types") {
    val schema = StructType(Seq(StructField("id", StringType), StructField("age", StringType)))
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("1", "30"), Row("2", "25"))),
      schema)

    val writableColumns = Seq("id", "age")
    val fields = Seq(
      new DataField(0, "id", DataTypes.INT()),
      new DataField(1, "age", DataTypes.INT())
    )

    val castedDf = validator.castColumns(df, writableColumns, fields)
    assert(castedDf.schema("id").dataType == IntegerType)
    assert(castedDf.schema("age").dataType == IntegerType)

    val rows = castedDf.collect()
    assert(rows(0).getInt(0) == 1)
    assert(rows(0).getInt(1) == 30)
  }

  test("castAndValidate: pass when all casts are valid") {
    val schema = StructType(Seq(StructField("id", StringType), StructField("age", StringType)))
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("1", "30"), Row("2", "25"))),
      schema)

    val writableColumns = Seq("id", "age")
    val fields = Seq(
      new DataField(0, "id", DataTypes.INT()),
      new DataField(1, "age", DataTypes.INT())
    )

    val castedDf = validator.castAndValidate(df, writableColumns, fields)
    assert(castedDf.schema("id").dataType == IntegerType)
    assert(castedDf.schema("age").dataType == IntegerType)
  }

  test("castAndValidate: throw exception when cast fails") {
    val schema = StructType(Seq(StructField("id", StringType), StructField("age", StringType)))
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("1", "invalid"), Row("2", "25"))),
      schema)

    val writableColumns = Seq("id", "age")
    val fields = Seq(
      new DataField(0, "id", DataTypes.INT()),
      new DataField(1, "age", DataTypes.INT())
    )

    val exception = intercept[IllegalArgumentException] {
      validator.castAndValidate(df, writableColumns, fields)
    }
    assert(exception.getMessage.contains("ON_ERROR = ABORT_STATEMENT"))
    assert(exception.getMessage.contains("Cast failure"))
  }
}
