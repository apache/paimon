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
import org.apache.paimon.spark.catalyst.plans.logical.{CopyFileFormat, FileFormatType}
import org.apache.paimon.types.{DataField, DataTypes}

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class CopyIntoDataFrameBuilderTest extends PaimonSparkTestBase {

  private def createBuilder(
      formatType: FileFormatType,
      columns: Option[Seq[String]] = None): CopyIntoDataFrameBuilder = {
    val fileFormat = new CopyFileFormat(formatType = formatType, options = Map.empty)
    new CopyIntoDataFrameBuilder(spark, fileFormat, columns)
  }

  test("buildStringSchema: CSV format with positional columns") {
    val builder = createBuilder(FileFormatType.CSV)
    val targetColumns = Seq("id", "name", "age")
    val schema = builder.buildStringSchema(targetColumns)

    assert(schema.fields.length == 3)
    assert(schema.fields(0).name == "_c0")
    assert(schema.fields(1).name == "_c1")
    assert(schema.fields(2).name == "_c2")
    assert(schema.fields.forall(_.dataType == StringType))
  }

  test("buildStringSchema: JSON format with named columns") {
    val builder = createBuilder(FileFormatType.JSON)
    val targetColumns = Seq("id", "name", "age")
    val schema = builder.buildStringSchema(targetColumns)

    assert(schema.fields.length == 3)
    assert(schema.fields(0).name == "id")
    assert(schema.fields(1).name == "name")
    assert(schema.fields(2).name == "age")
    assert(schema.fields.forall(_.dataType == StringType))
  }

  test("buildParquetDataFrame: map source columns to target by name") {
    val builder = createBuilder(FileFormatType.PARQUET)
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

    val result = builder.buildParquetDataFrame(df, targetColumns, writableColumns, fields)
    assert(result.columns.toSeq == Seq("id", "name"))
    assert(result.count() == 2)
  }

  test("buildParquetDataFrame: fill NULL for missing source columns") {
    val builder = createBuilder(FileFormatType.PARQUET)
    val schema = StructType(Seq(StructField("id", IntegerType)))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row(1), Row(2))), schema)

    val targetColumns = Seq("id", "name")
    val writableColumns = Seq("id", "name")
    val fields = Seq(
      new DataField(0, "id", DataTypes.INT()),
      new DataField(1, "name", DataTypes.STRING())
    )

    val result = builder.buildParquetDataFrame(df, targetColumns, writableColumns, fields)
    assert(result.columns.toSeq == Seq("id", "name"))
    val rows = result.collect()
    assert(rows(0).isNullAt(1)) // name should be NULL
  }

  test("buildParquetDataFrame: fill default value for unmapped columns") {
    val builder = createBuilder(FileFormatType.PARQUET, Some(Seq("id")))
    val schema = StructType(Seq(StructField("id", IntegerType)))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row(1), Row(2))), schema)

    val targetColumns = Seq("id")
    val writableColumns = Seq("id", "status")
    val fields = Seq(
      new DataField(0, "id", DataTypes.INT()),
      new DataField(1, "status", DataTypes.STRING(), "'active'")
    )

    val result = builder.buildParquetDataFrame(df, targetColumns, writableColumns, fields)
    assert(result.columns.toSeq == Seq("id", "status"))
  }

  test("buildFinalDataFrame: rename CSV positional columns to named columns") {
    val builder = createBuilder(FileFormatType.CSV, Some(Seq("id", "name")))
    val schema = StructType(Seq(StructField("_c0", StringType), StructField("_c1", StringType)))
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("1", "Alice"), Row("2", "Bob"))),
      schema)

    val targetColumns = Seq("id", "name")
    val writableColumns = Seq("id", "name")
    val fields = Seq(
      new DataField(0, "id", DataTypes.INT()),
      new DataField(1, "name", DataTypes.STRING())
    )

    val result = builder.buildFinalDataFrame(df, targetColumns, writableColumns, fields)
    assert(result.columns.toSeq == Seq("id", "name"))
  }

  test("buildFinalDataFrame: keep JSON named columns as-is") {
    val builder = createBuilder(FileFormatType.JSON)
    val schema = StructType(Seq(StructField("id", StringType), StructField("name", StringType)))
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("1", "Alice"), Row("2", "Bob"))),
      schema)

    val targetColumns = Seq("id", "name")
    val writableColumns = Seq("id", "name")
    val fields = Seq(
      new DataField(0, "id", DataTypes.INT()),
      new DataField(1, "name", DataTypes.STRING())
    )

    val result = builder.buildFinalDataFrame(df, targetColumns, writableColumns, fields)
    assert(result.columns.toSeq == Seq("id", "name"))
  }

  test("buildFinalDataFrame: preserve extra columns") {
    val builder = createBuilder(FileFormatType.CSV, Some(Seq("id", "name")))
    val schema = StructType(
      Seq(
        StructField("_c0", StringType),
        StructField("_c1", StringType),
        StructField("__file__", StringType)))
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("1", "Alice", "file1.csv"))),
      schema)

    val targetColumns = Seq("id", "name")
    val writableColumns = Seq("id", "name")
    val fields = Seq(
      new DataField(0, "id", DataTypes.INT()),
      new DataField(1, "name", DataTypes.STRING())
    )

    val result =
      builder.buildFinalDataFrame(df, targetColumns, writableColumns, fields, Seq("__file__"))
    assert(result.columns.toSeq == Seq("id", "name", "__file__"))
  }

  test("applyNullTransforms: replace NULL_IF values with null") {
    val fileFormat = new CopyFileFormat(
      formatType = FileFormatType.CSV,
      options = Map("NULL_IF" -> Seq("N/A", "NULL").mkString(CopyFileFormat.LIST_SEPARATOR)))
    val builder = new CopyIntoDataFrameBuilder(spark, fileFormat, None)

    val schema = StructType(Seq(StructField("col1", StringType), StructField("col2", StringType)))
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("value", "N/A"), Row("NULL", "data"))),
      schema)

    val result = builder.applyNullTransforms(df, Seq("col1", "col2"))
    val rows = result.collect()
    assert(rows(0).getString(0) == "value")
    assert(rows(0).isNullAt(1)) // "N/A" -> null
    assert(rows(1).isNullAt(0)) // "NULL" -> null
    assert(rows(1).getString(1) == "data")
  }

  test("applyNullTransforms: replace empty strings with null when EMPTY_FIELD_AS_NULL is true") {
    val fileFormat = new CopyFileFormat(
      formatType = FileFormatType.CSV,
      options = Map("EMPTY_FIELD_AS_NULL" -> "TRUE"))
    val builder = new CopyIntoDataFrameBuilder(spark, fileFormat, None)

    val schema = StructType(Seq(StructField("col1", StringType), StructField("col2", StringType)))
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("value", ""), Row("", "data"))),
      schema)

    val result = builder.applyNullTransforms(df, Seq("col1", "col2"))
    val rows = result.collect()
    assert(rows(0).getString(0) == "value")
    assert(rows(0).isNullAt(1)) // "" -> null
    assert(rows(1).isNullAt(0)) // "" -> null
    assert(rows(1).getString(1) == "data")
  }

  test("applyNullTransforms: apply both NULL_IF and EMPTY_FIELD_AS_NULL") {
    val fileFormat = new CopyFileFormat(
      formatType = FileFormatType.CSV,
      options = Map(
        "NULL_IF" -> Seq("N/A").mkString(CopyFileFormat.LIST_SEPARATOR),
        "EMPTY_FIELD_AS_NULL" -> "TRUE"))
    val builder = new CopyIntoDataFrameBuilder(spark, fileFormat, None)

    val schema = StructType(Seq(StructField("col1", StringType), StructField("col2", StringType)))
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("N/A", ""), Row("value", "data"))),
      schema)

    val result = builder.applyNullTransforms(df, Seq("col1", "col2"))
    val rows = result.collect()
    assert(rows(0).isNullAt(0)) // "N/A" -> null
    assert(rows(0).isNullAt(1)) // "" -> null
    assert(rows(1).getString(0) == "value")
    assert(rows(1).getString(1) == "data")
  }
}
