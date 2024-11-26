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

package org.apache.spark.sql.paimon.shims

import org.apache.paimon.spark.catalyst.parser.extensions.PaimonSpark4SqlExtensionsParser
import org.apache.paimon.spark.data.{Spark4ArrayData, Spark4InternalRow, SparkArrayData, SparkInternalRow}
import org.apache.paimon.types.{DataType, RowType}

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Identifier, Table, TableCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.internal.ExpressionUtils
import org.apache.spark.sql.types.StructType

import java.util.{Map => JMap}

class Spark4Shim extends SparkShim {

  override def createSparkParser(delegate: ParserInterface): ParserInterface = {
    new PaimonSpark4SqlExtensionsParser(delegate)
  }
  override def createSparkInternalRow(rowType: RowType): SparkInternalRow = {
    new Spark4InternalRow(rowType)
  }

  override def createSparkArrayData(elementType: DataType): SparkArrayData = {
    new Spark4ArrayData(elementType)
  }

  def supportsHashAggregate(
      aggregateBufferAttributes: Seq[Attribute],
      groupingExpression: Seq[Expression]): Boolean = {
    Aggregate.supportsHashAggregate(aggregateBufferAttributes, groupingExpression)
  }

  def createTable(
      tableCatalog: TableCatalog,
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: JMap[String, String]): Table = {
    val columns = CatalogV2Util.structTypeToV2Columns(schema)
    tableCatalog.createTable(ident, columns, partitions, properties)
  }

  def column(expr: Expression): Column = ExpressionUtils.column(expr)

  def convertToExpression(spark: SparkSession, column: Column): Expression =
    spark.expression(column)
}
