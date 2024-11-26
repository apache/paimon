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

import org.apache.paimon.spark.catalyst.parser.extensions.PaimonSpark3SqlExtensionsParser
import org.apache.paimon.spark.data.{Spark3ArrayData, Spark3InternalRow, SparkArrayData, SparkInternalRow}
import org.apache.paimon.types.{DataType, RowType}

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType

import java.util.{Map => JMap}

class Spark3Shim extends SparkShim {

  override def createSparkParser(delegate: ParserInterface): ParserInterface = {
    new PaimonSpark3SqlExtensionsParser(delegate)
  }

  override def createSparkInternalRow(rowType: RowType): SparkInternalRow = {
    new Spark3InternalRow(rowType)
  }

  override def createSparkArrayData(elementType: DataType): SparkArrayData = {
    new Spark3ArrayData(elementType)
  }

  override def supportsHashAggregate(
      aggregateBufferAttributes: Seq[Attribute],
      groupingExpression: Seq[Expression]): Boolean = {
    Aggregate.supportsHashAggregate(aggregateBufferAttributes)
  }

  override def createTable(
      tableCatalog: TableCatalog,
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: JMap[String, String]): Table = {
    tableCatalog.createTable(ident, schema, partitions, properties)
  }

  override def column(expr: Expression): Column = new Column(expr)

  override def convertToExpression(spark: SparkSession, column: Column): Expression = column.expr

}
