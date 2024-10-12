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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.{InternalRow => SparkInternalRow}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.parser.{CompoundBody, ParserInterface => SparkParserInterface}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate => SparkAggregate}
import org.apache.spark.sql.catalyst.util.{ArrayData => SparkArrayData}
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Identifier, Table, TableCatalog => SparkTableCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.internal.{ExpressionUtils => SparkExpressionUtils}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.VariantVal

import java.util.{Map => JMap}

/** Shims for Spark 4.x in [[org.apache.spark.sql]]. */
object shims {

  /** In [[org.apache.spark.sql.catalyst]]. */

  abstract class ParserInterface extends SparkParserInterface {
    val delegate: SparkParserInterface

    def parseScript(sqlScriptText: String): CompoundBody = delegate.parseScript(sqlScriptText)
  }

  abstract class ArrayData extends SparkArrayData {
    def getVariant(ordinal: Int): VariantVal = throw new UnsupportedOperationException
  }

  abstract class InternalRow extends SparkInternalRow {
    override def getVariant(i: Int): VariantVal = throw new UnsupportedOperationException
  }

  object Aggregate {
    def supportsHashAggregate(
        aggregateBufferAttributes: Seq[Attribute],
        groupingExpression: Seq[Expression]): Boolean = {
      SparkAggregate.supportsHashAggregate(aggregateBufferAttributes, groupingExpression)
    }
  }

  /** In [[org.apache.spark.sql.connector]]. */

  def createTable(
      tableCatalog: SparkTableCatalog,
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: JMap[String, String]): Table = {
    tableCatalog.createTable(
      ident,
      CatalogV2Util.structTypeToV2Columns(schema),
      partitions,
      properties)
  }

  /** In [[org.apache.spark.sql.internal]]. */

  object ExpressionUtils {
    def column(expr: Expression): Column = SparkExpressionUtils.column(expr)

    def convertToExpression(spark: SparkSession, column: Column): Expression = {
      spark.expression(column)
    }
  }
}
