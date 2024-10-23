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
import org.apache.spark.sql.catalyst.parser.{ParserInterface => SparkParserInterface}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate => SparkAggregate}
import org.apache.spark.sql.catalyst.util.{ArrayData => SparkArrayData}
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog => SparkTableCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType

import java.util.{Map => JMap}

/** Shims for Spark 3.x in [[org.apache.spark.sql]]. */
object shims {

  /** In [[org.apache.spark.sql.catalyst]]. */

  abstract class ParserInterface extends SparkParserInterface {
    val delegate: SparkParserInterface
  }

  abstract class ArrayData extends SparkArrayData {}

  abstract class InternalRow extends SparkInternalRow {}

  object Aggregate {
    def supportsHashAggregate(
        aggregateBufferAttributes: Seq[Attribute],
        groupingExpression: Seq[Expression]): Boolean = {
      SparkAggregate.supportsHashAggregate(aggregateBufferAttributes)
    }
  }

  /** In [[org.apache.spark.sql.connector]]. */

  def createTable(
      tableCatalog: SparkTableCatalog,
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: JMap[String, String]): Table = {
    tableCatalog.createTable(ident, schema, partitions, properties)
  }

  /** In [[org.apache.spark.sql.internal]]. */

  object ExpressionUtils {
    def column(expr: Expression): Column = new Column(expr)

    def convertToExpression(spark: SparkSession, column: Column): Expression = column.expr
  }
}
