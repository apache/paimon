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

package org.apache.paimon.spark.catalyst.analysis

import org.apache.paimon.catalog.Catalog.SYSTEM_DATABASE_NAME
import org.apache.paimon.spark.catalog.SupportV1Function
import org.apache.paimon.spark.catalog.functions.PaimonFunctions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, Literal}
import org.apache.spark.sql.catalyst.parser.extensions.UnResolvedPaimonV1Function
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.UNRESOLVED_FUNCTION
import org.apache.spark.sql.connector.catalog.CatalogPlugin
import org.apache.spark.sql.types.{BinaryType, DataType, DayTimeIntervalType, LongType, NullType, StringType}
import org.apache.spark.unsafe.types.UTF8String

case class PaimonFunctionResolver(spark: SparkSession) extends Rule[LogicalPlan] {

  protected lazy val catalogManager = spark.sessionState.catalogManager

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan.resolveOperatorsUpWithPruning(_.containsAnyPattern(UNRESOLVED_FUNCTION)) {
      case l: LogicalPlan =>
        l.transformExpressionsWithPruning(_.containsAnyPattern(UNRESOLVED_FUNCTION)) {
          case u: UnresolvedFunction
              if isDescriptorToPresignedUrlFunction(u.nameParts) &&
                u.arguments.forall(_.resolved) =>
            resolveDescriptorToPresignedUrl(u)
          case u: UnresolvedFunction
              if isBlobViewFunction(u.nameParts) && u.arguments.forall(_.resolved) =>
            resolveBlobView(u)
          case u: UnResolvedPaimonV1Function if u.arguments.forall(_.resolved) =>
            u.funcIdent.catalog match {
              case Some(catalog) =>
                catalogManager.catalog(catalog) match {
                  case v1FunctionCatalog: SupportV1Function =>
                    v1FunctionCatalog.registerAndResolveV1Function(u)
                  case _ =>
                    throw new IllegalArgumentException(
                      s"Catalog $catalog is not a v1 function catalog")
                }
              case None => throw new IllegalArgumentException("Catalog name is not defined")
            }
        }
    }

  private def resolveBlobView(u: UnresolvedFunction): Expression = {
    if (u.arguments.length != 3) {
      throw new UnsupportedOperationException(
        s"Function 'blob_view' requires 3 arguments " +
          s"(tableName STRING, fieldName STRING, rowId BIGINT), but found ${u.arguments.length}")
    }
    val tableName = literalString(u.arguments(0), "tableName")
    val fieldName = literalString(u.arguments(1), "fieldName")
    if (u.arguments(2).dataType != LongType) {
      throw new UnsupportedOperationException(
        "The third argument of 'blob_view' must be BIGINT type.")
    }
    ReplacePaimonFunctions.resolveBlobView(spark, tableName, fieldName, u.arguments(2))
  }

  private def resolveDescriptorToPresignedUrl(u: UnresolvedFunction): Expression = {
    val functionName = u.nameParts.last
    if (u.arguments.length != 4) {
      throw new UnsupportedOperationException(
        s"Function '$functionName' requires 4 arguments " +
          s"(sourceTable STRING, descriptor BINARY, extension STRING, " +
          s"validity INTERVAL DAY TO SECOND), but found ${u.arguments.length}")
    }

    val sourceTable = literalString(u.arguments(0), "sourceTable")
    if (sourceTable == null) {
      throw new UnsupportedOperationException("sourceTable must not be null")
    }

    val descriptor = castNullable(u.arguments(1), BinaryType, "descriptor")
    val extension = castNullable(u.arguments(2), StringType, "extension")
    val intervalType = DayTimeIntervalType.DEFAULT
    val validity = u.arguments(3).dataType match {
      case _: DayTimeIntervalType => Cast(u.arguments(3), intervalType)
      case NullType => Cast(u.arguments(3), intervalType)
      case other =>
        throw new UnsupportedOperationException(
          s"validity must be INTERVAL DAY TO SECOND type, but found ${other.simpleString}")
    }
    val ignoreErrors =
      functionName.equalsIgnoreCase(PaimonFunctions.TRY_DESCRIPTOR_TO_PRESIGNED_URL)

    ReplacePaimonFunctions.resolveDescriptorToPresignedUrl(
      spark,
      functionCatalog(u.nameParts),
      sourceTable,
      descriptor,
      extension,
      validity,
      ignoreErrors)
  }

  private def castNullable(
      expression: Expression,
      expectedType: DataType,
      argumentName: String): Expression = {
    expression.dataType match {
      case NullType => Cast(expression, expectedType)
      case actual if actual == expectedType => expression
      case actual =>
        throw new UnsupportedOperationException(
          s"$argumentName must be ${expectedType.simpleString.toUpperCase} type, " +
            s"but found ${actual.simpleString}")
    }
  }

  private def functionCatalog(nameParts: Seq[String]): CatalogPlugin = {
    nameParts.length match {
      case 2 => catalogManager.currentCatalog
      case 3 => catalogManager.catalog(nameParts.head)
      case _ =>
        throw new UnsupportedOperationException(
          s"Invalid function identifier: ${nameParts.mkString(".")}")
    }
  }

  private def literalString(expr: Expression, argumentName: String): String = {
    if (!expr.isInstanceOf[Literal]) {
      throw new UnsupportedOperationException(s"$argumentName must be a literal")
    }
    if (expr.dataType != StringType) {
      throw new UnsupportedOperationException(s"$argumentName must be STRING type")
    }

    val value = expr.eval()
    if (value == null) {
      null
    } else {
      value.asInstanceOf[UTF8String].toString
    }
  }

  private def isBlobViewFunction(nameParts: Seq[String]): Boolean = {
    nameParts.length >= 2 &&
    nameParts.last.equalsIgnoreCase(PaimonFunctions.BLOB_VIEW) &&
    nameParts(nameParts.length - 2).equalsIgnoreCase(SYSTEM_DATABASE_NAME)
  }

  private def isDescriptorToPresignedUrlFunction(nameParts: Seq[String]): Boolean = {
    nameParts.length >= 2 &&
    (nameParts.last.equalsIgnoreCase(PaimonFunctions.DESCRIPTOR_TO_PRESIGNED_URL) ||
      nameParts.last.equalsIgnoreCase(PaimonFunctions.TRY_DESCRIPTOR_TO_PRESIGNED_URL)) &&
    nameParts(nameParts.length - 2).equalsIgnoreCase(SYSTEM_DATABASE_NAME)
  }
}
