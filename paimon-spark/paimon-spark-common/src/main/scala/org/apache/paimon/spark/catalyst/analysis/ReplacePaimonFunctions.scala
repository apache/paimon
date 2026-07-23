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

import org.apache.paimon.spark.{DataConverter, SparkTable, SparkTypeUtils, SparkUtils}
import org.apache.paimon.spark.catalog.SparkBaseCatalog
import org.apache.paimon.spark.catalog.functions.PaimonFunctions
import org.apache.paimon.spark.function.{BlobViewFieldIdSparkFunction, BlobViewSparkFunction, DescriptorToPresignedUrlFunction, ResolvedDescriptorToPresignedUrlFunction}
import org.apache.paimon.spark.utils.CatalogUtils
import org.apache.paimon.table.DataTable
import org.apache.paimon.types.DataTypeRoot
import org.apache.paimon.utils.{InternalRowUtils, TypeUtils}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{ApplyFunctionExpression, Cast, Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.catalyst.plans.logical.{AnalysisHelper, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier}
import org.apache.spark.sql.connector.catalog.PaimonCatalogImplicits._
import org.apache.spark.sql.types.{BinaryType, DataType, DayTimeIntervalType, NullType, StringType}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._

object ReplacePaimonFunctions {

  def resolveDescriptorToPresignedUrl(
      spark: SparkSession,
      functionCatalog: CatalogPlugin,
      sourceTable: String,
      descriptor: Expression,
      extension: Expression,
      validity: Expression,
      ignoreErrors: Boolean): Expression = {
    if (!functionCatalog.isInstanceOf[SparkBaseCatalog]) {
      throw new UnsupportedOperationException(s"$functionCatalog is not a Paimon catalog")
    }

    val parts = spark.sessionState.sqlParser.parseMultipartIdentifier(sourceTable)
    val identifier = parts.length match {
      case 2 => Identifier.of(Array(parts.head), parts(1))
      case 3 =>
        if (!parts.head.equalsIgnoreCase(functionCatalog.name())) {
          throw new UnsupportedOperationException(
            s"Source table catalog '${parts.head}' must match function catalog " +
              s"'${functionCatalog.name()}'.")
        }
        Identifier.of(Array(parts(1)), parts(2))
      case _ =>
        throw new UnsupportedOperationException(
          "sourceTable must be database.table or catalog.database.table")
    }

    val table = functionCatalog.asTableCatalog.loadTable(identifier)
    if (!table.isInstanceOf[SparkTable]) {
      throw new UnsupportedOperationException(s"$sourceTable is not a Paimon table")
    }
    val dataTable = table.asInstanceOf[SparkTable].table match {
      case table: DataTable => table
      case _ =>
        throw new UnsupportedOperationException(s"$sourceTable is not a Paimon data table")
    }
    val fileIO = dataTable.fileIO()
    fileIO.isObjectStore

    ApplyFunctionExpression(
      new ResolvedDescriptorToPresignedUrlFunction(fileIO, dataTable.location(), ignoreErrors),
      Seq(descriptor, extension, validity))
  }

  def resolveBlobView(
      spark: SparkSession,
      tableName: String,
      fieldName: String,
      rowId: Expression): Expression = {
    if (tableName == null || fieldName == null) {
      Literal(null, BinaryType)
    } else {
      val catalogAndIdentifier = SparkUtils
        .catalogAndIdentifier(spark, tableName, spark.sessionState.catalogManager.currentCatalog)
      if (!catalogAndIdentifier.catalog().isInstanceOf[SparkBaseCatalog]) {
        throw new UnsupportedOperationException(
          s"${catalogAndIdentifier.catalog()} is not a Paimon catalog")
      }

      val table =
        catalogAndIdentifier.catalog.asTableCatalog.loadTable(catalogAndIdentifier.identifier())
      assert(table.isInstanceOf[SparkTable])
      val sparkTable = table.asInstanceOf[SparkTable]
      val paimonIdentifier =
        CatalogUtils.toIdentifier(
          catalogAndIdentifier.identifier(),
          catalogAndIdentifier.catalog().name())
      if (!sparkTable.table.rowType().containsField(fieldName)) {
        throw new IllegalArgumentException(
          s"Cannot find blob field $fieldName in upstream table ${paimonIdentifier.getFullName}.")
      }
      val field = sparkTable.table.rowType().getField(fieldName)
      if (!field.`type`().is(DataTypeRoot.BLOB)) {
        throw new IllegalArgumentException(
          s"Field $fieldName in upstream table ${paimonIdentifier.getFullName} " +
            "is not a BLOB field.")
      }

      ApplyFunctionExpression(
        new BlobViewFieldIdSparkFunction,
        Seq(Literal(paimonIdentifier.getFullName), Literal(field.id()), rowId))
    }
  }
}

/** A rule to replace Paimon functions with literal values. */
case class ReplacePaimonFunctions(spark: SparkSession) extends Rule[LogicalPlan] {
  private lazy val catalogManager = spark.sessionState.catalogManager

  private def replaceMaxPt(func: ApplyFunctionExpression): Expression = {
    assert(func.children.size == 1)
    assert(func.children.head.dataType == StringType)
    if (!func.children.head.isInstanceOf[Literal]) {
      throw new UnsupportedOperationException("Table name must be a literal")
    }
    val tableName = func.children.head.eval().asInstanceOf[UTF8String]
    if (tableName == null) {
      throw new UnsupportedOperationException("Table name cannot be null")
    }
    val catalogAndIdentifier = SparkUtils
      .catalogAndIdentifier(
        spark,
        tableName.toString,
        spark.sessionState.catalogManager.currentCatalog)
    if (!catalogAndIdentifier.catalog().isInstanceOf[SparkBaseCatalog]) {
      throw new UnsupportedOperationException(
        s"${catalogAndIdentifier.catalog()} is not a Paimon catalog")
    }

    val table =
      catalogAndIdentifier.catalog.asTableCatalog.loadTable(catalogAndIdentifier.identifier())
    assert(table.isInstanceOf[SparkTable])
    val sparkTable = table.asInstanceOf[SparkTable]
    if (sparkTable.table.partitionKeys().size() == 0) {
      throw new UnsupportedOperationException(s"$table is not a partitioned table")
    }

    val toplevelPartitionType =
      TypeUtils.project(sparkTable.table.rowType, sparkTable.table.partitionKeys()).getTypeAt(0)
    val partitionValues = sparkTable.table.newReadBuilder.newScan
      .listPartitionEntries()
      .asScala
      .filter(_.fileCount() > 0)
      .map {
        partitionEntry => InternalRowUtils.get(partitionEntry.partition(), 0, toplevelPartitionType)
      }
      .sortWith(InternalRowUtils.compare(_, _, toplevelPartitionType.getTypeRoot) < 0)
      .map(DataConverter.fromPaimon(_, toplevelPartitionType))
    if (partitionValues.isEmpty) {
      throw new UnsupportedOperationException(
        s"$table has no partitions or none of the partitions have any data")
    }

    val sparkType = SparkTypeUtils.fromPaimonType(toplevelPartitionType)
    val literal = Literal(partitionValues.last, sparkType)
    Cast(literal, func.dataType)
  }

  private def replaceBlobView(arguments: Seq[Expression]): Expression = {
    assert(arguments.size == 3)
    val tableName = literalString(arguments(0), "tableName")
    val fieldName = literalString(arguments(1), "fieldName")
    ReplacePaimonFunctions.resolveBlobView(spark, tableName, fieldName, arguments(2))
  }

  private def replaceDescriptorToPresignedUrl(
      arguments: Seq[Expression],
      function: DescriptorToPresignedUrlFunction): Expression = {
    assert(arguments.size == 4)
    val sourceTable = literalString(arguments(0), "sourceTable")
    if (sourceTable == null) {
      throw new UnsupportedOperationException("sourceTable must not be null")
    }
    if (arguments(0).dataType != StringType) {
      throw new UnsupportedOperationException("sourceTable must be STRING type")
    }

    val descriptor = castNullable(arguments(1), BinaryType, "descriptor")
    val extension = castNullable(arguments(2), StringType, "extension")
    val intervalType = DayTimeIntervalType.DEFAULT
    val validity = arguments(3).dataType match {
      case _: DayTimeIntervalType => Cast(arguments(3), intervalType)
      case NullType => Cast(arguments(3), intervalType)
      case other =>
        throw new UnsupportedOperationException(
          s"validity must be INTERVAL DAY TO SECOND type, but found ${other.simpleString}")
    }
    val functionCatalog = Option(function.catalogName())
      .map(catalogManager.catalog)
      .getOrElse(catalogManager.currentCatalog)

    ReplacePaimonFunctions.resolveDescriptorToPresignedUrl(
      spark,
      functionCatalog,
      sourceTable,
      descriptor,
      extension,
      validity,
      function.ignoreErrors())
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

  private def literalString(child: Expression, argumentName: String): String = {
    if (!child.isInstanceOf[Literal]) {
      throw new UnsupportedOperationException(s"$argumentName must be a literal")
    }
    val value = child.eval()
    if (value == null) {
      null
    } else {
      value.asInstanceOf[UTF8String].toString
    }
  }

  private def isBlobViewInvoke(invoke: Invoke): Boolean = {
    if (invoke.functionName != "invoke" || !invoke.targetObject.foldable) {
      false
    } else {
      invoke.targetObject.eval().isInstanceOf[BlobViewSparkFunction]
    }
  }

  private def descriptorToPresignedUrlFunction(
      invoke: Invoke): Option[DescriptorToPresignedUrlFunction] = {
    if (invoke.functionName != "invoke" || !invoke.targetObject.foldable) {
      None
    } else {
      invoke.targetObject.eval() match {
        case function: DescriptorToPresignedUrlFunction => Some(function)
        case _ => None
      }
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    AnalysisHelper.allowInvokingTransformsInAnalyzer {
      plan.transformAllExpressions {
        case func: ApplyFunctionExpression
            if func.function.name() == "max_pt" &&
              func.function.canonicalName().startsWith("paimon") =>
          replaceMaxPt(func)
        case func: ApplyFunctionExpression
            if func.function.name() == PaimonFunctions.BLOB_VIEW &&
              func.function.canonicalName().startsWith("paimon") =>
          replaceBlobView(func.children)
        case func: ApplyFunctionExpression
            if func.function.isInstanceOf[DescriptorToPresignedUrlFunction] =>
          replaceDescriptorToPresignedUrl(
            func.children,
            func.function.asInstanceOf[DescriptorToPresignedUrlFunction])
        case invoke: Invoke if isBlobViewInvoke(invoke) =>
          replaceBlobView(invoke.arguments)
        case invoke: Invoke if descriptorToPresignedUrlFunction(invoke).isDefined =>
          replaceDescriptorToPresignedUrl(
            invoke.arguments,
            descriptorToPresignedUrlFunction(invoke).get)
      }
    }
  }
}
