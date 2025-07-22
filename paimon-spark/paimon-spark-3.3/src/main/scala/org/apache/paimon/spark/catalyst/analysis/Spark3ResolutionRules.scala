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

import org.apache.paimon.CoreOptions
import org.apache.paimon.CoreOptions.MergeEngine
import org.apache.paimon.spark.SparkTable
import org.apache.paimon.spark.catalyst.Compatibility
import org.apache.paimon.spark.commands.{PaimonShowTablePartitionCommand, PaimonShowTablesExtendedCommand}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis.{NamedRelation, PartitionSpec, ResolvedNamespace, UnresolvedPartitionSpec}
import org.apache.spark.sql.catalyst.expressions.{Alias, ArrayTransform, Attribute, CreateStruct, Expression, GetArrayItem, GetStructField, If, IsNull, LambdaFunction, Literal, NamedExpression, NamedLambdaVariable}
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, LogicalPlan, Project, ShowTableExtended}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, Metadata, StructField, StructType}

import scala.collection.JavaConverters._
import scala.collection.mutable

case class Spark3ResolutionRules(session: SparkSession)
  extends Rule[LogicalPlan]
  with SQLConfHelper {

  import org.apache.spark.sql.connector.catalog.PaimonCatalogImplicits._

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsDown {
    case ShowTableExtended(
          ResolvedNamespace(catalog, ns),
          pattern,
          partitionSpec @ (None | Some(UnresolvedPartitionSpec(_, _))),
          output) =>
      partitionSpec
        .map {
          spec: PartitionSpec =>
            val table = Identifier.of(ns.toArray, pattern)
            val resolvedSpec =
              PaimonResolvePartitionSpec.resolve(catalog.asTableCatalog, table, spec)
            PaimonShowTablePartitionCommand(output, catalog.asTableCatalog, table, resolvedSpec)
        }
        .getOrElse {
          PaimonShowTablesExtendedCommand(catalog.asTableCatalog, ns, pattern, output)
        }

    case insertPlan @ InsertIntoStatement(
          r @ DataSourceV2Relation(t: SparkTable, _, _, _, _),
          _,
          userSpecifiedCols,
          query,
          _,
          _)
        if t.table
          .options()
          .asScala
          .get(CoreOptions.MERGE_ENGINE.key())
          .contains(MergeEngine.PARTIAL_UPDATE.toString)
          && userSpecifiedCols.nonEmpty =>
      if (userSpecifiedCols.length == r.output.length) return insertPlan

      if (userSpecifiedCols.length != query.output.length) {
        throw new RuntimeException(
          s"Cannot write incompatible data for the table `${t.table.name}`, " +
            "the number of data columns don't match with the specified number of columns.")
      }

      val aliasCols =
        userSpecifiedCols.zip(query.output).map { case (str, attribute) => Alias(attribute, str)() }

      val aliasQuery = Project(aliasCols, query)

      val newQuery = resolveQueryColumns(aliasQuery, r)
      val newUserSpecifiedCols = r.output.map(_.name)
      insertPlan.copy(query = newQuery, userSpecifiedCols = newUserSpecifiedCols)

  }

  private def resolveQueryColumns(query: LogicalPlan, table: NamedRelation): LogicalPlan = {

    val inputCols = query.output
    val expectedCols = table.output
    if (inputCols.size > expectedCols.size) {
      throw new RuntimeException(
        s"Cannot write incompatible data for the table `${table.name}`, " +
          "the number of data columns don't match with the table schema's.")
    }

    val matchedCols = mutable.HashSet.empty[String]
    val reorderedCols = expectedCols.map {
      expectedCol =>
        val matched = inputCols.filter(col => conf.resolver(col.name, expectedCol.name))
        if (matched.isEmpty) {
          // TODO: Support Spark default value framework if Paimon supports to change default values.
          if (!expectedCol.nullable) {
            throw new RuntimeException(
              s"Cannot write incompatible data for the table `${table.name}`, " +
                s"due to non-nullable column `${expectedCol.name}` has no specified value.")
          }
          Alias(Literal(null, expectedCol.dataType), expectedCol.name)()
        } else if (matched.length > 1) {
          throw new RuntimeException(
            s"Cannot write incompatible data for the table `${table.name}`, due to column name conflicts: ${matched
                .mkString(", ")}.")
        } else {
          matchedCols += matched.head.name
          val matchedCol = matched.head
          addCastToColumn(matchedCol, expectedCol)
        }
    }

    assert(reorderedCols.length == expectedCols.length)
    if (matchedCols.size < inputCols.length) {
      val extraCols = inputCols
        .filterNot(col => matchedCols.contains(col.name))
        .map(col => s"${col.name}")
        .mkString(", ")
      // There are seme unknown column names
      throw new RuntimeException(
        s"Cannot write incompatible data for the table `${table.name}`, due to unknown column names: $extraCols.")
    }
    Project(reorderedCols, query)

  }

  private def addCastToColumn(attr: Attribute, targetAttr: Attribute): NamedExpression = {

    val expr = (attr.dataType, targetAttr.dataType) match {
      case (s, t) if s == t =>
        attr
      case (s: StructType, t: StructType) if s != t =>
        addCastToStructByName(attr, s, t)

      case (ArrayType(s: StructType, sNull: Boolean), ArrayType(t: StructType, _: Boolean))
          if s != t =>
        val castToStructFunc = addCastToStructByName _

        castToArrayStruct(attr, s, t, sNull, castToStructFunc)
      case _ =>
        cast(attr, targetAttr.dataType)
    }
    Alias(stringLengthCheck(expr, targetAttr.metadata), targetAttr.name)(explicitMetadata =
      Option(targetAttr.metadata))
  }

  private def addCastToStructByName(
      parent: NamedExpression,
      source: StructType,
      target: StructType): NamedExpression = {
    val fields = target.map {
      case targetField @ StructField(name, nested: StructType, _, _) =>
        val sourceIndex = source.fieldIndex(name)
        val sourceField = source(sourceIndex)
        sourceField.dataType match {
          case s: StructType =>
            val subField = castStructField(parent, sourceIndex, sourceField.name, targetField)
            addCastToStructByName(subField, s, nested)
          case o =>
            throw new RuntimeException(s"Can not support to cast $o to StructType.")
        }
      case targetField =>
        val sourceIndex = source.fieldIndex(targetField.name)
        val sourceField = source(sourceIndex)
        castStructField(parent, sourceIndex, sourceField.name, targetField)
    }
    structAlias(fields, parent)
  }

  private def structAlias(
      fields: Seq[NamedExpression],
      parent: NamedExpression): NamedExpression = {

    val struct = CreateStruct(fields)
    val res = if (parent.nullable) {
      If(IsNull(parent), Literal(null, struct.dataType), struct)
    } else {
      struct
    }
    Alias(res, parent.name)(parent.exprId, parent.qualifier, Option(parent.metadata))
  }

  private def castStructField(
      parent: NamedExpression,
      i: Int,
      sourceFieldName: String,
      targetField: StructField): NamedExpression = {
    Alias(
      stringLengthCheck(
        cast(GetStructField(parent, i, Option(sourceFieldName)), targetField.dataType),
        targetField.metadata),
      targetField.name)(explicitMetadata = Option(targetField.metadata))
  }

  private def cast(expr: Expression, dataType: DataType): Expression = {
    val cast = Compatibility.cast(expr, dataType, Option(conf.sessionLocalTimeZone))
    cast.setTagValue(Compatibility.castByTableInsertionTag, ())
    cast
  }

  private def stringLengthCheck(expr: Expression, metadata: Metadata): Expression = {
    if (!conf.charVarcharAsString) {
      CharVarcharUtils
        .getRawType(metadata)
        .map(rawType => CharVarcharUtils.stringLengthCheck(expr, rawType))
        .getOrElse(expr)
    } else {
      expr
    }
  }

  private def castToArrayStruct(
      parent: NamedExpression,
      source: StructType,
      target: StructType,
      sourceNullable: Boolean,
      castToStructFunc: (NamedExpression, StructType, StructType) => NamedExpression
  ): Expression = {
    val structConverter: (Expression, Expression) => Expression = (_, i) =>
      castToStructFunc(Alias(GetArrayItem(parent, i), i.toString)(), source, target)
    val transformLambdaFunc = {
      val elementVar = NamedLambdaVariable("elementVar", source, sourceNullable)
      val indexVar = NamedLambdaVariable("indexVar", IntegerType, false)
      LambdaFunction(structConverter(elementVar, indexVar), Seq(elementVar, indexVar))
    }
    ArrayTransform(parent, transformLambdaFunc)
  }
}
