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

import org.apache.paimon.spark.catalyst.Compatibility

import org.apache.spark.sql.PaimonUtils
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.types._

import scala.collection.mutable

/**
 * Forked `TableOutputResolver` parameterized by [[MissingFieldBehavior]]. Top-level always
 * NULL-fills missing columns (mirrors Spark INSERT FILL). Source-extras kept at any depth relies on
 * Paimon's `ACCEPT_ANY_SCHEMA` short-circuiting `outputResolved`.
 */
object PaimonOutputResolver extends SQLConfHelper {

  /**
   * How nested struct field misalignment is handled:
   *   - [[FailMissing]]: strict — nested missing target / source-extra throws.
   *   - [[NullForMissing]]: merge-schema for INSERT / explicit UPDATE — missing NULL-fills,
   *     source-extras kept so [[org.apache.paimon.spark.commands.SchemaEvolutionHelper]] evolves
   *     the table.
   *   - [[PreserveTarget]]: merge-schema for `UPDATE *` struct — missing source field substitutes
   *     `GetStructField(targetExpr, ordinal)` to keep the current target value.
   */
  sealed trait MissingFieldBehavior
  object MissingFieldBehavior {
    case object FailMissing extends MissingFieldBehavior
    case object NullForMissing extends MissingFieldBehavior
    case object PreserveTarget extends MissingFieldBehavior

    def of(mergeSchemaEnabled: Boolean): MissingFieldBehavior =
      if (mergeSchemaEnabled) NullForMissing else FailMissing
  }

  import MissingFieldBehavior._

  def resolveOutputColumns(
      tableName: String,
      expected: Seq[Attribute],
      query: LogicalPlan,
      byName: Boolean,
      mergeSchemaEnabled: Boolean): LogicalPlan = {
    val resolved: Seq[NamedExpression] = if (byName) {
      reorderColumnsByName(
        tableName,
        query.output,
        expected,
        MissingFieldBehavior.of(mergeSchemaEnabled),
        None,
        Nil)
    } else {
      resolveColumnsByPosition(tableName, query.output, expected, Nil)
    }
    if (resolved == query.output) {
      query
    } else {
      Project(resolved, query)
    }
  }

  /**
   * Align a MERGE/UPDATE assignment value to its target attribute. Returns the raw value (no outer
   * Alias) — the downstream rewrite re-wraps it. `targetExpr` is required when
   * `behavior = PreserveTarget`.
   */
  def resolveValue(
      value: Expression,
      expected: Attribute,
      behavior: MissingFieldBehavior,
      targetExpr: Option[Expression],
      colPath: Seq[String]): Expression = {
    if (behavior == PreserveTarget && targetExpr.isEmpty) {
      throw new IllegalArgumentException("PreserveTarget behavior requires a targetExpr")
    }
    val named: NamedExpression = value match {
      case ne: NamedExpression => ne
      case other => Alias(other, expected.name)()
    }
    stripOuterAlias(
      resolveField(
        tableName = "",
        input = named,
        expected = restoreActualType(expected),
        byName = true,
        behavior = behavior,
        targetExpr = targetExpr,
        colPath = colPath))
  }

  private def reorderColumnsByName(
      tableName: String,
      inputCols: Seq[NamedExpression],
      expectedCols: Seq[Attribute],
      behavior: MissingFieldBehavior,
      targetExpr: Option[Expression],
      colPath: Seq[String]): Seq[NamedExpression] = {
    val isTopLevel = colPath.isEmpty
    val matchedNames = mutable.HashSet.empty[String]
    val reordered = expectedCols.zipWithIndex.map {
      case (expectedCol, ordinal) =>
        val matches = inputCols.filter(col => conf.resolver(col.name, expectedCol.name))
        val newColPath = colPath :+ expectedCol.name
        if (matches.isEmpty) {
          fillMissing(tableName, expectedCol, ordinal, isTopLevel, behavior, targetExpr, newColPath)
        } else if (matches.length > 1) {
          throw new RuntimeException(
            s"Cannot write to `$tableName`, " +
              s"due to ambiguous column name `${newColPath.mkString(".")}`.")
        } else {
          matchedNames += matches.head.name
          val childTarget =
            if (behavior == PreserveTarget) {
              targetExpr.map(t => GetStructField(t, ordinal, Some(expectedCol.name)))
            } else None
          resolveField(
            tableName,
            matches.head,
            restoreActualType(expectedCol),
            byName = true,
            behavior,
            childTarget,
            newColPath)
        }
    }

    if (matchedNames.size < inputCols.length) {
      val extras = inputCols.filterNot(col => matchedNames.contains(col.name))
      if (behavior == FailMissing) {
        val extrasStr = extras.map(c => s"`${c.name}`").mkString(", ")
        val msg = if (isTopLevel) {
          s"Cannot write to `$tableName`, extra columns: $extrasStr"
        } else {
          s"Cannot write to `$tableName`, " +
            s"extra struct fields at `${colPath.mkString(".")}`: $extrasStr"
        }
        throw new RuntimeException(msg)
      }
      reordered ++ extras.map(preserveAsAlias)
    } else {
      reordered
    }
  }

  private def fillMissing(
      tableName: String,
      expectedCol: Attribute,
      ordinal: Int,
      isTopLevel: Boolean,
      behavior: MissingFieldBehavior,
      targetExpr: Option[Expression],
      newColPath: Seq[String]): NamedExpression = behavior match {
    // Top-level always NULL-fills (mirrors Spark INSERT FILL). Nested-level strict throws.
    case FailMissing if !isTopLevel =>
      throw new RuntimeException(
        s"Cannot write to `$tableName`, nested struct field " +
          s"`${newColPath.mkString(".")}` is missing in source. " +
          s"Enable 'spark.paimon.write.merge-schema' to fill it with NULL.")
    case PreserveTarget if targetExpr.isDefined =>
      applyColumnMetadata(
        GetStructField(targetExpr.get, ordinal, Some(expectedCol.name)),
        expectedCol)
    case _ =>
      nullFill(expectedCol)
  }

  private def resolveColumnsByPosition(
      tableName: String,
      inputCols: Seq[NamedExpression],
      expectedCols: Seq[Attribute],
      colPath: Seq[String]): Seq[NamedExpression] = {
    val actualExpectedCols = expectedCols.map(restoreActualType)
    if (inputCols.size != actualExpectedCols.size) {
      val where = if (colPath.isEmpty) {
        s"`$tableName`"
      } else {
        s"`$tableName` at `${colPath.mkString(".")}`"
      }
      throw new RuntimeException(s"Cannot write to $where, " +
        s"the number of data columns (${inputCols.size}) doesn't match the table schema's (${actualExpectedCols.size}).")
    }
    inputCols.zip(actualExpectedCols).map {
      case (inputCol, expectedCol) =>
        resolveField(
          tableName,
          inputCol,
          expectedCol,
          byName = false,
          behavior = FailMissing,
          targetExpr = None,
          colPath :+ expectedCol.name)
    }
  }

  private def resolveField(
      tableName: String,
      input: NamedExpression,
      expected: Attribute,
      byName: Boolean,
      behavior: MissingFieldBehavior,
      targetExpr: Option[Expression],
      colPath: Seq[String]): NamedExpression = {
    (input.dataType, expected.dataType) match {
      case (sourceType: StructType, targetType: StructType) =>
        resolveStructType(
          tableName,
          input,
          sourceType,
          expected,
          targetType,
          byName,
          behavior,
          targetExpr,
          colPath)
      case (sourceType: ArrayType, targetType: ArrayType) =>
        resolveArrayType(
          tableName,
          input,
          sourceType,
          expected,
          targetType,
          byName,
          behavior,
          colPath)
      case (sourceType: MapType, targetType: MapType) =>
        resolveMapType(
          tableName,
          input,
          sourceType,
          expected,
          targetType,
          byName,
          behavior,
          colPath)
      case _ =>
        checkField(input, expected, colPath)
    }
  }

  private def resolveStructType(
      tableName: String,
      input: Expression,
      sourceType: StructType,
      expected: Attribute,
      targetType: StructType,
      byName: Boolean,
      behavior: MissingFieldBehavior,
      targetExpr: Option[Expression],
      colPath: Seq[String]): NamedExpression = {
    val nullCheckedInput = checkNullability(input, expected, colPath)
    val fields = sourceType.zipWithIndex.map {
      case (f, i) =>
        Alias(GetStructField(nullCheckedInput, i, Option(f.name)), f.name)()
    }
    val resolved = if (byName) {
      reorderColumnsByName(
        tableName,
        fields,
        PaimonUtils.toAttributes(targetType),
        behavior,
        targetExpr,
        colPath)
    } else {
      resolveColumnsByPosition(tableName, fields, PaimonUtils.toAttributes(targetType), colPath)
    }
    val targetNamedStruct = CreateStruct(resolved)
    val res = maybeWrapWithNullPreservation(
      sourceExpr = nullCheckedInput,
      sourceType = sourceType,
      targetType = targetType,
      targetNamedStructExpr = targetNamedStruct,
      originalTargetExprOpt = if (behavior == PreserveTarget) targetExpr else None
    )
    applyColumnMetadata(res, expected)
  }

  /**
   * Collapse `NULL -> struct(NULL, ...)` expansion back to NULL. Under `PreserveTarget` with
   * target-only fields, also requires the original target to be NULL so preserved leaves survive.
   */
  private def maybeWrapWithNullPreservation(
      sourceExpr: Expression,
      sourceType: StructType,
      targetType: StructType,
      targetNamedStructExpr: Expression,
      originalTargetExprOpt: Option[Expression]): Expression = {
    if (!sourceExpr.nullable) return targetNamedStructExpr

    val sourceNullCondition = IsNull(sourceExpr)
    val targetHasExtraFieldsToPreserveValue =
      hasExtraStructFieldsToPreserveValue(sourceType, targetType)
    val fullNullCondition = originalTargetExprOpt match {
      case Some(originalTargetExpr) if targetHasExtraFieldsToPreserveValue =>
        And(sourceNullCondition, IsNull(originalTargetExpr))
      case Some(_) | None => sourceNullCondition
    }
    If(fullNullCondition, Literal(null, targetNamedStructExpr.dataType), targetNamedStructExpr)
  }

  /** True if `targetStruct` has fields, at any nesting level, missing from `sourceStruct`. */
  private def hasExtraStructFieldsToPreserveValue(
      sourceStruct: StructType,
      targetStruct: StructType): Boolean = {
    if (targetStruct.length > sourceStruct.length) return true

    val (commonFields, targetOnlyFields) = targetStruct.fields.partition {
      targetField => sourceStruct.exists(f => conf.resolver(f.name, targetField.name))
    }
    if (targetOnlyFields.nonEmpty) return true

    commonFields.exists {
      targetField =>
        sourceStruct.find(f => conf.resolver(f.name, targetField.name)).exists {
          sourceField =>
            (sourceField.dataType, targetField.dataType) match {
              case (s: StructType, t: StructType) => hasExtraStructFieldsToPreserveValue(s, t)
              case _ => false
            }
        }
    }
  }

  private def resolveArrayType(
      tableName: String,
      input: Expression,
      sourceType: ArrayType,
      expected: Attribute,
      targetType: ArrayType,
      byName: Boolean,
      behavior: MissingFieldBehavior,
      colPath: Seq[String]): NamedExpression = {
    val nullCheckedInput = checkNullability(input, expected, colPath)
    val param = NamedLambdaVariable("element", sourceType.elementType, sourceType.containsNull)
    val fakeAttr =
      AttributeReference("element", targetType.elementType, targetType.containsNull)()
    val resolved = if (byName) {
      reorderColumnsByName(tableName, Seq(param), Seq(fakeAttr), behavior, None, colPath)
    } else {
      resolveColumnsByPosition(tableName, Seq(param), Seq(fakeAttr), colPath)
    }
    assert(resolved.length == 1)
    val elementExpr = stripOuterAlias(resolved.head)
    val transformed = if (elementExpr.fastEquals(param)) {
      nullCheckedInput
    } else {
      ArrayTransform(nullCheckedInput, LambdaFunction(elementExpr, Seq(param)))
    }
    applyColumnMetadata(transformed, expected)
  }

  private def resolveMapType(
      tableName: String,
      input: Expression,
      sourceType: MapType,
      expected: Attribute,
      targetType: MapType,
      byName: Boolean,
      behavior: MissingFieldBehavior,
      colPath: Seq[String]): NamedExpression = {
    val nullCheckedInput = checkNullability(input, expected, colPath)
    val keyParam = NamedLambdaVariable("key", sourceType.keyType, nullable = false)
    val fakeKeyAttr = AttributeReference("key", targetType.keyType, nullable = false)()
    val resolvedKey = if (byName) {
      reorderColumnsByName(tableName, Seq(keyParam), Seq(fakeKeyAttr), behavior, None, colPath)
    } else {
      resolveColumnsByPosition(tableName, Seq(keyParam), Seq(fakeKeyAttr), colPath)
    }

    val valueParam =
      NamedLambdaVariable("value", sourceType.valueType, sourceType.valueContainsNull)
    val fakeValueAttr =
      AttributeReference("value", targetType.valueType, targetType.valueContainsNull)()
    val resolvedValue = if (byName) {
      reorderColumnsByName(tableName, Seq(valueParam), Seq(fakeValueAttr), behavior, None, colPath)
    } else {
      resolveColumnsByPosition(tableName, Seq(valueParam), Seq(fakeValueAttr), colPath)
    }

    assert(resolvedKey.length == 1 && resolvedValue.length == 1)
    val keyExpr = stripOuterAlias(resolvedKey.head)
    val valueExpr = stripOuterAlias(resolvedValue.head)
    val transformed = if (keyExpr.fastEquals(keyParam) && valueExpr.fastEquals(valueParam)) {
      nullCheckedInput
    } else {
      val newKeys = if (keyExpr.fastEquals(keyParam)) {
        MapKeys(nullCheckedInput)
      } else {
        ArrayTransform(MapKeys(nullCheckedInput), LambdaFunction(keyExpr, Seq(keyParam)))
      }
      val newValues = if (valueExpr.fastEquals(valueParam)) {
        MapValues(nullCheckedInput)
      } else {
        ArrayTransform(MapValues(nullCheckedInput), LambdaFunction(valueExpr, Seq(valueParam)))
      }
      MapFromArrays(newKeys, newValues)
    }
    applyColumnMetadata(transformed, expected)
  }

  private def checkField(
      input: NamedExpression,
      expected: Attribute,
      colPath: Seq[String]): NamedExpression = {
    val attrTypeHasCharVarchar = CharVarcharUtils.hasCharVarchar(expected.dataType)
    val attrTypeWithoutCharVarchar = if (attrTypeHasCharVarchar) {
      CharVarcharUtils.replaceCharVarcharWithString(expected.dataType)
    } else {
      expected.dataType
    }
    val casted = if (input.dataType == attrTypeWithoutCharVarchar) {
      input
    } else {
      addCast(input, attrTypeWithoutCharVarchar)
    }
    val withStrLenCheck = if (conf.charVarcharAsString || !attrTypeHasCharVarchar) {
      casted
    } else {
      CharVarcharUtils.stringLengthCheck(casted, expected.dataType)
    }
    val nullChecked = checkNullability(withStrLenCheck, expected, colPath)
    applyColumnMetadata(nullChecked, expected)
  }

  private def checkNullability(
      input: Expression,
      expected: Attribute,
      colPath: Seq[String]): Expression = {
    if (input.nullable && !expected.nullable) {
      AssertNotNull(input, colPath)
    } else {
      input
    }
  }

  private def addCast(expr: Expression, dataType: DataType): Expression = {
    val cast = Compatibility.cast(expr, dataType, Option(conf.sessionLocalTimeZone))
    cast.setTagValue(Compatibility.castByTableInsertionTag, ())
    cast
  }

  private def nullFill(expected: Attribute): NamedExpression = {
    applyColumnMetadata(Literal(null, expected.dataType), expected)
  }

  private def preserveAsAlias(expr: NamedExpression): NamedExpression = {
    applyColumnMetadata(expr, expr.toAttribute)
  }

  private def stripOuterAlias(expr: Expression): Expression = expr match {
    case a: Alias => a.child
    case other => other
  }

  private def restoreActualType(attr: Attribute): Attribute = {
    attr.withDataType(CharVarcharUtils.getRawType(attr.metadata).getOrElse(attr.dataType))
  }

  // Inlined `CharVarcharUtils.CHAR_VARCHAR_TYPE_STRING_METADATA_KEY` — the constant is
  // `private[sql]` but stable across Spark 3.2–4.1.
  private val CHAR_VARCHAR_TYPE_STRING_KEY = "__CHAR_VARCHAR_TYPE_STRING"

  // Mirrors `CharVarcharUtils.cleanMetadata` (public only in 4.1). Strips the read-side marker
  // before metadata reaches a Write-side Alias.
  private def cleanMetadata(metadata: Metadata): Metadata =
    new MetadataBuilder().withMetadata(metadata).remove(CHAR_VARCHAR_TYPE_STRING_KEY).build()

  // Mirrors `TableOutputResolver.applyColumnMetadata` (SPARK-52772). The explicit-metadata Alias
  // prevents later rewrites from inlining the inner AttributeReference and leaking source-side
  // metadata (CURRENT_DEFAULT, char/varchar marker) into the Write, which would flip the plan
  // back to unresolved.
  private def applyColumnMetadata(expr: Expression, expected: Attribute): NamedExpression = {
    val required = cleanMetadata(expected.metadata)
    expr match {
      case v: NamedLambdaVariable if v.name == expected.name && v.metadata == required => v
      case _ =>
        val stripped = expr match {
          case a: Alias => a.child
          case _ => expr
        }
        Alias(stripped, expected.name)(explicitMetadata = Some(required))
    }
  }
}
