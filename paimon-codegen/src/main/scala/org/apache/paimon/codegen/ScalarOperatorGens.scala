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

package org.apache.paimon.codegen

import org.apache.paimon.codegen.GenerateUtils._
import org.apache.paimon.data.serializer.InternalMapSerializer
import org.apache.paimon.types._
import org.apache.paimon.types.DataTypeChecks.{getFieldTypes, isCompositeType}
import org.apache.paimon.utils.InternalRowUtils
import org.apache.paimon.utils.TypeCheckUtils._
import org.apache.paimon.utils.TypeUtils.isInteroperable

import scala.collection.JavaConverters._

/**
 * Utilities to generate SQL scalar operators, e.g. arithmetic operator, compare operator, equal
 * operator, etc.
 */
object ScalarOperatorGens {

  def generateEquals(
      ctx: CodeGeneratorContext,
      left: GeneratedExpression,
      right: GeneratedExpression,
      resultType: DataType): GeneratedExpression = {
    // In the current use case, there is no need to support implicit type conversion,
    // and we temporarily enforce that the compared types are the same.
    if (left.resultType != right.resultType) {
      throw new CodeGenException(
        "implicit type conversion between " +
          s"${left.resultType.getTypeRoot}" +
          s" and " +
          s"${right.resultType.getTypeRoot}" +
          s" is not supported now")
    }
    val canEqual = isInteroperable(left.resultType, right.resultType)
    if (isCharacterString(left.resultType) && isCharacterString(right.resultType)) {
      generateOperatorIfNotNull(ctx, resultType, left, right)(
        (leftTerm, rightTerm) => s"$leftTerm.equals($rightTerm)")
    }
    // numeric types
    else if (isNumeric(left.resultType) && isNumeric(right.resultType)) {
      generateComparison(ctx, "==", left, right, resultType)
    }
    // array types
    else if (isArray(left.resultType) && canEqual) {
      generateArrayComparison(ctx, left, right, resultType)
    }
    // map types
    else if (isMap(left.resultType) && canEqual) {
      val mapType = left.resultType.asInstanceOf[MapType]
      generateMapComparison(ctx, left, right, mapType.getKeyType, mapType.getValueType, resultType)
    }
    // multiset types
    else if (isMultiset(left.resultType) && canEqual) {
      val multisetType = left.resultType.asInstanceOf[MultisetType]
      generateMapComparison(
        ctx,
        left,
        right,
        multisetType.getElementType,
        new IntType(false),
        resultType)
    }
    // comparable types of same type
    else if (isComparable(left.resultType) && canEqual) {
      generateComparison(ctx, "==", left, right, resultType)
    } else if (isCompositeType(left.resultType) && canEqual) {
      val equaliserTerm = generateRowEqualiser(ctx, left.resultType)
      generateOperatorIfNotNull(ctx, resultType, left, right)(
        (leftTerm, rightTerm) => s"$equaliserTerm.equals($leftTerm, $rightTerm)")
    }
    // non comparable types
    else {
      generateOperatorIfNotNull(ctx, resultType, left, right) {
        if (isReference(left.resultType)) {
          (leftTerm, rightTerm) => s"$leftTerm.equals($rightTerm)"
        } else if (isReference(right.resultType)) {
          (leftTerm, rightTerm) => s"$rightTerm.equals($leftTerm)"
        } else {
          throw new CodeGenException(
            s"Incomparable types: ${left.resultType} and " +
              s"${right.resultType}")
        }
      }
    }
  }

  /** Generates [[RecordEqualiser]] code for row and return equaliser name. */
  def generateRowEqualiser(ctx: CodeGeneratorContext, fieldType: DataType): String = {
    val equaliserGenerator =
      new EqualiserCodeGenerator(getFieldTypes(fieldType).asScala.toArray)
    val generatedEqualiser =
      equaliserGenerator.generateRecordEqualiser("fieldGeneratedEqualiser")
    val generatedEqualiserTerm =
      ctx.addReusableObject(generatedEqualiser, "fieldGeneratedEqualiser")
    val equaliserTypeTerm = classOf[RecordEqualiser].getCanonicalName
    val equaliserTerm = newName("equaliser")
    ctx.addReusableMember(s"private $equaliserTypeTerm $equaliserTerm = null;")
    ctx.addReusableInitStatement(
      s"""
         |$equaliserTerm = ($equaliserTypeTerm)
         |  $generatedEqualiserTerm.newInstance(this.getClass().getClassLoader());
         |""".stripMargin)
    equaliserTerm
  }

  /** Generates comparison code for numeric types and comparable types of same type. */
  def generateComparison(
      ctx: CodeGeneratorContext,
      operator: String,
      left: GeneratedExpression,
      right: GeneratedExpression,
      resultType: DataType): GeneratedExpression = {
    generateOperatorIfNotNull(ctx, resultType, left, right) {
      // either side is decimal
      if (isDecimal(left.resultType) || isDecimal(right.resultType)) {
        (leftTerm, rightTerm) => s"$leftTerm.compareTo($rightTerm) $operator 0"
      }
      // both sides are numeric
      else if (isNumeric(left.resultType) && isNumeric(right.resultType)) {
        (leftTerm, rightTerm) => s"$leftTerm $operator $rightTerm"
      }

      // both sides are timestamp
      else if (isTimestamp(left.resultType) && isTimestamp(right.resultType)) {
        (leftTerm, rightTerm) => s"$leftTerm.compareTo($rightTerm) $operator 0"
      }

      // both sides are timestamp with local zone
      else if (
        isTimestampWithLocalZone(left.resultType) &&
        isTimestampWithLocalZone(right.resultType)
      ) { (leftTerm, rightTerm) => s"$leftTerm.compareTo($rightTerm) $operator 0" }

      // both sides are temporal of same type
      else if (
        isTemporal(left.resultType) &&
        isInteroperable(left.resultType, right.resultType)
      ) { (leftTerm, rightTerm) => s"$leftTerm $operator $rightTerm" }
      // both sides are boolean
      else if (
        isBoolean(left.resultType) &&
        isInteroperable(left.resultType, right.resultType)
      ) {
        operator match {
          case "==" | "!=" => (leftTerm, rightTerm) => s"$leftTerm $operator $rightTerm"
          case ">" | "<" | "<=" | ">=" =>
            (leftTerm, rightTerm) => s"java.lang.Boolean.compare($leftTerm, $rightTerm) $operator 0"
          case _ => throw new CodeGenException(s"Unsupported boolean comparison '$operator'.")
        }
      }
      // both sides are binary type
      else if (
        isBinaryString(left.resultType) &&
        isInteroperable(left.resultType, right.resultType)
      ) {
        val utilName = classOf[InternalRowUtils].getCanonicalName
        val dataTypeRootName = classOf[DataTypeRoot].getCanonicalName
        (leftTerm, rightTerm) =>
          s"$utilName.compare($leftTerm, $rightTerm, $dataTypeRootName.${left.resultType.getTypeRoot}) $operator 0"
      }
      // both sides are same comparable type
      else if (
        isComparable(left.resultType) &&
        isInteroperable(left.resultType, right.resultType)
      ) {
        (leftTerm, rightTerm) =>
          s"(($leftTerm == null) ? (($rightTerm == null) ? 0 : -1) : (($rightTerm == null) ? " +
            s"1 : ($leftTerm.compareTo($rightTerm)))) $operator 0"
      } else {
        throw new CodeGenException(
          s"Incomparable types: ${left.resultType} and " +
            s"${right.resultType}")
      }
    }
  }

  private def generateArrayComparison(
      ctx: CodeGeneratorContext,
      left: GeneratedExpression,
      right: GeneratedExpression,
      resultType: DataType): GeneratedExpression = {
    generateCallWithStmtIfArgsNotNull(ctx, resultType, Seq(left, right)) {
      args =>
        val leftTerm = args.head
        val rightTerm = args(1)

        val resultTerm = newName("compareResult")

        val elementType = left.resultType.asInstanceOf[ArrayType].getElementType
        val elementCls = primitiveTypeTermForType(elementType)
        val elementDefault = primitiveDefaultValue(elementType)

        val leftElementTerm = newName("leftElement")
        val leftElementNullTerm = newName("leftElementIsNull")
        val leftElementExpr =
          GeneratedExpression(leftElementTerm, leftElementNullTerm, "", elementType)

        val rightElementTerm = newName("rightElement")
        val rightElementNullTerm = newName("rightElementIsNull")
        val rightElementExpr =
          GeneratedExpression(rightElementTerm, rightElementNullTerm, "", elementType)

        val indexTerm = newName("index")
        val elementEqualsExpr = generateEquals(
          ctx,
          leftElementExpr,
          rightElementExpr,
          new BooleanType(elementType.isNullable))

        val stmt =
          s"""
             |boolean $resultTerm;
             |if ($leftTerm instanceof $BINARY_ARRAY && $rightTerm instanceof $BINARY_ARRAY) {
             |  $resultTerm = $leftTerm.equals($rightTerm);
             |} else {
             |  if ($leftTerm.size() == $rightTerm.size()) {
             |    $resultTerm = true;
             |    for (int $indexTerm = 0; $indexTerm < $leftTerm.size(); $indexTerm++) {
             |      $elementCls $leftElementTerm = $elementDefault;
             |      boolean $leftElementNullTerm = $leftTerm.isNullAt($indexTerm);
             |      if (!$leftElementNullTerm) {
             |        $leftElementTerm =
             |          ${rowFieldReadAccess(indexTerm, leftTerm, elementType)};
             |      }
             |
             |      $elementCls $rightElementTerm = $elementDefault;
             |      boolean $rightElementNullTerm = $rightTerm.isNullAt($indexTerm);
             |      if (!$rightElementNullTerm) {
             |        $rightElementTerm =
             |          ${rowFieldReadAccess(indexTerm, rightTerm, elementType)};
             |      }
             |
             |      ${elementEqualsExpr.code}
             |      if (!${elementEqualsExpr.resultTerm}) {
             |        $resultTerm = false;
             |        break;
             |      }
             |    }
             |  } else {
             |    $resultTerm = false;
             |  }
             |}
             """.stripMargin
        (stmt, resultTerm)
    }
  }

  private def generateMapComparison(
      ctx: CodeGeneratorContext,
      left: GeneratedExpression,
      right: GeneratedExpression,
      keyType: DataType,
      valueType: DataType,
      resultType: DataType): GeneratedExpression =
    generateCallWithStmtIfArgsNotNull(ctx, resultType, Seq(left, right)) {
      args =>
        val leftTerm = args.head
        val rightTerm = args(1)

        val resultTerm = newName("compareResult")

        val mapCls = className[java.util.Map[_, _]]
        val keyCls = boxedTypeTermForType(keyType)
        val valueCls = boxedTypeTermForType(valueType)

        val leftMapTerm = newName("leftMap")
        val leftKeyTerm = newName("leftKey")
        val leftValueTerm = newName("leftValue")
        val leftValueNullTerm = newName("leftValueIsNull")
        val leftValueExpr =
          GeneratedExpression(leftValueTerm, leftValueNullTerm, "", valueType)

        val rightMapTerm = newName("rightMap")
        val rightValueTerm = newName("rightValue")
        val rightValueNullTerm = newName("rightValueIsNull")
        val rightValueExpr =
          GeneratedExpression(rightValueTerm, rightValueNullTerm, "", valueType)

        val entryTerm = newName("entry")
        val entryCls = classOf[java.util.Map.Entry[AnyRef, AnyRef]].getCanonicalName
        val valueEqualsExpr =
          generateEquals(ctx, leftValueExpr, rightValueExpr, new BooleanType(valueType.isNullable))

        val internalTypeCls = classOf[DataType].getCanonicalName
        val keyTypeTerm = ctx.addReusableObject(keyType, "keyType", internalTypeCls)
        val valueTypeTerm = ctx.addReusableObject(valueType, "valueType", internalTypeCls)
        val mapDataUtil = className[InternalMapSerializer]

        val stmt =
          s"""
             |boolean $resultTerm;
             |if ($leftTerm.size() == $rightTerm.size()) {
             |  $resultTerm = true;
             |  $mapCls $leftMapTerm = $mapDataUtil
             |      .convertToJavaMap($leftTerm, $keyTypeTerm, $valueTypeTerm);
             |  $mapCls $rightMapTerm = $mapDataUtil
             |      .convertToJavaMap($rightTerm, $keyTypeTerm, $valueTypeTerm);
             |
             |  for ($entryCls $entryTerm : $leftMapTerm.entrySet()) {
             |    $keyCls $leftKeyTerm = ($keyCls) $entryTerm.getKey();
             |    if ($rightMapTerm.containsKey($leftKeyTerm)) {
             |      $valueCls $leftValueTerm = ($valueCls) $entryTerm.getValue();
             |      $valueCls $rightValueTerm = ($valueCls) $rightMapTerm.get($leftKeyTerm);
             |      boolean $leftValueNullTerm = ($leftValueTerm == null);
             |      boolean $rightValueNullTerm = ($rightValueTerm == null);
             |
             |      ${valueEqualsExpr.code}
             |      if (!${valueEqualsExpr.resultTerm}) {
             |        $resultTerm = false;
             |        break;
             |      }
             |    } else {
             |      $resultTerm = false;
             |      break;
             |    }
             |  }
             |} else {
             |  $resultTerm = false;
             |}
             """.stripMargin
        (stmt, resultTerm)
    }

  // ----------------------------------------------------------------------------------------
  // private generate utils
  // ----------------------------------------------------------------------------------------

  private def generateOperatorIfNotNull(
      ctx: CodeGeneratorContext,
      returnType: DataType,
      left: GeneratedExpression,
      right: GeneratedExpression,
      resultNullable: Boolean = false)(expr: (String, String) => String): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, returnType, Seq(left, right), resultNullable) {
      args => expr(args.head, args(1))
    }
  }

  // ----------------------------------------------------------------------------------------------
}
