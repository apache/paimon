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
import org.apache.paimon.codegen.ScalarOperatorGens.{generateEquals, generateRowEqualiser}
import org.apache.paimon.types.{BooleanType, DataType}
import org.apache.paimon.types.DataTypeChecks.isCompositeType
import org.apache.paimon.types.DataTypeRoot._
import org.apache.paimon.utils.TypeUtils.isPrimitive

class EqualiserCodeGenerator(fieldTypes: Array[DataType], fields: Array[Int]) {

  private val RECORD_EQUALISER = className[RecordEqualiser]
  private val LEFT_INPUT = "left"
  private val RIGHT_INPUT = "right"

  def this(fieldTypes: Array[DataType]) = {
    this(fieldTypes, fieldTypes.indices.toArray)
  }

  def generateRecordEqualiser(name: String): GeneratedClass[RecordEqualiser] = {
    // ignore time zone
    val ctx = new CodeGeneratorContext
    val className = newName(name)

    val containsIgnoreFields = fieldTypes.length > fields.length
    val equalsMethodCodes = for (idx <- fields) yield generateEqualsMethod(ctx, idx)
    val equalsMethodCalls = for (idx <- fields) yield {
      val methodName = getEqualsMethodName(idx)
      s"""result = result && $methodName($LEFT_INPUT, $RIGHT_INPUT);"""
    }

    val classCode =
      s"""
        public final class $className implements $RECORD_EQUALISER {
          ${ctx.reuseMemberCode()}

          public $className(Object[] references) throws Exception {
            ${ctx.reuseInitCode()}
          }

          @Override
          public boolean equals($ROW_DATA $LEFT_INPUT, $ROW_DATA $RIGHT_INPUT) {
            if ($LEFT_INPUT instanceof $BINARY_ROW && $RIGHT_INPUT instanceof $BINARY_ROW && !$containsIgnoreFields) {
              return $LEFT_INPUT.equals($RIGHT_INPUT);
            }

            if ($LEFT_INPUT.getRowKind() != $RIGHT_INPUT.getRowKind()) {
              return false;
            }

            boolean result = true;
            ${equalsMethodCalls.mkString("\n")}
            return result;
          }

          ${equalsMethodCodes.mkString("\n")}
        }
      """.stripMargin

    new GeneratedClass(className, classCode, ctx.references.toArray)
  }

  private def getEqualsMethodName(idx: Int) = s"""equalsAtIndex$idx"""

  private def generateEqualsMethod(ctx: CodeGeneratorContext, idx: Int): String = {
    val methodName = getEqualsMethodName(idx)
    ctx.startNewLocalVariableStatement(methodName)

    val Seq(leftNullTerm, rightNullTerm) = ctx.addReusableLocalVariables(
      ("boolean", "isNullLeft"),
      ("boolean", "isNullRight")
    )

    val fieldType = fieldTypes(idx)
    val fieldTypeTerm = primitiveTypeTermForType(fieldType)
    val Seq(leftFieldTerm, rightFieldTerm) = ctx.addReusableLocalVariables(
      (fieldTypeTerm, "leftField"),
      (fieldTypeTerm, "rightField")
    )

    val leftReadCode = rowFieldReadAccess(idx, LEFT_INPUT, fieldType)
    val rightReadCode = rowFieldReadAccess(idx, RIGHT_INPUT, fieldType)

    val (equalsCode, equalsResult) =
      generateEqualsCode(ctx, fieldType, leftFieldTerm, rightFieldTerm, leftNullTerm, rightNullTerm)

    s"""
       |private boolean $methodName($ROW_DATA $LEFT_INPUT, $ROW_DATA $RIGHT_INPUT) {
       |  ${ctx.reuseLocalVariableCode(methodName)}
       |
       |  $leftNullTerm = $LEFT_INPUT.isNullAt($idx);
       |  $rightNullTerm = $RIGHT_INPUT.isNullAt($idx);
       |  if ($leftNullTerm && $rightNullTerm) {
       |    return true;
       |  }
       |
       |  if ($leftNullTerm || $rightNullTerm) {
       |    return false;
       |  }
       |
       |  $leftFieldTerm = $leftReadCode;
       |  $rightFieldTerm = $rightReadCode;
       |  $equalsCode
       |
       |  return $equalsResult;
       |}
      """.stripMargin
  }

  private def generateEqualsCode(
      ctx: CodeGeneratorContext,
      fieldType: DataType,
      leftFieldTerm: String,
      rightFieldTerm: String,
      leftNullTerm: String,
      rightNullTerm: String) = {
    if (isInternalPrimitive(fieldType)) {
      ("", s"$leftFieldTerm == $rightFieldTerm")
    } else if (isCompositeType(fieldType)) {
      val equaliserTerm = generateRowEqualiser(ctx, fieldType)
      ("", s"$equaliserTerm.equals($leftFieldTerm, $rightFieldTerm)")
    } else {
      val left = GeneratedExpression(leftFieldTerm, leftNullTerm, "", fieldType)
      val right = GeneratedExpression(rightFieldTerm, rightNullTerm, "", fieldType)
      val resultType = new BooleanType(fieldType.isNullable)
      val gen = generateEquals(ctx, left, right, resultType)
      (gen.code, gen.resultTerm)
    }
  }

  private def isInternalPrimitive(t: DataType): Boolean = t.getTypeRoot match {
    case _ if isPrimitive(t) => true

    case DATE | TIME_WITHOUT_TIME_ZONE => true

    case _ => false
  }
}
