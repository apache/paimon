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

import org.apache.paimon.annotation.VisibleForTesting
import org.apache.paimon.codegen.GenerateUtils.{newName, newNames}
import org.apache.paimon.data.serializer.InternalSerializers
import org.apache.paimon.types.DataType
import org.apache.paimon.utils.InstantiationUtil

import scala.collection.mutable

/**
 * The context for code generator, maintaining various reusable statements that could be insert into
 * different code sections in the final generated class.
 */
class CodeGeneratorContext {

  // holding a list of objects that could be used passed into generated class
  val references: mutable.ArrayBuffer[AnyRef] = new mutable.ArrayBuffer[AnyRef]()

  // set of member statements that will be added only once
  // we use a LinkedHashSet to keep the insertion order
  private val reusableMemberStatements: mutable.LinkedHashSet[String] =
    mutable.LinkedHashSet[String]()

  // set of constructor statements that will be added only once
  // we use a LinkedHashSet to keep the insertion order
  private val reusableInitStatements: mutable.LinkedHashSet[String] =
    mutable.LinkedHashSet[String]()

  // map of type serializer that will be added only once
  // LogicalType -> reused_term
  private val reusableTypeSerializers: mutable.Map[DataType, String] =
    mutable.Map[DataType, String]()

  /**
   * The current method name for [[reusableLocalVariableStatements]]. You can start a new local
   * variable statements for another method using [[startNewLocalVariableStatement()]]
   */
  private var currentMethodNameForLocalVariables = "DEFAULT"

  // method_name -> local_variable_statements
  private val reusableLocalVariableStatements = mutable.Map[String, mutable.LinkedHashSet[String]](
    (currentMethodNameForLocalVariables, mutable.LinkedHashSet[String]()))

  /**
   * Starts a new local variable statements for a generated class with the given method name.
   *
   * @param methodName
   *   the method name which the fields will be placed into if code is not split.
   */
  def startNewLocalVariableStatement(methodName: String): Unit = {
    currentMethodNameForLocalVariables = methodName
    reusableLocalVariableStatements(methodName) = mutable.LinkedHashSet[String]()
  }

  /**
   * Adds a reusable local variable statement with the given type term and field name. The local
   * variable statements will be placed in methods or class member area depends on whether the
   * method length excess max code length.
   *
   * @param fieldName
   *   the field name prefix
   * @param fieldTypeTerm
   *   the field type term
   * @return
   *   a new generated unique field name
   */
  def addReusableLocalVariable(fieldTypeTerm: String, fieldName: String): String = {
    val fieldTerm = newName(fieldName)
    reusableLocalVariableStatements
      .getOrElse(currentMethodNameForLocalVariables, mutable.LinkedHashSet[String]())
      .add(s"$fieldTypeTerm $fieldTerm;")
    fieldTerm
  }

  /**
   * Adds multiple pairs of local variables. The local variable statements will be placed in methods
   * or class member area depends on whether the method length excess max code length.
   *
   * @param fieldTypeAndNames
   *   pairs of local variables with left is field type term and right is field name
   * @return
   *   the new generated unique field names for each variable pairs
   */
  def addReusableLocalVariables(fieldTypeAndNames: (String, String)*): Seq[String] = {
    val fieldTerms = newNames(fieldTypeAndNames.map(_._2): _*)
    fieldTypeAndNames.map(_._1).zip(fieldTerms).foreach {
      case (fieldTypeTerm, fieldTerm) =>
        reusableLocalVariableStatements
          .getOrElse(currentMethodNameForLocalVariables, mutable.LinkedHashSet[String]())
          .add(s"$fieldTypeTerm $fieldTerm;")
    }
    fieldTerms
  }

  /**
   * Adds a reusable member field statement to the member area.
   *
   * @param memberStatement
   *   the member field declare statement
   */
  def addReusableMember(memberStatement: String): Unit = {
    reusableMemberStatements.add(memberStatement)
  }

  /**
   * Adds a reusable Object to the member area of the generated class
   * @param obj
   *   the object to be added to the generated class
   * @param fieldNamePrefix
   *   prefix field name of the generated member field term
   * @param fieldTypeTerm
   *   field type class name
   * @return
   *   the generated unique field term
   */
  def addReusableObject(
      obj: AnyRef,
      fieldNamePrefix: String,
      fieldTypeTerm: String = null): String = {
    addReusableObjectWithName(obj, newName(fieldNamePrefix), fieldTypeTerm)
  }

  def addReusableObjectWithName(
      obj: AnyRef,
      fieldTerm: String,
      fieldTypeTerm: String = null): String = {
    val clsName = Option(fieldTypeTerm).getOrElse(obj.getClass.getCanonicalName)
    addReusableObjectInternal(obj, fieldTerm, clsName)
    fieldTerm
  }

  @VisibleForTesting
  def addReusableObjectInternal(obj: AnyRef, fieldTerm: String, fieldTypeTerm: String): Unit = {
    val idx = references.length
    // make a deep copy of the object
    val byteArray = InstantiationUtil.serializeObject(obj)
    val objCopy: AnyRef =
      InstantiationUtil.deserializeObject(byteArray, obj.getClass.getClassLoader)
    references += objCopy

    reusableMemberStatements.add(s"private transient $fieldTypeTerm $fieldTerm;")
    reusableInitStatements.add(s"$fieldTerm = ((($fieldTypeTerm) references[$idx]));")
  }

  /**
   * @return
   *   code block of statements that need to be placed in the member area of the class (e.g. member
   *   variables and their initialization)
   */
  def reuseMemberCode(): String = {
    reusableMemberStatements.mkString("\n")
  }

  /** @return code block of statements that need to be placed in the constructor */
  def reuseInitCode(): String = {
    reusableInitStatements.mkString("\n")
  }

  /**
   * @return
   *   code block of statements that will be placed in the member area of the class if generated
   *   code is split or in local variables of method
   */
  def reuseLocalVariableCode(methodName: String = currentMethodNameForLocalVariables): String = {
    if (methodName == null) {
      reusableLocalVariableStatements(currentMethodNameForLocalVariables).mkString("\n")
    } else {
      reusableLocalVariableStatements(methodName).mkString("\n")
    }
  }

  /** Adds a reusable init statement which will be placed in constructor. */
  def addReusableInitStatement(s: String): Unit = reusableInitStatements.add(s)

  /**
   * Adds a reusable TypeSerializer to the member area of the generated class.
   *
   * @param t
   *   the internal type which used to generate internal type serializer
   * @return
   *   member variable term
   */
  def addReusableTypeSerializer(t: DataType): String = {
    // if type serializer has been used before, we can reuse the code that
    // has already been generated
    reusableTypeSerializers.get(t) match {
      case Some(term) => term

      case None =>
        val term = newName("typeSerializer")
        val ser = InternalSerializers.create(t)
        addReusableObjectInternal(ser, term, ser.getClass.getCanonicalName)
        reusableTypeSerializers(t) = term
        term
    }
  }
}
