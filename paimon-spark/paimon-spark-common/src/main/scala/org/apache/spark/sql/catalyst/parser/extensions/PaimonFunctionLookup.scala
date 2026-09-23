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

package org.apache.spark.sql.catalyst.parser.extensions

import org.apache.paimon.catalog.Catalog.SYSTEM_DATABASE_NAME
import org.apache.paimon.function.{Function => PaimonFunction}
import org.apache.paimon.function.FunctionDefinition
import org.apache.paimon.spark.SparkCatalog.FUNCTION_DEFINITION_NAME
import org.apache.paimon.spark.catalog.SupportV1Function
import org.apache.paimon.spark.catalog.functions.PaimonFunctions

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedFunctionName, UnresolvedIdentifier}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.{CatalogManager, CatalogPlugin, LookupCatalog}

/** Resolves Paimon function identifiers in parser-stage plans. */
case class PaimonFunctionLookup(catalogManager: CatalogManager) extends LookupCatalog {

  object CatalogAndFunctionIdentifier {

    def unapply(unresolved: LogicalPlan): Option[(CatalogPlugin, FunctionIdentifier, Boolean)] =
      unresolved match {
        case ui: UnresolvedIdentifier =>
          unapply(ui.nameParts)
        case name: UnresolvedFunctionName =>
          unapply(name.multipartIdentifier)
        case _ =>
          None
      }

    def unapply(nameParts: Seq[String]): Option[(CatalogPlugin, FunctionIdentifier, Boolean)] = {
      nameParts match {
        // Spark's built-in or tmp functions is without database name or catalog name.
        case Seq(funName) if isSparkBuiltInFunction(FunctionIdentifier(funName)) =>
          None
        case Seq(funName) if isSparkTmpFunc(FunctionIdentifier(funName)) =>
          Some(null, FunctionIdentifier(funName), true)
        case CatalogAndIdentifier(v1FunctionCatalog: SupportV1Function, ident)
            if v1FunctionCatalog.v1FunctionEnabled() =>
          Some(
            v1FunctionCatalog,
            FunctionIdentifier(
              ident.name(),
              Some(ident.namespace().last),
              Some(v1FunctionCatalog.name)),
            false)
        case _ =>
          None
      }
    }
  }

  def isPaimonBuiltInFunction(funcIdent: FunctionIdentifier): Boolean = {
    funcIdent.database match {
      case Some(db)
          if db == SYSTEM_DATABASE_NAME && PaimonFunctions.names.contains(funcIdent.funcName) =>
        true
      case _ => false
    }
  }

  def isSparkBuiltInFunction(funcIdent: FunctionIdentifier): Boolean = {
    catalogManager.v1SessionCatalog.isBuiltinFunction(funcIdent)
  }

  def isSparkTmpFunc(funcIdent: FunctionIdentifier): Boolean = {
    catalogManager.v1SessionCatalog.isTemporaryFunction(funcIdent)
  }
}

object PaimonFunctionLookup {

  def isPaimonFileFunction(fun: PaimonFunction): Boolean =
    fun.definition(FUNCTION_DEFINITION_NAME).isInstanceOf[FunctionDefinition.FileFunctionDefinition]

  def isPaimonSQLFunction(fun: PaimonFunction): Boolean =
    fun.definition(FUNCTION_DEFINITION_NAME).isInstanceOf[FunctionDefinition.SQLFunctionDefinition]

  def isPaimonV1Function(fun: PaimonFunction): Boolean =
    isPaimonFileFunction(fun) || isPaimonSQLFunction(fun)
}
