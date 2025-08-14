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

package org.apache.paimon.spark.catalog.functions

import org.apache.paimon.catalog.Identifier
import org.apache.paimon.function.{Function, FunctionDefinition, FunctionImpl}
import org.apache.paimon.spark.SparkCatalog.FUNCTION_DEFINITION_NAME

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogFunction, FunctionResource, FunctionResourceType}

import scala.collection.JavaConverters._

object V1FunctionConverter {

  /** Converts spark [[FunctionIdentifier]] to paimon [[Identifier]]. */
  def fromFunctionIdentifier(ident: FunctionIdentifier): Identifier = {
    new Identifier(ident.database.get, ident.funcName)
  }

  /** Converts paimon [[Identifier]] to spark [[FunctionIdentifier]]. */
  def toFunctionIdentifier(ident: Identifier): FunctionIdentifier = {
    new FunctionIdentifier(ident.getObjectName, Some(ident.getDatabaseName))
  }

  /** Converts spark [[CatalogFunction]] to paimon [[Function]]. */
  def fromV1Function(v1Function: CatalogFunction): Function = {
    val functionIdentifier = v1Function.identifier
    val identifier = fromFunctionIdentifier(functionIdentifier)
    val fileResources = v1Function.resources
      .map(r => new FunctionDefinition.FunctionFileResource(r.resourceType.resourceType, r.uri))
      .toList

    val functionDefinition: FunctionDefinition = FunctionDefinition.file(
      fileResources.asJava,
      "JAVA", // Apache Spark only supports JAR persistent function now.
      v1Function.className,
      functionIdentifier.funcName)
    val definitions = Map(FUNCTION_DEFINITION_NAME -> functionDefinition).asJava

    new FunctionImpl(identifier, definitions)
  }

  /** Converts paimon [[Function]] to spark [[CatalogFunction]]. */
  def toV1Function(paimonFunction: Function): CatalogFunction = {
    paimonFunction.definition(FUNCTION_DEFINITION_NAME) match {
      case functionDefinition: FunctionDefinition.FileFunctionDefinition =>
        val fileResources = functionDefinition
          .fileResources()
          .asScala
          .map(r => FunctionResource(FunctionResourceType.fromString(r.resourceType()), r.uri()))
          .toSeq

        CatalogFunction(
          new FunctionIdentifier(
            paimonFunction.name(),
            Some(paimonFunction.identifier().getDatabaseName)),
          functionDefinition.className(),
          fileResources)

      case other =>
        throw new UnsupportedOperationException(s"Unsupported function definition $other")
    }
  }
}
