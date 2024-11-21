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

package org.apache.paimon.spark.execution

import org.apache.paimon.spark.catalog.SupportView
import org.apache.paimon.spark.leafnode.PaimonLeafV2CommandExec

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow}
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

case class CreatePaimonViewExec(
    catalog: SupportView,
    ident: Identifier,
    queryText: String,
    viewSchema: StructType,
    columnAliases: Seq[String],
    columnComments: Seq[Option[String]],
    queryColumnNames: Seq[String],
    comment: Option[String],
    properties: Map[String, String],
    allowExisting: Boolean,
    replace: Boolean
) extends PaimonLeafV2CommandExec {

  override def output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    if (columnAliases.nonEmpty || columnComments.nonEmpty || queryColumnNames.nonEmpty) {
      throw new UnsupportedOperationException(
        "columnAliases, columnComments and queryColumnNames are not supported now")
    }

    // Note: for replace just drop then create ,this operation is non-atomic.
    if (replace) {
      catalog.dropView(ident, true)
    }

    catalog.createView(
      ident,
      viewSchema,
      queryText,
      comment.orNull,
      properties.asJava,
      allowExisting)

    Nil
  }

  override def simpleString(maxFields: Int): String = {
    s"CreatePaimonViewExec: $ident"
  }
}

case class DropPaimonViewExec(catalog: SupportView, ident: Identifier, ifExists: Boolean)
  extends PaimonLeafV2CommandExec {

  override lazy val output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    catalog.dropView(ident, ifExists)
    Nil
  }

  override def simpleString(maxFields: Int): String = {
    s"DropPaimonViewExec: $ident"
  }
}

case class ShowPaimonViewsExec(
    output: Seq[Attribute],
    catalog: SupportView,
    namespace: Seq[String],
    pattern: Option[String])
  extends PaimonLeafV2CommandExec {

  override protected def run(): Seq[InternalRow] = {
    val rows = new ArrayBuffer[InternalRow]()
    catalog.listViews(namespace.toArray).asScala.map {
      viewName =>
        if (pattern.forall(StringUtils.filterPattern(Seq(viewName), _).nonEmpty)) {
          rows += new GenericInternalRow(
            Array(
              UTF8String.fromString(namespace.mkString(".")),
              UTF8String.fromString(viewName),
              false))
        }
    }
    rows.toSeq
  }

  override def simpleString(maxFields: Int): String = {
    s"ShowPaimonViewsExec: $namespace"
  }
}
