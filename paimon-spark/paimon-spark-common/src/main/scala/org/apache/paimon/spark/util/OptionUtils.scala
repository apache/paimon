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

package org.apache.paimon.spark.util

import org.apache.paimon.catalog.Identifier
import org.apache.paimon.table.Table

import org.apache.spark.sql.catalyst.SQLConfHelper

import java.util.{Map => JMap}
import java.util.regex.Pattern

import scala.collection.JavaConverters._

object OptionUtils extends SQLConfHelper {

  private val PAIMON_OPTION_PREFIX = "spark.paimon."
  private val SPARK_CATALOG_PREFIX = "spark.sql.catalog."

  def extractCatalogName(): Option[String] = {
    val sparkCatalogTemplate = String.format("%s([^.]*)$", SPARK_CATALOG_PREFIX)
    val sparkCatalogPattern = Pattern.compile(sparkCatalogTemplate)
    conf.getAllConfs.filterKeys(_.startsWith(SPARK_CATALOG_PREFIX)).foreach {
      case (key, _) =>
        val matcher = sparkCatalogPattern.matcher(key)
        if (matcher.find())
          return Option(matcher.group(1))
    }
    Option.empty
  }

  def mergeSQLConfWithIdentifier(
      extraOptions: JMap[String, String],
      catalogName: String,
      ident: Identifier): JMap[String, String] = {
    val tableOptionsTemplate = String.format(
      "(%s)(%s|\\*)\\.(%s|\\*)\\.(%s|\\*)\\.(.+)",
      PAIMON_OPTION_PREFIX,
      catalogName,
      ident.getDatabaseName,
      ident.getObjectName)
    val tableOptionsPattern = Pattern.compile(tableOptionsTemplate)
    val mergedOptions = org.apache.paimon.options.OptionsUtils
      .convertToDynamicTableProperties(
        conf.getAllConfs.asJava,
        PAIMON_OPTION_PREFIX,
        tableOptionsPattern,
        5)
    mergedOptions.putAll(extraOptions)
    mergedOptions
  }

  def copyWithSQLConf[T <: Table](
      table: T,
      catalogName: String,
      ident: Identifier,
      extraOptions: JMap[String, String]): T = {
    val mergedOptions: JMap[String, String] =
      mergeSQLConfWithIdentifier(extraOptions, catalogName, ident)
    if (mergedOptions.isEmpty) {
      table
    } else {
      table.copy(mergedOptions).asInstanceOf[T]
    }
  }
}
