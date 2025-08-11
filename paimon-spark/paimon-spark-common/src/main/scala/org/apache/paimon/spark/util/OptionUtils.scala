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
import org.apache.paimon.options.ConfigOption
import org.apache.paimon.spark.SparkConnectorOptions
import org.apache.paimon.table.Table

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.internal.StaticSQLConf

import java.util.{HashMap => JHashMap, Map => JMap}
import java.util.regex.Pattern

import scala.collection.JavaConverters._

object OptionUtils extends SQLConfHelper {

  private val PAIMON_OPTION_PREFIX = "spark.paimon."
  private val SPARK_CATALOG_PREFIX = "spark.sql.catalog."

  def paimonExtensionEnabled: Boolean = {
    conf
      .getConf(StaticSQLConf.SPARK_SESSION_EXTENSIONS)
      .getOrElse(Seq.empty)
      .contains("org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
  }

  def getOptionString(option: ConfigOption[_]): String = {
    conf.getConfString(s"$PAIMON_OPTION_PREFIX${option.key()}", option.defaultValue().toString)
  }

  def checkRequiredConfigurations(): Unit = {
    if (getOptionString(SparkConnectorOptions.REQUIRED_SPARK_CONFS_CHECK_ENABLED).toBoolean) {
      if (!paimonExtensionEnabled) {
        throw new RuntimeException(
          """
            |When using Paimon, it is necessary to configure `spark.sql.extensions` and ensure that it includes `org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions`.
            |You can disable this check by configuring `spark.paimon.requiredSparkConfsCheck.enabled` to `false`, but it is strongly discouraged to do so.
            |""".stripMargin)
      }
    }
  }

  def useV2Write(): Boolean = {
    getOptionString(SparkConnectorOptions.USE_V2_WRITE).toBoolean
  }

  def writeMergeSchemaEnabled(): Boolean = {
    getOptionString(SparkConnectorOptions.MERGE_SCHEMA).toBoolean
  }

  def writeMergeSchemaExplicitCastEnabled(): Boolean = {
    getOptionString(SparkConnectorOptions.EXPLICIT_CAST).toBoolean
  }

  private def mergeSQLConf(extraOptions: JMap[String, String]): JMap[String, String] = {
    val mergedOptions = new JHashMap[String, String](
      conf.getAllConfs
        .filterKeys(_.startsWith(PAIMON_OPTION_PREFIX))
        .map {
          case (key, value) =>
            key.stripPrefix(PAIMON_OPTION_PREFIX) -> value
        }
        .toMap
        .asJava)
    mergedOptions.putAll(extraOptions)
    mergedOptions
  }

  private def mergeSQLConfWithIdentifier(
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
      catalogName: String = null,
      ident: Identifier = null,
      extraOptions: JMap[String, String] = new JHashMap[String, String]()): T = {
    val mergedOptions = if (catalogName != null && ident != null) {
      mergeSQLConfWithIdentifier(extraOptions, catalogName, ident)
    } else {
      mergeSQLConf(extraOptions)
    }
    if (mergedOptions.isEmpty) {
      table
    } else {
      table.copy(mergedOptions).asInstanceOf[T]
    }
  }
}
