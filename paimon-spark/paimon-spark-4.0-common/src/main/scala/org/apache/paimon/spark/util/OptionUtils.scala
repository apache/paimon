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

import org.apache.paimon.table.Table

import org.apache.spark.sql.catalyst.SQLConfHelper

import java.util.{HashMap => JHashMap, Map => JMap}

import scala.collection.JavaConverters._

object OptionUtils extends SQLConfHelper {

  private val PAIMON_OPTION_PREFIX = "spark.paimon."

  def mergeSQLConf(extraOptions: JMap[String, String]): JMap[String, String] = {
    val mergedOptions = new JHashMap[String, String](
      conf.getAllConfs
        .view.filterKeys(_.startsWith(PAIMON_OPTION_PREFIX))
        .map {
          case (key, value) =>
            key.stripPrefix(PAIMON_OPTION_PREFIX) -> value
        }.toMap
        .asJava)
    mergedOptions.putAll(extraOptions)
    mergedOptions
  }

  def copyWithSQLConf[T <: Table](table: T, extraOptions: JMap[String, String]): T = {
    val mergedOptions = mergeSQLConf(extraOptions)
    if (mergedOptions.isEmpty) {
      table
    } else {
      table.copy(mergedOptions).asInstanceOf[T]
    }
  }
}
