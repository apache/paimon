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

package org.apache.paimon.spark.catalog

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import java.util.regex.Pattern

object Catalogs {

  /**
   * Copy from [[org.apache.spark.sql.connector.catalog.Catalogs]].
   *
   * Extracts a named catalog's configuration from a SQLConf.
   *
   * @param name
   *   a catalog name
   * @param conf
   *   a SQLConf
   * @return
   *   a case insensitive string map of options starting with spark.sql.catalog.(name).
   */
  def catalogOptions(name: String, conf: SQLConf): CaseInsensitiveStringMap = {
    val prefix = Pattern.compile("^spark\\.sql\\.catalog\\." + name + "\\.(.+)")
    val options = new util.HashMap[String, String]
    conf.getAllConfs.foreach {
      case (key, value) =>
        val matcher = prefix.matcher(key)
        if (matcher.matches && matcher.groupCount > 0) options.put(matcher.group(1), value)
    }
    new CaseInsensitiveStringMap(options)
  }
}
