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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.functions.col

object CopyIntoUtils {

  def quoteIdentifier(catalogName: String, ident: Identifier): String = {
    val parts = Seq(catalogName) ++
      ident.namespace().toSeq ++
      Seq(ident.name())
    parts.filter(_.nonEmpty).map(p => s"`${p.replace("`", "``")}`").mkString(".")
  }

  def extractBaseName(fullPath: String): String = {
    fullPath.substring(fullPath.lastIndexOf('/') + 1)
  }

  /** Count rows per file, keyed by base file name. */
  def countPerFile(df: DataFrame, fileCol: String): Map[String, Long] = {
    df.groupBy(col(fileCol))
      .count()
      .collect()
      .map(row => extractBaseName(row.getString(0)) -> row.getLong(1))
      .toMap
  }
}
