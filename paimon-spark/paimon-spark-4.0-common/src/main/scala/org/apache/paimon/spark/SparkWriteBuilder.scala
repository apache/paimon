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

package org.apache.paimon.spark

import org.apache.paimon.options.Options
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.connector.write.{SupportsOverwrite, WriteBuilder}
import org.apache.spark.sql.sources.{And, Filter}

private class SparkWriteBuilder(table: FileStoreTable, options: Options)
  extends WriteBuilder
  with SupportsOverwrite {

  private var saveMode: SaveMode = InsertInto

  override def build = new SparkWrite(table, saveMode, options)

  override def overwrite(filters: Array[Filter]): WriteBuilder = {
    val conjunctiveFilters = if (filters.nonEmpty) {
      Some(filters.reduce((l, r) => And(l, r)))
    } else {
      None
    }
    this.saveMode = Overwrite(conjunctiveFilters)
    this
  }

}
