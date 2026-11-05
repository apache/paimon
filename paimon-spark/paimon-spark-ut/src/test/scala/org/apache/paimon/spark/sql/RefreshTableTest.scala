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

package org.apache.paimon.spark.sql

import org.apache.paimon.catalog.{CachingCatalog, Identifier}
import org.apache.paimon.schema.SchemaChange
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.types.DataTypes

import org.apache.spark.sql.Row

class RefreshTableTest extends PaimonSparkTestBase {

  test("Refresh Table: clear cache and reload schema") {
    withTable("t") {
      sql("CREATE TABLE t (f0 INT, f1 INT)")
      sql("INSERT INTO t VALUES (1, 2)")
      val catalog = paimonCatalog.asInstanceOf[CachingCatalog]
      val identifier = Identifier.create(dbName0, "t")
      val cachedTable = catalog.getTable(identifier)
      val tableCache = catalog.tableCache().asMap()
      assert(tableCache.containsKey(identifier))

      // Simulate an external schema change without invalidating this catalog's cache.
      catalog.wrapped().alterTable(identifier, SchemaChange.addColumn("f2", DataTypes.INT()), false)
      assert(sql("SELECT * FROM t").schema.fieldNames.toSeq == Seq("f0", "f1"))
      assert(catalog.getTable(identifier).eq(cachedTable))

      sql("REFRESH TABLE t")
      assert(!tableCache.containsKey(identifier))
      assert(!catalog.getTable(identifier).eq(cachedTable))
      assert(sql("SELECT * FROM t").schema.fieldNames.toSeq == Seq("f0", "f1", "f2"))
      checkAnswer(sql("SELECT * FROM t"), Seq(Row(1, 2, null)))
      assert(catalog.getTable(identifier).rowType().getFieldNames.contains("f2"))
    }
  }
}
