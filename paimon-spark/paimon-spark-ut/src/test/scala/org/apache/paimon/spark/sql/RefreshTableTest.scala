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
import org.apache.paimon.spark.PaimonSparkTestBase

class RefreshTableTest extends PaimonSparkTestBase {

  test("Refresh Table: clear cache") {
    withTable("t") {
      sql(s"CREATE TABLE t (f0 INT, f1 INT)")
      sql(s"INSERT INTO t VALUES (1, 2)")
      val tableCache = paimonCatalog.asInstanceOf[CachingCatalog].tableCache().asMap()
      assert(tableCache.containsKey(Identifier.create(dbName0, "t")))
      sql("REFRESH TABLE t")
      assert(!tableCache.containsKey(Identifier.create(dbName0, "t")))
    }
  }
}
