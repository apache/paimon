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

import org.apache.paimon.catalog.Identifier
import org.apache.paimon.schema.FileSystemSchemaManager
import org.apache.paimon.spark.PaimonHiveTestBase
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.Row

import java.util.{HashMap => JHashMap}

class HiveCatalogMetadataOperationsTest extends PaimonHiveTestBase {

  Seq(sparkCatalogName, paimonHiveCatalogName).foreach {
    catalogName =>
      test(s"metadata operations with invalid options through $catalogName") {
        sql(s"USE $catalogName")
        sql(s"USE $hiveDbName")
        withTable("legacy_metadata") {
          sql("CREATE TABLE legacy_metadata (id INT) USING paimon")
          sql("INSERT INTO legacy_metadata VALUES (1)")
          val catalog = paimonCatalog
          val identifier = Identifier.create(hiveDbName, "legacy_metadata")
          val table = catalog.getTable(identifier).asInstanceOf[FileStoreTable]
          val schemas = new FileSystemSchemaManager(table.fileIO(), table.location())

          def invalidateOptions(): Unit = {
            val schema = schemas.latest().get()
            val options = new JHashMap[String, String](schema.options())
            options.put("record-level.time-field", "missing")
            table
              .fileIO()
              .overwriteFileUtf8(schemas.toSchemaPath(schema.id()), schema.copy(options).toString)
            catalog.invalidateTable(identifier)
          }

          invalidateOptions()
          assert(sql("SHOW CREATE TABLE legacy_metadata").head().getString(0).contains("missing"))
          assertInvalid(sql("SELECT * FROM legacy_metadata").collect())
          assertInvalid(sql("INSERT INTO legacy_metadata VALUES (2)").collect())
          sql("ALTER TABLE legacy_metadata UNSET TBLPROPERTIES ('record-level.time-field')")
          checkAnswer(sql("SELECT * FROM legacy_metadata"), Seq(Row(1)))
          sql("INSERT INTO legacy_metadata VALUES (2)")
          checkAnswer(sql("SELECT * FROM legacy_metadata"), Seq(Row(1), Row(2)))

          invalidateOptions()
          sql("DROP TABLE legacy_metadata")
          assert(!catalog.listTables(hiveDbName).contains("legacy_metadata"))
        }
      }
  }

  private def assertInvalid(action: => Unit): Unit = {
    val error = intercept[Exception](action)
    assert(
      Iterator
        .iterate(error: Throwable)(_.getCause)
        .takeWhile(_ != null)
        .exists(e => Option(e.getMessage).exists(_.contains("missing"))),
      error.toString)
  }
}
