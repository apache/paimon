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
import org.apache.paimon.spark.{PaimonSparkTestBase, SparkCatalog}
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.Row

import java.util.{HashMap => JHashMap}

class CatalogMetadataOperationsTest extends PaimonSparkTestBase {

  for (cacheEnabled <- Seq(false, true); v2Write <- Seq(false, true)) {
    test(s"metadata operations with invalid options, cache=$cacheEnabled, v2Write=$v2Write") {
      val catalogName = s"metadata_${cacheEnabled}_$v2Write"
      withSparkSQLConf(
        s"spark.sql.catalog.$catalogName" -> classOf[SparkCatalog].getName,
        s"spark.sql.catalog.$catalogName.warehouse" -> s"${tempDBDir.getCanonicalPath}/$catalogName",
        s"spark.sql.catalog.$catalogName.cache-enabled" -> cacheEnabled.toString,
        "spark.paimon.write.use-v2-write" -> v2Write.toString
      ) {
        sql(s"USE $catalogName")
        sql("CREATE DATABASE IF NOT EXISTS default")
        sql("USE default")
        try {
          sql("""CREATE TABLE legacy (id INT, seq INT, v INT) USING paimon
                |TBLPROPERTIES ('primary-key'='id', 'bucket'='1',
                |'merge-engine'='partial-update', 'changelog-producer'='lookup',
                |'fields.seq.sequence-group'='v')""".stripMargin)
          sql("INSERT INTO legacy VALUES (1, 10, 100)")
          val catalog = paimonCatalog
          val identifier = Identifier.create("default", "legacy")
          val table = catalog.getTable(identifier).asInstanceOf[FileStoreTable]
          val schemas = new FileSystemSchemaManager(table.fileIO(), table.location())

          def overwriteOption(key: String, value: String): Unit = {
            val schema = schemas.latest().get()
            val options = new JHashMap[String, String](schema.options())
            if (value == null) {
              options.remove(key)
            } else {
              options.put(key, value)
            }
            table
              .fileIO()
              .overwriteFileUtf8(schemas.toSchemaPath(schema.id()), schema.copy(options).toString)
            catalog.invalidateTable(identifier)
          }

          overwriteOption("fields.seq.sequence-group", "id,v")
          val ddl = sql("SHOW CREATE TABLE legacy").head().getString(0)
          assert(ddl.contains("fields.seq.sequence-group"))
          assert(ddl.contains("id,v"))
          assert(catalog.getTable(identifier).options().containsKey("fields.seq.sequence-group"))
          assertInvalid("primary key")(sql("SELECT * FROM legacy").collect())
          assertInvalid("primary key")(sql("INSERT INTO legacy VALUES (2, 20, 200)").collect())
          assertInvalid("primary key")(sql("ALTER TABLE legacy SET TBLPROPERTIES ('bucket'='1')"))
          assert(schemas.latest().get().id() == 0L)

          sql("ALTER TABLE legacy SET TBLPROPERTIES ('fields.seq.sequence-group'='v')")
          checkAnswer(sql("SELECT * FROM legacy"), Seq(Row(1, 10, 100)))
          sql("INSERT INTO legacy VALUES (2, 20, 200)")
          checkAnswer(sql("SELECT * FROM legacy"), Seq(Row(1, 10, 100), Row(2, 20, 200)))

          overwriteOption("fields.seq.sequence-group", null)
          overwriteOption("fields.v.aggregate-function", "max")
          assert(
            sql("SHOW CREATE TABLE legacy")
              .head()
              .getString(0)
              .contains("fields.v.aggregate-function"))
          assertInvalid("Must use sequence group")(sql("SELECT * FROM legacy").collect())
          assertInvalid("Must use sequence group")(
            sql("INSERT INTO legacy VALUES (3, 30, 300)").collect())
          sql("ALTER TABLE legacy UNSET TBLPROPERTIES ('fields.v.aggregate-function')")
          sql("ALTER TABLE legacy SET TBLPROPERTIES ('fields.seq.sequence-group'='v')")
          checkAnswer(sql("SELECT * FROM legacy"), Seq(Row(1, 10, 100), Row(2, 20, 200)))

          overwriteOption("full-compaction.delta-commits", "1")
          intercept[UnsupportedOperationException](
            catalog.getTable(identifier).asInstanceOf[FileStoreTable].newScan())
          sql("ALTER TABLE legacy UNSET TBLPROPERTIES ('full-compaction.delta-commits')")
          assert(
            !catalog.getTable(identifier).options().containsKey("full-compaction.delta-commits"))

          overwriteOption("fields.seq.sequence-group", "id,v")
          sql("DROP TABLE legacy")
          assert(!catalog.listTables("default").contains("legacy"))
          assert(!table.fileIO().exists(table.location()))

          sql("CREATE TABLE legacy (id INT) USING paimon")
          overwriteOption("record-level.time-field", "missing")
          assert(sql("SHOW CREATE TABLE legacy").head().getString(0).contains("missing"))
          assertInvalid("missing")(sql("SELECT * FROM legacy").collect())
          assertInvalid("missing")(sql("INSERT INTO legacy VALUES (1)").collect())
          sql("ALTER TABLE legacy UNSET TBLPROPERTIES ('record-level.time-field')")
          sql("INSERT INTO legacy VALUES (1)")
          checkAnswer(sql("SELECT * FROM legacy"), Seq(Row(1)))
          sql("DELETE FROM legacy WHERE id = 1")
          assert(sql("SELECT * FROM legacy").collect().isEmpty)
        } finally {
          sql("DROP TABLE IF EXISTS legacy")
          sql("USE paimon")
          sql(s"USE $dbName0")
        }
      }
    }
  }

  private def assertInvalid(message: String)(action: => Unit): Unit = {
    val error = intercept[Exception](action)
    assert(
      Iterator
        .iterate(error: Throwable)(_.getCause)
        .takeWhile(_ != null)
        .exists(e => Option(e.getMessage).exists(_.contains(message))),
      error.toString)
  }
}
