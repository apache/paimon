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

import org.apache.paimon.CoreOptions.{FORMAT_TABLE_IMPLEMENTATION, METASTORE_PARTITIONED_TABLE}
import org.apache.paimon.catalog.Identifier
import org.apache.paimon.fs.local.LocalFileIO
import org.apache.paimon.table.FormatTable
import org.apache.paimon.table.FormatTable.Format
import org.apache.paimon.types.{DataTypes, RowType}

import org.apache.spark.sql.internal.SQLConf
import org.scalatest.funsuite.AnyFunSuite

import java.util.Collections

import scala.collection.JavaConverters._

/** Tests for [[OptionUtils]]. */
class OptionUtilsTest extends AnyFunSuite {

  test("reject engine implementation from SQL conf for managed format table") {
    val exception = intercept[IllegalArgumentException] {
      SQLConf.withExistingConf(engineSQLConf) {
        OptionUtils.copyWithSQLConf(formatTable(managed = true))
      }
    }

    assert(exception.getMessage.contains(METASTORE_PARTITIONED_TABLE.key()))
    assert(exception.getMessage.contains(FORMAT_TABLE_IMPLEMENTATION.key()))
  }

  test("allow engine implementation from SQL conf for unmanaged format table") {
    val copied = SQLConf.withExistingConf(engineSQLConf) {
      OptionUtils.copyWithSQLConf(formatTable(managed = false))
    }

    assert(copied.options().get(FORMAT_TABLE_IMPLEMENTATION.key()) == "engine")
  }

  test("reject invalid managed format table option from SQL conf with option context") {
    val sqlConf = new SQLConf
    sqlConf.setConfString(s"spark.paimon.${METASTORE_PARTITIONED_TABLE.key()}", "yes")

    val exception = intercept[IllegalArgumentException] {
      SQLConf.withExistingConf(sqlConf) {
        OptionUtils.copyWithSQLConf(formatTable(managed = false))
      }
    }

    assert(exception.getMessage.contains("yes"))
    assert(exception.getMessage.contains(METASTORE_PARTITIONED_TABLE.key()))
  }

  test("session-level metastore.partitioned-table is ignored, not failed, on a managed table") {
    val sqlConf = new SQLConf
    sqlConf.setConfString(s"spark.paimon.${METASTORE_PARTITIONED_TABLE.key()}", "false")

    val copied = SQLConf.withExistingConf(sqlConf) {
      OptionUtils.copyWithSQLConf(formatTable(managed = true))
    }

    // The persisted managed flag wins; the session-global override is dropped with a warning
    // instead of failing every managed format table load in the session.
    assert(copied.options().get(METASTORE_PARTITIONED_TABLE.key()) == "true")
  }

  test("session-level metastore.partitioned-table is ignored, not failed, on an unmanaged table") {
    val sqlConf = new SQLConf
    sqlConf.setConfString(s"spark.paimon.${METASTORE_PARTITIONED_TABLE.key()}", "true")

    val copied = SQLConf.withExistingConf(sqlConf) {
      OptionUtils.copyWithSQLConf(formatTable(managed = false))
    }

    assert(copied.options().get(METASTORE_PARTITIONED_TABLE.key()) == "false")
  }

  private def engineSQLConf: SQLConf = {
    val sqlConf = new SQLConf
    sqlConf.setConfString(
      s"spark.paimon.${FORMAT_TABLE_IMPLEMENTATION.key()}",
      "engine"
    )
    sqlConf
  }

  private def formatTable(managed: Boolean): FormatTable = {
    FormatTable
      .builder()
      .fileIO(new LocalFileIO)
      .identifier(Identifier.create("test_db", "format_table"))
      .rowType(RowType.of(DataTypes.INT(), DataTypes.STRING()))
      .partitionKeys(Collections.singletonList("dt"))
      .location("file:///tmp/test_db.db/format_table")
      .format(Format.PARQUET)
      .options(Map(
        METASTORE_PARTITIONED_TABLE.key() -> managed.toString,
        FORMAT_TABLE_IMPLEMENTATION.key() -> "paimon").asJava)
      .build()
  }
}
