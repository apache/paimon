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

import org.apache.paimon.spark.PaimonHiveTestBase

import org.apache.hadoop.conf.Configuration

class DDLWithDisallowIncompatibleColType extends DDLWithIncompatibleColType {
  val disallowIncompatible: Boolean = true
}

class DDLWithAllowIncompatibleColType extends DDLWithIncompatibleColType {
  val disallowIncompatible: Boolean = false
}

abstract class DDLWithIncompatibleColType extends PaimonHiveTestBase {

  def disallowIncompatible: Boolean

  override def configuration: Configuration = {
    val conf_ = super.configuration
    conf_.set(
      "hive.metastore.disallow.incompatible.col.type.changes",
      disallowIncompatible.toString)
    conf_
  }

  test("Paimon DDL with hive catalog: alter with incompatible col type") {
    withTable("t") {
      spark.sql("CREATE TABLE t (a INT, b INT, c STRUCT<f1: INT>) USING paimon")
      if (disallowIncompatible) {
        val e = intercept[Exception] {
          spark.sql("ALTER TABLE t DROP COLUMN b")
        }
        assert(
          e.getMessage.contains(
            "The following columns have types incompatible with the existing columns"))
      } else {
        spark.sql("ALTER TABLE t DROP COLUMN b")
      }
    }
  }
}
