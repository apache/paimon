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

package org.apache.paimon.spark.procedure

import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.spark.analysis.NoSuchProcedureException

import org.apache.spark.sql.catalyst.parser.extensions.PaimonParseException
import org.assertj.core.api.Assertions.assertThatThrownBy

abstract class ProcedureTestBase extends PaimonSparkTestBase {

  test(s"test call unknown procedure") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING, dt STRING)
                 |""".stripMargin)

    assertThatThrownBy(() => spark.sql("CALL sys.unknown_procedure(table => 'test.T')"))
      .isInstanceOf(classOf[ParseException])
  }

  test(s"test parse exception") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING, dt STRING)
                 |""".stripMargin)

    // Using Chinese comma to simulate parser exception
    assertThatThrownBy(
      () => spark.sql("CALL sys.expire_snapshots(table => 'test.T'，retain_max => 1)"))
      .isInstanceOf(classOf[PaimonParseException])
  }
}
