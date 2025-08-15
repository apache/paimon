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

import org.apache.paimon.spark.PaimonSparkTestWithRestCatalogBase

import org.apache.spark.sql.Row
import org.assertj.core.api.Assertions.assertThat

/** Test for Function Procedure. */
class FunctionProcedureTest extends PaimonSparkTestWithRestCatalogBase {

  test(s"test function procedure") {
    val functionName = "function_test"
    checkAnswer(
      spark.sql(s"CALL sys.create_function('$functionName', " +
        "'[{\"id\": 0, \"name\":\"length\", \"type\":\"INT\"}, {\"id\": 1, \"name\":\"width\", \"type\":\"INT\"}]'," +
        " '[{\"id\": 0, \"name\":\"area\", \"type\":\"BIGINT\"}]', true, 'comment', 'k1=v1,k2=v2')"),
      Row(true)
    )
    assertThat(
      spark
        .sql("SHOW FUNCTIONS")
        .collect()
        .map(r => r.getString(0))
        .count(v => v == s"paimon.test.$functionName")).isEqualTo(1)
    checkAnswer(
      spark.sql(s"CALL sys.alter_function('$functionName', " +
        "'{\"action\" : \"addDefinition\", \"name\" : \"spark\", \"definition\" : {\"type\" : \"lambda\", \"definition\" : \"(Integer length, Integer width) -> { return (long) length * width; }\", \"language\": \"JAVA\" } }')"),
      Row(true)
    );
    checkAnswer(
      spark.sql(s"select paimon.test.$functionName(1, 2)"),
      Row(2)
    )
    checkAnswer(
      spark.sql(s"CALL sys.drop_function('$functionName')"),
      Row(true)
    )
    assertThat(
      spark
        .sql("SHOW FUNCTIONS")
        .collect()
        .map(r => r.getString(0))
        .count(v => v == s"paimon.test.$functionName")).isEqualTo(0);
  }
}
