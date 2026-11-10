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

package org.apache.paimon.spark.catalyst.plans.logical

import org.apache.paimon.spark.query.SparkEsSearchRequest

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Literal}
import org.apache.spark.sql.types.StringType
import org.scalatest.funsuite.AnyFunSuite

/** Resolution rejects malformed requests before accessing a catalog or loading native code. */
class PaimonEsSearchQueryTest extends AnyFunSuite {
  private val json = """{"version":1,"operation":"SEARCH","targets":[{
    "name":"docs","database":"db","table":"t","indexes":[[7,9]],
    "bindings":{"text":9}}],"request":{"query":{"match":{"text":"hello"}},"size":3}}"""

  test("resolve constant arguments without catalog access") {
    val relation =
      PaimonEsSearchQuery(Seq(Literal("missing_catalog"), Literal(json))).resolveSearch()
    assert(relation.catalogName == "missing_catalog")
    assert(
      relation.output.map(_.name).take(4) ==
        Seq("record_kind", "query_id", "query_ordinal", "execution_query_id"))
    assert(relation.output.size == 17)
    val request = SparkEsSearchRequest.parse(json)
    assert(request.indexFields == Seq(7, 9))
    assert(request.bindings == Map("text" -> 9))
    assert(request.query.limit() == 3)
    assert(!request.fetchSource)
  }

  test("require two non-null constant strings") {
    Seq(
      Seq(Literal("paimon")),
      Seq(Literal("paimon"), Literal(5)),
      Seq(Literal("paimon"), Literal.create(null, StringType)),
      Seq(AttributeReference("catalog", StringType)(), Literal(json))
    ).foreach(
      args => intercept[IllegalArgumentException](PaimonEsSearchQuery(args).resolveSearch()))
  }

  test("validate supported operation, members and selected index bindings") {
    Seq(
      json.replace("SEARCH", "AGGREGATE"),
      json.replace("\"version\":1", "\"version\":2"),
      json.replace("\"text\":9", "\"text\":8"),
      json.replace("[[7,9]]", "[[7,9],[7]]"),
      json.replace("\"size\":3", "\"size\":3,\"sort\":[]"),
      json.replace("\"size\":3", "\"size\":3,\"from\":9999")
    ).foreach(value => intercept[IllegalArgumentException](SparkEsSearchRequest.parse(value)))
  }

  test("preserve 64-bit snapshot identity and include offset in shard limit") {
    val request = SparkEsSearchRequest.parse(
      json
        .replace(
          "\"name\":\"docs\"",
          """"name":"docs","snapshot":{
        "kind":"snapshot","snapshot_id":"9007199254740993","tag":null}""")
        .replace("\"size\":3", "\"size\":3,\"from\":2,\"fetch\":\"SOURCE\""))
    assert(request.snapshotId.contains(9007199254740993L))
    assert(request.query.limit() == 5)
    assert(request.fetchSource)
  }

  test("reject unsupported scoring and branches") {
    val options = json.replace("\"request\":", "\"options\":{\"scoring\":\"DFS\"},\"request\":")
    intercept[IllegalArgumentException](SparkEsSearchRequest.parse(options))
    val branch = json.replace("\"name\":\"docs\"", "\"name\":\"docs\",\"branch\":\"dev\"")
    intercept[IllegalArgumentException](SparkEsSearchRequest.parse(branch))
  }
}
