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

import org.apache.paimon.predicate.MultiVectorSearch
import org.apache.paimon.table.InnerTable
import org.apache.paimon.types.{ArrayType, DataType, DataTypes, RowType}

import org.apache.spark.sql.catalyst.expressions.{CreateArray, CreateMap, Expression, Literal}
import org.scalatest.funsuite.AnyFunSuite

import java.lang.reflect.{InvocationHandler, Method, Proxy}

/** Tests for [[VectorSearchQuery]]. */
class VectorSearchQueryTest extends AnyFunSuite {

  test("create multi vector search with query map and fusion options") {
    val search = MultiVectorSearchQuery(Seq.empty).createMultiVectorSearch(
      innerTable,
      Seq(
        CreateMap(
          Seq(
            Literal("title_vec"),
            CreateArray(Seq(Literal(1.0f), Literal(0.0f))),
            Literal("body_vec"),
            CreateArray(Seq(Literal(0.0f), Literal(1.0f))))),
        Literal(3),
        CreateMap(
          Seq(
            Literal("fusion"),
            Literal("rrf"),
            Literal("route_limit"),
            Literal("10"),
            Literal("weights"),
            Literal("title_vec=2.0,body_vec=1.0")))))

    assert(search.isInstanceOf[MultiVectorSearch])
    assert(search.limit() == 3)
    assert(search.fusion() == "rrf")
    assert(search.routes().size() == 2)
    assert(search.routes().get(0).fieldName() == "title_vec")
    assert(search.routes().get(0).limit() == 10)
    assert(search.routes().get(0).weight() == 2.0f)
    assert(search.routes().get(1).fieldName() == "body_vec")
    assert(search.routes().get(1).weight() == 1.0f)
  }

  test("create vector search with string options") {
    val vectorSearch = createVectorSearch(
      Literal("v"),
      CreateArray(Seq(Literal(1.0f), Literal(2.0f))),
      Literal(5),
      Literal("ivf.nprobe=16;hnsw.ef_search=64"))

    assert(vectorSearch.options().get("ivf.nprobe") == "16")
    assert(vectorSearch.options().get("hnsw.ef_search") == "64")
  }

  test("create vector search with map options") {
    val vectorSearch = createVectorSearch(
      Literal("v"),
      CreateArray(Seq(Literal(1.0f), Literal(2.0f))),
      Literal(5),
      CreateMap(Seq(Literal("ivf.nprobe"), Literal("16"), Literal("hnsw.ef_search"), Literal("64")))
    )

    assert(vectorSearch.options().get("ivf.nprobe") == "16")
    assert(vectorSearch.options().get("hnsw.ef_search") == "64")
  }

  private def createVectorSearch(args: Expression*) =
    VectorSearchQuery(Seq.empty).createVectorSearch(innerTable, args)

  private val innerTable =
    Proxy
      .newProxyInstance(
        classOf[InnerTable].getClassLoader,
        Array(classOf[InnerTable]),
        new InvocationHandler {
          private val rowType =
            RowType.of(
              Array[DataType](
                new ArrayType(DataTypes.FLOAT()),
                new ArrayType(DataTypes.FLOAT()),
                new ArrayType(DataTypes.FLOAT())),
              Array[String]("v", "title_vec", "body_vec"))

          override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef = {
            method.getName match {
              case "name" => "T"
              case "rowType" => rowType
              case other => throw new UnsupportedOperationException(other)
            }
          }
        }
      )
      .asInstanceOf[InnerTable]
}
