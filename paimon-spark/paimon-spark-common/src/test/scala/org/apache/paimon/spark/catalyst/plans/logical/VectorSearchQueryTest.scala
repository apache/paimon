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

import org.apache.paimon.table.InnerTable
import org.apache.paimon.types.{ArrayType, DataType, DataTypes, RowType}

import org.apache.spark.sql.catalyst.expressions.{CreateArray, CreateMap, CreateNamedStruct, Expression, Literal}
import org.scalatest.funsuite.AnyFunSuite

import java.lang.reflect.{InvocationHandler, Method, Proxy}

/** Tests for [[VectorSearchQuery]]. */
class VectorSearchQueryTest extends AnyFunSuite {

  test("create hybrid search with vector route configs") {
    val search = HybridSearchQuery(Seq.empty).createHybridSearch(
      innerTable,
      Seq(
        CreateArray(
          Seq(
            CreateNamedStruct(Seq(
              Literal("vector_column"),
              Literal("title_vec"),
              Literal("query_vector"),
              CreateArray(Seq(Literal(1.0f), Literal(0.0f))),
              Literal("limit"),
              Literal(20),
              Literal("weight"),
              Literal(2.0f),
              Literal("options"),
              CreateMap(Seq(Literal("ivf.nprobe"), Literal("32")))
            )),
            CreateNamedStruct(Seq(
              Literal("vector_column"),
              Literal("body_vec"),
              Literal("query_vector"),
              CreateArray(Seq(Literal(0.0f), Literal(1.0f))),
              Literal("limit"),
              Literal(10),
              Literal("weight"),
              Literal(1.0f),
              Literal("options"),
              CreateMap(Seq(Literal("ivf.nprobe"), Literal("16")))
            ))
          )),
        CreateArray(Seq.empty),
        Literal(3),
        Literal("weighted_score")
      )
    )

    assert(search.ranker() == "weighted_score")
    assert(search.routes().size() == 2)
    assert(search.routes().get(0).limit() == 20)
    assert(search.routes().get(0).weight() == 2.0f)
    assert(search.routes().get(0).options().get("ivf.nprobe") == "32")
    assert(search.routes().get(1).limit() == 10)
    assert(search.routes().get(1).weight() == 1.0f)
    assert(search.routes().get(1).options().get("ivf.nprobe") == "16")
  }

  test("default hybrid vector route limit to final limit") {
    val search = HybridSearchQuery(Seq.empty).createHybridSearch(
      innerTable,
      Seq(
        CreateArray(
          Seq(
            CreateNamedStruct(
              Seq(
                Literal("vector_column"),
                Literal("title_vec"),
                Literal("query_vector"),
                CreateArray(Seq(Literal(1.0f), Literal(0.0f)))
              )))),
        CreateArray(Seq.empty),
        Literal(7)
      )
    )

    assert(search.limit() == 7)
    assert(search.ranker() == "rrf")
    assert(search.routes().get(0).limit() == 7)
    assert(search.routes().get(0).weight() == 1.0f)
    assert(search.routes().get(0).options().isEmpty)
  }

  test("create hybrid search with full-text route configs") {
    val search = HybridSearchQuery(Seq.empty).createHybridSearch(
      innerTable,
      Seq(
        CreateArray(Seq.empty),
        CreateArray(
          Seq(
            CreateNamedStruct(Seq(
              Literal("query"),
              Literal("""{"match":{"column":"content","terms":"paimon lake","operator":"And"}}"""),
              Literal("limit"),
              Literal(20),
              Literal("weight"),
              Literal(1.5f),
              Literal("options"),
              CreateMap(Seq.empty)
            ))
          )),
        Literal(5),
        Literal("rrf")
      )
    )

    assert(search.routes().size() == 1)
    assert(search.routes().get(0).isFullText)
    assert(search.routes().get(0).fieldName() == "content")
    assert(search.routes().get(0).fullTextQuery().queryText() == "paimon lake")
    assert(search.routes().get(0).fullTextQuery().toJson.contains("\"operator\":\"And\""))
    assert(search.routes().get(0).limit() == 20)
    assert(search.routes().get(0).weight() == 1.5f)
  }

  test("create full-text search") {
    val search = FullTextSearchQuery(Seq.empty).createFullTextSearch(
      innerTable,
      Seq(Literal("""{"match":{"column":"content","terms":"paimon lake"}}"""), Literal(10)))

    assert(search.fieldName() == "content")
    assert(search.query().queryText() == "paimon lake")
    assert(search.limit() == 10)
  }

  test("create full-text search with explicit query operator") {
    val search = FullTextSearchQuery(Seq.empty).createFullTextSearch(
      innerTable,
      Seq(
        Literal("""{"match":{"column":"content","terms":"paimon lake","operator":"And"}}"""),
        Literal(10)))

    assert(search.fieldName() == "content")
    assert(search.query().queryText() == "paimon lake")
    assert(search.limit() == 10)
    assert(search.query().toJson.contains("\"operator\":\"And\""))
  }

  test("create multi-column full-text search") {
    val search = FullTextSearchQuery(Seq.empty).createFullTextSearch(
      innerTable,
      Seq(
        Literal(
          """{"multi_match":{"query":"paimon","columns":["title","content"],"boost":[2.0,1.0]}}"""),
        Literal(10)))

    assert(search.columns().size() == 2)
    assert(search.columns().contains("title"))
    assert(search.columns().contains("content"))
    assert(search.query().toJson.contains("\"multi_match\""))
  }

  test("reject full-text search column that does not exist") {
    val exception = intercept[RuntimeException] {
      FullTextSearchQuery(Seq.empty).createFullTextSearch(
        innerTable,
        Seq(Literal("""{"match":{"column":"missing","terms":"paimon lake"}}"""), Literal(10)))
    }

    assert(exception.getMessage.contains("Column missing does not exist"))
  }

  test("reject hybrid search query map") {
    val exception = intercept[RuntimeException] {
      HybridSearchQuery(Seq.empty).createHybridSearch(
        innerTable,
        Seq(
          CreateMap(
            Seq(
              Literal("title_vec"),
              CreateArray(Seq(Literal(1.0f), Literal(0.0f))),
              Literal("body_vec"),
              CreateArray(Seq(Literal(0.0f), Literal(1.0f))))),
          CreateArray(Seq.empty),
          Literal(3)
        )
      )
    }

    assert(exception.getMessage.contains("Cannot extract vector routes"))
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
                new ArrayType(DataTypes.FLOAT()),
                DataTypes.STRING(),
                DataTypes.STRING()),
              Array[String]("v", "title_vec", "body_vec", "title", "content")
            )

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
