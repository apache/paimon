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

package org.apache.paimon.spark.query

import org.apache.paimon.data.serializer.InternalRowSerializer
import org.apache.paimon.io.DataInputDeserializer
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.table.SpecialFields
import org.apache.paimon.types.RowType
import org.apache.paimon.utils.JsonSerdeUtil

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row

/** Java acceptance: real Spark writes, builds, executor queries and SDK parity. */
class PaimonEsSearchJavaTest extends PaimonSparkTestBase {
  override protected def sparkConf: SparkConf = super.sparkConf
    .setMaster("local[2]")
    .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
    .set("spark.sql.shuffle.partitions", "2")
    .set("spark.ui.enabled", "false")

  private val vector = (0 until 33).map(i => (i * 0.01).toFloat).mkString("[", ",", "]")

  private def createFixture(algorithm: String = "hnsw", deletionVectors: Boolean = false): Unit = {
    sql(s"""CREATE TABLE T (id INT, embedding ARRAY<FLOAT>, title STRING, tag STRING)
      USING paimon TBLPROPERTIES ('bucket'='-1', 'file.format'='parquet',
      'row-tracking.enabled'='true', 'data-evolution.enabled'='true',
      'deletion-vectors.enabled'='$deletionVectors',
      'compaction.min.file-num'='100', 'snapshot.num-retained.min'='20',
      'global-index.row-count-per-shard'='8',
      'global-index.es-index.fields.embedding.algorithm'='$algorithm',
      'global-index.es-index.fields.embedding.dimension'='33',
      'global-index.es-index.fields.embedding.metric'='l2',
      'global-index.es-index.fields.title.type'='fulltext',
      'global-index.es-index.fields.tag.type'='keyword')""")
    val coordinates = (0 until 33).map(i => s"cast(id * 0.25 + ${i * 0.01} as float)").mkString(",")
    sql(s"""INSERT INTO T SELECT cast(id as int), array($coordinates),
      IF(id % 4 = 0, 'paimon lake', 'river meadow'), IF(id % 2 = 0, 'even', 'odd')
      FROM range(32)""")
    buildIndex()
    val shards = sql("SELECT * FROM `T$table_indexes` WHERE index_type = 'es-index'").collect()
    assert(shards.length >= 4, "The test must exercise multiple index shards")
  }

  private def buildIndex(columns: String = "embedding,title,tag,id"): Unit = {
    val result = sql(
      "CALL paimon.sys.create_global_index(table => 'test.T', " +
        s"index_column => '$columns', index_type => 'es-index')").collect()
    assert(result.head.getBoolean(0))
  }

  private def envelope(
      query: String,
      size: Int = 32,
      from: Int = 0,
      source: Boolean = true,
      snapshot: Option[Long] = None): String = {
    val selector = snapshot
      .map(id => s""", "snapshot":{"kind":"snapshot","snapshot_id":"$id","tag":null}""")
      .getOrElse("")
    s"""{"version":1,"operation":"SEARCH","targets":[{
      "name":"docs","database":"test","table":"T","indexes":[[1,2,3,0]],
      "bindings":{"v":1,"text":2,"label":3,"number":0}$selector}],
      "request":{"query":$query,"size":$size,"from":$from,
      "fetch":"${if (source) "SOURCE" else "NONE"}"}}"""
  }

  private def search(json: String): Array[Row] = {
    val result = sql(s"SELECT * FROM paimon_es_search('paimon', '$json')")
    assert(result.queryExecution.executedPlan.exists(_.isInstanceOf[SparkPaimonSearchTableExec]))
    val records = result.collect()
    assert(records.count(_.getAs[String]("record_kind") == "PAGE_END") == 1)
    val hits = records.filter(_.getAs[String]("record_kind") == "HIT")
    val sdk = org.apache.paimon.globalindex.EsIndexSearch.search(paimonCatalog, json)
    import scala.collection.JavaConverters._
    assert(
      sdk.hits().asScala.map(_.rowId).toSeq == hits
        .sortBy(_.getAs[Long]("rank"))
        .map(_.getAs[Long]("row_id"))
        .toSeq)
    sdk.hits().asScala.zip(hits.sortBy(_.getAs[Long]("rank"))).foreach {
      case (expected, actual) =>
        assert(math.abs(expected.score - actual.getAs[Float]("score")) < 0.00001f)
    }
    val snapshot = records.head.getAs[Long]("snapshot_id")
    val oracle = sql(s"SELECT _ROW_ID, * FROM T VERSION AS OF $snapshot")
      .collect()
      .map(row => row.getLong(0) -> row)
      .toMap
    hits.foreach {
      row =>
        assert(oracle.contains(row.getAs[Long]("row_id")))
        assert(java.lang.Float.isFinite(row.getAs[Float]("score")))
        if (!row.isNullAt(row.fieldIndex("source_binary"))) {
          val schema =
            JsonSerdeUtil.fromJson(row.getAs[String]("source_schema_json"), classOf[RowType])
          val decoded = new InternalRowSerializer(schema).deserialize(
            new DataInputDeserializer(row.getAs[Array[Byte]]("source_binary")))
          val expected = oracle(row.getAs[Long]("row_id"))
          assert(decoded.getInt(0) == expected.getInt(1))
          assert((0 until 33).map(decoded.getArray(1).getFloat) == expected.getSeq[Float](2))
          assert(decoded.getString(2).toString == expected.getString(3))
          assert(decoded.getString(3).toString == expected.getString(4))
          assert(
            decoded.getLong(schema.getFieldIndex(SpecialFields.ROW_ID.name())) ==
              row.getAs[Long]("row_id"))
          assert(row.getAs[String]("source_encoding") == "paimon-internal-row-v1")
        }
    }
    hits.sortBy(_.getAs[Long]("rank"))
  }

  private def ids(hits: Array[Row]): Seq[Int] = hits.map {
    row =>
      val schema = JsonSerdeUtil.fromJson(row.getAs[String]("source_schema_json"), classOf[RowType])
      new InternalRowSerializer(schema)
        .deserialize(new DataInputDeserializer(row.getAs[Array[Byte]]("source_binary")))
        .getInt(0)
  }.toSeq

  test("Spark text, bool filters and empty results") {
    createFixture()
    assert(ids(search(envelope("""{"match":{"text":"paimon"}}"""))).sorted == (0 until 32 by 4))
    assert(
      ids(search(envelope("""{"match_phrase":{"text":"paimon lake"}}"""))).sorted ==
        (0 until 32 by 4))
    val bool = """{"bool":{"filter":[{"term":{"label":"odd"}},
      {"range":{"number":{"gte":10,"lt":20}}}],"must_not":{"term":{"number":15}}}}"""
    assert(ids(search(envelope(bool))).sorted == Seq(11, 13, 17, 19))
    assert(ids(search(envelope("""{"terms":{"number":[2,7]}}"""))).sorted == Seq(2, 7))
    assert(search(envelope("""{"exists":{"field":"text"}}""")).length == 32)
    assert(search(envelope("""{"match_none":{}}""")).isEmpty)
  }

  for (algorithm <- Seq("hnsw", "int8_hnsw", "diskbbq")) {
    test(s"Spark $algorithm KNN and filter-KNN merge shard scores") {
      createFixture(algorithm)
      val knn = s"""{"knn":{"field":"v","query_vector":$vector,"k":3,"num_candidates":32}}"""
      assert(ids(search(envelope(knn, size = 3))) == Seq(0, 1, 2))
      val filtered = s"""{"knn":{"field":"v","query_vector":$vector,"k":3,
        "num_candidates":32,"filter":{"term":{"label":"odd"}}}}"""
      assert(ids(search(envelope(filtered, size = 3))) == Seq(1, 3, 5))
    }
  }

  test("global offset and deterministic tie order without source fetch") {
    createFixture()
    val hits = search(envelope("""{"match_all":{}}""", size = 5, from = 6, source = false))
    assert(hits.map(_.getAs[Long]("row_id")).toSeq == (6L until 11L))
    assert(hits.map(_.getAs[Long]("rank")).toSeq == (7L until 12L))
    assert(hits.forall(row => row.isNullAt(row.fieldIndex("source_binary"))))
  }

  test("reject incomplete latest snapshot, retain explicit snapshot, then refresh") {
    createFixture()
    val snapshot = loadTable("T").snapshotManager().latestSnapshotId().longValue()
    sql("INSERT INTO T SELECT 32, embedding, 'paimon lake', 'even' FROM T WHERE id = 0")
    val error = intercept[Exception](search(envelope("""{"match_all":{}}""")))
    assert(
      Iterator
        .iterate[Throwable](error)(_.getCause)
        .takeWhile(_ != null)
        .exists(value => Option(value.getMessage).exists(_.contains("does not exactly cover"))))
    assert(search(envelope("""{"match_all":{}}""", snapshot = Some(snapshot))).length == 32)
    buildIndex()
    assert(search(envelope("""{"match_all":{}}""", size = 33)).length == 33)
  }

  test("schema rename retains field-ID bindings and historical index names") {
    createFixture()
    sql("ALTER TABLE T RENAME COLUMN title TO renamed_title")
    sql("INSERT INTO T SELECT 32, embedding, 'paimon lake', 'even' FROM T WHERE id = 0")
    buildIndex("embedding,renamed_title,tag,id")
    assert(ids(search(envelope("""{"match":{"text":"paimon"}}"""))).sorted == (0 to 32 by 4))
  }

  test("reject stale indexed columns until Spark rebuilds them") {
    createFixture()
    sql("ALTER TABLE T SET TBLPROPERTIES ('global-index.column-update-action'='ignore')")
    sql("UPDATE T SET title = 'changed text' WHERE id = 0")
    val error = intercept[Exception](search(envelope("""{"match":{"text":"paimon"}}""")))
    assert(
      Iterator
        .iterate[Throwable](error)(_.getCause)
        .takeWhile(_ != null)
        .exists(value => Option(value.getMessage).exists(_.contains("changed since construction"))))
    buildIndex()
    assert(ids(search(envelope("""{"match":{"text":"changed"}}"""))) == Seq(0))
  }

  test("reject deletion vectors before returning index hits") {
    createFixture(deletionVectors = true)
    sql("DELETE FROM T WHERE id = 0")
    val error = intercept[Exception](search(envelope("""{"match_all":{}}""")))
    assert(
      Iterator
        .iterate[Throwable](error)(_.getCause)
        .takeWhile(_ != null)
        .exists(value => Option(value.getMessage).exists(_.contains("deletion vectors"))))
  }
}
