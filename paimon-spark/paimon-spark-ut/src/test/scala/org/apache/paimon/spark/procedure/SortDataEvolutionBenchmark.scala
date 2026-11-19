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

import org.apache.paimon.fs.{Path, SeekableInputStream, SeekableInputStreamWrapper, VectoredReadable}
import org.apache.paimon.fs.local.LocalFileIO
import org.apache.paimon.predicate.PredicateBuilder
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.table.{FileStoreTable, FileStoreTableFactory}

import org.scalatest.DoNotDiscover

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._

/** Run explicitly with ScalaTest's suites option to compare two layouts of the same snapshot. */
@DoNotDiscover
class SortDataEvolutionBenchmark extends PaimonSparkTestBase {

  test("selective multimodal read after ordinary compaction and range sorting") {
    withTable("T") {
      sql("""
            |CREATE TABLE T (id INT, k INT, payload BINARY, embedding ARRAY<FLOAT>)
            |TBLPROPERTIES ('data-evolution.enabled'='true', 'row-tracking.enabled'='true',
            | 'blob-field'='payload', 'vector-field'='embedding', 'field.embedding.vector-dim'='2',
            | 'vector.file.format'='parquet', 'write-only'='true', 'write-buffer-size'='1 mb',
            | 'target-file-row-num'='2048', 'target-file-size'='1536 kb',
            | 'source.split.open-file-cost'='1 mb', 'compaction.min.file-num'='2',
            | 'blob-compaction.enabled'='true', 'vector-compaction.enabled'='true')
            |""".stripMargin)
      sql("""INSERT INTO T SELECT CAST(id AS INT), CAST((id*25173)%32768 AS INT),
            | CAST(sha2(CAST(id AS STRING), 256) AS BINARY), array(CAST(id AS FLOAT), 1.0f)
            | FROM range(0, 32768, 1, 1)""".stripMargin)
      val input = loadTable("T").latestSnapshot().get().id()
      sql(s"CALL sys.create_tag('test.T', 'input', $input)")
      sql("CALL sys.create_branch('test.T', 'baseline', 'input')")
      val baseline = "`T$branch_baseline`"
      def rewriteSeconds(command: String): Double = {
        val start = System.nanoTime()
        sql(command).collect()
        (System.nanoTime() - start) / 1e9
      }
      val ordinarySeconds = rewriteSeconds(s"CALL sys.compact('test.$baseline')")
      assert(loadTable("T").switchToBranch("baseline").latestSnapshot().get().id() > input)
      sql("ALTER TABLE T SET TBLPROPERTIES ('target-file-row-num'='4096')")
      val sortedSeconds = rewriteSeconds("CALL sys.sort_data_evolution('test.T', 0L, 32767L, 'k')")
      info(
        f"Maintenance elapsed time: ordinary=$ordinarySeconds%.2f s, sorted=$sortedSeconds%.2f s")

      val io = new CountingDataFileIO
      val sorted = FileStoreTableFactory.create(io, loadTable("T").location())
      val ordinary = sorted.switchToBranch("baseline")
      def sizes(table: FileStoreTable): Seq[Long] =
        table
          .newSnapshotReader()
          .read()
          .dataSplits()
          .asScala
          .flatMap(_.dataFiles().asScala)
          .filter(f => !f.fileName().endsWith(".blob") && !f.fileName().contains(".vector."))
          .map(_.rowCount())
          .sorted
          .toVector
      assert(sizes(sorted) == sizes(ordinary) && sizes(sorted).size == 8)
      def measureIO(table: FileStoreTable): (Int, Long) = {
        val pb = new PredicateBuilder(table.rowType())
        val pred =
          PredicateBuilder.and(pb.greaterOrEqual(1, Int.box(8192)), pb.lessThan(1, Int.box(8256)))
        val builder = table.newReadBuilder().withFilter(pred)
        io.files.clear()
        io.bytes.set(0L)
        val reader = builder.newRead().executeFilter().createReader(builder.newScan().plan())
        val rows = reader.toCloseableIterator()
        var count = 0
        try {
          while (rows.hasNext) {
            val row = rows.next()
            assert(row.getBlob(2).toData.length == 64)
            assert(row.getVector(3).toFloatArray()(0) == row.getInt(0).toFloat)
            count += 1
          }
        } finally rows.close()
        assert(count == 64)
        (io.files.size(), io.bytes.get())
      }
      val beforeIO = measureIO(ordinary)
      val afterIO = measureIO(sorted)
      assert(afterIO._1 < beforeIO._1 && afterIO._2 < beforeIO._2)
      def measureSql(name: String): Double = {
        val start = System.nanoTime()
        val result = sql(s"""SELECT id, k, payload, embedding[0] FROM $name
                            |WHERE k >= 8192 AND k < 8256""".stripMargin).collect()
        assert(result.length == 64)
        (System.nanoTime() - start) / 1e6
      }
      for (_ <- 1 to 3; name <- Seq(baseline, "T")) measureSql(name)
      val samples = (0 until 20)
        .flatMap {
          i =>
            val names = if (i % 2 == 0) Seq(baseline, "T") else Seq("T", baseline)
            names.map(name => name -> measureSql(name))
        }
        .groupBy(_._1)
      for ((name, (files, bytes)) <- Seq(baseline -> beforeIO, "T" -> afterIO)) {
        val times = samples(name).map(_._2).sorted
        info(
          f"$name dataFiles=$files dataBytes=$bytes p50Ms=${times(9)}%.1f p95Ms=${times(18)}%.1f")
      }
    }
  }

  private class CountingDataFileIO extends LocalFileIO {
    val bytes = new AtomicLong
    val files = ConcurrentHashMap.newKeySet[String]()
    override def newInputStream(path: Path): SeekableInputStream = {
      val stream = super.newInputStream(path)
      if (!path.getName.endsWith(".parquet") && !path.getName.endsWith(".blob")) return stream
      files.add(path.toString)
      new SeekableInputStreamWrapper(stream) with VectoredReadable {
        private def counted(n: Int): Int = {
          if (n > 0) bytes.addAndGet(n)
          n
        }
        override def read(): Int = {
          val b = super.read()
          if (b >= 0) bytes.incrementAndGet()
          b
        }
        override def read(b: Array[Byte], off: Int, len: Int): Int = counted(
          super.read(b, off, len))
        override def pread(pos: Long, b: Array[Byte], off: Int, len: Int): Int =
          counted(stream.asInstanceOf[VectoredReadable].pread(pos, b, off, len))
      }
    }
  }
}
