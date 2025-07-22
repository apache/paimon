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

package org.apache.paimon.spark.fileindex

import org.apache.paimon.operation.AppendOnlyFileStoreScan
import org.apache.paimon.predicate.PredicateBuilder
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.table.FileStoreTable

import scala.jdk.CollectionConverters._

class FileIndexTest extends PaimonSparkTestBase {

  test("test file index in plan phase") {
    Seq("bloom-filter", "bitmap", "bsi").foreach {
      filter =>
        Seq("1B", "1MB").foreach {
          size =>
            val fileIndexProp = if (filter == "bloom-filter") {
              s" 'file-index.bloom-filter.columns' = 'id'"
            } else if (filter.equals("bitmap")) {
              s" 'file-index.bitmap.columns' = 'id'"
            } else if (filter.equals("bsi")) {
              s" 'file-index.bsi.columns' = 'id'"
            }

            withTable("T") {
              spark.sql(s"""
                           |create table T (
                           |id int,
                           |name string)
                           |USING paimon
                           |TBLPROPERTIES(
                           |  $fileIndexProp,
                           |  'file-index.in-manifest-threshold'= '$size')
                           |""".stripMargin)

              spark.sql("insert into T values(1,'a')")
              spark.sql("insert into T values(2,'b')")
              spark.sql("insert into T values(3,'c')")
              assert(sql("select * from `T$files`").collect().length == 3)

              val fileNames = spark.sql("select input_file_name() from T where id == 2 ").collect()
              assert(fileNames.length == 1)
              val location = fileNames(0).getString(0)
              val fileName = location.substring(location.lastIndexOf('/') + 1)

              val table: FileStoreTable = loadTable("T")
              val predicateBuilder = new PredicateBuilder(table.rowType)
              // predicate is 'id=2'
              val predicate = predicateBuilder.equal(0, 2)
              val files = table
                .store()
                .newScan()
                .asInstanceOf[AppendOnlyFileStoreScan]
                .withFilter(predicate)
                .plan()
                .files()
                .asScala
                .map(x => x.fileName())
              assert(files.length == 1)
              assert(fileName.equals(files.head))
            }
        }
    }
  }
}
