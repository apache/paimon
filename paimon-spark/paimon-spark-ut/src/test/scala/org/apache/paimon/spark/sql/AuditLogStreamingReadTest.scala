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

import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.sql.Row
import org.assertj.core.api.Assertions.assertThat

class AuditLogStreamingReadTest extends PaimonSparkTestBase {

  test(s"test delete with primary key, partial-update, remove-record-on-delete, lookup") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING, age INT)
                 |TBLPROPERTIES (
                 |  'primary-key' = 'id', 'bucket' = '4',
                 |  'merge-engine' = 'partial-update',
                 |  'partial-update.remove-record-on-delete' = 'true',
                 |  'changelog-producer' = 'lookup')
                 |""".stripMargin)

    withTempDir {
      checkpointDir =>
        {
          val readStream = spark.readStream
            .format("paimon")
            .table("`T$audit_log`")
            .writeStream
            .format("memory")
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .queryName("mem_table")
            .outputMode("append")
            .start()
          val currentResult = () => spark.sql("SELECT * FROM mem_table")

          try {
            // insert
            spark.sql("INSERT INTO T VALUES (1, 'a', NULL)")
            spark.sql("INSERT INTO T VALUES (2, 'b', NULL)")
            // update
            spark.sql("INSERT INTO T VALUES (1, NULL, 16)")

            assertThat(spark.sql("SELECT * FROM T").collectAsList().toString)
              .isEqualTo("[[2,b,null], [1,a,16]]")

            // delete
            spark.sql("DELETE FROM T WHERE id = 1")

            assertThat(spark.sql("SELECT * FROM T").collectAsList().toString)
              .isEqualTo("[[2,b,null]]")

            readStream.processAllAvailable()
            checkAnswer(
              currentResult(),
              Row("+I", 0L, 1, "a", null) ::
                Row("+I", 0L, 2, "b", null) ::
                Row("-U", 0L, 1, "a", null) ::
                Row("+U", 1L, 1, "a", 16) ::
                Row("-D", 1L, 1, "a", 16) :: Nil
            )
          } finally {
            readStream.stop()
          }
        }
    }
  }
}
