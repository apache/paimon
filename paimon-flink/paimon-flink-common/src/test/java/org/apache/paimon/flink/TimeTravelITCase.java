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

package org.apache.paimon.flink;

import org.apache.paimon.utils.BlockingIterator;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** IT case for flink time travel. */
public class TimeTravelITCase extends CatalogITCaseBase {

    // -------------------------------------------------------
    //                          Batch
    // -------------------------------------------------------

    @Test
    public void testTravelToTimestampString() throws Exception {
        sql("CREATE TABLE t (k INT, v STRING)");

        // snapshot 1
        sql("INSERT INTO t VALUES(1, 'hello'), (2, 'world')");
        Thread.sleep(3000);
        String anchor = now();
        // snapshot 2
        sql("INSERT INTO t VALUES(1, 'flink'), (2, 'paimon')");

        List<Row> result = sql("SELECT * FROM t");
        assertThat(result.toString())
                .isEqualTo("[+I[1, hello], +I[2, world], +I[1, flink], +I[2, paimon]]");

        // time travel to snapshot 1
        result = sql("SELECT * FROM t FOR SYSTEM_TIME AS OF TIMESTAMP '%s'", anchor);
        assertThat(result.toString()).isEqualTo("[+I[1, hello], +I[2, world]]");
    }

    @Test
    public void testExpression() throws Exception {
        sql("CREATE TABLE t (k INT, v STRING)");

        // snapshot 1
        sql("INSERT INTO t VALUES(1, 'hello'), (2, 'world')");

        String anchor = now();
        Thread.sleep(3000);

        // snapshot 2
        sql("INSERT INTO t VALUES(1, 'flink'), (2, 'paimon')");

        List<Row> result = sql("SELECT * FROM t");
        assertThat(result.toString())
                .isEqualTo("[+I[1, hello], +I[2, world], +I[1, flink], +I[2, paimon]]");

        // time travel to snapshot 1
        result =
                sql(
                        "SELECT * FROM t FOR SYSTEM_TIME AS OF TIMESTAMP '%s' + INTERVAL '1' SECOND",
                        anchor);
        assertThat(result.toString()).isEqualTo("[+I[1, hello], +I[2, world]]");
    }

    @Test
    public void testTravelToOldSchema() throws Exception {
        // old schema
        sql("CREATE TABLE t (k INT, v STRING)");

        // snapshot 1
        sql("INSERT INTO t VALUES(1, 'hello'), (2, 'world')");

        Thread.sleep(3000);
        String anchor = now();

        // new schema
        sql("ALTER TABLE t ADD dt STRING");

        // snapshot 2
        sql("INSERT INTO t VALUES(1, 'flink', '2020-01-01'), (2, 'paimon', '2020-01-02')");

        List<Row> result = sql("SELECT * FROM t");
        assertThat(result.toString())
                .isEqualTo(
                        "[+I[1, hello, null], +I[2, world, null], +I[1, flink, 2020-01-01], +I[2, paimon, 2020-01-02]]");

        // time travel to snapshot 1
        result = sql("SELECT * FROM t FOR SYSTEM_TIME AS OF TIMESTAMP '%s'", anchor);
        assertThat(result.toString()).isEqualTo("[+I[1, hello], +I[2, world]]");
    }

    @Test
    public void testTravelToNonExistedTimestamp() {
        sql("CREATE TABLE t (k INT, v STRING)");
        sql("INSERT INTO t VALUES(1, 'hello'), (2, 'world')");
        assertThat(sql("SELECT * FROM t FOR SYSTEM_TIME AS OF TIMESTAMP '1900-01-01 00:00:00'").toString())
                .isEqualTo("[+I[1, hello], +I[2, world]]");
    }

    @Test
    public void testSystemTableTimeTravel() throws Exception {
        sql("CREATE TABLE t (k INT, v STRING)");

        // snapshot 1
        sql("INSERT INTO t VALUES(1, 'hello'), (2, 'world')");

        Thread.sleep(3000);
        String anchor = now();

        // snapshot 2
        sql("INSERT INTO t VALUES(1, 'flink'), (2, 'paimon')");

        List<Row> result = sql("SELECT * FROM t$files");
        assertThat(result.size()).isEqualTo(2);

        // time travel to snapshot 1
        result = sql("SELECT * FROM t$files FOR SYSTEM_TIME AS OF TIMESTAMP '%s'", anchor);

        assertThat(result.size()).isEqualTo(1);
    }

    // -------------------------------------------------------
    //                        Streaming
    // -------------------------------------------------------

    @Test
    public void testStreamingTravel() throws Exception {
        sql("CREATE TABLE t (k INT PRIMARY KEY NOT ENFORCED, v STRING)");

        BlockingIterator<Row, Row> streamIter =
                streamSqlBlockIter("SELECT * FROM t FOR SYSTEM_TIME AS OF TIMESTAMP '%s'", now());

        // snapshot 1
        sql("INSERT INTO t VALUES(1, 'hello')");
        // snapshot 2
        sql("INSERT INTO t VALUES(1, 'apache')");

        List<Row> result = streamIter.collect(3);
        assertThat(result.toString()).isEqualTo("[+I[1, hello], -U[1, hello], +U[1, apache]]");

        streamIter.close();
    }

    private String now() {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }
}
