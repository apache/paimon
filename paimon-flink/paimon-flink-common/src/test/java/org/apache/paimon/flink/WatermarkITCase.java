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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/** SQL ITCase for watermark definition. */
public class WatermarkITCase extends CatalogITCaseBase {

    @Override
    protected int defaultParallelism() {
        return 1;
    }

    @Test
    public void testWatermark() throws Exception {
        innerTestWatermark();
    }

    @Test
    public void testAuditLogWatermark() throws Exception {
        String[] options =
                new String[] {
                    "'scan.watermark.idle-timeout'='1s'",
                    "'scan.watermark.alignment.group'='group'",
                    "'scan.watermark.alignment.update-interval'='2s'",
                    "'scan.watermark.alignment.max-drift'='1s'"
                };
        sql(
                "CREATE TABLE T (f0 INT, ts TIMESTAMP(3), WATERMARK FOR ts AS ts) WITH ("
                        + String.join(",", options)
                        + ")");

        BlockingIterator<Row, Row> select =
                BlockingIterator.of(
                        streamSqlIter(
                                "SELECT window_start, window_end, SUM(f0) FROM TABLE("
                                        + "TUMBLE(TABLE T$audit_log, DESCRIPTOR(ts), INTERVAL '10' MINUTES))\n"
                                        + "  GROUP BY window_start, window_end;"));

        sql("INSERT INTO T VALUES (1, TIMESTAMP '2023-02-02 12:00:00')");
        sql("INSERT INTO T VALUES (1, TIMESTAMP '2023-02-02 12:10:05')");

        assertThat(select.collect(1))
                .containsExactlyInAnyOrder(
                        Row.of(
                                LocalDateTime.parse("2023-02-02T12:00"),
                                LocalDateTime.parse("2023-02-02T12:10"),
                                1));
        select.close();
    }

    @Disabled // TODO unstable alignment may block watermark generation
    @Test
    public void testWatermarkAlignment() throws Exception {
        innerTestWatermark(
                "'scan.watermark.idle-timeout'='1s'",
                "'scan.watermark.alignment.group'='group'",
                "'scan.watermark.alignment.update-interval'='2s'",
                "'scan.watermark.alignment.max-drift'='1s'");
    }

    private void innerTestWatermark(String... options) throws Exception {
        sql(
                "CREATE TABLE T (f0 INT, ts TIMESTAMP(3), WATERMARK FOR ts AS ts) WITH ("
                        + String.join(",", options)
                        + ")");

        BlockingIterator<Row, Row> select =
                BlockingIterator.of(
                        streamSqlIter(
                                "SELECT window_start, window_end, SUM(f0) FROM TABLE("
                                        + "TUMBLE(TABLE T, DESCRIPTOR(ts), INTERVAL '10' MINUTES))\n"
                                        + "  GROUP BY window_start, window_end;"));

        sql("INSERT INTO T VALUES (1, TIMESTAMP '2023-02-02 12:00:00')");
        sql("INSERT INTO T VALUES (1, TIMESTAMP '2023-02-02 12:10:01')");

        assertThat(select.collect(1))
                .containsExactlyInAnyOrder(
                        Row.of(
                                LocalDateTime.parse("2023-02-02T12:00"),
                                LocalDateTime.parse("2023-02-02T12:10"),
                                1));
        select.close();
    }
}
