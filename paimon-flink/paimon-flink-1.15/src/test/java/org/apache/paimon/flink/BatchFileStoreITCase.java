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

import org.apache.paimon.CoreOptions;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.apache.paimon.testutils.assertj.PaimonAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for batch file store. */
public class BatchFileStoreITCase extends CatalogITCaseBase {

    @Override
    protected List<String> ddl() {
        return Collections.singletonList("CREATE TABLE IF NOT EXISTS T (a INT, b INT, c INT)");
    }

    @Override
    protected int defaultParallelism() {
        return 1;
    }

    @Test
    public void testOverwriteEmpty() {
        batchSql("INSERT INTO T VALUES (1, 11, 111), (2, 22, 222)");
        assertThat(batchSql("SELECT * FROM T"))
                .containsExactlyInAnyOrder(Row.of(1, 11, 111), Row.of(2, 22, 222));
        batchSql("INSERT OVERWRITE T SELECT * FROM T WHERE 1 <> 1");
        assertThat(batchSql("SELECT * FROM T")).isEmpty();
    }

    @Test
    public void testTimeTravelRead() throws Exception {
        batchSql("INSERT INTO T VALUES (1, 11, 111), (2, 22, 222)");
        long time1 = System.currentTimeMillis();

        Thread.sleep(10);
        batchSql("INSERT INTO T VALUES (3, 33, 333), (4, 44, 444)");
        long time2 = System.currentTimeMillis();

        Thread.sleep(10);
        batchSql("INSERT INTO T VALUES (5, 55, 555), (6, 66, 666)");
        long time3 = System.currentTimeMillis();

        Thread.sleep(10);
        batchSql("INSERT INTO T VALUES (7, 77, 777), (8, 88, 888)");

        paimonTable("T").createTag("tag2", 2);

        assertThat(batchSql("SELECT * FROM T /*+ OPTIONS('scan.snapshot-id'='1') */"))
                .containsExactlyInAnyOrder(Row.of(1, 11, 111), Row.of(2, 22, 222));
        assertThat(
                        batchSql(
                                "SELECT * FROM T /*+ OPTIONS('scan.mode'='from-snapshot-full','scan.snapshot-id'='1') */"))
                .containsExactlyInAnyOrder(Row.of(1, 11, 111), Row.of(2, 22, 222));

        assertThatThrownBy(() -> batchSql("SELECT * FROM T /*+ OPTIONS('scan.snapshot-id'='0') */"))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "The specified scan snapshotId 0 is out of available snapshotId range [1, 4]."));

        assertThatThrownBy(
                        () ->
                                batchSql(
                                        "SELECT * FROM T /*+ OPTIONS('scan.mode'='from-snapshot-full','scan.snapshot-id'='0') */"))
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "The specified scan snapshotId 0 is out of available snapshotId range [1, 4]."));

        assertThat(
                        batchSql(
                                String.format(
                                        "SELECT * FROM T /*+ OPTIONS('scan.timestamp-millis'='%s') */",
                                        time1)))
                .containsExactlyInAnyOrder(Row.of(1, 11, 111), Row.of(2, 22, 222));

        assertThat(batchSql("SELECT * FROM T /*+ OPTIONS('scan.snapshot-id'='2') */"))
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111),
                        Row.of(2, 22, 222),
                        Row.of(3, 33, 333),
                        Row.of(4, 44, 444));
        assertThat(
                        batchSql(
                                "SELECT * FROM T /*+ OPTIONS('scan.mode'='from-snapshot-full','scan.snapshot-id'='2') */"))
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111),
                        Row.of(2, 22, 222),
                        Row.of(3, 33, 333),
                        Row.of(4, 44, 444));
        assertThat(
                        batchSql(
                                String.format(
                                        "SELECT * FROM T /*+ OPTIONS('scan.timestamp-millis'='%s') */",
                                        time2)))
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111),
                        Row.of(2, 22, 222),
                        Row.of(3, 33, 333),
                        Row.of(4, 44, 444));

        assertThat(batchSql("SELECT * FROM T /*+ OPTIONS('scan.snapshot-id'='3') */"))
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111),
                        Row.of(2, 22, 222),
                        Row.of(3, 33, 333),
                        Row.of(4, 44, 444),
                        Row.of(5, 55, 555),
                        Row.of(6, 66, 666));
        assertThat(
                        batchSql(
                                "SELECT * FROM T /*+ OPTIONS('scan.mode'='from-snapshot-full','scan.snapshot-id'='3') */"))
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111),
                        Row.of(2, 22, 222),
                        Row.of(3, 33, 333),
                        Row.of(4, 44, 444),
                        Row.of(5, 55, 555),
                        Row.of(6, 66, 666));
        assertThat(
                        batchSql(
                                String.format(
                                        "SELECT * FROM T /*+ OPTIONS('scan.timestamp-millis'='%s') */",
                                        time3)))
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111),
                        Row.of(2, 22, 222),
                        Row.of(3, 33, 333),
                        Row.of(4, 44, 444),
                        Row.of(5, 55, 555),
                        Row.of(6, 66, 666));

        assertThatThrownBy(
                        () ->
                                batchSql(
                                        String.format(
                                                "SELECT * FROM T /*+ OPTIONS('scan.timestamp-millis'='%s', 'scan.snapshot-id'='1') */",
                                                time3)))
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage(
                        "[scan.snapshot-id] must be null when you set [scan.timestamp-millis]");

        assertThatThrownBy(
                        () ->
                                batchSql(
                                        "SELECT * FROM T /*+ OPTIONS('scan.mode'='full', 'scan.snapshot-id'='1') */"))
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage(
                        "%s must be null when you use latest-full for scan.mode",
                        CoreOptions.SCAN_SNAPSHOT_ID.key());

        // travel to tag
        assertThat(batchSql("SELECT * FROM T /*+ OPTIONS('scan.tag-name'='tag2') */"))
                .containsExactlyInAnyOrder(
                        Row.of(1, 11, 111),
                        Row.of(2, 22, 222),
                        Row.of(3, 33, 333),
                        Row.of(4, 44, 444));

        assertThatThrownBy(
                        () -> batchSql("SELECT * FROM T /*+ OPTIONS('scan.tag-name'='unknown') */"))
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage("Tag 'unknown' doesn't exist.");
    }
}
