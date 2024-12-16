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

package org.apache.paimon.flink.procedure;

import org.apache.paimon.flink.CatalogITCaseBase;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** IT Case for {@link RollbackToWatermarkProcedure}. */
public class RollbackToWatermarkProcedureITCase extends CatalogITCaseBase {

    @Test
    public void testCreateTagsFromSnapshotsWatermark() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " k STRING,"
                        + " dt STRING,"
                        + " PRIMARY KEY (k, dt) NOT ENFORCED"
                        + ") PARTITIONED BY (dt) WITH ("
                        + " 'bucket' = '1'"
                        + ")");

        // create snapshot 1 with watermark 1000.
        sql(
                "insert into T/*+ OPTIONS('end-input.watermark'= '1000') */ values('k1', '2024-12-02')");
        // create snapshot 2 with watermark 2000.
        sql(
                "insert into T/*+ OPTIONS('end-input.watermark'= '2000') */ values('k2', '2024-12-02')");
        // create snapshot 3 with watermark 3000.
        sql(
                "insert into T/*+ OPTIONS('end-input.watermark'= '3000') */ values('k3', '2024-12-02')");

        FileStoreTable table = paimonTable("T");

        long watermark1 = table.snapshotManager().snapshot(1).watermark();
        long watermark2 = table.snapshotManager().snapshot(2).watermark();
        long watermark3 = table.snapshotManager().snapshot(3).watermark();

        assertThat(watermark1 == 1000).isTrue();
        assertThat(watermark2 == 2000).isTrue();
        assertThat(watermark3 == 3000).isTrue();

        assertThat(sql("select * from T").stream().map(Row::toString))
                .containsExactlyInAnyOrder(
                        "+I[k1, 2024-12-02]", "+I[k2, 2024-12-02]", "+I[k3, 2024-12-02]");

        sql("CALL sys.rollback_to_watermark(`table` => 'default.T',`watermark` => 2001)");

        // check for snapshot 2
        assertThat(sql("select * from T").stream().map(Row::toString))
                .containsExactlyInAnyOrder("+I[k1, 2024-12-02]", "+I[k2, 2024-12-02]");

        sql("CALL sys.rollback_to_watermark(`table` => 'default.T',`watermark` => 1001)");

        // check for snapshot 1
        assertThat(sql("select * from T").stream().map(Row::toString))
                .containsExactlyInAnyOrder("+I[k1, 2024-12-02]");
    }
}
