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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/** IT Case for {@link ExpireSnapshotsProcedure}. */
public class ExpireSnapshotsProcedureITCase extends CatalogITCaseBase {

    @Test
    public void testExpire() throws Exception {
        sql(
                "CREATE TABLE T ("
                        + " k INT,"
                        + " v INT,"
                        + " PRIMARY KEY (k) NOT ENFORCED"
                        + ") WITH ("
                        + " 'bucket' = '1'"
                        + ")");
        FileStoreTable table = paimonTable("T");

        for (int i = 0; i < 15; i++) {
            sql("INSERT INTO T VALUES (1, 1)");
        }

        sql("CALL sys.expire_snapshots('default.T', 10)");
        Assertions.assertThat(
                        table.snapshotManager()
                                .fileIO()
                                .exists(table.snapshotManager().changelogDirectory()))
                .isFalse();

        sql(
                "CREATE TABLE T_1 ("
                        + " k INT,"
                        + " v INT,"
                        + " PRIMARY KEY (k) NOT ENFORCED"
                        + ") WITH ("
                        + " 'bucket' = '1',"
                        + " 'snapshot.num-retained.max' = '12',"
                        + " 'changelog.num-retained.max' = '12'"
                        + ")");
        table = paimonTable("T_1");

        for (int i = 0; i < 15; i++) {
            sql("INSERT INTO T_1 VALUES (1, 1)");
        }
        Assertions.assertThat(table.snapshotManager().changelogs().hasNext()).isFalse();

        sql("CALL sys.expire_snapshots('default.T_1', 10)");
        Assertions.assertThat(table.snapshotManager().changelogs().hasNext()).isTrue();
    }
}
