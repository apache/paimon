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

package org.apache.flink.table.store.connector;

import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for batch file store. */
public class BatchFileStoreITCase extends FileStoreTableITCase {

    @Override
    protected List<String> ddl() {
        return Collections.singletonList("CREATE TABLE IF NOT EXISTS T (a INT, b INT, c INT)");
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
    public void testCompactedScanModeEmpty() {
        batchSql("INSERT INTO T VALUES (1, 11, 111), (2, 22, 222)");
        assertThat(batchSql("SELECT * FROM T /*+ OPTIONS('scan.mode'='compacted') */")).isEmpty();
    }
}
