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

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test write and read table with blob type. */
public class BlobTableITCase extends CatalogITCaseBase {

    @Override
    protected List<String> ddl() {
        return Collections.singletonList(
                "CREATE TABLE IF NOT EXISTS blob_table (id INT, data STRING, picture BYTES) WITH ('row-tracking.enabled'='true', 'data-evolution.enabled'='true', 'blob.field'='picture')");
    }

    @Test
    public void testBasic() {
        batchSql("SELECT * FROM blob_table");
        batchSql("INSERT INTO blob_table VALUES (1, 'paimon', X'48656C6C6F')");
        assertThat(batchSql("SELECT * FROM blob_table"))
                .containsExactlyInAnyOrder(
                        Row.of(1, "paimon", new byte[] {72, 101, 108, 108, 111}));
        assertThat(batchSql("SELECT file_path FROM `blob_table$files`").size()).isEqualTo(2);
    }
}
