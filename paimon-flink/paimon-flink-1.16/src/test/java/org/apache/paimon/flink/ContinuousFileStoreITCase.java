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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** SQL ITCase for continuous file store. */
public class ContinuousFileStoreITCase extends CatalogITCaseBase {

    @Override
    protected List<String> ddl() {
        return Arrays.asList(
                "CREATE TABLE IF NOT EXISTS T1 (a STRING, b STRING, c STRING) WITH ('bucket' = '1', 'bucket-key' = 'a')",
                "CREATE TABLE IF NOT EXISTS T2 (a STRING, b STRING, c STRING, PRIMARY KEY (a) NOT ENFORCED)"
                        + " WITH ('changelog-producer'='input', 'bucket' = '1')");
    }

    @Test
    public void testFlinkMemoryPool() {
        // Check if the configuration is effective
        assertThatThrownBy(
                        () ->
                                batchSql(
                                        "INSERT INTO %s /*+ OPTIONS('sink.use-managed-memory-allocator'='true', 'sink.managed.writer-buffer-memory'='0M') */ "
                                                + "VALUES ('1', '2', '3'), ('4', '5', '6')",
                                        "T1"))
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage(
                        "Weights for operator scope use cases must be greater than 0.");

        batchSql(
                "INSERT INTO %s /*+ OPTIONS('sink.use-managed-memory-allocator'='true', 'sink.managed.writer-buffer-memory'='1M') */ "
                        + "VALUES ('1', '2', '3'), ('4', '5', '6')",
                "T1");
        assertThat(batchSql("SELECT * FROM T1").size()).isEqualTo(2);
    }
}
