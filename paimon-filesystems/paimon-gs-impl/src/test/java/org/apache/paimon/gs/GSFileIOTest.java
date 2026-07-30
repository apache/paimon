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

package org.apache.paimon.gs;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link GSFileIO}. */
public class GSFileIOTest {

    @Test
    public void testGsPrefixedKeyIsMappedToHadoopKey() {
        Options options = new Options();
        options.set("gs.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE");

        Options hadoopConfig = loadHadoopConfig(options);

        assertThat(hadoopConfig.get("fs.gs.auth.type")).isEqualTo("SERVICE_ACCOUNT_JSON_KEYFILE");
        assertThat(hadoopConfig.containsKey("gs.auth.type")).isFalse();
    }

    @Test
    public void testHadoopPrefixedKeyIsKept() {
        Options options = new Options();
        options.set("fs.gs.project.id", "my-project");

        Options hadoopConfig = loadHadoopConfig(options);

        assertThat(hadoopConfig.get("fs.gs.project.id")).isEqualTo("my-project");
    }

    @Test
    public void testUnrelatedKeysAreIgnored() {
        Options options = new Options();
        options.set("warehouse", "gs://bucket/path");
        options.set("s3.endpoint", "http://localhost:9000");

        Options hadoopConfig = loadHadoopConfig(options);

        assertThat(hadoopConfig.toMap()).isEmpty();
    }

    private static Options loadHadoopConfig(Options options) {
        return new GSFileIO().loadHadoopConfigFromContext(CatalogContext.create(options));
    }
}
