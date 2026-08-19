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

package org.apache.paimon.operation;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CoreFullVersion}. */
public class CoreFullVersionTest {

    @Test
    public void testFullVersion() throws Exception {
        String fullVersion = readResource("/META-INF/paimon-core.full-version");
        String commitId = readResource("/META-INF/paimon-core.commit-id");

        assertThat(CoreFullVersion.get()).isEqualTo(fullVersion);
        assertThat(fullVersion).endsWith("-" + commitId).isNotEqualTo(commitId);
    }

    private String readResource(String path) throws Exception {
        InputStream inputStream = CoreFullVersion.class.getResourceAsStream(path);
        assertThat(inputStream).isNotNull();
        try (BufferedReader reader =
                new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            return reader.readLine().trim();
        }
    }
}
