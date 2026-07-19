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

package org.apache.paimon.rest.auth;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DLFLocalFileTokenLoader}. */
public class DLFLocalFileTokenLoaderTest {

    @TempDir Path tempDir;

    @Test
    public void testMalformedTokenFileDoesNotLeakCredentials() throws Exception {
        String secret = "STSSECRET_AKID_9999";
        Path tokenFile = tempDir.resolve("token.json");
        Files.write(
                tokenFile,
                ("{\"AccessKeyId\":\"akid\",\"AccessKeySecret\":\"" + secret + "\" INVALID_JSON")
                        .getBytes());

        assertThatThrownBy(() -> DLFLocalFileTokenLoader.readToken(tokenFile.toString(), 1))
                .hasNoCause()
                .satisfies(e -> assertThat(String.valueOf(e)).doesNotContain(secret));
    }
}
