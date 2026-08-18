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

import javax.annotation.Nullable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/** Loads the Git commit ID of the current Paimon Core build. */
final class CoreCommitId {

    @Nullable private static final String COMMIT_ID = load();

    private CoreCommitId() {}

    @Nullable
    static String get() {
        return COMMIT_ID;
    }

    @Nullable
    private static String load() {
        InputStream inputStream =
                CoreCommitId.class.getResourceAsStream("/META-INF/paimon-core.commit-id");
        if (inputStream == null) {
            return null;
        }

        try (BufferedReader reader =
                new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            String commitId = reader.readLine();
            if (commitId == null || commitId.trim().isEmpty()) {
                return null;
            }

            commitId = commitId.trim();
            return "UNKNOWN".equals(commitId) ? null : commitId;
        } catch (IOException e) {
            return null;
        }
    }
}
