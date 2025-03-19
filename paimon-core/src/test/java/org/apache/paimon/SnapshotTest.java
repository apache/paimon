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

package org.apache.paimon;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;

import static org.apache.paimon.utils.BranchManager.DEFAULT_MAIN_BRANCH;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for snapshots. */
public class SnapshotTest {

    @Test
    public void testJsonIgnoreProperties() {
        Snapshot.fromJson(
                "{\n"
                        + "  \"version\" : 3,\n"
                        + "  \"id\" : 5,\n"
                        + "  \"schemaId\" : 0,\n"
                        + "  \"baseManifestList\" : null,\n"
                        + "  \"deltaManifestList\" : null,\n"
                        + "  \"changelogManifestList\" : null,\n"
                        + "  \"commitUser\" : null,\n"
                        + "  \"commitIdentifier\" : 0,\n"
                        + "  \"commitKind\" : \"APPEND\",\n"
                        + "  \"timeMillis\" : 1234,\n"
                        + "  \"totalRecordCount\" : null,\n"
                        + "  \"deltaRecordCount\" : null,\n"
                        + "  \"unknownKey\" : 22222\n"
                        + "}");
    }

    @Test
    public void testSnapshotWithSizes() {
        String json =
                "{\n"
                        + "  \"version\" : 3,\n"
                        + "  \"id\" : 5,\n"
                        + "  \"schemaId\" : 0,\n"
                        + "  \"baseManifestList\" : null,\n"
                        + "  \"baseManifestListSize\" : 6,\n"
                        + "  \"deltaManifestList\" : null,\n"
                        + "  \"deltaManifestListSize\" : 8,\n"
                        + "  \"changelogManifestListSize\" : 10,\n"
                        + "  \"commitUser\" : null,\n"
                        + "  \"commitIdentifier\" : 0,\n"
                        + "  \"commitKind\" : \"APPEND\",\n"
                        + "  \"timeMillis\" : 1234,\n"
                        + "  \"totalRecordCount\" : null,\n"
                        + "  \"deltaRecordCount\" : null,\n"
                        + "  \"unknownKey\" : 22222\n"
                        + "}";
        Snapshot snapshot = Snapshot.fromJson(json);
        assertThat(snapshot.baseManifestListSize).isEqualTo(6);
        assertThat(snapshot.deltaManifestListSize).isEqualTo(8);
        assertThat(snapshot.changelogManifestListSize).isEqualTo(10);
        assertThat(Snapshot.fromJson(snapshot.toJson())).isEqualTo(snapshot);
    }

    public static SnapshotManager newSnapshotManager(FileIO fileIO, Path tablePath) {
        return newSnapshotManager(fileIO, tablePath, DEFAULT_MAIN_BRANCH);
    }

    public static ChangelogManager newChangelogManager(FileIO fileIO, Path tablePath) {
        return new ChangelogManager(fileIO, tablePath, DEFAULT_MAIN_BRANCH);
    }

    public static SnapshotManager newSnapshotManager(FileIO fileIO, Path tablePath, String branch) {
        return new SnapshotManager(fileIO, tablePath, branch, null, null);
    }
}
