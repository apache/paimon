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
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.utils.SnapshotManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.UUID;

import static org.apache.paimon.catalog.Identifier.DEFAULT_MAIN_BRANCH;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for snapshot version compatibility between old and new Paimon versions. */
public class SnapshotVersionCompatibilityTest {

    @TempDir java.nio.file.Path tempDir;

    // ==================== Reading old-version snapshots ====================

    @Test
    public void testReadVersion1Snapshot() {
        // Version 1 snapshots: no "uuid", no "writerVersion", no "operation",
        // no "baseManifestListSize", no "deltaManifestListSize", no "changelogManifestListSize",
        // no "nextRowId", no "properties"
        String version1Json =
                "{\n"
                        + "  \"version\" : 1,\n"
                        + "  \"id\" : 1,\n"
                        + "  \"schemaId\" : 0,\n"
                        + "  \"baseManifestList\" : \"manifest-list-1\",\n"
                        + "  \"deltaManifestList\" : \"manifest-list-2\",\n"
                        + "  \"commitUser\" : \"user1\",\n"
                        + "  \"commitIdentifier\" : 1,\n"
                        + "  \"commitKind\" : \"APPEND\",\n"
                        + "  \"timeMillis\" : 1000,\n"
                        + "  \"totalRecordCount\" : 100,\n"
                        + "  \"deltaRecordCount\" : 10\n"
                        + "}";

        Snapshot snapshot = Snapshot.fromJson(version1Json);

        assertThat(snapshot.version()).isEqualTo(1);
        assertThat(snapshot.id()).isEqualTo(1);
        assertThat(snapshot.schemaId()).isEqualTo(0);
        assertThat(snapshot.baseManifestList()).isEqualTo("manifest-list-1");
        assertThat(snapshot.deltaManifestList()).isEqualTo("manifest-list-2");
        assertThat(snapshot.commitUser()).isEqualTo("user1");
        assertThat(snapshot.commitIdentifier()).isEqualTo(1);
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);
        assertThat(snapshot.timeMillis()).isEqualTo(1000);
        assertThat(snapshot.totalRecordCount()).isEqualTo(100);
        assertThat(snapshot.deltaRecordCount()).isEqualTo(10);

        // Fields not present in version 1 should be null
        assertThat(snapshot.uuid()).isNull();
        assertThat(snapshot.writerVersion()).isNull();
        assertThat(snapshot.operation()).isNull();
        assertThat(snapshot.baseManifestListSize()).isNull();
        assertThat(snapshot.deltaManifestListSize()).isNull();
        assertThat(snapshot.changelogManifestListSize()).isNull();
        assertThat(snapshot.changelogManifestList()).isNull();
        assertThat(snapshot.changelogRecordCount()).isNull();
        assertThat(snapshot.watermark()).isNull();
        assertThat(snapshot.statistics()).isNull();
        assertThat(snapshot.indexManifest()).isNull();
        assertThat(snapshot.nextRowId()).isNull();
        assertThat(snapshot.properties()).isNull();
    }

    @Test
    public void testReadVersion2Snapshot() {
        // Version 2 snapshots: added "uuid", "writerVersion"
        // Still no "operation", no "nextRowId", no "properties"
        String version2Json =
                "{\n"
                        + "  \"version\" : 2,\n"
                        + "  \"uuid\" : \"550e8400-e29b-41d4-a716-446655440000\",\n"
                        + "  \"id\" : 2,\n"
                        + "  \"schemaId\" : 0,\n"
                        + "  \"baseManifestList\" : \"manifest-list-1\",\n"
                        + "  \"deltaManifestList\" : \"manifest-list-2\",\n"
                        + "  \"commitUser\" : \"user2\",\n"
                        + "  \"writerVersion\" : \"java-1.0-abcdef1234567890abcdef1234567890abcdef12\",\n"
                        + "  \"commitIdentifier\" : 2,\n"
                        + "  \"commitKind\" : \"COMPACT\",\n"
                        + "  \"timeMillis\" : 2000,\n"
                        + "  \"totalRecordCount\" : 200,\n"
                        + "  \"deltaRecordCount\" : 20\n"
                        + "}";

        Snapshot snapshot = Snapshot.fromJson(version2Json);

        assertThat(snapshot.version()).isEqualTo(2);
        assertThat(snapshot.id()).isEqualTo(2);
        assertThat(snapshot.uuid()).isEqualTo("550e8400-e29b-41d4-a716-446655440000");
        assertThat(snapshot.writerVersion())
                .isEqualTo("java-1.0-abcdef1234567890abcdef1234567890abcdef12");
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);

        // Fields added in version 3 should be null
        assertThat(snapshot.operation()).isNull();
        assertThat(snapshot.nextRowId()).isNull();
        assertThat(snapshot.properties()).isNull();
        assertThat(snapshot.baseManifestListSize()).isNull();
        assertThat(snapshot.deltaManifestListSize()).isNull();
        assertThat(snapshot.changelogManifestListSize()).isNull();
    }

    @Test
    public void testReadVersion3Snapshot() {
        // Version 3 snapshots: all current fields
        String version3Json =
                "{\n"
                        + "  \"version\" : 3,\n"
                        + "  \"uuid\" : \"660e8400-e29b-41d4-a716-446655440001\",\n"
                        + "  \"id\" : 3,\n"
                        + "  \"schemaId\" : 1,\n"
                        + "  \"baseManifestList\" : \"manifest-list-1\",\n"
                        + "  \"baseManifestListSize\" : 1024,\n"
                        + "  \"deltaManifestList\" : \"manifest-list-2\",\n"
                        + "  \"deltaManifestListSize\" : 512,\n"
                        + "  \"changelogManifestList\" : \"manifest-list-3\",\n"
                        + "  \"changelogManifestListSize\" : 256,\n"
                        + "  \"indexManifest\" : \"index-manifest-1\",\n"
                        + "  \"commitUser\" : \"user3\",\n"
                        + "  \"writerVersion\" : \"java-2.0-abcd1234567890abcdef1234567890abcdef1234\",\n"
                        + "  \"commitIdentifier\" : 3,\n"
                        + "  \"commitKind\" : \"APPEND\",\n"
                        + "  \"timeMillis\" : 3000,\n"
                        + "  \"totalRecordCount\" : 300,\n"
                        + "  \"deltaRecordCount\" : 30,\n"
                        + "  \"changelogRecordCount\" : 15,\n"
                        + "  \"watermark\" : 2500,\n"
                        + "  \"statistics\" : \"stats-file-1\",\n"
                        + "  \"properties\" : {\"key1\" : \"value1\"},\n"
                        + "  \"nextRowId\" : 1000,\n"
                        + "  \"operation\" : \"MERGE\"\n"
                        + "}";

        Snapshot snapshot = Snapshot.fromJson(version3Json);

        assertThat(snapshot.version()).isEqualTo(3);
        assertThat(snapshot.id()).isEqualTo(3);
        assertThat(snapshot.schemaId()).isEqualTo(1);
        assertThat(snapshot.uuid()).isEqualTo("660e8400-e29b-41d4-a716-446655440001");
        assertThat(snapshot.baseManifestList()).isEqualTo("manifest-list-1");
        assertThat(snapshot.baseManifestListSize()).isEqualTo(1024);
        assertThat(snapshot.deltaManifestList()).isEqualTo("manifest-list-2");
        assertThat(snapshot.deltaManifestListSize()).isEqualTo(512);
        assertThat(snapshot.changelogManifestList()).isEqualTo("manifest-list-3");
        assertThat(snapshot.changelogManifestListSize()).isEqualTo(256);
        assertThat(snapshot.indexManifest()).isEqualTo("index-manifest-1");
        assertThat(snapshot.commitUser()).isEqualTo("user3");
        assertThat(snapshot.writerVersion())
                .isEqualTo("java-2.0-abcd1234567890abcdef1234567890abcdef1234");
        assertThat(snapshot.commitIdentifier()).isEqualTo(3);
        assertThat(snapshot.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);
        assertThat(snapshot.timeMillis()).isEqualTo(3000);
        assertThat(snapshot.totalRecordCount()).isEqualTo(300);
        assertThat(snapshot.deltaRecordCount()).isEqualTo(30);
        assertThat(snapshot.changelogRecordCount()).isEqualTo(15);
        assertThat(snapshot.watermark()).isEqualTo(2500);
        assertThat(snapshot.statistics()).isEqualTo("stats-file-1");
        assertThat(snapshot.properties()).containsEntry("key1", "value1");
        assertThat(snapshot.nextRowId()).isEqualTo(1000);
        assertThat(snapshot.operation()).isEqualTo(Snapshot.Operation.MERGE);
    }

    @Test
    public void testReadSnapshotWithoutUuid() {
        // Snapshot without uuid (old format)
        String json =
                "{\n"
                        + "  \"version\" : 3,\n"
                        + "  \"id\" : 10,\n"
                        + "  \"schemaId\" : 0,\n"
                        + "  \"baseManifestList\" : \"m-1\",\n"
                        + "  \"deltaManifestList\" : \"m-2\",\n"
                        + "  \"commitUser\" : \"user\",\n"
                        + "  \"commitIdentifier\" : 10,\n"
                        + "  \"commitKind\" : \"APPEND\",\n"
                        + "  \"timeMillis\" : 5000,\n"
                        + "  \"totalRecordCount\" : 50,\n"
                        + "  \"deltaRecordCount\" : 5\n"
                        + "}";

        Snapshot snapshot = Snapshot.fromJson(json);
        assertThat(snapshot.uuid()).isNull();
        assertThat(snapshot.id()).isEqualTo(10);
        assertThat(snapshot.version()).isEqualTo(3);
    }

    @Test
    public void testReadSnapshotWithoutWriterVersion() {
        // Snapshot without writerVersion (old format, before writer version tracking)
        String json =
                "{\n"
                        + "  \"version\" : 3,\n"
                        + "  \"id\" : 11,\n"
                        + "  \"schemaId\" : 0,\n"
                        + "  \"baseManifestList\" : \"m-1\",\n"
                        + "  \"deltaManifestList\" : \"m-2\",\n"
                        + "  \"commitUser\" : \"user\",\n"
                        + "  \"commitIdentifier\" : 11,\n"
                        + "  \"commitKind\" : \"APPEND\",\n"
                        + "  \"timeMillis\" : 6000,\n"
                        + "  \"totalRecordCount\" : 60,\n"
                        + "  \"deltaRecordCount\" : 6\n"
                        + "}";

        Snapshot snapshot = Snapshot.fromJson(json);
        assertThat(snapshot.writerVersion()).isNull();
        assertThat(snapshot.id()).isEqualTo(11);
    }

    @Test
    public void testReadSnapshotWithoutOperation() {
        // Snapshot without operation field (old format)
        String json =
                "{\n"
                        + "  \"version\" : 3,\n"
                        + "  \"id\" : 12,\n"
                        + "  \"schemaId\" : 0,\n"
                        + "  \"baseManifestList\" : \"m-1\",\n"
                        + "  \"deltaManifestList\" : \"m-2\",\n"
                        + "  \"commitUser\" : \"user\",\n"
                        + "  \"commitIdentifier\" : 12,\n"
                        + "  \"commitKind\" : \"APPEND\",\n"
                        + "  \"timeMillis\" : 7000,\n"
                        + "  \"totalRecordCount\" : 70,\n"
                        + "  \"deltaRecordCount\" : 7\n"
                        + "}";

        Snapshot snapshot = Snapshot.fromJson(json);
        assertThat(snapshot.operation()).isNull();
        assertThat(snapshot.id()).isEqualTo(12);
    }

    @Test
    public void testReadSnapshotWithUnknownFields() {
        // New Paimon reads old metadata with unknown fields - should be ignored
        String json =
                "{\n"
                        + "  \"version\" : 3,\n"
                        + "  \"id\" : 13,\n"
                        + "  \"schemaId\" : 0,\n"
                        + "  \"baseManifestList\" : \"m-1\",\n"
                        + "  \"deltaManifestList\" : \"m-2\",\n"
                        + "  \"commitUser\" : \"user\",\n"
                        + "  \"commitIdentifier\" : 13,\n"
                        + "  \"commitKind\" : \"APPEND\",\n"
                        + "  \"timeMillis\" : 8000,\n"
                        + "  \"totalRecordCount\" : 80,\n"
                        + "  \"deltaRecordCount\" : 8,\n"
                        + "  \"futureField1\" : \"someValue\",\n"
                        + "  \"futureField2\" : 12345,\n"
                        + "  \"futureField3\" : {\"nested\" : true}\n"
                        + "}";

        Snapshot snapshot = Snapshot.fromJson(json);
        assertThat(snapshot.id()).isEqualTo(13);
        assertThat(snapshot.timeMillis()).isEqualTo(8000);
        // Unknown fields should be silently ignored
    }

    // ==================== Write-read round-trip tests ====================

    @Test
    public void testSnapshotWriteReadRoundTrip() {
        // Create a snapshot with all fields, serialize to JSON, deserialize back
        Snapshot original =
                new Snapshot(
                        100,
                        5,
                        "base-manifest-list",
                        1024L,
                        "delta-manifest-list",
                        512L,
                        "changelog-manifest-list",
                        256L,
                        "index-manifest",
                        "commitUser",
                        "java-2.0-test",
                        100,
                        Snapshot.CommitKind.APPEND,
                        9000,
                        500,
                        50,
                        25L,
                        8000L,
                        "stats-file",
                        java.util.Collections.singletonMap("k", "v"),
                        2000L,
                        Snapshot.Operation.MERGE);

        String json = original.toJson();
        Snapshot deserialized = Snapshot.fromJson(json);

        assertThat(deserialized).isEqualTo(original);
        assertThat(deserialized.version()).isEqualTo(3);
        assertThat(deserialized.uuid()).isNotNull();
        assertThat(UUID.fromString(deserialized.uuid()).toString())
                .isEqualTo(deserialized.uuid());
    }

    @Test
    public void testSnapshotJsonRoundTripPreservesAllFields() {
        // Verify that serializing and deserializing preserves all fields
        Snapshot snapshot =
                new Snapshot(
                        200,
                        10,
                        "base-ml",
                        null,
                        "delta-ml",
                        null,
                        null,
                        null,
                        null,
                        "testUser",
                        "java-3.0-snapshot",
                        200,
                        Snapshot.CommitKind.COMPACT,
                        10000,
                        1000,
                        100,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null);

        String json = snapshot.toJson();
        Snapshot roundTripped = Snapshot.fromJson(json);

        assertThat(roundTripped).isEqualTo(snapshot);
        assertThat(roundTripped.id()).isEqualTo(200);
        assertThat(roundTripped.schemaId()).isEqualTo(10);
        assertThat(roundTripped.commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);
        assertThat(roundTripped.commitUser()).isEqualTo("testUser");
        assertThat(roundTripped.writerVersion()).isEqualTo("java-3.0-snapshot");
        assertThat(roundTripped.uuid()).isNotNull();
    }

    // ==================== Forward compatibility (old reads new) ====================

    @Test
    public void testNewSnapshotJsonCompatibleWithOldReader() {
        // Verify that the JSON output of a new snapshot does not contain fields
        // that would break an old version reader (old readers ignore unknown fields
        // via @JsonIgnoreProperties(ignoreUnknown = true), but we verify the JSON is valid)
        Snapshot snapshot =
                new Snapshot(
                        300,
                        0,
                        "base-ml",
                        null,
                        "delta-ml",
                        null,
                        null,
                        null,
                        null,
                        "user",
                        null,
                        300,
                        Snapshot.CommitKind.APPEND,
                        11000,
                        200,
                        20,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null);

        String json = snapshot.toJson();

        // The JSON should be parseable by any version reader
        Snapshot parsed = Snapshot.fromJson(json);
        assertThat(parsed.id()).isEqualTo(300);
        assertThat(parsed.commitKind()).isEqualTo(Snapshot.CommitKind.APPEND);

        // Null fields with @JsonInclude(NON_NULL) should not appear in JSON
        assertThat(json).doesNotContain("\"changelogManifestList\"");
        assertThat(json).doesNotContain("\"changelogManifestListSize\"");
        assertThat(json).doesNotContain("\"indexManifest\"");
        assertThat(json).doesNotContain("\"operation\"");
        assertThat(json).doesNotContain("\"statistics\"");
        assertThat(json).doesNotContain("\"properties\"");
        assertThat(json).doesNotContain("\"nextRowId\"");
    }

    @Test
    public void testNewSnapshotJsonWithOperationIsValid() {
        // When operation is present, it should be serialized correctly
        Snapshot snapshot =
                new Snapshot(
                        400,
                        0,
                        "base-ml",
                        null,
                        "delta-ml",
                        null,
                        null,
                        null,
                        null,
                        "user",
                        null,
                        400,
                        Snapshot.CommitKind.APPEND,
                        12000,
                        300,
                        30,
                        null,
                        null,
                        null,
                        null,
                        null,
                        Snapshot.Operation.MERGE);

        String json = snapshot.toJson();
        assertThat(json).contains("\"operation\"");
        assertThat(json).contains("\"MERGE\"");

        // Old reader should be able to parse this (unknown fields ignored)
        Snapshot parsed = Snapshot.fromJson(json);
        assertThat(parsed.operation()).isEqualTo(Snapshot.Operation.MERGE);
    }

    // ==================== Persistence and file I/O tests ====================

    @Test
    public void testWriteAndReadSnapshotFromFile() throws Exception {
        FileIO fileIO = LocalFileIO.create();
        SnapshotManager snapshotManager =
                new SnapshotManager(fileIO, new Path(tempDir.toString()), DEFAULT_MAIN_BRANCH, null, null);

        // Write a snapshot with all features
        Snapshot original =
                new Snapshot(
                        1,
                        0,
                        "base-ml",
                        100L,
                        "delta-ml",
                        50L,
                        "changelog-ml",
                        25L,
                        "index-ml",
                        "testUser",
                        "java-test-version",
                        1,
                        Snapshot.CommitKind.APPEND,
                        13000,
                        400,
                        40,
                        20L,
                        12000L,
                        "stats",
                        java.util.Collections.singletonMap("prop", "val"),
                        500L,
                        Snapshot.Operation.MERGE);

        fileIO.tryToWriteAtomic(snapshotManager.snapshotPath(1), original.toJson());

        // Read back and verify
        Snapshot read = snapshotManager.snapshot(1);
        assertThat(read).isEqualTo(original);
        assertThat(read.version()).isEqualTo(3);
        assertThat(read.uuid()).isEqualTo(original.uuid());
        assertThat(read.writerVersion()).isEqualTo("java-test-version");
        assertThat(read.operation()).isEqualTo(Snapshot.Operation.MERGE);
        assertThat(read.nextRowId()).isEqualTo(500);
        assertThat(read.properties()).containsEntry("prop", "val");
    }

    @Test
    public void testWriteAndReadMinimalSnapshotFromFile() throws Exception {
        FileIO fileIO = LocalFileIO.create();
        SnapshotManager snapshotManager =
                new SnapshotManager(fileIO, new Path(tempDir.toString()), DEFAULT_MAIN_BRANCH, null, null);

        // Write a minimal snapshot (similar to old version format)
        Snapshot minimal =
                new Snapshot(
                        2,
                        0,
                        "base-ml",
                        null,
                        "delta-ml",
                        null,
                        null,
                        null,
                        null,
                        "minimalUser",
                        null,
                        2,
                        Snapshot.CommitKind.APPEND,
                        14000,
                        10,
                        1,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null);

        fileIO.tryToWriteAtomic(snapshotManager.snapshotPath(2), minimal.toJson());

        // Read back and verify
        Snapshot read = snapshotManager.snapshot(2);
        assertThat(read).isEqualTo(minimal);
        assertThat(read.id()).isEqualTo(2);
        assertThat(read.commitUser()).isEqualTo("minimalUser");
        assertThat(read.operation()).isNull();
        assertThat(read.nextRowId()).isNull();
        assertThat(read.properties()).isNull();
    }

    @Test
    public void testMultipleSnapshotVersionsCanCoexist() throws Exception {
        FileIO fileIO = LocalFileIO.create();
        SnapshotManager snapshotManager =
                new SnapshotManager(fileIO, new Path(tempDir.toString()), DEFAULT_MAIN_BRANCH, null, null);

        // Write a version 1 style snapshot JSON directly
        String version1Json =
                "{\n"
                        + "  \"version\" : 1,\n"
                        + "  \"id\" : 1,\n"
                        + "  \"schemaId\" : 0,\n"
                        + "  \"baseManifestList\" : \"ml-1\",\n"
                        + "  \"deltaManifestList\" : \"ml-2\",\n"
                        + "  \"commitUser\" : \"v1user\",\n"
                        + "  \"commitIdentifier\" : 1,\n"
                        + "  \"commitKind\" : \"APPEND\",\n"
                        + "  \"timeMillis\" : 1000,\n"
                        + "  \"totalRecordCount\" : 100,\n"
                        + "  \"deltaRecordCount\" : 10\n"
                        + "}";
        fileIO.tryToWriteAtomic(snapshotManager.snapshotPath(1), version1Json);

        // Write a version 3 snapshot normally
        Snapshot v3Snapshot =
                new Snapshot(
                        2,
                        0,
                        "ml-3",
                        null,
                        "ml-4",
                        null,
                        null,
                        null,
                        null,
                        "v3user",
                        "java-3.0",
                        2,
                        Snapshot.CommitKind.COMPACT,
                        2000,
                        200,
                        20,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null);
        fileIO.tryToWriteAtomic(snapshotManager.snapshotPath(2), v3Snapshot.toJson());

        // Read both - should work correctly
        Snapshot v1 = snapshotManager.snapshot(1);
        Snapshot v3 = snapshotManager.snapshot(2);

        assertThat(v1.version()).isEqualTo(1);
        assertThat(v1.uuid()).isNull();
        assertThat(v1.commitUser()).isEqualTo("v1user");

        assertThat(v3.version()).isEqualTo(3);
        assertThat(v3.uuid()).isNotNull();
        assertThat(v3.commitUser()).isEqualTo("v3user");
        assertThat(v3.commitKind()).isEqualTo(Snapshot.CommitKind.COMPACT);
    }
}