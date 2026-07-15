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

package org.apache.paimon.clone;

import org.apache.paimon.Changelog;
import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.tag.Tag;
import org.apache.paimon.utils.Pair;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.apache.paimon.catalog.Identifier.DEFAULT_MAIN_BRANCH;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Computes a stable identity for the metadata roots of a full-history clone source. */
public class FullHistorySourceFingerprint {

    public static void verify(FileStoreTable table, String expected) throws IOException {
        String actual = compute(table);
        checkState(
                expected.equals(actual),
                "Source table changed during full-history clone. Expected fingerprint %s but found %s.",
                expected,
                actual);
    }

    public static String compute(FileStoreTable table) throws IOException {
        MessageDigest digest = sha256();
        update(digest, "full-history-source-v2");
        update(digest, "table-location");
        update(digest, new Path(table.location().toString()).toString());

        List<String> branches = new ArrayList<>(table.branchManager().branches());
        branches.add(DEFAULT_MAIN_BRANCH);
        Collections.sort(branches);
        for (String branch : branches) {
            update(digest, "branch");
            update(digest, branch);
            FileStoreTable branchTable = table.switchToBranch(branch);
            List<TableSchema> schemas = new ArrayList<>(branchTable.schemaManager().listAll());
            schemas.sort(Comparator.comparingLong(TableSchema::id));
            for (TableSchema schema : schemas) {
                update(digest, "schema");
                update(digest, schema.toString());
            }
            List<Snapshot> snapshots =
                    new ArrayList<>(branchTable.snapshotManager().safelyGetAllSnapshots());
            snapshots.sort(Comparator.comparingLong(Snapshot::id));
            for (Snapshot snapshot : snapshots) {
                update(digest, "snapshot");
                update(digest, snapshot.toJson());
            }
            List<Pair<Tag, String>> tags = new ArrayList<>(branchTable.tagManager().tagObjects());
            tags.sort(Comparator.comparing(Pair::getRight));
            for (Pair<Tag, String> tagAndName : tags) {
                Tag tag = tagAndName.getLeft();
                update(digest, "tag");
                update(digest, tagAndName.getRight());
                update(digest, tag.toJson());
                if (tag.getTagCreateTime() == null) {
                    update(
                            digest,
                            String.valueOf(
                                    branchTable
                                            .fileIO()
                                            .getFileStatus(
                                                    branchTable
                                                            .tagManager()
                                                            .tagPath(tagAndName.getRight()))
                                            .getModificationTime()));
                }
            }
            List<Changelog> changelogs =
                    new ArrayList<>(branchTable.changelogManager().safelyGetAllChangelogs());
            changelogs.sort(Comparator.comparingLong(Changelog::id));
            for (Changelog changelog : changelogs) {
                update(digest, "changelog");
                update(digest, changelog.toJson());
            }
        }
        return "v2:" + toHex(digest.digest());
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available.", e);
        }
    }

    private static void update(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update((byte) (bytes.length >>> 24));
        digest.update((byte) (bytes.length >>> 16));
        digest.update((byte) (bytes.length >>> 8));
        digest.update((byte) bytes.length);
        digest.update(bytes);
    }

    private static String toHex(byte[] bytes) {
        char[] result = new char[bytes.length * 2];
        char[] digits = "0123456789abcdef".toCharArray();
        for (int i = 0; i < bytes.length; i++) {
            int value = bytes[i] & 0xff;
            result[i * 2] = digits[value >>> 4];
            result[i * 2 + 1] = digits[value & 0x0f];
        }
        return new String(result);
    }

    private FullHistorySourceFingerprint() {}
}
