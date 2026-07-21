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

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.utils.StringUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Identifies a target root owned by a specific full-history clone. */
public class FullHistoryCloneMarker {

    public static final String FILE_NAME = "_full_history_clone";
    public static final String SUCCESS_FILE_NAME = "_SUCCESS";

    public static boolean prepare(
            FileIO targetFileIO,
            FullHistoryClonePlan clonePlan,
            PathMapping mapping,
            boolean cloneIfExists)
            throws IOException {
        return prepare(targetFileIO, clonePlan, mapping, null, null, cloneIfExists);
    }

    public static boolean prepare(
            FileIO targetFileIO,
            FullHistoryClonePlan clonePlan,
            PathMapping mapping,
            @Nullable String targetDatabase,
            @Nullable String targetTable,
            boolean cloneIfExists)
            throws IOException {
        String expected = content(clonePlan, mapping, targetDatabase, targetTable);
        boolean resumed =
                prepareRoot(
                        targetFileIO,
                        clonePlan.targetRoot(),
                        "Target table root",
                        expected,
                        cloneIfExists);
        for (Path externalRoot : clonePlan.externalTargetRoots()) {
            prepareRoot(
                    targetFileIO,
                    externalRoot,
                    "The external target root",
                    expected,
                    cloneIfExists);
        }
        if (isSuccessful(targetFileIO, clonePlan, mapping, targetDatabase, targetTable)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Full-history clone at target table root %s is already completed.",
                            clonePlan.targetRoot()));
        }
        return resumed;
    }

    public static void markSuccessful(
            FileIO targetFileIO,
            FullHistoryClonePlan clonePlan,
            PathMapping mapping,
            @Nullable String targetDatabase,
            @Nullable String targetTable)
            throws IOException {
        String expected = content(clonePlan, mapping, targetDatabase, targetTable);
        Path marker = new Path(clonePlan.targetRoot(), FILE_NAME);
        checkArgument(
                targetFileIO.exists(marker),
                "Target table root %s is not owned by a full-history clone.",
                clonePlan.targetRoot());
        checkArgument(
                expected.equals(targetFileIO.readFileUtf8(marker)),
                "Target table root %s belongs to a different full-history clone.",
                clonePlan.targetRoot());
        validateExternalMarkers(targetFileIO, clonePlan, expected);

        Path success = new Path(clonePlan.targetRoot(), SUCCESS_FILE_NAME);
        if (isSuccessful(targetFileIO, clonePlan, mapping, targetDatabase, targetTable)) {
            return;
        }
        if (!targetFileIO.tryToWriteAtomic(success, expected)) {
            checkArgument(
                    expected.equals(targetFileIO.readFileUtf8(success)),
                    "Success marker at target table root %s belongs to a different clone.",
                    clonePlan.targetRoot());
        }
    }

    public static boolean isSuccessful(
            FileIO targetFileIO,
            FullHistoryClonePlan clonePlan,
            PathMapping mapping,
            @Nullable String targetDatabase,
            @Nullable String targetTable)
            throws IOException {
        Path success = new Path(clonePlan.targetRoot(), SUCCESS_FILE_NAME);
        if (!targetFileIO.exists(success)) {
            return false;
        }
        String expected = content(clonePlan, mapping, targetDatabase, targetTable);
        Path marker = new Path(clonePlan.targetRoot(), FILE_NAME);
        checkArgument(
                targetFileIO.exists(marker) && expected.equals(targetFileIO.readFileUtf8(marker)),
                "Target table root %s is not owned by this full-history clone.",
                clonePlan.targetRoot());
        validateExternalMarkers(targetFileIO, clonePlan, expected);
        checkArgument(
                expected.equals(targetFileIO.readFileUtf8(success)),
                "Success marker at target table root %s belongs to a different clone.",
                clonePlan.targetRoot());
        return true;
    }

    private static String content(
            FullHistoryClonePlan clonePlan,
            PathMapping mapping,
            @Nullable String targetDatabase,
            @Nullable String targetTable) {
        StringBuilder builder = new StringBuilder();
        appendField(builder, "version", "4");
        appendField(builder, "source", clonePlan.sourceRoot().toString());
        appendField(builder, "target", clonePlan.targetRoot().toString());
        appendField(builder, "fingerprint", clonePlan.sourceFingerprint());
        appendField(builder, "targetDatabase", normalizeIdentifier(targetDatabase));
        appendField(builder, "targetTable", normalizeIdentifier(targetTable));
        appendField(builder, "mappings", mapping.identity());
        appendField(
                builder,
                "externalTargetRoots",
                clonePlan.externalTargetRoots().stream()
                        .map(Path::toString)
                        .collect(Collectors.joining("\n")));
        return builder.toString();
    }

    private static boolean prepareRoot(
            FileIO fileIO, Path root, String description, String expected, boolean cloneIfExists)
            throws IOException {
        Path marker = new Path(root, FILE_NAME);
        boolean empty = !fileIO.exists(root) || fileIO.listStatus(root).length == 0;
        if (empty && fileIO.tryToWriteAtomic(marker, expected)) {
            return false;
        }

        checkArgument(
                cloneIfExists,
                "%s already contains files: %s. Set clone_if_exists=true to resume.",
                description,
                root);
        checkArgument(
                fileIO.exists(marker),
                "%s %s is not owned by a full-history clone and cannot be resumed.",
                description,
                root);
        checkArgument(
                expected.equals(fileIO.readFileUtf8(marker)),
                "%s %s belongs to a different full-history clone.",
                description,
                root);
        return true;
    }

    private static void validateExternalMarkers(
            FileIO fileIO, FullHistoryClonePlan clonePlan, String expected) throws IOException {
        for (Path externalRoot : clonePlan.externalTargetRoots()) {
            Path marker = new Path(externalRoot, FILE_NAME);
            checkArgument(
                    fileIO.exists(marker) && expected.equals(fileIO.readFileUtf8(marker)),
                    "External target root %s is not owned by this full-history clone.",
                    externalRoot);
        }
    }

    private static void appendField(StringBuilder builder, String name, String value) {
        String encoded =
                Base64.getUrlEncoder()
                        .withoutPadding()
                        .encodeToString(value.getBytes(StandardCharsets.UTF_8));
        builder.append(name).append('=').append(encoded.length()).append(':').append(encoded);
    }

    private static String normalizeIdentifier(@Nullable String identifier) {
        return StringUtils.isNullOrWhitespaceOnly(identifier) ? "" : identifier.trim();
    }

    private FullHistoryCloneMarker() {}
}
