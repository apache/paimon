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

import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.DeleteOrderingGuardFile;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.paimon.catalog.Identifier.DEFAULT_MAIN_BRANCH;
import static org.apache.paimon.utils.BranchManager.BRANCH_PREFIX;
import static org.apache.paimon.utils.FileUtils.listVersionedDirectories;

/** Finds retained guarded views in other branches before deleting their shared files. */
public class DeleteOrderingGuardBranchRetention {

    private DeleteOrderingGuardBranchRetention() {}

    public static List<Snapshot> snapshots(SnapshotManager current) {
        try {
            List<String> branches = new ArrayList<>();
            branches.add(DEFAULT_MAIN_BRANCH);
            Path directory = new Path(current.tablePath(), "branch");
            if (current.fileIO().exists(directory)) {
                branches.addAll(
                        listVersionedDirectories(current.fileIO(), directory, BRANCH_PREFIX)
                                .map(s -> s.getPath().getName().substring(BRANCH_PREFIX.length()))
                                .collect(Collectors.toList()));
            }
            List<Snapshot> result = new ArrayList<>();
            for (String branch : branches) {
                if (!branch.equals(current.branch())) {
                    result.addAll(current.copyWithBranch(branch).safelyGetAllSnapshots());
                    result.addAll(
                            new TagManager(current.fileIO(), current.tablePath(), branch)
                                    .taggedSnapshots());
                }
            }
            return result.stream()
                    .filter(DeleteOrderingGuardFile::mayContainGuards)
                    .collect(Collectors.toList());
        } catch (IOException failure) {
            // Incomplete discovery must never turn into permission to delete shared protection.
            throw new UncheckedIOException(
                    "Failed to discover guarded branch references.", failure);
        }
    }
}
