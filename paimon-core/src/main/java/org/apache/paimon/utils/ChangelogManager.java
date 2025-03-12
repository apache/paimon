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

package org.apache.paimon.utils;

import org.apache.paimon.Changelog;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.BranchManager.branchPath;
import static org.apache.paimon.utils.FileUtils.listVersionedFiles;
import static org.apache.paimon.utils.HintFileUtils.commitEarliestHint;
import static org.apache.paimon.utils.HintFileUtils.commitLatestHint;
import static org.apache.paimon.utils.HintFileUtils.findEarliest;
import static org.apache.paimon.utils.HintFileUtils.findLatest;
import static org.apache.paimon.utils.ThreadPoolUtils.createCachedThreadPool;
import static org.apache.paimon.utils.ThreadPoolUtils.randomlyOnlyExecute;

/**
 * Manager for {@link Changelog}, providing utility methods related to paths and changelog hints.
 */
public class ChangelogManager implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ChangelogManager.class);

    public static final String CHANGELOG_PREFIX = "changelog-";

    private final FileIO fileIO;
    private final Path tablePath;
    private final String branch;

    public ChangelogManager(FileIO fileIO, Path tablePath, @Nullable String branchName) {
        this.fileIO = fileIO;
        this.tablePath = tablePath;
        this.branch = BranchManager.normalizeBranch(branchName);
    }

    public FileIO fileIO() {
        return fileIO;
    }

    public @Nullable Long latestLongLivedChangelogId() {
        try {
            return findLatest(
                    fileIO, changelogDirectory(), CHANGELOG_PREFIX, this::longLivedChangelogPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to find latest changelog id", e);
        }
    }

    public @Nullable Long earliestLongLivedChangelogId() {
        try {
            return findEarliest(
                    fileIO, changelogDirectory(), CHANGELOG_PREFIX, this::longLivedChangelogPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to find earliest changelog id", e);
        }
    }

    public boolean longLivedChangelogExists(long snapshotId) {
        Path path = longLivedChangelogPath(snapshotId);
        try {
            return fileIO.exists(path);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to determine if changelog #" + snapshotId + " exists in path " + path,
                    e);
        }
    }

    public Changelog longLivedChangelog(long snapshotId) {
        return Changelog.fromPath(fileIO, longLivedChangelogPath(snapshotId));
    }

    public Changelog changelog(long snapshotId) {
        Path changelogPath = longLivedChangelogPath(snapshotId);
        return Changelog.fromPath(fileIO, changelogPath);
    }

    public Path longLivedChangelogPath(long snapshotId) {
        return new Path(
                branchPath(tablePath, branch) + "/changelog/" + CHANGELOG_PREFIX + snapshotId);
    }

    public Path changelogDirectory() {
        return new Path(branchPath(tablePath, branch) + "/changelog");
    }

    public void commitChangelog(Changelog changelog, long id) throws IOException {
        fileIO.writeFile(longLivedChangelogPath(id), changelog.toJson(), true);
    }

    public void commitLongLivedChangelogLatestHint(long snapshotId) throws IOException {
        commitLatestHint(fileIO, snapshotId, changelogDirectory());
    }

    public void commitLongLivedChangelogEarliestHint(long snapshotId) throws IOException {
        commitEarliestHint(fileIO, snapshotId, changelogDirectory());
    }

    public Changelog tryGetChangelog(long snapshotId) throws FileNotFoundException {
        Path changelogPath = longLivedChangelogPath(snapshotId);
        return Changelog.tryFromPath(fileIO, changelogPath);
    }

    public Iterator<Changelog> changelogs() throws IOException {
        return listVersionedFiles(fileIO, changelogDirectory(), CHANGELOG_PREFIX)
                .map(this::changelog)
                .sorted(Comparator.comparingLong(Changelog::id))
                .iterator();
    }

    public List<Changelog> safelyGetAllChangelogs() throws IOException {
        List<Path> paths =
                listVersionedFiles(fileIO, changelogDirectory(), CHANGELOG_PREFIX)
                        .map(this::longLivedChangelogPath)
                        .collect(Collectors.toList());

        List<Changelog> changelogs = Collections.synchronizedList(new ArrayList<>(paths.size()));
        collectSnapshots(
                path -> {
                    try {
                        changelogs.add(Changelog.fromJson(fileIO.readFileUtf8(path)));
                    } catch (IOException e) {
                        if (!(e instanceof FileNotFoundException)) {
                            throw new RuntimeException(e);
                        }
                    }
                },
                paths);

        return changelogs;
    }

    private static void collectSnapshots(Consumer<Path> pathConsumer, List<Path> paths)
            throws IOException {
        ExecutorService executor =
                createCachedThreadPool(
                        Runtime.getRuntime().availableProcessors(), "CHANGELOG_COLLECTOR");

        try {
            randomlyOnlyExecute(executor, pathConsumer, paths);
        } catch (RuntimeException e) {
            throw new IOException(e);
        } finally {
            executor.shutdown();
        }
    }
}
