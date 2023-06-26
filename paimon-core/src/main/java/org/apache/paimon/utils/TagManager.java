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

import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Manager for {@code Tag}. */
public class TagManager {

    private static final String TAG_PREFIX = "tag-";

    private final FileIO fileIO;
    private final Path tablePath;

    public TagManager(FileIO fileIO, Path tablePath) {
        this.fileIO = fileIO;
        this.tablePath = tablePath;
    }

    /** Return the root Directory of tags. */
    public Path tagDirectory() {
        return new Path(tablePath + "/tag");
    }

    /** Return the path of a tag. */
    public Path tagPath(String tagName) {
        return new Path(tablePath + "/tag/" + TAG_PREFIX + tagName);
    }

    /** Create a tag from given snapshot and save it in the storage. */
    public void createTag(Snapshot snapshot, String tagName) {
        checkArgument(!StringUtils.isBlank(tagName), "Tag name '%s' is blank.", tagName);
        checkArgument(!tagExists(tagName), "Tag name '%s' already exists.", tagName);
        checkArgument(
                !tagName.chars().allMatch(Character::isDigit),
                "Tag name cannot be pure numeric string but is '%s'.",
                tagName);

        Path newTagPath = tagPath(tagName);
        try {
            fileIO.writeFileUtf8(newTagPath, snapshot.toJson());
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Exception occurs when committing tag '%s' (path %s). "
                                    + "Cannot clean up because we can't determine the success.",
                            tagName, newTagPath),
                    e);
        }
    }

    /** Check if a tag exists. */
    public boolean tagExists(String tagName) {
        Path path = tagPath(tagName);
        try {
            return fileIO.exists(path);
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to determine if tag '%s' exists in path %s.", tagName, path),
                    e);
        }
    }

    /** Get the tagged snapshot by name. */
    public Snapshot taggedSnapshot(String tagName) {
        checkArgument(tagExists(tagName), "Tag '%s' doesn't exist.", tagName);
        return Snapshot.fromPath(fileIO, tagPath(tagName));
    }

    public int tagCount() {
        return listStatus().length;
    }

    /** Get all tagged snapshots sorted by snapshot id. */
    public List<Snapshot> taggedSnapshots() {
        return new ArrayList<>(tags().keySet());
    }

    /** Get all tagged snapshots with names sorted by snapshot id. */
    public SortedMap<Snapshot, String> tags() {
        FileStatus[] statuses = listStatus();
        TreeMap<Snapshot, String> tags = new TreeMap<>(Comparator.comparingLong(Snapshot::id));

        for (FileStatus status : statuses) {
            Path path = status.getPath();
            if (path.getName().startsWith(TAG_PREFIX)) {
                tags.put(
                        Snapshot.fromPath(fileIO, path),
                        path.getName().substring(TAG_PREFIX.length()));
            }
        }
        return tags;
    }

    private FileStatus[] listStatus() {
        Path tagDirectory = tagDirectory();
        try {
            if (!fileIO.exists(tagDirectory)) {
                return new FileStatus[0];
            }

            FileStatus[] statuses = fileIO.listStatus(tagDirectory);

            if (statuses == null) {
                throw new RuntimeException(
                        String.format(
                                "The return value is null of the listStatus for the '%s' directory.",
                                tagDirectory));
            }

            return statuses;
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format("Failed to list status in the '%s' directory.", tagDirectory), e);
        }
    }
}
