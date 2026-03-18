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

package org.apache.paimon.postpone;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.table.sink.CommitMessageImpl;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A utility class to track and manage file changes for a specific bucket in a postpone bucket
 * table.
 */
public class BucketFiles {
    private final DataFilePathFactory pathFactory;
    private final FileIO fileIO;

    private @Nullable Integer totalBuckets;
    private final Map<String, DataFileMeta> newFiles;
    private final List<DataFileMeta> compactBefore;
    private final List<DataFileMeta> compactAfter;
    private final List<DataFileMeta> changelogFiles;
    private final List<IndexFileMeta> newIndexFiles;
    private final List<IndexFileMeta> deletedIndexFiles;

    public BucketFiles(DataFilePathFactory pathFactory, FileIO fileIO) {
        this.pathFactory = pathFactory;
        this.fileIO = fileIO;

        this.newFiles = new LinkedHashMap<>();
        this.compactBefore = new ArrayList<>();
        this.compactAfter = new ArrayList<>();
        this.changelogFiles = new ArrayList<>();
        this.newIndexFiles = new ArrayList<>();
        this.deletedIndexFiles = new ArrayList<>();
    }

    public void update(CommitMessageImpl message) {
        totalBuckets = message.totalBuckets();

        for (DataFileMeta file : message.newFilesIncrement().newFiles()) {
            newFiles.put(file.fileName(), file);
        }

        Map<String, Path> toDelete = new HashMap<>();
        for (DataFileMeta file : message.compactIncrement().compactBefore()) {
            if (newFiles.containsKey(file.fileName())) {
                toDelete.put(file.fileName(), pathFactory.toPath(file));
                newFiles.remove(file.fileName());
            } else {
                compactBefore.add(file);
            }
        }

        for (DataFileMeta file : message.compactIncrement().compactAfter()) {
            compactAfter.add(file);
            toDelete.remove(file.fileName());
        }

        changelogFiles.addAll(message.newFilesIncrement().changelogFiles());
        changelogFiles.addAll(message.compactIncrement().changelogFiles());

        newIndexFiles.addAll(message.compactIncrement().newIndexFiles());
        deletedIndexFiles.addAll(message.compactIncrement().deletedIndexFiles());

        toDelete.forEach((fileName, path) -> fileIO.deleteQuietly(path));
    }

    public CommitMessageImpl makeMessage(BinaryRow partition, int bucket) {
        List<DataFileMeta> realCompactAfter = new ArrayList<>(newFiles.values());
        realCompactAfter.addAll(compactAfter);
        return new CommitMessageImpl(
                partition,
                bucket,
                totalBuckets,
                DataIncrement.emptyIncrement(),
                new CompactIncrement(
                        compactBefore,
                        realCompactAfter,
                        changelogFiles,
                        newIndexFiles,
                        deletedIndexFiles));
    }
}
