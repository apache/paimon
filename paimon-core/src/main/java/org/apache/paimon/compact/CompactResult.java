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

package org.apache.paimon.compact;

import org.apache.paimon.io.DataFileMeta;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Result of compaction. */
public class CompactResult {

    private final List<DataFileMeta> before;
    private final List<DataFileMeta> after;
    private final List<DataFileMeta> changelog;

    @Nullable private CompactDeletionFile deletionFile;

    public CompactResult() {
        this(Collections.emptyList(), Collections.emptyList());
    }

    public CompactResult(DataFileMeta before, DataFileMeta after) {
        this(Collections.singletonList(before), Collections.singletonList(after));
    }

    public CompactResult(List<DataFileMeta> before, List<DataFileMeta> after) {
        this(before, after, Collections.emptyList());
    }

    public CompactResult(
            List<DataFileMeta> before, List<DataFileMeta> after, List<DataFileMeta> changelog) {
        this.before = new ArrayList<>(before);
        this.after = new ArrayList<>(after);
        this.changelog = new ArrayList<>(changelog);
    }

    public List<DataFileMeta> before() {
        return before;
    }

    public List<DataFileMeta> after() {
        return after;
    }

    public List<DataFileMeta> changelog() {
        return changelog;
    }

    public void setDeletionFile(@Nullable CompactDeletionFile deletionFile) {
        this.deletionFile = deletionFile;
    }

    @Nullable
    public CompactDeletionFile deletionFile() {
        return deletionFile;
    }

    public void merge(CompactResult that) {
        before.addAll(that.before);
        after.addAll(that.after);
        changelog.addAll(that.changelog);

        if (deletionFile != null || that.deletionFile != null) {
            throw new UnsupportedOperationException(
                    "There is a bug, deletionFile can't be set before merge.");
        }
    }
}
