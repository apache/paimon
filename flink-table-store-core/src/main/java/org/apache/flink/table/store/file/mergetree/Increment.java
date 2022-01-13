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

package org.apache.flink.table.store.file.mergetree;

import org.apache.flink.table.store.file.mergetree.sst.SstFileMeta;

import java.util.Collections;
import java.util.List;

/**
 * Incremental files for merge tree. It consists of two parts:
 *
 * <ul>
 *   <li>New files: The new files generated in this snapshot cycle. They must be committed.
 *   <li>Compact files: The {@link #compactBefore} files are compacted to {@link #compactAfter}
 *       files in this snapshot cycle. The compaction is an optimization of files.
 * </ul>
 */
public class Increment {

    private final List<SstFileMeta> newFiles;

    private final List<SstFileMeta> compactBefore;

    private final List<SstFileMeta> compactAfter;

    public Increment(
            List<SstFileMeta> newFiles,
            List<SstFileMeta> beCompacted,
            List<SstFileMeta> compacted) {
        this.newFiles = Collections.unmodifiableList(newFiles);
        this.compactBefore = Collections.unmodifiableList(beCompacted);
        this.compactAfter = Collections.unmodifiableList(compacted);
    }

    public List<SstFileMeta> newFiles() {
        return newFiles;
    }

    public List<SstFileMeta> compactBefore() {
        return compactBefore;
    }

    public List<SstFileMeta> compactAfter() {
        return compactAfter;
    }
}
