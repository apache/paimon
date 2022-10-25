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

package org.apache.flink.table.store.file.compact;

import org.apache.flink.table.store.file.io.DataFileMeta;

import java.util.ArrayList;
import java.util.List;

/** Result of compaction. */
public class CompactResult {

    private final List<DataFileMeta> before;
    private final List<DataFileMeta> after;
    private final List<DataFileMeta> changelog;

    public CompactResult() {
        this.before = new ArrayList<>();
        this.after = new ArrayList<>();
        this.changelog = new ArrayList<>();
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

    public void addBefore(DataFileMeta file) {
        this.before.add(file);
    }

    public void addBefore(List<DataFileMeta> before) {
        this.before.addAll(before);
    }

    public void addAfter(DataFileMeta file) {
        this.after.add(file);
    }

    public void addAfter(List<DataFileMeta> after) {
        this.after.addAll(after);
    }

    public void addChangelog(List<DataFileMeta> changelog) {
        this.changelog.addAll(changelog);
    }
}
