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

package org.apache.flink.table.store.connector.sink;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.store.table.sink.FileCommittable;

import java.util.List;

/** Writer metrics for flink table store. */
public class WriterMetrics {

    private final Counter newDataFiles;
    private final Counter changeLogFiles;
    private final Counter compactBeforeFiles;
    private final Counter compactAfterFiles;
    private final Counter compactChangelogFiles;

    public WriterMetrics(MetricGroup metrics, String tableName) {
        MetricGroup writerMetrics =
                metrics.addGroup("FlinkTableStoreWriter").addGroup("table", tableName);
        this.newDataFiles = writerMetrics.counter("newDataFiles");
        this.changeLogFiles = writerMetrics.counter("changeLogFiles");
        this.compactBeforeFiles = writerMetrics.counter("compactBeforeFiles");
        this.compactAfterFiles = writerMetrics.counter("compactAfterFiles");
        this.compactChangelogFiles = writerMetrics.counter("compactChangelogFiles");
    }

    public void updateCommitMetrics(List<FileCommittable> fileCommittableList) {
        newDataFiles.inc(
                fileCommittableList.stream()
                        .mapToInt(m -> m.newFilesIncrement().newFiles().size())
                        .sum());
        changeLogFiles.inc(
                fileCommittableList.stream()
                        .mapToInt(m -> m.newFilesIncrement().changelogFiles().size())
                        .sum());
        compactBeforeFiles.inc(
                fileCommittableList.stream()
                        .mapToInt(m -> m.compactIncrement().compactBefore().size())
                        .sum());
        compactAfterFiles.inc(
                fileCommittableList.stream()
                        .mapToInt(m -> m.compactIncrement().compactAfter().size())
                        .sum());
        compactChangelogFiles.inc(
                fileCommittableList.stream()
                        .mapToInt(m -> m.compactIncrement().changelogFiles().size())
                        .sum());
    }
}
