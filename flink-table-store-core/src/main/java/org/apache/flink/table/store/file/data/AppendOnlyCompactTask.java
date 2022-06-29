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

package org.apache.flink.table.store.file.data;

import org.apache.flink.table.store.file.compact.CompactResult;
import org.apache.flink.table.store.file.compact.CompactRewriter;
import org.apache.flink.table.store.file.compact.CompactTask;
import org.apache.flink.table.store.file.compact.CompactUnit;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/** Task for append only compaction. */
public class AppendOnlyCompactTask extends CompactTask {

    private final CompactRewriter<DataFileMeta> rewriter;
    private final List<CompactUnit> units;

    public AppendOnlyCompactTask(CompactRewriter<DataFileMeta> rewriter, List<CompactUnit> units) {
        this.rewriter = rewriter;
        this.units = units;
    }

    protected CompactResult compact() throws Exception {
        List<List<DataFileMeta>> sections =
                units.stream().map(CompactUnit::files).collect(Collectors.toList());
        sections.forEach(this::collectBeforeStats);
        List<DataFileMeta> result = rewriter.rewrite(DataFileMeta.DUMMY_LEVEL, true, sections);
        collectAfterStats(result);
        return result(
                sections.stream().flatMap(Collection::stream).collect(Collectors.toList()), result);
    }
}
