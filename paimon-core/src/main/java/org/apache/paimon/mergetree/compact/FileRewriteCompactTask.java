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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.compact.CompactTask;
import org.apache.paimon.compact.CompactUnit;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.operation.metrics.CompactionMetrics;

import javax.annotation.Nullable;

import java.util.List;

import static java.util.Collections.singletonList;

/** Compact task for file rewrite compaction. */
public class FileRewriteCompactTask extends CompactTask {

    private final CompactRewriter rewriter;
    private final int outputLevel;
    private final List<DataFileMeta> files;
    private final boolean dropDelete;

    public FileRewriteCompactTask(
            CompactRewriter rewriter,
            CompactUnit unit,
            boolean dropDelete,
            @Nullable CompactionMetrics.Reporter metricsReporter,
            @Nullable String type) {
        super(metricsReporter, type);
        this.rewriter = rewriter;
        this.outputLevel = unit.outputLevel();
        this.files = unit.files();
        this.dropDelete = dropDelete;
    }

    @Override
    protected CompactResult doCompact() throws Exception {
        CompactResult result = new CompactResult();
        for (DataFileMeta file : files) {
            rewriteFile(file, result);
        }
        return result;
    }

    private void rewriteFile(DataFileMeta file, CompactResult toUpdate) throws Exception {
        List<List<SortedRun>> candidate = singletonList(singletonList(SortedRun.fromSingle(file)));
        toUpdate.merge(rewriter.rewrite(outputLevel, dropDelete, candidate));
    }
}
