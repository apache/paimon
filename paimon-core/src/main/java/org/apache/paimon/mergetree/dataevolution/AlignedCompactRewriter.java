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

package org.apache.paimon.mergetree.dataevolution;

import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.mergetree.SortedRun;
import org.apache.paimon.mergetree.compact.CompactRewriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Compaction rewriter of a bucket holding column files. A base file is always rewritten together
 * with its column files: they are removed with the base file, and the rewritten rows carry the
 * stitched columns. A base file with column files is never upgraded, since an upgrade would keep
 * its alignment range while the merge tree moves it to another level, leaving the column files
 * behind.
 *
 * <p>The reader factory of the wrapped rewriter must stitch column files into their base files.
 */
public class AlignedCompactRewriter implements CompactRewriter {

    private final CompactRewriter rewriter;
    private final ColumnFileGroups groups;

    public AlignedCompactRewriter(CompactRewriter rewriter, ColumnFileGroups groups) {
        this.rewriter = rewriter;
        this.groups = groups;
    }

    @Override
    public CompactResult rewrite(
            int outputLevel, boolean dropDelete, List<List<SortedRun>> sections) throws Exception {
        return withColumnFiles(rewriter.rewrite(outputLevel, dropDelete, sections));
    }

    @Override
    public CompactResult upgrade(int outputLevel, DataFileMeta file) throws Exception {
        if (groups.columnFilesOf(file).isEmpty()) {
            return rewriter.upgrade(outputLevel, file);
        }
        // keep delete records, the target level may not be the last one
        return rewrite(
                outputLevel,
                false,
                Collections.singletonList(Collections.singletonList(SortedRun.fromSingle(file))));
    }

    private CompactResult withColumnFiles(CompactResult result) {
        // groups must not be changed here: the result may still be discarded, for example when
        // the compaction is cancelled, and the next compaction needs the column files again
        List<DataFileMeta> columnFiles = new ArrayList<>();
        for (DataFileMeta before : result.before()) {
            columnFiles.addAll(groups.columnFilesOf(before));
        }
        result.before().addAll(columnFiles);
        return result;
    }

    @Override
    public void close() throws IOException {
        rewriter.close();
    }
}
