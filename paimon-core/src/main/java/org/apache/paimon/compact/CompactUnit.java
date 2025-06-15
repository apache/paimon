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
import org.apache.paimon.mergetree.LevelSortedRun;

import java.util.ArrayList;
import java.util.List;

/** A files unit for compaction. */
public class CompactUnit {

    private final int outputLevel;
    private final List<DataFileMeta> files;
    private final boolean fileRewrite;

    public CompactUnit(int outputLevel, List<DataFileMeta> files, boolean fileRewrite) {
        this.outputLevel = outputLevel;
        this.files = files;
        this.fileRewrite = fileRewrite;
    }

    public int outputLevel() {
        return outputLevel;
    }

    public List<DataFileMeta> files() {
        return files;
    }

    public boolean fileRewrite() {
        return fileRewrite;
    }

    public static CompactUnit fromLevelRuns(int outputLevel, List<LevelSortedRun> runs) {
        List<DataFileMeta> files = new ArrayList<>();
        for (LevelSortedRun run : runs) {
            files.addAll(run.run().files());
        }
        return fromFiles(outputLevel, files, false);
    }

    public static CompactUnit fromFiles(
            int outputLevel, List<DataFileMeta> files, boolean fileRewrite) {
        return new CompactUnit(outputLevel, files, fileRewrite);
    }
}
