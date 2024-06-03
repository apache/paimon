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

package org.apache.paimon.io;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** Files changed before and after compaction, with changelog produced during compaction. */
public class CompactIncrement {

    private final List<DataFileMeta> compactBefore;
    private final List<DataFileMeta> compactAfter;
    private final List<DataFileMeta> changelogFiles;

    public CompactIncrement(
            List<DataFileMeta> compactBefore,
            List<DataFileMeta> compactAfter,
            List<DataFileMeta> changelogFiles) {
        this.compactBefore = compactBefore;
        this.compactAfter = compactAfter;
        this.changelogFiles = changelogFiles;
    }

    public List<DataFileMeta> compactBefore() {
        return compactBefore;
    }

    public List<DataFileMeta> compactAfter() {
        return compactAfter;
    }

    public List<DataFileMeta> changelogFiles() {
        return changelogFiles;
    }

    public boolean isEmpty() {
        return compactBefore.isEmpty() && compactAfter.isEmpty() && changelogFiles.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CompactIncrement that = (CompactIncrement) o;
        return Objects.equals(compactBefore, that.compactBefore)
                && Objects.equals(compactAfter, that.compactAfter)
                && Objects.equals(changelogFiles, that.changelogFiles);
    }

    @Override
    public int hashCode() {
        return Objects.hash(compactBefore, compactAfter, changelogFiles);
    }

    @Override
    public String toString() {
        return String.format(
                "CompactIncrement {compactBefore = [\n%s\n], compactAfter = [\n%s\n], changelogFiles = [\n%s\n]}",
                compactBefore.stream()
                        .map(DataFileMeta::fileName)
                        .collect(Collectors.joining(",\n")),
                compactAfter.stream()
                        .map(DataFileMeta::fileName)
                        .collect(Collectors.joining(",\n")),
                changelogFiles.stream()
                        .map(DataFileMeta::fileName)
                        .collect(Collectors.joining(",\n")));
    }

    public static CompactIncrement emptyIncrement() {
        return new CompactIncrement(
                Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    }
}
