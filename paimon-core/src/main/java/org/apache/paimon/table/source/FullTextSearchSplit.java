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

package org.apache.paimon.table.source;

import org.apache.paimon.index.IndexFileMeta;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/** Split of full-text search. */
public class FullTextSearchSplit implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long rowRangeStart;
    private final long rowRangeEnd;
    private final List<IndexFileMeta> fullTextIndexFiles;

    public FullTextSearchSplit(
            long rowRangeStart, long rowRangeEnd, List<IndexFileMeta> fullTextIndexFiles) {
        this.rowRangeStart = rowRangeStart;
        this.rowRangeEnd = rowRangeEnd;
        this.fullTextIndexFiles = fullTextIndexFiles;
    }

    public long rowRangeStart() {
        return rowRangeStart;
    }

    public long rowRangeEnd() {
        return rowRangeEnd;
    }

    public List<IndexFileMeta> fullTextIndexFiles() {
        return fullTextIndexFiles;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FullTextSearchSplit that = (FullTextSearchSplit) o;
        return rowRangeStart == that.rowRangeStart
                && rowRangeEnd == that.rowRangeEnd
                && Objects.equals(fullTextIndexFiles, that.fullTextIndexFiles);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowRangeStart, rowRangeEnd, fullTextIndexFiles);
    }

    @Override
    public String toString() {
        return "FullTextSearchSplit{"
                + "rowRangeStart="
                + rowRangeStart
                + ", rowRangeEnd="
                + rowRangeEnd
                + ", fullTextIndexFiles="
                + fullTextIndexFiles
                + '}';
    }
}
