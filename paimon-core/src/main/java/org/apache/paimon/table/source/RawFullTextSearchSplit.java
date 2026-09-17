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

import org.apache.paimon.utils.Range;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Split to scan raw rows for full-text search. */
public class RawFullTextSearchSplit extends FullTextSearchSplit {

    private static final long serialVersionUID = 1L;

    private final List<Range> rowRanges;

    public RawFullTextSearchSplit(List<Range> rowRanges) {
        this.rowRanges = Collections.unmodifiableList(new ArrayList<>(rowRanges));
    }

    public List<Range> rowRanges() {
        return rowRanges;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RawFullTextSearchSplit that = (RawFullTextSearchSplit) o;
        return Objects.equals(rowRanges, that.rowRanges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowRanges);
    }

    @Override
    public String toString() {
        return "RawFullTextSearchSplit{" + "rowRanges=" + rowRanges + '}';
    }
}
