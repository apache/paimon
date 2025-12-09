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

package org.apache.paimon.globalindex;

import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.Range;

import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;

import java.util.List;

public class IndexedSplit implements Split {
    private final DataSplit split;
    @NotNull private final List<Range> rowRanges;
    @Nullable private final Float[] scores;

    public IndexedSplit(DataSplit split, @NotNull List<Range> rowRanges, @Nullable Float[] scores) {
        this.split = split;
        this.rowRanges = rowRanges;
        this.scores = scores;
    }

    public DataSplit dataSplit() {
        return split;
    }

    public List<Range> rowRanges() {
        return rowRanges;
    }

    @Nullable
    public Float[] scores() {
        return scores;
    }

    @Override
    public long rowCount() {
        return rowRanges.stream().mapToLong(r -> r.to - r.from + 1).sum();
    }
}
