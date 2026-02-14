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

package org.apache.paimon.diskann.index;

import org.apache.paimon.globalindex.ScoreGetter;
import org.apache.paimon.globalindex.ScoredGlobalIndexResult;
import org.apache.paimon.utils.RoaringNavigableMap64;

import java.util.HashMap;

/** Vector search global index result for DiskANN vector index. */
public class DiskAnnScoredGlobalIndexResult implements ScoredGlobalIndexResult {

    private final HashMap<Long, Float> id2scores;
    private final RoaringNavigableMap64 results;

    public DiskAnnScoredGlobalIndexResult(
            RoaringNavigableMap64 results, HashMap<Long, Float> id2scores) {
        this.id2scores = id2scores;
        this.results = results;
    }

    @Override
    public ScoreGetter scoreGetter() {
        return id2scores::get;
    }

    @Override
    public RoaringNavigableMap64 results() {
        return this.results;
    }
}
