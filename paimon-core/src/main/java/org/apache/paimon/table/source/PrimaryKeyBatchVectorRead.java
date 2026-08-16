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

import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Executes snapshot-consistent primary-key vector search for multiple query vectors. */
public class PrimaryKeyBatchVectorRead implements BatchVectorRead, Serializable {

    private static final long serialVersionUID = 1L;

    private final float[][] queries;
    private final List<PrimaryKeyVectorRead> queryReads;

    public PrimaryKeyBatchVectorRead(
            FileStoreTable table,
            DataField vectorField,
            float[][] queries,
            int limit,
            Map<String, String> searchOptions,
            @Nullable Predicate filter) {
        checkArgument(queries != null && queries.length > 0, "Query vectors cannot be empty.");
        this.queries = new float[queries.length][];
        this.queryReads = new ArrayList<>(queries.length);
        for (int i = 0; i < queries.length; i++) {
            float[] query = checkNotNull(queries[i], "Query vector cannot be null.").clone();
            this.queries[i] = query;
            this.queryReads.add(
                    new PrimaryKeyVectorRead(
                            table, vectorField, query, limit, searchOptions, filter));
        }
    }

    @Override
    public List<GlobalIndexResult> readBatch(VectorScan.Plan plan) {
        PrimaryKeyVectorRead firstRead = queryReads.get(0);
        PrimaryKeyVectorScan.Plan primaryKeyPlan = firstRead.primaryKeyPlan(plan);
        List<PrimaryKeyVectorRead.SearchResult> searchResults =
                firstRead.searchBuckets(firstRead.bucketSplits(primaryKeyPlan), queries);
        checkArgument(
                searchResults.size() == queryReads.size(),
                "Primary-key vector batch result count does not match query count.");

        List<GlobalIndexResult> results = new ArrayList<>(queryReads.size());
        for (int i = 0; i < queryReads.size(); i++) {
            results.add(queryReads.get(i).createResult(primaryKeyPlan, searchResults.get(i)));
        }
        return Collections.unmodifiableList(results);
    }
}
