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

import org.apache.paimon.predicate.BatchVectorSearch;
import org.apache.paimon.predicate.FullTextSearch;
import org.apache.paimon.predicate.FunctionVisitor;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.ScalarSearch;
import org.apache.paimon.predicate.VectorSearch;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Index reader for global index, return {@link GlobalIndexResult}. */
public interface GlobalIndexReader
        extends FunctionVisitor<CompletableFuture<Optional<GlobalIndexResult>>>, Closeable {

    @Override
    default CompletableFuture<Optional<GlobalIndexResult>> visitAnd(
            List<CompletableFuture<Optional<GlobalIndexResult>>> children) {
        throw new UnsupportedOperationException();
    }

    @Override
    default CompletableFuture<Optional<GlobalIndexResult>> visitOr(
            List<CompletableFuture<Optional<GlobalIndexResult>>> children) {
        throw new UnsupportedOperationException();
    }

    @Override
    default CompletableFuture<Optional<GlobalIndexResult>> visitNonFieldLeaf(
            LeafPredicate predicate) {
        throw new UnsupportedOperationException();
    }

    default CompletableFuture<Optional<ScoredGlobalIndexResult>> visitVectorSearch(
            VectorSearch vectorSearch) {
        throw new UnsupportedOperationException();
    }

    default CompletableFuture<Optional<ScoredGlobalIndexResult>> visitFullTextSearch(
            FullTextSearch fullTextSearch) {
        throw new UnsupportedOperationException();
    }

    /** Returns a bounded candidate superset for scalar TopN, not an exact predicate result. */
    default CompletableFuture<Optional<GlobalIndexResult>> visitScalarSearch(
            ScalarSearch scalarSearch) {
        throw new UnsupportedOperationException();
    }

    /** Batch search; result {@code i} matches vector {@code i}. */
    default CompletableFuture<List<Optional<ScoredGlobalIndexResult>>> visitBatchVectorSearch(
            BatchVectorSearch batchVectorSearch) {
        List<CompletableFuture<Optional<ScoredGlobalIndexResult>>> futures = new ArrayList<>();
        for (int i = 0; i < batchVectorSearch.vectorCount(); i++) {
            futures.add(visitVectorSearch(batchVectorSearch.forIndex(i)));
        }
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .thenApply(
                        ignored -> {
                            List<Optional<ScoredGlobalIndexResult>> results =
                                    new ArrayList<>(futures.size());
                            for (CompletableFuture<Optional<ScoredGlobalIndexResult>> future :
                                    futures) {
                                results.add(future.join());
                            }
                            return results;
                        });
    }
}
