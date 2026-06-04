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

import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FullTextSearch;
import org.apache.paimon.predicate.VectorSearch;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * A {@link GlobalIndexReader} that wraps another reader and applies an offset to all row IDs in the
 * results.
 */
public class OffsetGlobalIndexReader implements GlobalIndexReader {

    private final GlobalIndexReader wrapped;
    private final long offset;
    private final long to;

    public OffsetGlobalIndexReader(GlobalIndexReader wrapped, long offset, long to) {
        this.wrapped = wrapped;
        this.offset = offset;
        this.to = to;
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitIsNotNull(FieldRef fieldRef) {
        return wrapped.visitIsNotNull(fieldRef).thenApply(this::applyOffset);
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitIsNull(FieldRef fieldRef) {
        return wrapped.visitIsNull(fieldRef).thenApply(this::applyOffset);
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitStartsWith(
            FieldRef fieldRef, Object literal) {
        return wrapped.visitStartsWith(fieldRef, literal).thenApply(this::applyOffset);
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitEndsWith(
            FieldRef fieldRef, Object literal) {
        return wrapped.visitEndsWith(fieldRef, literal).thenApply(this::applyOffset);
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitContains(
            FieldRef fieldRef, Object literal) {
        return wrapped.visitContains(fieldRef, literal).thenApply(this::applyOffset);
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitLike(
            FieldRef fieldRef, Object literal) {
        return wrapped.visitLike(fieldRef, literal).thenApply(this::applyOffset);
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitLessThan(
            FieldRef fieldRef, Object literal) {
        return wrapped.visitLessThan(fieldRef, literal).thenApply(this::applyOffset);
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitGreaterOrEqual(
            FieldRef fieldRef, Object literal) {
        return wrapped.visitGreaterOrEqual(fieldRef, literal).thenApply(this::applyOffset);
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitNotEqual(
            FieldRef fieldRef, Object literal) {
        return wrapped.visitNotEqual(fieldRef, literal).thenApply(this::applyOffset);
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitLessOrEqual(
            FieldRef fieldRef, Object literal) {
        return wrapped.visitLessOrEqual(fieldRef, literal).thenApply(this::applyOffset);
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitEqual(
            FieldRef fieldRef, Object literal) {
        return wrapped.visitEqual(fieldRef, literal).thenApply(this::applyOffset);
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitGreaterThan(
            FieldRef fieldRef, Object literal) {
        return wrapped.visitGreaterThan(fieldRef, literal).thenApply(this::applyOffset);
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitIn(
            FieldRef fieldRef, List<Object> literals) {
        return wrapped.visitIn(fieldRef, literals).thenApply(this::applyOffset);
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitNotIn(
            FieldRef fieldRef, List<Object> literals) {
        return wrapped.visitNotIn(fieldRef, literals).thenApply(this::applyOffset);
    }

    @Override
    public CompletableFuture<Optional<GlobalIndexResult>> visitBetween(
            FieldRef fieldRef, Object from, Object to) {
        return wrapped.visitBetween(fieldRef, from, to).thenApply(this::applyOffset);
    }

    @Override
    public CompletableFuture<Optional<ScoredGlobalIndexResult>> visitVectorSearch(
            VectorSearch vectorSearch) {
        return wrapped.visitVectorSearch(vectorSearch.offsetRange(this.offset, this.to))
                .thenApply(opt -> opt.map(r -> r.offset(offset)));
    }

    @Override
    public CompletableFuture<Optional<ScoredGlobalIndexResult>> visitFullTextSearch(
            FullTextSearch fullTextSearch) {
        return wrapped.visitFullTextSearch(fullTextSearch)
                .thenApply(opt -> opt.map(r -> r.offset(offset)));
    }

    private Optional<GlobalIndexResult> applyOffset(Optional<GlobalIndexResult> result) {
        return result.map(r -> r.offset(offset));
    }

    @Override
    public void close() throws IOException {
        wrapped.close();
    }
}
