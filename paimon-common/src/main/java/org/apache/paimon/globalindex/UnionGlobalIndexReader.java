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
import org.apache.paimon.predicate.VectorSearch;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * A {@link GlobalIndexReader} that combines results from multiple readers by performing a union
 * (OR) operation on their results.
 */
public class UnionGlobalIndexReader implements GlobalIndexReader {

    private final List<GlobalIndexReader> readers;

    public UnionGlobalIndexReader(List<GlobalIndexReader> readers) {
        this.readers = readers;
    }

    @Override
    public Optional<GlobalIndexResult> visitIsNotNull(FieldRef fieldRef) {
        return union(reader -> reader.visitIsNotNull(fieldRef));
    }

    @Override
    public Optional<GlobalIndexResult> visitIsNull(FieldRef fieldRef) {
        return union(reader -> reader.visitIsNull(fieldRef));
    }

    @Override
    public Optional<GlobalIndexResult> visitStartsWith(FieldRef fieldRef, Object literal) {
        return union(reader -> reader.visitStartsWith(fieldRef, literal));
    }

    @Override
    public Optional<GlobalIndexResult> visitEndsWith(FieldRef fieldRef, Object literal) {
        return union(reader -> reader.visitEndsWith(fieldRef, literal));
    }

    @Override
    public Optional<GlobalIndexResult> visitContains(FieldRef fieldRef, Object literal) {
        return union(reader -> reader.visitContains(fieldRef, literal));
    }

    @Override
    public Optional<GlobalIndexResult> visitLike(FieldRef fieldRef, Object literal) {
        return union(reader -> reader.visitLike(fieldRef, literal));
    }

    @Override
    public Optional<GlobalIndexResult> visitLessThan(FieldRef fieldRef, Object literal) {
        return union(reader -> reader.visitLessThan(fieldRef, literal));
    }

    @Override
    public Optional<GlobalIndexResult> visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
        return union(reader -> reader.visitGreaterOrEqual(fieldRef, literal));
    }

    @Override
    public Optional<GlobalIndexResult> visitNotEqual(FieldRef fieldRef, Object literal) {
        return union(reader -> reader.visitNotEqual(fieldRef, literal));
    }

    @Override
    public Optional<GlobalIndexResult> visitLessOrEqual(FieldRef fieldRef, Object literal) {
        return union(reader -> reader.visitLessOrEqual(fieldRef, literal));
    }

    @Override
    public Optional<GlobalIndexResult> visitEqual(FieldRef fieldRef, Object literal) {
        return union(reader -> reader.visitEqual(fieldRef, literal));
    }

    @Override
    public Optional<GlobalIndexResult> visitGreaterThan(FieldRef fieldRef, Object literal) {
        return union(reader -> reader.visitGreaterThan(fieldRef, literal));
    }

    @Override
    public Optional<GlobalIndexResult> visitIn(FieldRef fieldRef, List<Object> literals) {
        return union(reader -> reader.visitIn(fieldRef, literals));
    }

    @Override
    public Optional<GlobalIndexResult> visitNotIn(FieldRef fieldRef, List<Object> literals) {
        return union(reader -> reader.visitNotIn(fieldRef, literals));
    }

    @Override
    public Optional<GlobalIndexResult> visitVectorSearch(VectorSearch vectorSearch) {
        return union(reader -> reader.visitVectorSearch(vectorSearch));
    }

    private Optional<GlobalIndexResult> union(
            Function<GlobalIndexReader, Optional<GlobalIndexResult>> visitor) {
        Optional<GlobalIndexResult> result = Optional.empty();
        for (GlobalIndexReader reader : readers) {
            Optional<GlobalIndexResult> current = visitor.apply(reader);
            if (!current.isPresent()) {
                continue;
            }
            if (!result.isPresent()) {
                result = current;
            }
            result = Optional.of(result.get().or(current.get()));
        }
        return result;
    }

    @Override
    public void close() throws IOException {
        for (GlobalIndexReader reader : readers) {
            reader.close();
        }
    }
}
