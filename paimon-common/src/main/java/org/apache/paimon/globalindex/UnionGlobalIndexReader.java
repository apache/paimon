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
    public GlobalIndexResult visitIsNotNull(FieldRef fieldRef) {
        return union(reader -> reader.visitIsNotNull(fieldRef));
    }

    @Override
    public GlobalIndexResult visitIsNull(FieldRef fieldRef) {
        return union(reader -> reader.visitIsNull(fieldRef));
    }

    @Override
    public GlobalIndexResult visitStartsWith(FieldRef fieldRef, Object literal) {
        return union(reader -> reader.visitStartsWith(fieldRef, literal));
    }

    @Override
    public GlobalIndexResult visitEndsWith(FieldRef fieldRef, Object literal) {
        return union(reader -> reader.visitEndsWith(fieldRef, literal));
    }

    @Override
    public GlobalIndexResult visitContains(FieldRef fieldRef, Object literal) {
        return union(reader -> reader.visitContains(fieldRef, literal));
    }

    @Override
    public GlobalIndexResult visitLike(FieldRef fieldRef, Object literal) {
        return union(reader -> reader.visitLike(fieldRef, literal));
    }

    @Override
    public GlobalIndexResult visitLessThan(FieldRef fieldRef, Object literal) {
        return union(reader -> reader.visitLessThan(fieldRef, literal));
    }

    @Override
    public GlobalIndexResult visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
        return union(reader -> reader.visitGreaterOrEqual(fieldRef, literal));
    }

    @Override
    public GlobalIndexResult visitNotEqual(FieldRef fieldRef, Object literal) {
        return union(reader -> reader.visitNotEqual(fieldRef, literal));
    }

    @Override
    public GlobalIndexResult visitLessOrEqual(FieldRef fieldRef, Object literal) {
        return union(reader -> reader.visitLessOrEqual(fieldRef, literal));
    }

    @Override
    public GlobalIndexResult visitEqual(FieldRef fieldRef, Object literal) {
        return union(reader -> reader.visitEqual(fieldRef, literal));
    }

    @Override
    public GlobalIndexResult visitGreaterThan(FieldRef fieldRef, Object literal) {
        return union(reader -> reader.visitGreaterThan(fieldRef, literal));
    }

    @Override
    public GlobalIndexResult visitIn(FieldRef fieldRef, List<Object> literals) {
        return union(reader -> reader.visitIn(fieldRef, literals));
    }

    @Override
    public GlobalIndexResult visitNotIn(FieldRef fieldRef, List<Object> literals) {
        return union(reader -> reader.visitNotIn(fieldRef, literals));
    }

    @Override
    public GlobalIndexResult visitVectorSearch(FieldRef fieldRef, VectorSearch vectorSearch) {
        return union(reader -> reader.visitVectorSearch(null, vectorSearch));
    }

    private GlobalIndexResult union(Function<GlobalIndexReader, GlobalIndexResult> visitor) {
        GlobalIndexResult result = null;
        for (GlobalIndexReader reader : readers) {
            GlobalIndexResult current = visitor.apply(reader);
            if (current == null) {
                throw new IllegalStateException("Reader should not return null");
            }
            result = result == null ? current : result.or(current);
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
