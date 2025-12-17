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

/**
 * A {@link GlobalIndexReader} that wraps another reader and applies an offset to all row IDs in the
 * results.
 */
public class OffsetGlobalIndexReader implements GlobalIndexReader {

    private final GlobalIndexReader wrapped;
    private final long offset;

    public OffsetGlobalIndexReader(GlobalIndexReader wrapped, long offset) {
        this.wrapped = wrapped;
        this.offset = offset;
    }

    @Override
    public GlobalIndexResult visitIsNotNull(FieldRef fieldRef) {
        return applyOffset(wrapped.visitIsNotNull(fieldRef));
    }

    @Override
    public GlobalIndexResult visitIsNull(FieldRef fieldRef) {
        return applyOffset(wrapped.visitIsNull(fieldRef));
    }

    @Override
    public GlobalIndexResult visitStartsWith(FieldRef fieldRef, Object literal) {
        return applyOffset(wrapped.visitStartsWith(fieldRef, literal));
    }

    @Override
    public GlobalIndexResult visitEndsWith(FieldRef fieldRef, Object literal) {
        return applyOffset(wrapped.visitEndsWith(fieldRef, literal));
    }

    @Override
    public GlobalIndexResult visitContains(FieldRef fieldRef, Object literal) {
        return applyOffset(wrapped.visitContains(fieldRef, literal));
    }

    @Override
    public GlobalIndexResult visitLike(FieldRef fieldRef, Object literal) {
        return applyOffset(wrapped.visitLike(fieldRef, literal));
    }

    @Override
    public GlobalIndexResult visitLessThan(FieldRef fieldRef, Object literal) {
        return applyOffset(wrapped.visitLessThan(fieldRef, literal));
    }

    @Override
    public GlobalIndexResult visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
        return applyOffset(wrapped.visitGreaterOrEqual(fieldRef, literal));
    }

    @Override
    public GlobalIndexResult visitNotEqual(FieldRef fieldRef, Object literal) {
        return applyOffset(wrapped.visitNotEqual(fieldRef, literal));
    }

    @Override
    public GlobalIndexResult visitLessOrEqual(FieldRef fieldRef, Object literal) {
        return applyOffset(wrapped.visitLessOrEqual(fieldRef, literal));
    }

    @Override
    public GlobalIndexResult visitEqual(FieldRef fieldRef, Object literal) {
        return applyOffset(wrapped.visitEqual(fieldRef, literal));
    }

    @Override
    public GlobalIndexResult visitGreaterThan(FieldRef fieldRef, Object literal) {
        return applyOffset(wrapped.visitGreaterThan(fieldRef, literal));
    }

    @Override
    public GlobalIndexResult visitIn(FieldRef fieldRef, List<Object> literals) {
        return applyOffset(wrapped.visitIn(fieldRef, literals));
    }

    @Override
    public GlobalIndexResult visitNotIn(FieldRef fieldRef, List<Object> literals) {
        return applyOffset(wrapped.visitNotIn(fieldRef, literals));
    }

    @Override
    public GlobalIndexResult visitVectorSearch(FieldRef fieldRef, VectorSearch vectorSearch) {
        return applyOffset(wrapped.visitVectorSearch(null, vectorSearch));
    }

    private GlobalIndexResult applyOffset(GlobalIndexResult result) {
        if (result == null) {
            throw new IllegalStateException("Wrapped reader should not return null");
        }
        return result.offset(offset);
    }

    @Override
    public void close() throws IOException {
        wrapped.close();
    }
}
