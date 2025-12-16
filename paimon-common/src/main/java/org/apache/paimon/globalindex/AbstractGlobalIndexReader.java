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
import org.apache.paimon.utils.Range;

import java.util.List;

/** Abstract global index reader to wrap most method. */
public abstract class AbstractGlobalIndexReader implements GlobalIndexReader {

    private final long rangeEnd;

    protected AbstractGlobalIndexReader(long rangeEnd) {
        this.rangeEnd = rangeEnd;
    }

    @Override
    public GlobalIndexResult visitIsNotNull(FieldRef fieldRef) {
        return GlobalIndexResult.fromRange(new Range(0, rangeEnd));
    }

    @Override
    public GlobalIndexResult visitIsNull(FieldRef fieldRef) {
        return GlobalIndexResult.fromRange(new Range(0, rangeEnd));
    }

    @Override
    public GlobalIndexResult visitStartsWith(FieldRef fieldRef, Object literal) {
        return GlobalIndexResult.fromRange(new Range(0, rangeEnd));
    }

    @Override
    public GlobalIndexResult visitEndsWith(FieldRef fieldRef, Object literal) {
        return GlobalIndexResult.fromRange(new Range(0, rangeEnd));
    }

    @Override
    public GlobalIndexResult visitContains(FieldRef fieldRef, Object literal) {
        return GlobalIndexResult.fromRange(new Range(0, rangeEnd));
    }

    @Override
    public GlobalIndexResult visitLike(FieldRef fieldRef, Object literal) {
        return GlobalIndexResult.fromRange(new Range(0, rangeEnd));
    }

    @Override
    public GlobalIndexResult visitLessThan(FieldRef fieldRef, Object literal) {
        return GlobalIndexResult.fromRange(new Range(0, rangeEnd));
    }

    @Override
    public GlobalIndexResult visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
        return GlobalIndexResult.fromRange(new Range(0, rangeEnd));
    }

    @Override
    public GlobalIndexResult visitNotEqual(FieldRef fieldRef, Object literal) {
        return GlobalIndexResult.fromRange(new Range(0, rangeEnd));
    }

    @Override
    public GlobalIndexResult visitLessOrEqual(FieldRef fieldRef, Object literal) {
        return GlobalIndexResult.fromRange(new Range(0, rangeEnd));
    }

    @Override
    public GlobalIndexResult visitEqual(FieldRef fieldRef, Object literal) {
        return GlobalIndexResult.fromRange(new Range(0, rangeEnd));
    }

    @Override
    public GlobalIndexResult visitGreaterThan(FieldRef fieldRef, Object literal) {
        return GlobalIndexResult.fromRange(new Range(0, rangeEnd));
    }

    @Override
    public GlobalIndexResult visitIn(FieldRef fieldRef, List<Object> literals) {
        return GlobalIndexResult.fromRange(new Range(0, rangeEnd));
    }

    @Override
    public GlobalIndexResult visitNotIn(FieldRef fieldRef, List<Object> literals) {
        return GlobalIndexResult.fromRange(new Range(0, rangeEnd));
    }
}
