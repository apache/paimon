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

package org.apache.paimon.index.globalindex;

import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FunctionVisitor;
import org.apache.paimon.predicate.TransformPredicate;

import java.io.Closeable;
import java.util.List;

import static org.apache.paimon.index.globalindex.GlobalIndexResult.ALL;

/**
 * Read file index from serialized bytes. Return true, means we need to search this file, else means
 * needn't.
 */
public abstract class GlobalIndexLeafPredicator
        implements FunctionVisitor<GlobalIndexResult>, Closeable {

    @Override
    public GlobalIndexResult visitIsNotNull(FieldRef fieldRef) {
        return ALL;
    }

    @Override
    public GlobalIndexResult visitIsNull(FieldRef fieldRef) {
        return ALL;
    }

    @Override
    public GlobalIndexResult visitStartsWith(FieldRef fieldRef, Object literal) {
        return ALL;
    }

    @Override
    public GlobalIndexResult visitEndsWith(FieldRef fieldRef, Object literal) {
        return ALL;
    }

    @Override
    public GlobalIndexResult visitContains(FieldRef fieldRef, Object literal) {
        return ALL;
    }

    @Override
    public GlobalIndexResult visitLessThan(FieldRef fieldRef, Object literal) {
        return ALL;
    }

    @Override
    public GlobalIndexResult visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
        return ALL;
    }

    @Override
    public GlobalIndexResult visitNotEqual(FieldRef fieldRef, Object literal) {
        return ALL;
    }

    @Override
    public GlobalIndexResult visitLessOrEqual(FieldRef fieldRef, Object literal) {
        return ALL;
    }

    @Override
    public GlobalIndexResult visitEqual(FieldRef fieldRef, Object literal) {
        return ALL;
    }

    @Override
    public GlobalIndexResult visitGreaterThan(FieldRef fieldRef, Object literal) {
        return ALL;
    }

    @Override
    public GlobalIndexResult visitIn(FieldRef fieldRef, List<Object> literals) {
        GlobalIndexResult fileIndexResult = null;
        for (Object key : literals) {
            fileIndexResult =
                    fileIndexResult == null
                            ? visitEqual(fieldRef, key)
                            : fileIndexResult.or(visitEqual(fieldRef, key));
        }
        return fileIndexResult;
    }

    @Override
    public GlobalIndexResult visitNotIn(FieldRef fieldRef, List<Object> literals) {
        GlobalIndexResult fileIndexResult = null;
        for (Object key : literals) {
            fileIndexResult =
                    fileIndexResult == null
                            ? visitNotEqual(fieldRef, key)
                            : fileIndexResult.or(visitNotEqual(fieldRef, key));
        }
        return fileIndexResult;
    }

    @Override
    public GlobalIndexResult visitAnd(List<GlobalIndexResult> children) {
        throw new UnsupportedOperationException("Should not invoke this");
    }

    @Override
    public GlobalIndexResult visitOr(List<GlobalIndexResult> children) {
        throw new UnsupportedOperationException("Should not invoke this");
    }

    @Override
    public GlobalIndexResult visit(TransformPredicate predicate) {
        return ALL;
    }
}
