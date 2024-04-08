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

package org.apache.paimon.fileindex;

import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.FunctionVisitor;

import java.util.List;

/**
 * Read file index from serialized bytes. Return true, means we need to search this file, else means
 * needn't.
 */
public interface FileIndexReader extends FunctionVisitor<Boolean> {

    @Override
    default Boolean visitIsNotNull(FieldRef fieldRef) {
        return true;
    }

    @Override
    default Boolean visitIsNull(FieldRef fieldRef) {
        return true;
    }

    @Override
    default Boolean visitStartsWith(FieldRef fieldRef, Object literal) {
        return true;
    }

    @Override
    default Boolean visitLessThan(FieldRef fieldRef, Object literal) {
        return true;
    }

    @Override
    default Boolean visitGreaterOrEqual(FieldRef fieldRef, Object literal) {
        return true;
    }

    @Override
    default Boolean visitNotEqual(FieldRef fieldRef, Object literal) {
        return true;
    }

    @Override
    default Boolean visitLessOrEqual(FieldRef fieldRef, Object literal) {
        return true;
    }

    @Override
    default Boolean visitEqual(FieldRef fieldRef, Object literal) {
        return true;
    }

    @Override
    default Boolean visitGreaterThan(FieldRef fieldRef, Object literal) {
        return true;
    }

    @Override
    default Boolean visitIn(FieldRef fieldRef, List<Object> literals) {
        for (Object key : literals) {
            if (visitEqual(fieldRef, key)) {
                return true;
            }
        }
        return false;
    }

    @Override
    default Boolean visitNotIn(FieldRef fieldRef, List<Object> literals) {
        for (Object key : literals) {
            if (visitNotEqual(fieldRef, key)) {
                return true;
            }
        }
        return false;
    }

    @Override
    default Boolean visitAnd(List<Boolean> children) {
        throw new UnsupportedOperationException("Should not invoke this");
    }

    @Override
    default Boolean visitOr(List<Boolean> children) {
        throw new UnsupportedOperationException("Should not invoke this");
    }
}
