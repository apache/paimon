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

import org.apache.paimon.types.DataType;

/**
 * Secondary index filter interface. Return true, means we need to search this file, else means
 * needn't.
 */
public interface FilterInterface {

    void add(Object key);

    default boolean testStartsWith(Object key) {
        return true;
    }

    default boolean testLessThan(Object key) {
        return true;
    }

    default boolean testGreaterOrEqual(Object key) {
        return true;
    }

    default boolean testNotContains(Object key) {
        return true;
    }

    default boolean testLessOrEqual(Object key) {
        return true;
    }

    default boolean testContains(Object key) {
        return true;
    }

    default boolean testGreaterThan(Object key) {
        return true;
    }

    default boolean testIn(Object[] keys) {
        for (Object key : keys) {
            if (testContains(key)) {
                return true;
            }
        }
        return false;
    }

    default boolean testNotIn(Object[] keys) {
        for (Object key : keys) {
            if (testNotContains(key)) {
                return true;
            }
        }
        return false;
    }

    byte[] serializedBytes();

    FilterInterface recoverFrom(byte[] bytes);

    static FilterInterface getFilter(String type, DataType dataType) {
        switch (type) {
            default:
                throw new RuntimeException("Doesn't support filter type: " + type);
        }
    }
}
