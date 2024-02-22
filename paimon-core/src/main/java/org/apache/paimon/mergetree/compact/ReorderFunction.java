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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.KeyValue;

import java.util.List;

/**
 * Reorder function to reorder multiple {@link KeyValue}s.
 *
 * @param <T> result type
 */
public interface ReorderFunction<T> {

    /** Reset the reorder function to its default state. */
    void reset();

    /** Add the given {@link KeyValue} to the reorder function. */
    void add(KeyValue kv);

    /** Get current reorder values. */
    List<T> getResult();
}
