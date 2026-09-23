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

package org.apache.paimon.data.shredding;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/** File-local field name to field id dictionary for one shared-shredding MAP column. */
public class MapSharedShreddingFieldDict {

    private final Map<String, Integer> nameToId = new TreeMap<>();
    private int nextId = 0;

    public int getOrAssign(String name) {
        Integer id = nameToId.get(name);
        if (id != null) {
            return id;
        }
        int newId = nextId++;
        nameToId.put(name, newId);
        return newId;
    }

    public Map<String, Integer> nameToId() {
        return Collections.unmodifiableMap(nameToId);
    }

    public int size() {
        return nextId;
    }
}
