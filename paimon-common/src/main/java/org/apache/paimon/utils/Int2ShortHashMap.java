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

package org.apache.paimon.utils;

import it.unimi.dsi.fastutil.ints.Int2ShortOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.shorts.ShortArrayList;

/** Int to short hash map. */
public class Int2ShortHashMap {

    private final Int2ShortOpenHashMap map;

    public Int2ShortHashMap() {
        this.map = new Int2ShortOpenHashMap();
    }

    public Int2ShortHashMap(int capacity) {
        this.map = new Int2ShortOpenHashMap(capacity);
    }

    public void put(int key, short value) {
        map.put(key, value);
    }

    public boolean containsKey(int key) {
        return map.containsKey(key);
    }

    public short get(int key) {
        return map.get(key);
    }

    public int size() {
        return map.size();
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder of {@link Int2ShortHashMap}. */
    public static class Builder {

        private final IntArrayList keyList = new IntArrayList();
        private final ShortArrayList valueList = new ShortArrayList();

        public void put(int key, short value) {
            keyList.add(key);
            valueList.add(value);
        }

        public Int2ShortHashMap build() {
            Int2ShortHashMap map;
            try {
                map = new Int2ShortHashMap(keyList.size());
                for (int i = 0; i < keyList.size(); i++) {
                    map.put(keyList.getInt(i), valueList.getShort(i));
                }
            } catch (IllegalArgumentException e) {
                throw new RuntimeException(
                        "capacity of Int2ShortOpenHashMap is too large, advise raise your parallelism in your Flink/Spark job",
                        e);
            }
            return map;
        }
    }
}
