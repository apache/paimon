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

package org.apache.paimon.casting;

import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;

/**
 * An implementation of {@link InternalMap} which provides a casted view of the underlying {@link
 * InternalMap}.
 *
 * <p>It reads data from underlying {@link InternalMap} according to source logical type and casts
 * it with specific {@link CastExecutor}.
 */
public class CastedMap implements InternalMap {

    private final CastedArray castedValueArray;
    private InternalMap map;

    protected CastedMap(CastElementGetter castValueGetter) {
        this.castedValueArray = CastedArray.from(castValueGetter);
    }

    /**
     * Replaces the underlying {@link InternalMap} backing this {@link CastedMap}.
     *
     * <p>This method replaces the map in place and does not return a new object. This is done for
     * performance reasons.
     */
    public static CastedMap from(CastElementGetter castValueGetter) {
        return new CastedMap(castValueGetter);
    }

    public CastedMap replaceMap(InternalMap map) {
        this.castedValueArray.replaceArray(map.valueArray());
        this.map = map;
        return this;
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public InternalArray keyArray() {
        return map.keyArray();
    }

    @Override
    public InternalArray valueArray() {
        return castedValueArray;
    }
}
