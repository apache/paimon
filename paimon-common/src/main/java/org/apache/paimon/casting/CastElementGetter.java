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

/** Get element from array and cast it according to specific {@link CastExecutor}. */
public class CastElementGetter {

    private final InternalArray.ElementGetter elementGetter;
    private final CastExecutor<Object, Object> castExecutor;

    @SuppressWarnings("unchecked")
    public CastElementGetter(
            InternalArray.ElementGetter elementGetter, CastExecutor<?, ?> castExecutor) {
        this.elementGetter = elementGetter;
        this.castExecutor = (CastExecutor<Object, Object>) castExecutor;
    }

    @SuppressWarnings("unchecked")
    public <V> V getElementOrNull(InternalArray array, int pos) {
        Object value = elementGetter.getElementOrNull(array, pos);
        return value == null ? null : (V) castExecutor.cast(value);
    }
}
