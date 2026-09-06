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

package org.apache.paimon.data;

import java.io.Serializable;

/**
 * Placeholder for a map blob field in data-evolution blob files. It should never be exposed to
 * users.
 */
public final class BlobMapPlaceholder implements InternalMap, Serializable {

    private static final long serialVersionUID = 1L;

    public static final BlobMapPlaceholder INSTANCE = new BlobMapPlaceholder();

    private BlobMapPlaceholder() {}

    private Object readResolve() {
        return INSTANCE;
    }

    private static UnsupportedOperationException unsupported() {
        return new UnsupportedOperationException(
                "Should never call this method for placeholder blob map.");
    }

    @Override
    public int size() {
        throw unsupported();
    }

    @Override
    public InternalArray keyArray() {
        throw unsupported();
    }

    @Override
    public InternalArray valueArray() {
        throw unsupported();
    }
}
