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

import java.util.function.Supplier;

/** A class to lazy initialized field. Thread-safe via double-checked locking. */
public class LazyField<T> {

    private volatile boolean initialized;
    private Supplier<T> supplier;
    private T value;

    public LazyField(Supplier<T> supplier) {
        this.supplier = supplier;
    }

    public T get() {
        if (initialized) {
            return value;
        }
        synchronized (this) {
            if (!initialized) {
                value = supplier.get();
                initialized = true;
                supplier = null;
            }
            return value;
        }
    }

    public boolean initialized() {
        return initialized;
    }
}
