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

import java.io.Serializable;
import java.util.Objects;

/** Container that accommodates three fields. */
public final class Triple<T0, T1, T2> implements Serializable {

    private static final long serialVersionUID = 1L;

    public final T0 f0;
    public final T1 f1;
    public final T2 f2;

    public static <T0, T1, T2> Triple<T0, T1, T2> of(T0 f0, T1 f1, T2 f2) {
        return new Triple<>(f0, f1, f2);
    }

    private Triple(final T0 f0, final T1 f1, final T2 f2) {
        this.f0 = f0;
        this.f1 = f1;
        this.f2 = f2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Triple<?, ?, ?> triple = (Triple<?, ?, ?>) o;
        return Objects.equals(f0, triple.f0)
                && Objects.equals(f1, triple.f1)
                && Objects.equals(f2, triple.f2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(f0, f1, f2);
    }

    @Override
    public String toString() {
        return "(" + f0 + ',' + f1 + ',' + f2 + ')';
    }
}
