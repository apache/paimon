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

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** IntInt pojo class. */
public class PositiveIntInt implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int i1;
    private final int i2;

    public PositiveIntInt(int i1, int i2) {
        checkArgument(i1 >= 0);
        checkArgument(i2 >= 0);
        this.i1 = i1;
        this.i2 = i2;
    }

    public int i1() {
        return i1;
    }

    public int i2() {
        return i2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PositiveIntInt intInt = (PositiveIntInt) o;
        return i1 == intInt.i1 && i2 == intInt.i2;
    }

    @Override
    public int hashCode() {
        return Objects.hash(i1, i2);
    }

    @Override
    public String toString() {
        return "IntInt{" + "i1=" + i1 + ", i2=" + i2 + '}';
    }
}
