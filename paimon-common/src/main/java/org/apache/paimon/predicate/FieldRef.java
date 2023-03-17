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

package org.apache.paimon.predicate;

import org.apache.paimon.types.DataType;

import java.io.Serializable;
import java.util.Objects;

/** A reference to a field in an input. */
public class FieldRef implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int index;
    private final String name;
    private final DataType type;

    public FieldRef(int index, String name, DataType type) {
        this.index = index;
        this.name = name;
        this.type = type;
    }

    public int index() {
        return index;
    }

    public String name() {
        return name;
    }

    public DataType type() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FieldRef fieldRef = (FieldRef) o;
        return index == fieldRef.index
                && Objects.equals(name, fieldRef.name)
                && Objects.equals(type, fieldRef.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, name, type);
    }

    @Override
    public String toString() {
        return "FieldRef{" + "index=" + index + ", name='" + name + '\'' + ", type=" + type + '}';
    }
}
