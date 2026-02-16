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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;

import java.util.Collections;
import java.util.List;

/** A {@link Transform} that always returns {@code true}. Used for constant predicates. */
public class TrueTransform implements Transform {

    private static final long serialVersionUID = 1L;

    public static final String NAME = "TRUE";

    public static final TrueTransform INSTANCE = new TrueTransform();

    @JsonCreator
    private TrueTransform() {}

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public List<Object> inputs() {
        return Collections.emptyList();
    }

    @Override
    public DataType outputType() {
        return DataTypes.BOOLEAN();
    }

    @Override
    public Object transform(InternalRow row) {
        return true;
    }

    @Override
    public Transform copyWithNewInputs(List<Object> inputs) {
        return INSTANCE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o != null && getClass() == o.getClass();
    }

    @Override
    public int hashCode() {
        return NAME.hashCode();
    }

    @Override
    public String toString() {
        return NAME;
    }
}
