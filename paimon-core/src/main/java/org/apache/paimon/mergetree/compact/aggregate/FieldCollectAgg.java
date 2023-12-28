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

package org.apache.paimon.mergetree.compact.aggregate;

import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.types.ArrayType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

/** Collect elements into an ARRAY. */
public class FieldCollectAgg extends FieldAggregator {

    public static final String NAME = "collect";

    private final boolean distinct;
    private final InternalArray.ElementGetter elementGetter;

    public FieldCollectAgg(ArrayType dataType, boolean distinct) {
        super(dataType);
        this.distinct = distinct;
        this.elementGetter = InternalArray.createElementGetter(dataType.getElementType());
    }

    @Override
    String name() {
        return NAME;
    }

    @Override
    public Object agg(Object accumulator, Object inputField) {
        if (accumulator == null && inputField == null) {
            return null;
        }

        if ((accumulator == null || inputField == null) && !distinct) {
            return accumulator == null ? inputField : accumulator;
        }

        Collection<Object> collection = distinct ? new HashSet<>() : new ArrayList<>();

        collect(collection, accumulator);
        collect(collection, inputField);

        return new GenericArray(collection.toArray());
    }

    private void collect(Collection<Object> collection, @Nullable Object data) {
        if (data == null) {
            return;
        }

        InternalArray array = (InternalArray) data;
        for (int i = 0; i < array.size(); i++) {
            collection.add(elementGetter.getElementOrNull(array, i));
        }
    }
}
