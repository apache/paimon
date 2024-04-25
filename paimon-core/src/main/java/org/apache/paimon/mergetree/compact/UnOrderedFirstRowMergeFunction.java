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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.KeyValue;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

/**
 * This class is the same as FirstRowMergeFunction, but it is specifically for merging unordered
 * input.
 */
public class UnOrderedFirstRowMergeFunction extends FirstRowMergeFunction {
    protected UnOrderedFirstRowMergeFunction(RowType keyType, RowType valueType) {
        super(keyType, valueType);
    }

    public static MergeFunctionFactory<KeyValue> factory(RowType keyType, RowType valueType) {
        return new UnOrderedFirstRowMergeFunction.Factory(keyType, valueType);
    }

    private static class Factory implements MergeFunctionFactory<KeyValue> {

        private static final long serialVersionUID = 1L;
        private final RowType keyType;
        private final RowType valueType;

        public Factory(RowType keyType, RowType valueType) {
            this.keyType = keyType;
            this.valueType = valueType;
        }

        @Override
        public MergeFunction<KeyValue> create(@Nullable int[][] projection) {
            return new UnOrderedFirstRowMergeFunction(keyType, valueType);
        }
    }
}
