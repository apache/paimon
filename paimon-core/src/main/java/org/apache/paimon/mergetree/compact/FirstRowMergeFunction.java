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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

/**
 * A {@link MergeFunction} where key is primary key (unique) and value is the full record, only keep
 * the first one.
 */
public class FirstRowMergeFunction implements MergeFunction<KeyValue> {

    private final InternalRowSerializer keySerializer;
    private final InternalRowSerializer valueSerializer;
    private KeyValue first;
    public boolean containsHighLevel;
    private final boolean ignoreDelete;

    protected FirstRowMergeFunction(RowType keyType, RowType valueType, boolean ignoreDelete) {
        this.keySerializer = new InternalRowSerializer(keyType);
        this.valueSerializer = new InternalRowSerializer(valueType);
        this.ignoreDelete = ignoreDelete;
    }

    @Override
    public void reset() {
        this.first = null;
        this.containsHighLevel = false;
    }

    @Override
    public void add(KeyValue kv) {
        if (kv.valueKind().isRetract()) {
            if (ignoreDelete) {
                return;
            } else {
                throw new IllegalArgumentException(
                        "By default, First row merge engine can not accept DELETE/UPDATE_BEFORE records.\n"
                                + "You can config 'first-row.ignore-delete' to ignore the DELETE/UPDATE_BEFORE records.");
            }
        }

        if (first == null) {
            this.first = kv.copy(keySerializer, valueSerializer);
        }
        if (kv.level() > 0) {
            containsHighLevel = true;
        }
    }

    @Override
    public KeyValue getResult() {
        return first;
    }

    public static MergeFunctionFactory<KeyValue> factory(
            Options options, RowType keyType, RowType valueType) {
        return new FirstRowMergeFunction.Factory(
                keyType, valueType, options.get(CoreOptions.IGNORE_DELETE));
    }

    private static class Factory implements MergeFunctionFactory<KeyValue> {

        private static final long serialVersionUID = 1L;
        private final RowType keyType;
        private final RowType valueType;
        private final boolean ignoreDelete;

        public Factory(RowType keyType, RowType valueType, boolean ignoreDelete) {
            this.keyType = keyType;
            this.valueType = valueType;
            this.ignoreDelete = ignoreDelete;
        }

        @Override
        public MergeFunction<KeyValue> create(@Nullable int[][] projection) {
            return new FirstRowMergeFunction(keyType, valueType, ignoreDelete);
        }
    }
}
