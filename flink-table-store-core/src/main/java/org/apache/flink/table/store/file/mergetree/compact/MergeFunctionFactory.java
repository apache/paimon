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

package org.apache.flink.table.store.file.mergetree.compact;

import javax.annotation.Nullable;

import java.io.Serializable;

/** Factory to create {@link MergeFunction}. */
@FunctionalInterface
public interface MergeFunctionFactory<T> extends Serializable {

    MergeFunction<T> create(@Nullable int[][] projection);

    static <T> MergeFunctionFactory<T> of(MergeFunction<T> mergeFunction) {
        return new InstanceFactory<>(mergeFunction);
    }

    /** A {@link MergeFunctionFactory} from a {@link MergeFunction} instance. */
    class InstanceFactory<T> implements MergeFunctionFactory<T> {

        private static final long serialVersionUID = 1L;

        private final MergeFunction<T> mergeFunction;

        public InstanceFactory(MergeFunction<T> mergeFunction) {
            this.mergeFunction = mergeFunction;
        }

        @Override
        public MergeFunction<T> create(@Nullable int[][] projection) {
            return mergeFunction;
        }
    }
}
