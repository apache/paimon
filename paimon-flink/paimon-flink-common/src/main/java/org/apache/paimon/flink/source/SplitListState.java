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

package org.apache.paimon.flink.source;

import org.apache.paimon.utils.Preconditions;

import org.apache.flink.api.common.state.ListState;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Utility class to provide {@link ListState}-like experience for sources that use {@link
 * SimpleSourceSplit}.
 */
public class SplitListState<T> implements ListState<T> {
    private final String splitPrefix;
    private final List<T> values;
    private final Function<T, String> serializer;
    private final Function<String, T> deserializer;

    public SplitListState(
            String identifier, Function<T, String> serializer, Function<String, T> deserializer) {
        Preconditions.checkArgument(
                !Character.isDigit(identifier.charAt(0)),
                String.format("Identifier %s should not start with digits.", identifier));
        this.splitPrefix = identifier.length() + identifier;
        this.serializer = serializer;
        this.deserializer = deserializer;
        this.values = new ArrayList<>();
    }

    @Override
    public void add(T value) {
        values.add(value);
    }

    @Override
    public List<T> get() {
        return new ArrayList<>(values);
    }

    @Override
    public void update(List<T> values) {
        this.values.clear();
        this.values.addAll(values);
    }

    @Override
    public void addAll(List<T> values) throws Exception {
        this.values.addAll(values);
    }

    @Override
    public void clear() {
        values.clear();
    }

    public List<SimpleSourceSplit> snapshotState() {
        return values.stream()
                .map(x -> new SimpleSourceSplit(splitPrefix + serializer.apply(x)))
                .collect(Collectors.toList());
    }

    public void restoreState(List<SimpleSourceSplit> splits) {
        values.clear();
        splits.stream()
                .map(SimpleSourceSplit::value)
                .filter(x -> x.startsWith(splitPrefix))
                .map(x -> x.substring(splitPrefix.length()))
                .map(this.deserializer)
                .forEach(values::add);
    }
}
