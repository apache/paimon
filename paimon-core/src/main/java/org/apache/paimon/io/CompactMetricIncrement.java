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

package org.apache.paimon.io;

import org.apache.paimon.compact.CompactMetricMeta;

import javax.annotation.Nullable;

import java.util.Objects;

/** Compact metric. */
public class CompactMetricIncrement {

    @Nullable private final CompactMetricMeta metric;

    public CompactMetricIncrement() {
        this(null);
    }

    public CompactMetricIncrement(@Nullable CompactMetricMeta metric) {
        this.metric = metric;
    }

    public CompactMetricMeta metric() {
        return metric;
    }

    public CompactMetricMeta empty() {
        return new CompactMetricMeta();
    }

    public boolean isEmpty() {
        return metric == null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CompactMetricIncrement that = (CompactMetricIncrement) o;
        return Objects.equals(metric, that.metric);
    }

    @Override
    public int hashCode() {
        return Objects.hash(metric);
    }

    @Override
    public String toString() {
        return String.format("CompactMetricIncrement {metric = %s}", metric);
    }

    public static CompactMetricIncrement emptyIncrement() {
        return new CompactMetricIncrement();
    }
}
