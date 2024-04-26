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

package org.apache.paimon.flink.action.cdc;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/** A data change record from the CDC source. */
public class CdcSourceRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    @Nullable private final String topic;

    @Nullable private final Object key;

    // TODO Use generics to support more scenarios.
    private final Object value;

    public CdcSourceRecord(@Nullable String topic, @Nullable Object key, Object value) {
        this.topic = topic;
        this.key = key;
        this.value = value;
    }

    public CdcSourceRecord(Object value) {
        this(null, null, value);
    }

    @Nullable
    public String getTopic() {
        return topic;
    }

    @Nullable
    public Object getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof CdcSourceRecord)) {
            return false;
        }

        CdcSourceRecord that = (CdcSourceRecord) o;
        return Objects.equals(topic, that.topic)
                && Objects.equals(key, that.key)
                && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, key, value);
    }

    @Override
    public String toString() {
        return topic + ": " + key + " " + value;
    }
}
