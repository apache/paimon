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

package org.apache.paimon.compact;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import java.util.Arrays;

import static org.apache.paimon.utils.SerializationUtils.newStringType;

/**
 * Metadata of compact metric.
 *
 * @since 1.4.0
 */
@Public
public class CompactMetricMeta {

    static final RowType SCHEMA =
            new RowType(
                    false,
                    Arrays.asList(
                            new DataField(0, "_TYPE", newStringType(true)),
                            new DataField(1, "_DURATION", new BigIntType(true))));

    private final String type;
    private final long duration;

    public CompactMetricMeta() {
        this(null, -1L);
    }

    public CompactMetricMeta(String type, long duration) {
        this.type = type;
        this.duration = duration;
    }

    public String type() {
        return type;
    }

    public long duration() {
        return duration;
    }
}
