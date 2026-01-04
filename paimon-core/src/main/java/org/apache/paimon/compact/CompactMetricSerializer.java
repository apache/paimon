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

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.utils.ObjectSerializer;

/** Serializer for {@link DataFileMeta}. */
public class CompactMetricSerializer extends ObjectSerializer<CompactMetricMeta> {

    private static final long serialVersionUID = 1L;

    public CompactMetricSerializer() {
        super(CompactMetricMeta.SCHEMA);
    }

    @Override
    public InternalRow toRow(CompactMetricMeta meta) {
        return GenericRow.of(
                BinaryString.fromString(meta.type()), meta.duration(), meta.isNullable());
    }

    @Override
    public CompactMetricMeta fromRow(InternalRow row) {
        return new CompactMetricMeta(
                row.getString(0).toString(), row.getLong(1), row.getBoolean(2));
    }
}
