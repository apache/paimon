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

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

import static org.apache.paimon.utils.SerializationUtils.newStringType;

/** Serializer for {@link IndexFileMeta} with 1.4 version. */
public class CompactMetricMetaV1Deserializer implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final RowType SCHEMA =
            new RowType(
                    true,
                    Arrays.asList(
                            new DataField(0, "_TYPE", newStringType(true)),
                            new DataField(1, "_DURATION", new BigIntType(true))));


    protected final InternalRowSerializer rowSerializer;

    public CompactMetricMetaV1Deserializer() {
        this.rowSerializer = InternalSerializers.create(SCHEMA);
    }

    public CompactMetricMeta fromRow(InternalRow row) {
        if (row == null) {
            return new CompactMetricMeta();
        }
        return new CompactMetricMeta(
                row.getString(0).toString(),
                row.getLong(1));
    }

    public CompactMetricMeta deserialize(DataInputView in) throws IOException {
        if (in instanceof DataInputDeserializer) {
            DataInputDeserializer deserializer = (DataInputDeserializer) in;
            if (deserializer.available() == 0) {
                return new CompactMetricMeta();
            }
        }
        return fromRow(rowSerializer.deserialize(in));
    }
}
