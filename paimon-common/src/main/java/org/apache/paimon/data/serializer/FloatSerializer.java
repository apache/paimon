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

package org.apache.paimon.data.serializer;

import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;

import java.io.IOException;

/** Type serializer for {@code Float} (and {@code float}, via auto-boxing). */
public final class FloatSerializer extends SerializerSingleton<Float> {

    private static final long serialVersionUID = 1L;

    /** Sharable instance of the IntSerializer. */
    public static final FloatSerializer INSTANCE = new FloatSerializer();

    @Override
    public Float copy(Float from) {
        return from;
    }

    @Override
    public void serialize(Float record, DataOutputView target) throws IOException {
        target.writeFloat(record);
    }

    @Override
    public Float deserialize(DataInputView source) throws IOException {
        return source.readFloat();
    }

    @Override
    public String serializeToString(Float record) {
        return record.toString();
    }

    @Override
    public Float deserializeFromString(String s) {
        return Float.valueOf(s);
    }
}
