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

package org.apache.paimon.utils;

import org.apache.paimon.data.serializer.SerializerSingleton;
import org.apache.paimon.io.DataInputView;
import org.apache.paimon.io.DataOutputView;

import java.io.IOException;

import static org.apache.paimon.utils.VarLengthIntUtils.decodeInt;
import static org.apache.paimon.utils.VarLengthIntUtils.encodeInt;

/** A {@link SerializerSingleton} for {@link PositiveIntInt}. */
public class PositiveIntIntSerializer extends SerializerSingleton<PositiveIntInt> {

    @Override
    public PositiveIntInt copy(PositiveIntInt from) {
        return from;
    }

    @Override
    public void serialize(PositiveIntInt record, DataOutputView target) throws IOException {
        encodeInt(target, record.i1());
        encodeInt(target, record.i2());
    }

    @Override
    public PositiveIntInt deserialize(DataInputView source) throws IOException {
        int i1 = decodeInt(source);
        int i2 = decodeInt(source);
        return new PositiveIntInt(i1, i2);
    }
}
