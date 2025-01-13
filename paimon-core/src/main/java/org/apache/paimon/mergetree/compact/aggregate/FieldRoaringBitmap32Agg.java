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

package org.apache.paimon.mergetree.compact.aggregate;

import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.utils.RoaringBitmap32;

import java.io.IOException;
import java.nio.ByteBuffer;

/** roaring bitmap aggregate a field of a row. */
public class FieldRoaringBitmap32Agg extends FieldAggregator {

    private static final long serialVersionUID = 1L;
    private final RoaringBitmap32 roaringBitmapAcc;
    private final RoaringBitmap32 roaringBitmapInput;

    public FieldRoaringBitmap32Agg(String name, VarBinaryType dataType) {
        super(name, dataType);
        this.roaringBitmapAcc = new RoaringBitmap32();
        this.roaringBitmapInput = new RoaringBitmap32();
    }

    @Override
    public Object agg(Object accumulator, Object inputField) {
        if (accumulator == null || inputField == null) {
            return accumulator == null ? inputField : accumulator;
        }

        try {
            roaringBitmapAcc.deserialize(ByteBuffer.wrap((byte[]) accumulator));
            roaringBitmapInput.deserialize(ByteBuffer.wrap((byte[]) inputField));
            roaringBitmapAcc.or(roaringBitmapInput);
            return roaringBitmapAcc.serialize();
        } catch (IOException e) {
            throw new RuntimeException("Unable to se/deserialize roaring bitmap.", e);
        } finally {
            roaringBitmapAcc.clear();
            roaringBitmapInput.clear();
        }
    }
}
