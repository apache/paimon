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
import org.apache.paimon.utils.RoaringBitmap64;

import java.io.IOException;

/** roaring bitmap aggregate a field of a row. */
public class FieldRoaringBitmap64Agg extends FieldAggregator {

    private static final long serialVersionUID = 1L;
    private final RoaringBitmap64 roaringBitmapAcc;
    private final RoaringBitmap64 roaringBitmapInput;

    public FieldRoaringBitmap64Agg(String name, VarBinaryType dataType) {
        super(name, dataType);
        this.roaringBitmapAcc = new RoaringBitmap64();
        this.roaringBitmapInput = new RoaringBitmap64();
    }

    @Override
    public Object agg(Object accumulator, Object inputField) {
        if (accumulator == null || inputField == null) {
            return accumulator == null ? inputField : accumulator;
        }

        try {
            roaringBitmapAcc.deserialize((byte[]) accumulator);
            roaringBitmapInput.deserialize((byte[]) inputField);
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
