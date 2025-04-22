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

package org.apache.paimon.mergetree.compact.aggregate.ext;

import org.apache.paimon.mergetree.compact.aggregate.FieldAggregator;
import org.apache.paimon.types.VarBinaryType;

import com.starrocks.types.BitmapValue;

import java.io.IOException;

/**
 * roaring bitmap aggregate a field of a row. The current class is designed to be compatible with
 * the bitmap format of StarRocks.
 */
public class FieldStarRocksBitmapAgg extends FieldAggregator {

    private static final long serialVersionUID = 1L;
    private BitmapValue acc;
    private BitmapValue input;

    public FieldStarRocksBitmapAgg(String name, VarBinaryType dataType) {
        super(name, dataType);
    }

    @Override
    public Object agg(Object accumulator, Object inputField) {
        if (accumulator == null || inputField == null) {
            return accumulator == null ? inputField : accumulator;
        }

        try {
            this.acc = BitmapValue.bitmapFromBytes((byte[]) accumulator);
            this.input = BitmapValue.bitmapFromBytes((byte[]) inputField);
            this.acc.or(this.input);
            return BitmapValue.bitmapToBytes(this.acc);
        } catch (IOException e) {
            throw new RuntimeException("Unable to se/deserialize roaring bitmap.", e);
        } finally {
            if (this.acc != null) {
                this.acc.clear();
            }
            if (this.input != null) {
                this.input.clear();
            }
        }
    }
}
