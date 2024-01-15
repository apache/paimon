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

package org.apache.paimon.flink.sorter;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.JoinedRow;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/** Generate random suffix to avoid data skew. */
public class RandomBytesSuffixGenerator {

    private static final String FIELD_NAME = "_RANDOM_SUFFIX_";

    private final int byteSize;
    private final Random random;

    public RandomBytesSuffixGenerator(int byteSize) {
        if (byteSize < 0) {
            throw new IllegalArgumentException(
                    "byte size should be greater than or equal to zero.");
        }
        this.byteSize = byteSize;
        this.random = new Random();
    }

    public InternalRow addRandomSuffix(InternalRow input) {
        if (byteSize > 0) {
            GenericRow bytesRow = new GenericRow(1);
            byte[] randomBytes = new byte[byteSize];
            random.nextBytes(randomBytes);
            bytesRow.setField(0, randomBytes);

            return new JoinedRow(input.getRowKind(), input, bytesRow);
        }
        return input;
    }

    public byte[] addRandomSuffix(byte[] input) {
        if (byteSize > 0) {
            byte[] out = new byte[input.length + byteSize];
            byte[] randomBytes = new byte[byteSize];
            random.nextBytes(randomBytes);
            System.arraycopy(input, 0, out, 0, input.length);
            System.arraycopy(randomBytes, 0, out, input.length, randomBytes.length);
            return out;
        }
        return input;
    }

    public static RandomBytesSuffixGenerator create(int byteSize) {
        return new RandomBytesSuffixGenerator(byteSize);
    }

    public static RowType combine(RowType input, int byteSize) {
        if (byteSize > 0) {
            List<DataField> newFields = new ArrayList<>(input.getFields());
            newFields.add(new DataField(newFields.size(), FIELD_NAME, DataTypes.BINARY(byteSize)));
            return new RowType(newFields);
        }
        return input;
    }
}
