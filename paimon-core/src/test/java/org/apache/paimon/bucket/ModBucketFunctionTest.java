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

package org.apache.paimon.bucket;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link ModBucketFunction}. */
class ModBucketFunctionTest {

    @Test
    void testModBucketFunction() {
        // test for int
        ModBucketFunction modBucketFunction = new ModBucketFunction(RowType.of(DataTypes.INT()));
        int numOfBuckets = 5;
        assertThat(modBucketFunction.bucket(genRow(1), numOfBuckets)).isEqualTo(1);
        assertThat(modBucketFunction.bucket(genRow(7), numOfBuckets)).isEqualTo(2);
        assertThat(modBucketFunction.bucket(genRow(-2), numOfBuckets)).isEqualTo(3);

        // test for bigint
        modBucketFunction = new ModBucketFunction(RowType.of(DataTypes.BIGINT()));
        assertThat(modBucketFunction.bucket(genRow(8L), numOfBuckets)).isEqualTo(3);
        assertThat(modBucketFunction.bucket(genRow(0L), numOfBuckets)).isEqualTo(0);
        assertThat(modBucketFunction.bucket(genRow(-3L), numOfBuckets)).isEqualTo(2);
    }

    @Test
    void testModBucketFunctionUnSupportedType() {
        // bucket key contains two fields
        assertThatThrownBy(
                        () ->
                                new ModBucketFunction(
                                        RowType.of(DataTypes.INT(), DataTypes.BIGINT())))
                .hasMessage("bucket key must have exactly one field in mod bucket function");

        // bucket key data is neither int nor bigint
        assertThatThrownBy(() -> new ModBucketFunction(RowType.of(DataTypes.STRING())))
                .hasMessage(
                        "bucket key type must be INT or BIGINT in mod bucket function, but got STRING");
    }

    private BinaryRow genRow(int value) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(row);
        binaryRowWriter.writeInt(0, value);
        binaryRowWriter.complete();
        return row;
    }

    private BinaryRow genRow(long value) {
        BinaryRow row = new BinaryRow(1);
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(row);
        binaryRowWriter.writeLong(0, value);
        binaryRowWriter.complete();
        return row;
    }
}
