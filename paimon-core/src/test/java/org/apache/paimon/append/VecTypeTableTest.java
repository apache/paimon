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

package org.apache.paimon.append;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.ArrayBasedVec;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Tests for table with vector. */
public class VecTypeTableTest extends TableTestBase {

    private final float[] testVec = randomVec();

    @Test
    public void testBasic() throws Exception {
        createTableDefault();

        commitDefault(writeDataDefault(100, 1));

        AtomicInteger integer = new AtomicInteger(0);

        readDefault(
                row -> {
                    integer.incrementAndGet();
                    if (integer.get() % 50 == 0) {
                        Assertions.assertArrayEquals(row.getVec(2).toFloatArray(), testVec, 0);
                    }
                });

        assertThat(integer.get()).isEqualTo(100);
    }

    @Override
    protected Schema schemaDefault() {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("f0", DataTypes.INT());
        schemaBuilder.column("f1", DataTypes.STRING());
        schemaBuilder.column("f2", DataTypes.VECTOR(testVec.length, DataTypes.FLOAT()));
        // schemaBuilder.option(CoreOptions.TARGET_FILE_SIZE.key(), "25 MB");
        schemaBuilder.option(CoreOptions.FILE_FORMAT.key(), "json");
        schemaBuilder.option(CoreOptions.FILE_COMPRESSION.key(), "none");
        return schemaBuilder.build();
    }

    @Override
    protected InternalRow dataDefault(int time, int size) {
        return GenericRow.of(
                RANDOM.nextInt(),
                BinaryString.fromBytes(randomBytes()),
                ArrayBasedVec.from(new GenericArray(testVec)));
    }

    @Override
    protected byte[] randomBytes() {
        byte[] binary = new byte[RANDOM.nextInt(1024) + 1];
        RANDOM.nextBytes(binary);
        return binary;
    }

    private float[] randomVec() {
        byte[] randomBytes = randomBytes();
        float[] vec = new float[randomBytes.length];
        for (int i = 0; i < vec.length; i++) {
            vec[i] = randomBytes[i];
        }
        return vec;
    }
}
