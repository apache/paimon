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

package org.apache.flink.table.store.connector;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.store.data.BinaryRow;

import java.util.Random;

import static org.apache.flink.table.store.file.io.DataFileTestUtils.row;

/** Test for {@link BinaryRowTypeSerializer}. */
public class BinaryRowTypeSerializerTest extends SerializerTestBase<BinaryRow> {

    @Override
    protected TypeSerializer<BinaryRow> createSerializer() {
        return new BinaryRowTypeSerializer(2);
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected Class<BinaryRow> getTypeClass() {
        return BinaryRow.class;
    }

    @Override
    protected BinaryRow[] getTestData() {
        Random rnd = new Random();
        return new BinaryRow[] {
            row(1, 1),
            row(2, 2),
            row(rnd.nextInt(), rnd.nextInt()),
            row(rnd.nextInt(), rnd.nextInt())
        };
    }
}
