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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.mergetree.compact.aggregate.FieldAggregator;
import org.apache.paimon.mergetree.compact.aggregate.ext.factory.FieldStarRocksBitmapAggFactory;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldAggregatorFactory;
import org.apache.paimon.types.DataTypes;

import com.starrocks.types.BitmapValue;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

/** test whether {@link FieldAggregator}' subclasses behaviors are expected. */
public class FieldAggregatorTest {

    @Test
    public void testFieldStarRocksBitmapAgg() throws IOException {
        FieldStarRocksBitmapAgg agg =
                new FieldStarRocksBitmapAggFactory().create(DataTypes.VARBINARY(20), null, null);
        testAgg(agg);
    }

    @Test
    public void testCustomAgg() throws IOException {
        FieldAggregator agg =
                FieldAggregatorFactory.create(
                        DataTypes.VARBINARY(20),
                        "bitmap",
                        "bitmap_agg",
                        CoreOptions.fromMap(new HashMap<>()));

        testAgg(agg);
    }

    private void testAgg(FieldAggregator agg) throws IOException {
        byte[] inputVal = BitmapValue.bitmapToBytes(new BitmapValue(1L));
        BitmapValue bm1 = new BitmapValue(2L);
        bm1.add(3L);
        byte[] acc1 = BitmapValue.bitmapToBytes(bm1);

        BitmapValue bm2 = new BitmapValue(1L);
        bm2.add(2L);
        bm2.add(3L);
        byte[] acc2 = BitmapValue.bitmapToBytes(bm2);

        assertThat(agg.agg(null, null)).isNull();

        byte[] result1 = (byte[]) agg.agg(null, inputVal);
        assertThat(inputVal).isEqualTo(result1);

        byte[] result2 = (byte[]) agg.agg(acc1, null);
        assertThat(result2).isEqualTo(acc1);

        byte[] result3 = (byte[]) agg.agg(acc1, inputVal);
        assertThat(result3).isEqualTo(acc2);

        byte[] result4 = (byte[]) agg.agg(acc2, inputVal);
        assertThat(result4).isEqualTo(acc2);

        assertThat(BitmapValue.bitmapFromBytes(result4).cardinality()).isEqualTo(3L);
    }
}
