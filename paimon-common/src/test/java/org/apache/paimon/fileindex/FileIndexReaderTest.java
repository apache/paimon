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

package org.apache.paimon.fileindex;

import org.apache.paimon.fileindex.bitmap.BitmapIndexResult;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.types.IntType;
import org.apache.paimon.utils.RoaringBitmap32;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for default predicate composition in {@link FileIndexReader}. */
public class FileIndexReaderTest {

    @Test
    public void testNotInCombinesNotEqualResultsWithAnd() {
        FieldRef fieldRef = new FieldRef(0, "a", new IntType());
        FileIndexReader reader =
                new FileIndexReader() {
                    @Override
                    public FileIndexResult visitNotEqual(FieldRef fieldRef, Object literal) {
                        return new BitmapIndexResult(
                                () ->
                                        ((Integer) literal) == 1
                                                ? RoaringBitmap32.bitmapOf(0, 2)
                                                : RoaringBitmap32.bitmapOf(0, 1));
                    }
                };

        FileIndexResult result = reader.visitNotIn(fieldRef, Arrays.asList(1, 2));

        assertThat(((BitmapIndexResult) result).get()).isEqualTo(RoaringBitmap32.bitmapOf(0));
    }
}
