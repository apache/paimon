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

package org.apache.paimon.datagen;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;

/** Data generator for Paimon's internal {@link InternalRow} type. */
public class RowDataGenerator implements DataGenerator<InternalRow> {

    private final DataGenerator<?>[] fieldGenerators;

    public RowDataGenerator(DataGenerator<?>[] fieldGenerators) {
        this.fieldGenerators = fieldGenerators;
    }

    @Override
    public void open() {
        for (DataGenerator<?> fieldGenerator : fieldGenerators) {
            fieldGenerator.open();
        }
    }

    @Override
    public boolean hasNext() {
        for (DataGenerator<?> generator : fieldGenerators) {
            if (!generator.hasNext()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public InternalRow next() {
        GenericRow row = new GenericRow(fieldGenerators.length);
        for (int i = 0; i < fieldGenerators.length; i++) {
            row.setField(i, fieldGenerators[i].next());
        }
        return row;
    }
}
